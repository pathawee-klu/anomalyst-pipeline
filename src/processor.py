import asyncio
import json
import logging
import sys
import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Deque, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg

from src.config import settings
from src.schemas import StockTick, StockAnomalyEvent
from src.notifier.line_notifier import LineNotifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PROCESSOR] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("processor")

class StreamProcessor:
    def __init__(self):
        self.notifier = LineNotifier()
        self.db_pool = None
        self.consumer = None
        self.anomaly_producer = None
        self.running = True
        
        # Sliding windows per symbol: symbol -> deque of (timestamp_ms, price)
        self.sliding_windows: Dict[str, Deque[Tuple[int, float]]] = {}
        # Cooldown tracker to prevent alert spam for same anomaly window: symbol -> last_alert_timestamp_ms
        self.last_alert_time: Dict[str, int] = {}
        self.cooldown_ms = 15000  # 15 seconds cooldown per symbol

    async def start(self):
        # 1. Connect to TimescaleDB
        await self.init_db()

        # 2. Connect to Kafka Consumer & Producer
        await self.init_kafka()

        # 3. Main Stream Processing Loop
        try:
            logger.info("Stream Processor started. Waiting for ticks...")
            async for msg in self.consumer:
                if not self.running:
                    break
                try:
                    tick_data = json.loads(msg.value.decode("utf-8"))
                    tick = StockTick(**tick_data)
                    await self.process_tick(tick)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        finally:
            await self.shutdown()

    async def init_db(self):
        retry_count = 0
        while retry_count < 10:
            try:
                self.db_pool = await asyncpg.create_pool(
                    host=settings.POSTGRES_HOST,
                    port=settings.POSTGRES_PORT,
                    database=settings.POSTGRES_DB,
                    user=settings.POSTGRES_USER,
                    password=settings.POSTGRES_PASSWORD,
                    min_size=2,
                    max_size=10,
                )
                logger.info(f"Connected to TimescaleDB at {settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}")
                break
            except Exception as e:
                retry_count += 1
                logger.warning(f"Failed to connect to TimescaleDB (attempt {retry_count}/10): {e}. Retrying in 5s...")
                await asyncio.sleep(5)
        
        if retry_count == 10:
            logger.error("Could not connect to TimescaleDB. Exiting.")
            sys.exit(1)

    async def init_kafka(self):
        # Kafka Consumer for raw ticks
        self.consumer = AIOKafkaConsumer(
            settings.KAFKA_TOPIC_RAW_TICKS,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="anomaly-processor-group",
            auto_offset_reset="latest",
        )
        # Kafka Producer for anomalies
        self.anomaly_producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        
        await self.consumer.start()
        await self.anomaly_producer.start()
        logger.info(f"Kafka Consumer listening to '{settings.KAFKA_TOPIC_RAW_TICKS}'")

    async def process_tick(self, tick: StockTick):
        dt = tick.datetime_utc()
        
        # 1. Insert Raw Tick into TimescaleDB Hypertable
        async with self.db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO stock_ticks (timestamp, symbol, price, volume)
                VALUES ($1, $2, $3, $4)
                """,
                dt, tick.symbol, tick.price, tick.volume
            )

        # 2. Maintain Sliding Window for Anomaly Detection
        if tick.symbol not in self.sliding_windows:
            self.sliding_windows[tick.symbol] = deque()

        window = self.sliding_windows[tick.symbol]
        window.append((tick.timestamp, tick.price))

        # Evict ticks older than ANOMALY_WINDOW_SECONDS
        cutoff_ms = tick.timestamp - (settings.ANOMALY_WINDOW_SECONDS * 1000)
        while window and window[0][0] < cutoff_ms:
            window.popleft()

        # 3. Detect Anomaly (Spike / Drop)
        if len(window) > 1:
            baseline_price = window[0][1] # Oldest price in window
            percent_change = ((tick.price - baseline_price) / baseline_price) * 100.0
            
            if abs(percent_change) >= settings.ANOMALY_PERCENT_CHANGE_THRESHOLD:
                # Check cooldown to prevent duplicate alerts in rapid succession
                last_alert = self.last_alert_time.get(tick.symbol, 0)
                if tick.timestamp - last_alert >= self.cooldown_ms:
                    self.last_alert_time[tick.symbol] = tick.timestamp
                    anomaly_type = "SPIKE" if percent_change > 0 else "DROP"
                    
                    anomaly = StockAnomalyEvent(
                        symbol=tick.symbol,
                        current_price=tick.price,
                        previous_price=baseline_price,
                        percent_change=round(percent_change, 2),
                        anomaly_type=anomaly_type,
                        timestamp=tick.timestamp,
                    )
                    
                    await self.handle_anomaly(anomaly)

    async def handle_anomaly(self, anomaly: StockAnomalyEvent):
        logger.warning(
            f"ANOMALY DETECTED: {anomaly.symbol} {anomaly.anomaly_type}"
            f"({anomaly.percent_change:+.2f}%) Current: ${anomaly.current_price:.2f} | Base: ${anomaly.previous_price:.2f}"
        )

        # 1. Insert into TimescaleDB stock_anomalies table
        async with self.db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO stock_anomalies (timestamp, symbol, current_price, previous_price, percent_change, anomaly_type)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                anomaly.datetime_utc(),
                anomaly.symbol,
                anomaly.current_price,
                anomaly.previous_price,
                anomaly.percent_change,
                anomaly.anomaly_type
            )

        # 2. Publish event to Kafka stock-anomalies topic
        await self.anomaly_producer.send_and_wait(
            settings.KAFKA_TOPIC_ANOMALIES,
            value=anomaly.model_dump(),
            key=anomaly.symbol.encode("utf-8"),
        )

        # 3. Send Notification via LINE / Console Logger
        self.notifier.send_anomaly_alert(anomaly)

    async def shutdown(self):
        if self.consumer:
            await self.consumer.stop()
        if self.anomaly_producer:
            await self.anomaly_producer.stop()
        if self.db_pool:
            await self.db_pool.close()
        logger.info("Stream Processor shut down cleanly.")

async def main():
    processor = StreamProcessor()
    try:
        await processor.start()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Processor shutting down...")

if __name__ == "__main__":
    asyncio.run(main())
