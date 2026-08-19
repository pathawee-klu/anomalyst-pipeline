import asyncio
import json
import logging
import random
import time
import sys
from typing import Dict
from aiokafka import AIOKafkaProducer
import websockets

from src.config import settings
from src.schemas import StockTick

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PRODUCER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("producer")


class StockDataProducer:
    def __init__(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.running = True

    # FUNCTION FOR STARTING THE PRODUCER
    async def start(self):
        retry_count = 0
        while retry_count < 10:
            try:
                await self.producer.start()
                logger.info(f"Connected to Kafka broker at {settings.KAFKA_BOOTSTRAP_SERVERS}")
                break
            except Exception as e:
                retry_count += 1
                logger.warning(f"Failed to connect to Kafka (attempt {retry_count}/10): {e}. Retrying in 5s")
                await asyncio.sleep(5)
        
        if retry_count == 10:
            logger.error("Could not connect to Kafka broker. Exiting.")
            sys.exit(1)

        try:
            if settings.MOCK_MODE or not settings.FINNHUB_API_KEY:
                logger.info("Running in MOCK DATA GENERATOR mode")
                await self.run_mock_generator()
            else:
                logger.info("Running in LIVE FINNHUB WEBSOCKET mode")
                await self.run_finnhub_websocket()
        finally:
            await self.producer.stop()
            logger.info("Kafka Producer stopped.")

    # FUNCTION FOR MOCK MODE
    async def run_mock_generator(self):
        """Simulates real-time stock ticks with occasional injected anomalies."""
        base_prices: Dict[str, float] = {
            "AAPL": 225.0,
            "GOOGL": 178.0,
            "TSLA": 215.0,
            "NVDA": 128.0,
        }
        tick_counter = 0

        while self.running:
            tick_counter += 1
            now_ms = int(time.time() * 1000)

            for symbol in settings.SYMBOLS:
                current_base = base_prices.get(symbol, 100.0)
                
                # Every 25 ticks, inject a synthetic Spike (+3.5%) or Drop (-3.5%)
                if tick_counter % 25 == 0 and symbol == random.choice(settings.SYMBOLS):
                    anomaly_direction = random.choice([1.035, 0.965])
                    price = round(current_base * anomaly_direction, 2)
                    logger.info(f"INJECTING MOCK ANOMALY for {symbol}: Price shifted to ${price:.2f}")
                else:
                    # Normal random fluctuation (-0.3% to +0.3%)
                    change_pct = random.uniform(-0.003, 0.003)
                    price = round(current_base * (1 + change_pct), 2)
                
                base_prices[symbol] = price
                volume = round(random.uniform(10, 500), 2)

                tick = StockTick(
                    symbol=symbol,
                    price=price,
                    volume=volume,
                    timestamp=now_ms,
                )

                await self.producer.send_and_wait(
                    settings.KAFKA_TOPIC_RAW_TICKS,
                    value=tick.model_dump(),
                    key=symbol.encode("utf-8"),
                )

                logger.info(f"Published tick -> {symbol}: ${price:.2f} | Vol: {volume}")

            await asyncio.sleep(1.5)

    # FUNCTION FOR LIVE FINNHUB WEBSOCKET MODE
    async def run_finnhub_websocket(self):
        """Connects to Finnhub WebSocket API for real-time stock trades."""
        uri = f"wss://ws.finnhub.io?token={settings.FINNHUB_API_KEY}"
        
        async with websockets.connect(uri) as websocket:
            logger.info("Connected to Finnhub WebSocket API")
            
            # Subscribe to symbols
            for symbol in settings.SYMBOLS:
                msg = json.dumps({"type": "subscribe", "symbol": symbol})
                await websocket.send(msg)
                logger.info(f"Subscribed to Finnhub symbol: {symbol}")

            while self.running:
                response = await websocket.recv()
                data = json.loads(response)

                if data.get("type") == "trade":
                    for item in data.get("data", []):
                        tick = StockTick(
                            symbol=item["s"],
                            price=float(item["p"]),
                            volume=float(item["v"]),
                            timestamp=int(item["t"]),
                        )

                        await self.producer.send_and_wait(
                            settings.KAFKA_TOPIC_RAW_TICKS,
                            value=tick.model_dump(),
                            key=tick.symbol.encode("utf-8"),
                        )
                        logger.info(f"Published Finnhub trade -> {tick.symbol}: ${tick.price:.2f}")


# MAIN FUNCTION
async def main():
    producer_service = StockDataProducer()
    try:
        await producer_service.start()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Producer shutting down")


if __name__ == "__main__":
    asyncio.run(main())
