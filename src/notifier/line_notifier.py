import logging
import requests
from src.config import settings
from src.schemas import StockAnomalyEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("line_notifier")

class LineNotifier:
    def __init__(self):
        self.api_url = "https://api.line.me/v2/bot/message/push"
        self.token = settings.LINE_CHANNEL_ACCESS_TOKEN
        self.user_id = settings.LINE_USER_ID

    def send_anomaly_alert(self, anomaly: StockAnomalyEvent) -> bool:
        # icon = "📈" if anomaly.anomaly_type == "SPIKE" else "📉"
        sign = "+" if anomaly.percent_change >= 0 else ""
        formatted_time = anomaly.datetime_utc().strftime("%Y-%m-%d %H:%M:%S UTC")

        text_message = (
            # f"🚨 STOCK ANOMALY ALERT {icon}\n\n"
            f"• Symbol: {anomaly.symbol}\n"
            f"• Anomaly: {anomaly.anomaly_type} ({sign}{anomaly.percent_change:.2f}%)\n"
            f"• Current Price: ${anomaly.current_price:.2f}\n"
            f"• Previous Price: ${anomaly.previous_price:.2f}\n"
            f"• Timestamp: {formatted_time}"
        )

        logger.info(f"\n==================== [ALERT TRIGGERED] ====================\n{text_message}\n===========================================================")

        # Send via LINE Messaging API if credentials provided
        if self.token and self.user_id:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.token}",
            }
            payload = {
                "to": self.user_id,
                "messages": [
                    {
                        "type": "text",
                        "text": text_message,
                    }
                ],
            }
            try:
                response = requests.post(self.api_url, json=payload, headers=headers, timeout=5)
                if response.status_code == 200:
                    logger.info("Successfully pushed notification to LINE Messenger!")
                    return True
                else:
                    logger.warning(f"Failed to send LINE notification (HTTP {response.status_code}): {response.text}")
            except Exception as e:
                logger.error(f"Error sending LINE notification: {e}")
        else:
            logger.info("[NOTE] LINE credentials not set. Alert printed to console log.")
        
        return False
