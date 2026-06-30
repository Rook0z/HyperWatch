import asyncio
import logging
from hyperwatch.alerts.engine import process_event_for_alerts
from hyperwatch.alerts.rules import DEFAULT_RULES
from hyperwatch.notifications.dispatcher import dispatch_notification

example_event = {
    "type": "order_update",
    "wallet": "0xfa58f82875ee97464a8feef085e597b06590dd13",
    "timestamp": 1754567974025,
    "timestamp_readable": "2025-08-07T11:59:34.025000Z",
    "coin": "BOME",
    "side": "B",
    "price": 0.001841,
    "size": 1594567.0,
    "orderId": 128514335824,
    "status": "open",
    "raw": {
        "order": {
            "coin": "BOME",
            "side": "B",
            "limitPx": "0.001841",
            "sz": "1594567.0",
            "oid": 128514335824,
            "timestamp": 1754567974025,
            "origSz": "1594567.0",
            "cloid": "0xb3c6354bba5cb18148ac142e81efad40"
        },
        "status": "open",
        "statusTimestamp": 1754567974025
    }
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_alert_handler(message, event, channels=None):
    logger.info(f"📨 Test Alert: {message}")
    await dispatch_notification(event, channels)

async def main():
    logger.info("📥 Starting alert test...")

    # Call the main alert processor with your event and handler
    await process_event_for_alerts(example_event, rules=DEFAULT_RULES, alert_handler=test_alert_handler)

if __name__ == "__main__":
    asyncio.run(main())
