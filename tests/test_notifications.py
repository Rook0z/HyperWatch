import logging
import asyncio
import json

from hyperwatch.alerts.engine import process_event_for_alerts
from hyperwatch.alerts.rules import DEFAULT_RULES
from hyperwatch.notifications.dispatcher import dispatch_notification

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Async alert handler calls the dispatcher to send formatted notifications
async def alert_handler(message, event, channels=None):
    logger.info(f"📨 Alert: {message}")
    await dispatch_notification(event, channels=channels)

async def main():
    logger.info("📥 Starting alert test...")

    while True:
        event_json = input("Paste event JSON (or 'quit' to exit): ").strip()
        if event_json.lower() == "quit":
            break

        try:
            event = json.loads(event_json)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            continue

        # Process event for alerts, pass the async alert_handler
        await process_event_for_alerts(event, rules=DEFAULT_RULES, alert_handler=alert_handler)

        # async tasks complete
        await asyncio.sleep(0.1)

if __name__ == "__main__":
    asyncio.run(main())
