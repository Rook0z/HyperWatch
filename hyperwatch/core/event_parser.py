import logging
from datetime import datetime
from hyperwatch.core.coin_mapper import coin_mapper  
from hyperwatch.alerts.formatter import format_notification

logger = logging.getLogger(__name__)

try:
    from hyperwatch.core.event_deduplicator import event_deduplicator
except ImportError:
    # Fallback if deduplicator not available
    logger.warning("Event deduplicator not available - proceeding without deduplication")
    event_deduplicator = None

# Cache for price data to avoid frequent API calls
_price_cache = {}
_price_cache_time = 0
PRICE_CACHE_DURATION = 30  # seconds

def get_current_prices():
    """Get current prices with caching"""
    global _price_cache, _price_cache_time
    import time
    
    now = time.time()
    if now - _price_cache_time > PRICE_CACHE_DURATION:
        try:
            _price_cache = coin_mapper.fetch_all_prices()
            _price_cache_time = now
            logger.debug(f"🔄 Refreshed price cache with {len(_price_cache)} prices")
        except Exception as e:
            logger.warning(f"⚠️ Failed to refresh price cache: {e}")
    
    return _price_cache

def extract_wallet_from_event(event: dict, fallback_wallet: str = None) -> str:
    """Extract wallet address from event data with multiple fallback options"""
    wallet_fields = [
        'wallet', 'user', 'address', 'account', 'from', 'to', 
        'owner', 'trader', 'userAddress', 'walletAddress'
    ]
    
    # Check top-level fields
    for field in wallet_fields:
        if field in event and event[field]:
            wallet = str(event[field]).strip()
            if wallet and wallet != "unknown":
                return wallet
    
    # Check nested 'data'
    if 'data' in event and isinstance(event['data'], dict):
        for field in wallet_fields:
            if field in event['data'] and event['data'][field]:
                wallet = str(event['data'][field]).strip()
                if wallet and wallet != "unknown":
                    return wallet
    
    # Check first item if event is a list
    if isinstance(event, list) and len(event) > 0:
        first_item = event[0]
        if isinstance(first_item, dict):
            for field in wallet_fields:
                if field in first_item and first_item[field]:
                    wallet = str(first_item[field]).strip()
                    if wallet and wallet != "unknown":
                        return wallet
    
    return fallback_wallet or "unknown"

def detect_position_action(fill_data: dict) -> str:
    """Enhanced position action detection"""
    side = fill_data.get("side", "")
    size = float(fill_data.get("sz", 0))
    price = float(fill_data.get("px", 0))
    usd_value = size * price
    
    if side == "B":  # Buy side
        if usd_value > 1_000_000:
            return "Open Long (Large Position)"
        elif usd_value > 100_000:
            return "Open Long (Medium Position)"
        else:
            return "Open Long"
    elif side == "A":  # Sell side
        if usd_value > 1_000_000:
            return "Close Long (Large Position)"
        elif usd_value > 100_000:
            return "Close Long (Medium Position)"
        else:
            return "Close Long"
    
    return f"{side} Order"

def is_significant_position(usd_value, threshold=1000):  # Lowered threshold
    """Check if position is significant enough to alert on"""
    try:
        return float(usd_value) >= threshold
    except (ValueError, TypeError):
        return False

def _convert_timestamp(ts):
    """Convert timestamp to ISO format with better error handling"""
    try:
        if ts is None:
            return None
        if isinstance(ts, str):
            ts = float(ts)
        if ts < 1e10:
            logger.warning(f"⚠️ Unusual timestamp value: {ts}")
            return None
        elif ts < 1e12:
            ts *= 1000
        dt = datetime.utcfromtimestamp(ts / 1000)
        return dt.isoformat() + "Z"
    except Exception as e:
        logger.warning(f"⚠️ Failed to convert timestamp {ts}: {e}")
        return None

def validate_numeric_data(price, size, coin_name="Unknown"):
    """Validate and clean numeric data"""
    try:
        price_float = float(price) if price else 0.0
        size_float = float(size) if size else 0.0
        
        # Basic validation
        if price_float < 0 or size_float < 0:
            logger.warning(f"⚠️ Negative values detected for {coin_name}: price={price_float}, size={size_float}")
            return 0.0, 0.0
        
        # Check for reasonable values
        if price_float > 1_000_000:  # Very high price
            logger.warning(f"⚠️ Suspicious high price for {coin_name}: ${price_float}")
        
        return price_float, size_float
    except (ValueError, TypeError) as e:
        logger.warning(f"⚠️ Invalid numeric data for {coin_name}: price={price}, size={size} - {e}")
        return 0.0, 0.0

def is_valid_fill(fill_data: dict) -> bool:
    """Check if a fill has valid data worth processing"""
    try:
        size = float(fill_data.get("sz", 0))
        price = float(fill_data.get("px", 0))
        coin = fill_data.get("coin")
        
        # Basic validation
        if size <= 0 or price <= 0:
            return False
        
        if not coin or coin == "UNKNOWN":
            return False
            
        # Calculate USD value
        usd_value = size * price
        
        # Filter out very small trades (less than $1)
        if usd_value < 1.0:
            return False
            
        return True
    except (ValueError, TypeError):
        return False

def parse_event(event: dict, channel: str, wallet: str = None, platform: str = "telegram") -> list:
    """
    Parse raw events from HyperCore, normalize, and attach formatted messages.
    Now with better filtering to reduce spam and ensure wallet-specific outputs.
    """
    try:
        logger.debug(f"🔍 Processing event for wallet: {wallet[:8] if wallet else 'unknown'}... on channel: {channel}")
        
        # Get current prices for validation/correction
        current_prices = get_current_prices()
        
        if not wallet or wallet == "unknown":
            wallet = extract_wallet_from_event(event, wallet)
        
        raw_data = event.get("data", {}) if isinstance(event, dict) else event
        if isinstance(raw_data, list) and len(raw_data) > 0:
            if not wallet or wallet == "unknown":
                wallet = extract_wallet_from_event(raw_data[0], wallet)
        
        parsed_events = []

        if channel == "userFills":
            fills = raw_data if isinstance(raw_data, list) else raw_data.get("fills", [])
            
            # Better validation of fills data
            if not fills:
                logger.debug(f"📝 No fills data found for wallet {wallet[:8] if wallet else 'unknown'}...")
                return []  # Return empty list instead of logging warnings
            
            if not isinstance(fills, list):
                logger.debug(f"📝 Invalid fills format for wallet {wallet[:8] if wallet else 'unknown'}...")
                return []
            
            # Filter valid fills before processing
            valid_fills = [fill for fill in fills if is_valid_fill(fill)]
            
            if not valid_fills:
                logger.debug(f"📝 No valid fills found for wallet {wallet[:8] if wallet else 'unknown'}...")
                return []
            
            logger.info(f"🎯 Processing {len(valid_fills)} valid fills for wallet {wallet[:8] if wallet else 'unknown'}...")
            
            for i, fill in enumerate(sorted(valid_fills, key=lambda x: x.get("time", 0), reverse=True)):
                try:
                    fill_wallet = extract_wallet_from_event(fill, wallet)
                    raw_coin = fill.get("coin")
                    coin = coin_mapper.get_coin_name(raw_coin) if raw_coin else "UNKNOWN"
                    
                    # Validate and clean numeric data
                    raw_price = fill.get("px", 0)
                    raw_size = fill.get("sz", 0)
                    price, size = validate_numeric_data(raw_price, raw_size, coin)
                    
                    # Skip if still invalid after validation
                    if price <= 0 or size <= 0:
                        continue
                    
                    # Try to correct price using current market data
                    if current_prices and coin in current_prices:
                        market_price = float(current_prices[coin])
                        if price == 0 or abs(price - market_price) > market_price * 0.5:  # 50% tolerance
                            logger.info(f"🔄 Correcting price for {coin}: ${price} -> ${market_price}")
                            price = market_price
                    
                    position_action = detect_position_action(fill)
                    usd_value = price * size

                    # Create event structure that matches formatter expectations
                    base_event = {
                        "type": "user_fill",
                        "wallet": fill_wallet,
                        "timestamp": fill.get("time"),
                        "timestamp_readable": _convert_timestamp(fill.get("time")),
                        "coin": coin,
                        "coin_raw": raw_coin,
                        "side": fill.get("side"),
                        "price": price,
                        "size": size,
                        "usd_value": usd_value,
                        "orderId": fill.get("oid"),
                        "isTaker": fill.get("taker", False),
                        "position_action": position_action,
                        "is_significant": is_significant_position(usd_value),
                        "sequence": i,
                        "raw": fill
                    }

                    # Apply deduplication filter
                    if event_deduplicator and not event_deduplicator.should_allow_event(base_event):
                        logger.debug(f"🚫 Event suppressed by deduplicator: {coin} ${usd_value:,.2f}")
                        continue

                    # Attach formatted notification using the updated formatter
                    base_event["message"] = format_notification(base_event, platform, current_prices)
                    
                    # Only add events with valid messages
                    if base_event["message"]:
                        parsed_events.append(base_event)
                        logger.info(f"✅ Valid fill processed: {fill_wallet[:8]}... - {coin} ${usd_value:,.2f}")
                    else:
                        logger.debug(f"🗑️ Filtered out event with no message: {coin}")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Error parsing fill {i}: {e}")
                    continue

        elif channel == "orderUpdates":
            updates = []
            if isinstance(event, dict) and "order" in event:
                updates = [event]
            elif isinstance(raw_data, list):
                updates = raw_data
            elif isinstance(raw_data, dict):
                updates = [raw_data]

            if not updates:
                logger.debug(f"📝 No order updates for wallet {wallet[:8] if wallet else 'unknown'}...")
                return []

            logger.info(f"🎯 Processing {len(updates)} order updates for wallet {wallet[:8] if wallet else 'unknown'}...")

            for i, update in enumerate(updates):
                try:
                    update_wallet = extract_wallet_from_event(update, wallet)
                    order = update.get("order", update)
                    status = update.get("status") or order.get("status")
                    status_timestamp = update.get("statusTimestamp") or order.get("timestamp")
                    raw_coin = order.get("coin")
                    coin = coin_mapper.get_coin_name(raw_coin) if raw_coin else "UNKNOWN"
                    order_side = order.get("side", "")
                    
                    # Validate numeric data
                    raw_size = order.get("sz", 0)
                    raw_price = order.get("limitPx", 0)
                    price, size = validate_numeric_data(raw_price, raw_size, coin)
                    
                    # Try to correct price using current market data
                    if current_prices and coin in current_prices:
                        market_price = float(current_prices[coin])
                        if price == 0 or abs(price - market_price) > market_price * 0.5:  # 50% tolerance
                            logger.info(f"🔄 Correcting order price for {coin}: ${price} -> ${market_price}")
                            price = market_price
                    
                    usd_value = size * price

                    # Create event structure that matches formatter expectations
                    base_event = {
                        "type": "order_update",
                        "wallet": update_wallet,
                        "timestamp": status_timestamp or order.get("timestamp"),
                        "timestamp_readable": _convert_timestamp(status_timestamp or order.get("timestamp")),
                        "coin": coin,
                        "coin_raw": raw_coin,
                        "side": order_side,
                        "price": price,
                        "size": size,
                        "usd_value": usd_value,
                        "orderId": order.get("oid"),
                        "status": status,
                        "is_significant": is_significant_position(usd_value),
                        "sequence": i,
                        "raw": update
                    }

                    # Apply deduplication filter for orders too
                    if event_deduplicator and not event_deduplicator.should_allow_event(base_event):
                        logger.debug(f"🚫 Order event suppressed by deduplicator: {coin} ${usd_value:,.2f}")
                        continue

                    # Attach formatted notification
                    base_event["message"] = format_notification(base_event, platform, current_prices)
                    
                    # Only add events with valid messages
                    if base_event["message"]:
                        parsed_events.append(base_event)
                        logger.info(f"✅ Valid order processed: {update_wallet[:8]}... - {status} {coin}")
                    else:
                        logger.debug(f"🗑️ Filtered out order update with no message: {coin}")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Error parsing order update {i}: {e}")
                    continue

        else:
            # Handle unknown event types - but only if they have meaningful data
            if event and isinstance(event, dict):
                unknown_event = {
                    "type": "unknown_event",
                    "wallet": wallet,
                    "channel": channel,
                    "raw": event,
                    "timestamp": datetime.now().isoformat() + "Z"
                }
                unknown_event["message"] = format_notification(unknown_event, platform)
                if unknown_event["message"]:
                    parsed_events.append(unknown_event)

        # Add suppressed event summaries periodically
        if event_deduplicator:
            suppressed_summaries = event_deduplicator.get_all_suppressed_summaries()
            for summary in suppressed_summaries:
                summary["message"] = format_notification(summary, platform)
                if summary["message"]:
                    parsed_events.append(summary)
                    logger.info(f"📊 Added suppressed events summary: {summary['suppressed_count']} events")

        if parsed_events:
            logger.info(f"✅ Successfully parsed {len(parsed_events)} events from channel '{channel}' for wallet {wallet[:8] if wallet else 'unknown'}...")
        
        return parsed_events

    except Exception as e:
        logger.exception(f"❌ Failed to parse event from channel '{channel}' for wallet {wallet[:8] if wallet else 'unknown'}...")
        return []  # Return empty list instead of error event to reduce spam
