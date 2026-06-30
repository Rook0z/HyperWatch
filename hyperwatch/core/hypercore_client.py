import asyncio
import json
import logging
import time
import websockets
from typing import Dict, Set, Optional
from dataclasses import dataclass, field

from hyperwatch.core.config import HYPERCORE_WS_URL, WATCHED_WALLETS
from hyperwatch.core.event_parser import parse_event
from hyperwatch.alerts.engine import AlertEngine 
t
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Set specific loggers to WARNING to reduce spam
logging.getLogger("hyperwatch.core.event_parser").setLevel(logging.INFO)
logging.getLogger("websockets").setLevel(logging.WARNING)

@dataclass
class ConnectionStats:
    """Track connection statistics"""
    connected: bool = False
    total_messages: int = 0
    successful_parses: int = 0
    failed_parses: int = 0
    last_message_time: Optional[float] = None
    reconnect_count: int = 0
    subscription_count: int = 0
    start_time: float = field(default_factory=time.time)
    wallet_events: Dict[str, int] = field(default_factory=dict)

class SimpleRateLimiter:
    def __init__(self, cooldown_seconds):
        self.cooldown = cooldown_seconds
        self.last_logged = {}

    def can_log(self, key):
        now = time.time()
        if key not in self.last_logged or (now - self.last_logged[key]) > self.cooldown:
            self.last_logged[key] = now
            return True
        return False

# Rate limiters
cli_rate_limiter = SimpleRateLimiter(5)
error_rate_limiter = SimpleRateLimiter(30)  # Increased to reduce error spam
debug_rate_limiter = SimpleRateLimiter(10)  # For debug messages

# Create a singleton AlertEngine instance
alert_engine = AlertEngine()

class HyperCoreClient:
    def __init__(self):
        self.ws: Optional[websockets.WebSocketClientProtocol] = None  
        self.subscribed_wallets: Set[str] = set()
        self.wallet_subscriptions: Dict[str, Set[str]] = {}  # channel -> set of wallets
        self.stats = ConnectionStats()
        self.is_shutting_down = False
        self.health_check_task: Optional[asyncio.Task] = None
        self.last_health_check = time.time()
        self._connection_healthy = True
        
        # Better wallet validation and tracking
        self.watched_wallet_set = {w.lower() for w in WATCHED_WALLETS}
        self.watched_wallets_original = {w.lower(): w for w in WATCHED_WALLETS}  # Keep original case
        
        for wallet in WATCHED_WALLETS:
            self.stats.wallet_events[wallet] = 0

    def extract_wallet_from_event_data(self, event_data, raw_event):
        """Better wallet extraction with validation"""
        wallet_fields = ['user', 'wallet', 'address', 'account', 'from', 'to', 'owner', 'trader', 'userAddress']
        
        def check_dict_for_wallet(data, source_name):
            if not isinstance(data, dict):
                return None
            for field in wallet_fields:
                if field in data and data[field]:
                    wallet = str(data[field]).strip()
                    if wallet and wallet not in ['unknown', 'multiple_wallets', '0x0', 'null']:
                        if debug_rate_limiter.can_log(f"wallet_found_{wallet[:8]}"):
                            logging.debug(f"🔍 Found wallet '{wallet[:8]}...' in {source_name}['{field}']")
                        return wallet
            return None
        
        # Check raw_event first
        wallet = check_dict_for_wallet(raw_event, 'raw_event')
        if wallet:
            return wallet
        
        # Check event_data
        if isinstance(event_data, list) and event_data:
            wallet = check_dict_for_wallet(event_data[0], 'event_data[0]')
        elif isinstance(event_data, dict):
            wallet = check_dict_for_wallet(event_data, 'event_data')
            
            # Check nested structures
            if not wallet:
                nested_data = event_data.get('data')
                if nested_data:
                    wallet = check_dict_for_wallet(nested_data, 'nested_data')
                
                # Check fills array
                if not wallet and 'fills' in event_data:
                    fills = event_data['fills']
                    if isinstance(fills, list) and fills:
                        wallet = check_dict_for_wallet(fills[0], 'fills[0]')
        
        return wallet

    def is_watched_wallet(self, wallet: str) -> bool:
        """Check if wallet is in our watched list"""
        if not wallet or wallet == "unknown":
            return False
        return wallet.lower() in self.watched_wallet_set

    def get_original_wallet_case(self, wallet: str) -> str:
        """Get the original case of the wallet from our watched list"""
        if not wallet:
            return wallet
        return self.watched_wallets_original.get(wallet.lower(), wallet)

    async def health_check(self) -> bool:
        """Check WebSocket connection health using JSON ping instead of WebSocket ping frame"""
        try:
            # Check if websocket exists and is in a valid state
            if not self.ws:
                self._connection_healthy = False
                return False
            
            # For client connections, check the state instead of .closed
            if self.ws.state != websockets.protocol.State.OPEN:
                self._connection_healthy = False
                if debug_rate_limiter.can_log("ws_state"):
                    logging.debug(f"⚠️ WebSocket state is {self.ws.state}, not OPEN")
                return False
            
            # Send JSON ping message instead of WebSocket ping frame
            try:
                ping_message = {
                    "method": "ping",
                    "id": int(time.time() * 1000)  # Use timestamp as unique ID
                }
                
                # Send the ping message
                await self.ws.send(json.dumps(ping_message))
                
                # Wait for any response within timeout (don't expect specific pong format)
                # check if the connection is still responsive
                await asyncio.sleep(0.1)
                
                # If we get here without exception, connection is healthy
                self.last_health_check = time.time()
                self._connection_healthy = True
                
                if debug_rate_limiter.can_log("health_check_success"):
                    logging.debug("💚 Health check passed")
                return True
                    
            except asyncio.TimeoutError:
                self._connection_healthy = False
                if error_rate_limiter.can_log("health_timeout"):
                    logging.warning("⚠️ Health check timeout - connection may be unhealthy")
                return False
            except (websockets.exceptions.ConnectionClosed, 
                    websockets.exceptions.ConnectionClosedError,
                    ConnectionResetError, 
                    BrokenPipeError) as e:
                self._connection_healthy = False
                if error_rate_limiter.can_log("health_connection_closed"):
                    logging.warning(f"⚠️ Connection closed during health check: {e}")
                return False
                    
        except Exception as e:
            self._connection_healthy = False
            if error_rate_limiter.can_log("health_check_error"):
                logging.warning(f"⚠️ Health check failed: {e}")
            return False

    async def connection_monitor(self):
        """FIXED: Monitor connection health with faster recovery"""
        consecutive_failures = 0
        
        while not self.is_shutting_down:
            try:
                # Check more frequently initially, then back off
                check_interval = min(30, 5 + consecutive_failures * 2)  # 5-30 seconds
                await asyncio.sleep(check_interval)
                
                if not self._connection_healthy or not await self.health_check():
                    consecutive_failures += 1
                    
                    if consecutive_failures >= 2:  # Only log after multiple failures
                        logging.warning(f"💔 Connection unhealthy for {consecutive_failures} checks, triggering reconnect")
                        if self.ws and self.ws.state == websockets.protocol.State.OPEN:
                            try:
                                await self.ws.close(code=1000, reason="Health check failed")
                            except:
                                pass  # Connection might already be closed
                        break  # Exit monitor to trigger reconnection
                else:
                    if consecutive_failures > 0:
                        logging.info("💚 Connection recovered")
                    consecutive_failures = 0
                
                # Log detailed statistics every 5 minutes
                uptime = time.time() - self.stats.start_time
                if uptime > 300 and uptime % 300 < check_interval:  # Every 5 minutes
                    logging.info(
                        f"📊 Stats: {self.stats.total_messages} msgs, "
                        f"{self.stats.successful_parses} parsed, "
                        f"{self.stats.subscription_count} subs, "
                        f"uptime: {uptime/60:.1f}m"
                    )
                    
                    # Log per-wallet stats
                    for wallet, count in self.stats.wallet_events.items():
                        if count > 0:
                            logging.info(f"  └── {wallet[:8]}...: {count} events")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                if error_rate_limiter.can_log("monitor_error"):
                    logging.error(f"❌ Connection monitor error: {e}")

    async def subscribe_to_wallet_channel(self, wallet: str, channel_type: str):
        """Subscribe to specific wallet channel with better error handling"""
        sub_key = f"{channel_type}:{wallet}"
        if sub_key in self.subscribed_wallets:
            return True
        try:
            sub_msg = {
                "method": "subscribe",
                "subscription": {"type": channel_type, "user": wallet}
            }
            logging.debug(f"📤 Subscribing: {channel_type} for {wallet[:8]}...")
            await self.ws.send(json.dumps(sub_msg))
            self.subscribed_wallets.add(sub_key)
            if channel_type not in self.wallet_subscriptions:
                self.wallet_subscriptions[channel_type] = set()
            self.wallet_subscriptions[channel_type].add(wallet)
            self.stats.subscription_count += 1
            logging.info(f"✅ Subscribed to {channel_type} for wallet: {wallet[:8]}...")
            await asyncio.sleep(0.1)
            return True
        except Exception as e:
            logging.error(f"❌ Failed to subscribe {channel_type} for {wallet[:8]}: {e}")
            return False

    async def handle_event(self, raw_event: dict):
        """Enhanced event handling with better error management"""
        try:
            self.stats.total_messages += 1
            self.stats.last_message_time = time.time()
            channel = raw_event.get("channel", "unknown")
            
            if debug_rate_limiter.can_log(f"event_{channel}"):
                logging.debug(f"📥 Event on {channel}")
                
            if channel == "error":
                error_msg = raw_event.get("data", "Unknown error")
                logging.error(f"❌ Server Error: {error_msg}")
                return
            elif channel == "subscriptionResponse":
                await self._handle_subscription_response(raw_event)
                return
            elif channel in ["userFills", "userEvents", "orderUpdates"]:
                await self._handle_wallet_event(raw_event, channel)
            else:
                if debug_rate_limiter.can_log(f"general_{channel}"):
                    logging.debug(f"📦 General event on {channel}")
        except Exception as e:
            if error_rate_limiter.can_log(f"handle_event_{channel}"):
                logging.error(f"💥 Error handling event on {channel}: {e}")

    async def _handle_subscription_response(self, raw_event: dict):
        response_data = raw_event.get("data", {})
        if isinstance(response_data, dict):
            success = response_data.get("success", True)
            if success:
                logging.debug("✅ Subscription confirmed")
            else:
                error = response_data.get("error", "Unknown subscription error")
                logging.error(f"❌ Subscription failed: {error}")
        else:
            logging.debug("✅ Subscription confirmed (simple)")

    async def _handle_wallet_event(self, raw_event: dict, channel: str):
        """Handle wallet-specific events with proper wallet filtering"""
        event_data = raw_event.get("data")

        # Enhanced wallet resolution
        wallet = (
            raw_event.get("user") or
            raw_event.get("wallet") or
            self.extract_wallet_from_event_data(event_data, raw_event)
        )

        # Validate that this wallet is one we're actually watching
        if wallet and wallet != "unknown":
            if not self.is_watched_wallet(wallet):
                if debug_rate_limiter.can_log(f"filtered_{wallet[:8]}"):
                    logging.debug(f"🚫 Filtering out event from non-watched wallet: {wallet[:8]}...")
                return  # Skip events from wallets we're not watching
            
            # Use original case for consistency
            wallet = self.get_original_wallet_case(wallet)
        else:
            # Try to use first subscribed wallet as fallback
            if channel in self.wallet_subscriptions:
                subs = self.wallet_subscriptions[channel]
                if subs:
                    wallet = list(subs)[0]
                    logging.debug(f"♻️ Using subscribed wallet fallback for {channel}: {wallet[:8]}...")

        if not wallet or wallet == "unknown":
            if debug_rate_limiter.can_log(f"no_wallet_{channel}"):
                logging.debug(f"⚠️ No valid watched wallet identified for {channel} event")
            return  # Skip events without valid wallet

        # Update wallet statistics
        if wallet in self.stats.wallet_events:
            self.stats.wallet_events[wallet] += 1

        logging.debug(f"🎯 Processing {channel} for wallet: {wallet[:8]}...")

        try:
            if isinstance(event_data, list):
                for i, single_event in enumerate(event_data):
                    event_wallet = wallet
                    # Double-check wallet from individual event data
                    extracted_wallet = self.extract_wallet_from_event_data(single_event, raw_event)
                    if extracted_wallet and self.is_watched_wallet(extracted_wallet):
                        event_wallet = self.get_original_wallet_case(extracted_wallet)
                    elif extracted_wallet and not self.is_watched_wallet(extracted_wallet):
                        if debug_rate_limiter.can_log(f"batch_filter_{extracted_wallet[:8]}"):
                            logging.debug(f"🚫 Skipping event from non-watched wallet in batch: {extracted_wallet[:8]}...")
                        continue
                    
                    await self.process_single_event(single_event, channel, event_wallet, f"{channel}_{i}")
            elif isinstance(event_data, dict):
                await self.process_single_event(event_data, channel, wallet, channel)
            else:
                if debug_rate_limiter.can_log(f"format_{channel}"):
                    logging.warning(f"⚠️ Unexpected event data format on {channel}: {type(event_data)}")
        except Exception as e:
            if error_rate_limiter.can_log(f"process_{channel}"):
                logging.error(f"❌ Error processing {channel} event: {e}")

    async def process_single_event(self, event_data: dict, channel: str, wallet: str, event_id: str):
        """Process a single event with better validation and error handling"""
        try:
            if not isinstance(event_data, dict):
                if debug_rate_limiter.can_log(f"invalid_type_{event_id}"):
                    logging.warning(f"⚠️ Event {event_id} is not a dict: {type(event_data)}")
                return
            
            # Double-check wallet validation before processing
            if not self.is_watched_wallet(wallet):
                if debug_rate_limiter.can_log(f"unwatched_{wallet[:8]}"):
                    logging.debug(f"🚫 Skipping processing for non-watched wallet: {wallet[:8]}...")
                return
            
            # Parse the event
            parsed_events = parse_event(event_data, channel, wallet)
            
            if not parsed_events:
                return
                
            if not isinstance(parsed_events, list):
                parsed_events = [parsed_events]
            
            valid_events = [e for e in parsed_events if isinstance(e, dict)]
            if not valid_events:
                return
            
            self.stats.successful_parses += 1
            
            # Log successful processing with wallet context
            logging.info(f"🎉 Processed {len(valid_events)} event(s) for {wallet[:8]}... on {channel}")
            
            for event in valid_events:
                await self.process_parsed_event(event, event_id, wallet)
                
        except Exception as e:
            self.stats.failed_parses += 1
            if error_rate_limiter.can_log(f"process_event_{wallet[:8]}_{event_id}"):
                logging.error(f"💥 Error processing event {event_id} for {wallet[:8]}...: {e}")

    async def process_parsed_event(self, event: dict, event_id: str, wallet: str):
        """Process a successfully parsed event through the alert engine"""
        try:
            # Ensure the event has the correct wallet
            event["wallet"] = wallet
            
            logging.info(f"🚀 Alert sent for {wallet[:8]}...: {event.get('type', 'unknown')} - {event.get('coin', 'N/A')} ${event.get('usd_value', 0):,.2f}")
            
            # Send to alert engine for processing
            await alert_engine.process_event(event)
            
        except Exception as e:
            if error_rate_limiter.can_log(f"alert_engine_{wallet[:8]}_{event_id}"):
                logging.error(f"❌ Error in alert engine for {event_id} ({wallet[:8]}...): {e}")

    async def start(self):
        """Alias for connect_and_run() to maintain compatibility"""
        await self.connect_and_run()

    async def connect_and_run(self):
        """Main connection loop with faster reconnection and better error handling"""
        self.stats.start_time = time.time()
        
        logging.info(f"🎯 Starting HyperWatch for {len(WATCHED_WALLETS)} wallets:")
        for wallet in WATCHED_WALLETS:
            logging.info(f"  └── Watching: {wallet[:8]}...")
        
        consecutive_failures = 0
        
        while not self.is_shutting_down:
            try:
                logging.info(f"🔌 Connecting to {HYPERCORE_WS_URL}...")
                
                # Reset connection state
                self._connection_healthy = False
                
                async with websockets.connect(
                    HYPERCORE_WS_URL,
                    ping_interval=None,  # Disable automatic WebSocket ping since Hyperliquid doesn't support it
                    ping_timeout=None,   # Disable ping timeout
                    close_timeout=5,     # Faster close timeout
                    max_size=10**7       # Increased message size limit
                ) as websocket:
                    self.ws = websocket
                    self.stats.connected = True
                    self._connection_healthy = True
                    self.stats.reconnect_count += 1
                    consecutive_failures = 0  # Reset failure count on successful connection
                    
                    logging.info("✅ WebSocket connected successfully")
                    
                    # Start connection monitor
                    if not self.health_check_task or self.health_check_task.done():
                        self.health_check_task = asyncio.create_task(self.connection_monitor())
                    
                    # Subscribe to all configured wallets
                    success = await self.subscribe_to_all_wallets()
                    if not success:
                        logging.error("❌ Failed to establish subscriptions")
                        continue
                    
                    logging.info("🎧 Listening for events...")
                    
                    # Message processing loop
                    async for message in websocket:
                        try:
                            raw_event = json.loads(message)
                            await self.handle_event(raw_event)
                        except json.JSONDecodeError as e:
                            if error_rate_limiter.can_log("json_decode"):
                                logging.error(f"❌ Invalid JSON received: {e}")
                        except Exception as e:
                            if error_rate_limiter.can_log("message_processing"):
                                logging.error(f"💥 Error processing message: {e}")
                            
            except (websockets.exceptions.ConnectionClosed, 
                    websockets.exceptions.InvalidMessage,
                    websockets.exceptions.WebSocketException) as e:
                self.stats.connected = False
                self._connection_healthy = False
                consecutive_failures += 1
                
                if consecutive_failures <= 3:  # Only log first few failures
                    logging.warning(f"🔌 WebSocket error (attempt {consecutive_failures}): {e}")
                
            except Exception as e:
                self.stats.connected = False
                self._connection_healthy = False
                consecutive_failures += 1
                
                if error_rate_limiter.can_log("connection_error"):
                    logging.error(f"❌ Unexpected connection error: {e}")
                
            finally:
                self.stats.connected = False
                self._connection_healthy = False
                if self.health_check_task and not self.health_check_task.done():
                    self.health_check_task.cancel()
                    try:
                        await self.health_check_task
                    except asyncio.CancelledError:
                        pass
                    
            if not self.is_shutting_down:
                # Progressive backoff: start fast, then slow down
                if consecutive_failures <= 3:
                    reconnect_delay = 2  # Fast reconnection for first few failures
                elif consecutive_failures <= 10:
                    reconnect_delay = 5  # Medium delay
                else:
                    reconnect_delay = min(30, consecutive_failures)  # Slower for persistent issues
                
                if consecutive_failures <= 5:  # Only log for first few attempts
                    logging.info(f"⏳ Reconnecting in {reconnect_delay} seconds... (attempt {consecutive_failures})")
                
                await asyncio.sleep(reconnect_delay)

    async def subscribe_to_all_wallets(self):
        """Subscribe to all configured wallet channels"""
        if not self.ws or self.ws.state != websockets.protocol.State.OPEN:
            logging.error("❌ WebSocket not connected, cannot subscribe")
            return False
            
        channel_types = ["userFills", "userEvents", "orderUpdates"]
        success_count = 0
        total_subscriptions = len(WATCHED_WALLETS) * len(channel_types)
        
        logging.info(f"📡 Setting up {total_subscriptions} subscriptions...")
        
        for wallet in WATCHED_WALLETS:
            logging.info(f"📡 Subscribing wallet: {wallet[:8]}...")
            
            for channel_type in channel_types:
                success = await self.subscribe_to_wallet_channel(wallet, channel_type)
                if success:
                    success_count += 1
                await asyncio.sleep(0.3)  # Rate limiting between subscriptions
        
        logging.info(f"📊 Subscription complete: {success_count}/{total_subscriptions} successful")
        
        if success_count == 0:
            logging.error("❌ No subscriptions successful!")
            return False
        elif success_count < total_subscriptions:
            logging.warning(f"⚠️ Only {success_count}/{total_subscriptions} subscriptions successful")
        else:
            logging.info("✅ All subscriptions successful!")
            
        return success_count > 0

    async def shutdown(self):
        """Graceful shutdown"""
        logging.info("🛑 Shutting down HyperCore client...")
        self.is_shutting_down = True
        
        if self.health_check_task and not self.health_check_task.done():
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
            
        if self.ws and self.ws.state == websockets.protocol.State.OPEN:
            try:
                await self.ws.close(code=1000, reason="Graceful shutdown")
            except:
                pass  # Connection might already be closed
            
        logging.info("✅ Shutdown complete")
