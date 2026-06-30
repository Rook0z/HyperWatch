import json
import os
import time
from typing import Optional, Dict, List, Tuple, Callable

DEFAULT_COOLDOWN_SECONDS = 30  # 30 seconds

class RateLimiter:
    def __init__(
        self,
        cooldown: int = DEFAULT_COOLDOWN_SECONDS,
        persistence_file: Optional[str] = None,
        notify_callback: Optional[Callable[[str, str, str, str, List[dict]], None]] = None,
    ):
        self.cooldown = cooldown
        # key: (user, coin, event_type, channel)
        self.last_sent: Dict[Tuple[str, str, str, str], float] = {}
        self.pending_events: Dict[Tuple[str, str, str, str], List[dict]] = {}
        self.persistence_file = persistence_file or "rate_limiter_cache.json"
        self.notify_callback = notify_callback
        self.load_cache()

    def load_cache(self):
        """Load rate limiter state from disk"""
        if os.path.exists(self.persistence_file):
            try:
                with open(self.persistence_file, "r") as f:
                    data = json.load(f)
                    self.last_sent = {
                        tuple(k.split("|")): v for k, v in data.items()
                    }
            except Exception as e:
                print(f"⚠️ Failed to load rate limiter cache: {e}")

    def save_cache(self):
        """Save rate limiter state to disk"""
        try:
            with open(self.persistence_file, "w") as f:
                data = {
                    "|".join(k): v for k, v in self.last_sent.items()
                }
                json.dump(data, f)
        except Exception as e:
            print(f"⚠️ Failed to save rate limiter cache: {e}")

    def is_allowed(self, user: str, coin: str, event_type: str, channel: str) -> bool:
        key = (user, coin, event_type, channel)
        now = time.time()

        last_time = self.last_sent.get(key, 0)
        elapsed = now - last_time
        allowed = elapsed >= self.cooldown
        remaining = max(self.cooldown - elapsed, 0)

        # Reduced logging verbosity
        if not allowed:
            print(f"[RateLimiter] Rate limited: {key} | Remaining: {remaining:.1f}s")

        if allowed:
            self.last_sent[key] = now
            self.save_cache()
            return True
        else:
            return False

    def add_pending_event(self, user: str, coin: str, event_type: str, channel: str, event: dict):
        """Add event to pending queue"""
        key = (user, coin, event_type, channel)
        if key not in self.pending_events:
            self.pending_events[key] = []
        self.pending_events[key].append(event)

    def get_pending_events(self, user: str, coin: str, event_type: str, channel: str) -> List[dict]:
        """Get pending events for specific key"""
        key = (user, coin, event_type, channel)
        return self.pending_events.get(key, [])

    def clear_pending_events(self, user: str, coin: str, event_type: str, channel: str):
        """Clear pending events for specific key"""
        key = (user, coin, event_type, channel)
        if key in self.pending_events:
            del self.pending_events[key]

    def process_event(self, user: str, coin: str, event_type: str, channel: str, event: dict):
        if self.is_allowed(user, coin, event_type, channel):
            # Send notification immediately
            if self.notify_callback:
                # Send all pending events + current event in one notification
                pending = self.get_pending_events(user, coin, event_type, channel)
                all_events = pending + [event]
                self.notify_callback(user, coin, event_type, channel, all_events)
                self.clear_pending_events(user, coin, event_type, channel)
            else:
                print(f"Notification: {user} | {coin} | {event_type} | {channel} | Event: {event}")
        else:
            # Rate limit active, queue event for later summary
            self.add_pending_event(user, coin, event_type, channel, event)

    def flush_all(self):
        """
        Flush all pending events that are past their cooldown period.
        This is mainly kept for compatibility.
        """
        now = time.time()
        keys_to_flush = []

        for key, last_time in self.last_sent.items():
            user, coin, event_type, channel = key
            if now - last_time >= self.cooldown:
                if key in self.pending_events and self.pending_events[key]:
                    keys_to_flush.append(key)

        for key in keys_to_flush:
            user, coin, event_type, channel = key
            if self.notify_callback:
                self.notify_callback(user, coin, event_type, channel, self.pending_events[key])
            else:
                print(f"Flushing notifications for {key}: {self.pending_events[key]}")
            self.clear_pending_events(user, coin, event_type, channel)
            # Update last_sent timestamp so next cooldown begins now
            self.last_sent[key] = now
            self.save_cache()
