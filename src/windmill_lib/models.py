"""
This module defines the models needed in `EventBus` and some other modules
inside this library.

The modle content:
* `Event` - The event data. Describes an event that has a context, payload and metadata. Each event has an unique id.
* `Handler` - The handler data. A handler is basically a wrapper for a callback that handles some event. It also have priority and some other options.

**This module contains internal data and shouldn't be used directly by yourself.**
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Callable, Optional, Awaitable, List
import uuid
import time
import re

@dataclass
class Event:
    topic: str
    payload: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    identifier: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)

    @staticmethod
    def match_topic(pattern: str, topic: str) -> bool:
        regex_pattern = re.escape(pattern)
        regex_pattern = regex_pattern.replace(r"\*", "[^.]+").replace(r"\#", ".*")
        regex_pattern = f"^{regex_pattern}$"
        return re.match(regex_pattern, topic) is not None

@dataclass(order=True)
class Handler:
    neg_priority: int
    callback: Callable[[Event], Any] = field(compare=False)
    filter_fn: Optional[Callable[[Event], bool]] = field(compare=False)
    once: bool = field(default=False, compare=False)
    static: bool = field(default=False, compare=False)
    max_retries: int = field(default=0, compare=False)
    identifier: str = field(default_factory=lambda: str(uuid.uuid4()), compare=False)

    @property
    def priority(self) -> int:
        return -self.neg_priority

Middleware = Callable[[Event, Callable[[Event], Awaitable[None]]], Awaitable[None]]

@dataclass
class MiddlewareEntry:
    middleware: Middleware
    topics: List[str] = field(default_factory=list[str])
    
    def matches(self, topic: str) -> bool:
        if not self.topics:
            return True

        return any(Event.match_topic(pat, topic) for pat in self.topics)