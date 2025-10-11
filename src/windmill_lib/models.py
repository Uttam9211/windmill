"""
This module defines the models needed in `EventBus` and some other modules
inside this library.

The modle content:
* `Event` - The event data. Describes an event that has a context, payload and metadata. Each event has an unique id.
* `Handler` - The handler data. A handler is basically a wrapper for a callback that handles some event. It also have priority and some other options.

**This module contains internal data and shouldn't be used directly by yourself.**
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Callable
import uuid
import time

@dataclass
class Event:
    topic: str
    payload: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    identifier: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)

@dataclass(order=True)
class Handler:
    neg_priority: int
    callback: Callable[[Event], Any] = field(compare=False)
    once: bool = field(default=False, compare=False)
    static: bool = field(default=False, compare=False)
    max_retries: int = field(default=0, compare=False)
    identifier: str = field(default_factory=lambda: str(uuid.uuid4()), compare=False)

    @property
    def priority(self) -> int:
        return -self.neg_priority