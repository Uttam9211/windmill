from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable, Dict, List, Optional, Tuple
from .models import Event, Handler
import re

class EventBus:
    """
    The EventBus is the main entity of this library.
    It handles events through associated listeners and
    organizes the execution flow.
    """
    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Handler]] = {}
        self._lock = asyncio.Lock()
    
    async def subscribe_async(self, topic: str, handler: Callable[[Event], Any], *, priority: int = 0, once: bool = False, max_retries: int = 0) -> str:
        """Same of `subscribe`, but is asynchronous."""
        h = Handler(neg_priority=-priority, callback=handler, once=once, max_retries=max_retries)

        async with self._lock:
            self._subscribers.setdefault(topic, []).append(h)
        
        return h.identifier
    
    def subscribe(self, topic: str, handler: Callable[[Event], Any], *, priority: int = 0, once: bool = False, max_retries: int = 0) -> str:
        """Subscribes a listener to an event/topic. When the event is published, the listener is executed."""
        h = Handler(neg_priority=-priority, callback=handler, once=once, max_retries=max_retries)
        self._subscribers.setdefault(topic, []).append(h)
        
        return h.identifier
    
    async def unsubscribe_async(self, topic: str, handler_id: str) -> bool:
        """Same of `unsubscribe`, but is asynchronous."""
        async with self._lock:
            handlers = self._subscribers.get(topic)

            if not handlers:
                return False

            before = len(handlers)
            handlers[:] = [h for h in handlers if h.identifier != handler_id]

            if not handlers:
                self._subscribers.pop(topic, None)
            
            return len(handlers) != before
        
    def unsubscribe(self, topic: str, handler_id: str) -> bool:
        """Removes a listener from a topic."""
        handlers = self._subscribers.get(topic)

        if not handlers:
            return False

        before = len(handlers)
        handlers[:] = [h for h in handlers if h.identifier != handler_id]

        if not handlers:
            self._subscribers.pop(topic, None)
        
        return len(handlers) != before
    
    def on(self, topic: str, *, priority: int = 0, once: bool = False, max_retries: int = 0) -> Callable[[Callable[[Event], Any]], Callable[[Event], Any]]:
        """Decorator to help creating listeners. It does the same as `subscribe`, but in an easier way."""
        def decorator(fn: Callable[[Event], Any]) -> Callable[[Event], Any]:
            self.subscribe(topic, fn, priority=priority, once=once, max_retries=max_retries)
            return fn
        return decorator
            
    async def publish_async(self, topic: str, payload: Any = None, *, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Same of `publish`, but is asynchronous."""
        event = Event(topic=topic, payload=payload, metadata=metadata or {})

        async with self._lock:
            handlers = self._find_handlers(topic)
        
        if not handlers:
            return
        
        to_remove: List[str] = []

        for h in handlers:
            await self._execute_handler(h, event)

            if h.once:
                to_remove.append((topic, h.identifier))
            
        if to_remove:
            async with self._lock:
                for t, hid in to_remove:
                    hs = self._subscribers.get(t, [])
                    self._subscribers[t] = [x for x in hs if x.identifier != hid]

    def publish(self, topic: str, payload: Any = None, *, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Publishes an event. All the associated listeners will be executed."""
        asyncio.run(self.publish_async(topic, payload, metadata=metadata))
    
    def list_subscribers(self) -> Dict[str, List[Tuple[str, int, bool, int]]]:
        return {t: [(h.identifier, h.priority, h.once, h.max_retries) for h in hs] for t, hs in self._subscribers.items()}
    
    def clear(self) -> None:
        self._subscribers.clear()

    @staticmethod
    def _match_topic(pattern: str, topic: str) -> bool:
        regex_pattern = re.escape(pattern)
        regex_pattern = regex_pattern.replace(r"\*", "[^.]+")
        regex_pattern = regex_pattern.replace(r"\#", ".*")
        regex_pattern = f"^{regex_pattern}$"
        return re.match(regex_pattern, topic) is not None
    
    def _find_handlers(self, topic: str) -> List[Handler]:
        handlers: List[Handler] = []
        for pattern, hs in self._subscribers.items():
            if self._match_topic(pattern, topic):
                handlers.extend(hs)
            
        handlers.sort()
        return handlers
    
    async def _execute_handler(self, handler: Handler, event: Event) -> None:
        retries = 0
        backoff_base = 0.2

        while True:
            try:
                if inspect.iscoroutinefunction(handler.callback):
                    await handler.callback(event)
                else:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, handler.callback, event)
                break
            except Exception as e:
                retries += 1
                if retries > handler.max_retries:
                    await self.publish_async('dead.letter', {
                        'original_topic': event.topic,
                        'event_id': event.identifier,
                        'payload': event.payload,
                        'error': str(e),
                    })
                    break
                await asyncio.sleep(backoff_base * (2 ** (retries - 1)))