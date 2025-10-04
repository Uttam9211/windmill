from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable, Dict, List, Optional, Tuple
from .models import Event, Handler

class EventBus:
    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Handler]] = {}
        self._lock = asyncio.Lock()
    
    async def subscribe_async(self, topic: str, handler: Callable[[Event], Any], *, priority: int = 0, once: bool = False) -> str:
        h = Handler(neg_priority=-priority, callback=handler, once=once)

        async with self._lock:
            self._subscribers.setdefault(topic, []).append(h)
        
        return h.identifier
    
    def subscribe(self, topic: str, handler: Callable[[Event], Any], *, priority: int = 0, once: bool = False) -> str:
        h = Handler(neg_priority=-priority, callback=handler, once=once)
        self._subscribers.setdefault(topic, []).append(h)
        
        return h.identifier
    
    async def unsubscribe_async(self, topic: str, handler_id: str) -> bool:
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
        handlers = self._subscribers.get(topic)

        if not handlers:
            return False

        before = len(handlers)
        handlers[:] = [h for h in handlers if h.identifier != handler_id]

        if not handlers:
            self._subscribers.pop(topic, None)
        
        return len(handlers) != before
    
    def on(self, topic: str, *, priority: int = 0, once: bool = False) -> Callable[[Callable[[Event], Any]], Callable[[Event], Any]]:
        def decorator(fn: Callable[[Event], Any]):
            self.subscribe(topic, fn, priority=priority, once=once)
            return fn
        
        return decorator
    
    async def publish_async(self, topic: str, payload: Any = None, *, metadata: Optional[Dict[str, Any]] = None) -> None:
        event = Event(topic=topic, payload=payload, metadata=metadata or {})

        async with self._lock:
            handlers = list(self._subscribers.get(topic, []))
        
        if not handlers:
            return
        
        handlers.sort()
        loop = asyncio.get_running_loop()
        to_remove: List[str] = []

        for h in handlers:
            cb = h.callback

            try:
                if inspect.iscoroutinefunction(cb):
                    await cb(event)
                else:
                    await loop.run_in_executor(None, cb, event)
            except Exception:
                import traceback
                traceback.print_exc()
            finally:
                if h.once:
                    to_remove.append(h.identifier)
        
        if to_remove:
            async with self._lock:
                current = self._subscribers.get(topic, [])
                current[:] = [h for h in current if h.identifier not in to_remove]

                if not current:
                    self._subscribers.pop(topic, None)

    def publish(self, topic: str, payload: Any = None, *, metadata: Optional[Dict[str, Any]] = None) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        
        if loop is not None and loop.is_running():
            raise RuntimeError("synchronous publish cannot be called from a running asyncio event loop.")
        
        asyncio.run(self.publish_async(topic, payload, metadata=metadata))
    
    def list_subscribers(self, topic: Optional[str] = None) -> Dict[str, List[Tuple[str, int, bool]]]:
        out: Dict[str, List[Tuple[str, int, bool]]] = {}

        for t, handlers in self._subscribers.items():
            if topic is not None and t != topic:
                continue
            out[t] = [(h.identifier, h.priority, h.once) for h in handlers]
        
        return out
    
    def clear(self) -> None:
        self._subscribers.clear()