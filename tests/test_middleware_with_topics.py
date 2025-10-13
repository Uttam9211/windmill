if __name__ == '__main__':
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
    
    from windmill_lib.event_bus import EventBus, Event
    import asyncio

    bus = EventBus()

    async def logger(event, next):
        print(f"[LOG] {event.topic}")
        await next(event)

    async def uppercase_user(event, next):
        if isinstance(event.payload, dict) and "name" in event.payload:
            event.payload["name"] = event.payload["name"].upper()
        await next(event)

    # Middleware para "order.#"
    async def audit(event, next):
        print(f"[AUDIT] {event.topic} -> {event.payload}")
        await next(event)

    bus.use(logger)
    bus.use(uppercase_user, topics=["user.*"])
    bus.use(audit, topics=["order.#"])

    @bus.on("user.created")
    async def handle_user_created(event):
        print(f"User created: {event.payload}")

    @bus.on("user.deleted")
    async def handle_user_deleted(event):
        print(f"User deleted: {event.payload}")

    @bus.on("order.new")
    def handle_order(event):
        print(f"Order: {event.payload}")

    async def main():
        await bus.publish_async("user.created", {"name": "Alice"})
        await bus.publish_async("order.new", {"id": 42, "value": 100})
        await bus.publish_async("user.deleted", {"name": "Alice"})

    asyncio.run(main())