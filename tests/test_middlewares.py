if __name__ == '__main__':
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
    
    from windmill_lib.event_bus import EventBus, Event

    async def logger(event, next):
        print(f"[LOG] -> {event.topic}")
        await next(event)
        print(f"[LOG] <- {event.topic}")

    # Middleware 2: Transformação de payload
    async def uppercase_user(event: Event, next):
        if isinstance(event.payload, str):
            event.payload = event.payload.upper()
        await next(event)

    bus = EventBus()
    bus.use(logger)
    bus.use(uppercase_user)

    @bus.on('greet')
    def hello(event: Event):
        print(f"Hello, {event.payload}!")
    
    bus.publish('greet', 'Max')
    bus.publish('greet', 'Bob')