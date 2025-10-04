if __name__ == '__main__':
    import sys
    import os

    # adiciona o diretório src/ ao sys.path
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
    
    from windmill_lib.event_bus import EventBus, Event
    import asyncio

    bus = EventBus()

    # Synchronous listener with the second highest priority
    @bus.on('greet', priority=5)
    def hello_sync(event: Event):
        print(f"Hello, {event.payload}! (event id = {event.identifier})")

    # Async listener with the highest priority
    @bus.on('greet', priority=10)
    async def hello_async(event: Event):
        await asyncio.sleep(0.1)
        print(f"Hello async, {event.payload}!")
    
    # This is executed once and nevermore. It has the lowest priority.
    @bus.on('greet', priority=1, once=True)
    def hello_once(_: Event):
        print("This will run once")

    print("--- publish start ---")
    bus.publish('greet', 'Max')
    print("--- publish done ---")

    async def main():
        await bus.publish_async('greet', 'Bob')
        await bus.publish_async('greet', 'Carol')

    asyncio.run(main())
    print(bus.list_subscribers())