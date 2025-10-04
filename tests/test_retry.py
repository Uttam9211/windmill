if __name__ == '__main__':
    import sys
    import os

    # adiciona o diretório src/ ao sys.path
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

    import asyncio
    from windmill_lib.event_bus import EventBus, Event

    bus = EventBus()

    @bus.on('user.*', priority=5)
    def log_user(event: Event):
        print(f"[LOG] User event matched: {event.topic} -> {event.payload}")

    @bus.on('user.create', priority=10, max_retries=3)
    async def create_user(event: Event):
        print(f"Creating user {event.payload['name']} ...")
        if event.payload.get('fail'):
            raise ValueError("Simulated failure")
        
        await asyncio.sleep(0.1)
        print(f"User {event.payload['name']} created successfully!")

    @bus.on('dead.letter')
    def handle_dead(event: Event):
        print(f"[DLQ] Event {event.payload['original_topic']} failed permanently: {event.payload['error']}")

    async def main():
        await bus.publish_async('user.create', {'name': 'Alice'})
        await bus.publish_async('user.create', {'name': 'Bob', 'fail': True})
        await bus.publish_async('user.delete', {'id': 42})

    asyncio.run(main())