if __name__ == '__main__':
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
    
    from windmill_lib.event_bus import EventBus, Event

    bus = EventBus()

    @bus.on('greet', static=True)
    def hello(event: Event):
        print(f"Hello, {event.payload}!")
    
    bus.publish('greet', 'Max')
    bus.publish('greet', 'Max')
    bus.publish('greet', 'Bob')