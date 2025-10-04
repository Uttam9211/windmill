<div style="display: inline-flex; align-items: center;">
    <img src="windmill.png" width="128", height="128">
    <div style="margin: 0 0 0 30px;">
        <h1>Windmill</h1>
        <h2>Handling events on Python in a soft way</h2>
    </div>
</div>

---
## What is Windmill?
This project is a Python library that offers a more soft and minimalist way to handle events in your Python application. The main idea of Windmill is to provide a decoupled way to communicate a lot of parts of your code about events without creating any direct links or dependencies between isolated regions of code.

---
## The Features
This project currently supports the following features:
- Async event publishing
- Sync and async handlers (listeners)
- Priority level for events
- "once" execution for events
- Simple listeners subscription API and decorators

---
## How to install
You can install the library through `pip install` command:
```bash
pip install windmill-lib
```

---
## How to use
Here is a little example of how to use this library in your project:
```python
# Here we import the library
from windmill.event_bus import EventBus,  Event

# It creates an instance of EventBus
bus = EventBus()

# Here we define an event listener
# We are saying that when the 'greet' event is emitted, then executes something. This listener receives the emitted event instance as parameter.
@bus.on('greet')
def hello(event: Event):
    print(f"Hello world, {event.payload}!")

# Here we publish the 'greet' event with the 'Bob' as payload (data)
bus.publish('greet', 'Bob')
```
The expected output for the code above is:
```
Hello world, Bob!
```

### Async support
You can also create **async listeners** and publish events using asynchronous mode:

```python
import asyncio

@bus.on('greet')
async def hello_async(event: Event):
    await asyncio.sleep(1.0)
    print(f"Hello world, {event.payload}!")

bus.publish('greet', 'Max')
```
Publishing with asynchronous functions:
```python
async def main():
    await bus.publish_async('greet', 'Caroline')
    await bus.publish_async('greet', 'Daniel')

asyncio.run(main())
```

### Priority parameter
Listeners can also have **priority level** and may be executed in
different orders according to its priority. Higher priority grants
earlier execution.

```python
@bus.on('greet', priority=2)
def hello_first(event: Event):
    print(f"Hello world, {event.payload}! I am first.")

@bus.on('greet', priority=1)
def hello_second(event: Event):
    print(f"Hello world, {event.payload}! I am second.")

bus.publish('greet', 'Max')
```
This will generate the following output:
```
Hello world, Max! I am first.
Hello world, Max! I am second.
```

### "once" parameter
You can setup a listener to be executed only once before its deletion:

```python
@bus.on('greet', once=True)
def hello_once(event: Event):
    print(f"Hello, {event.payload}! Executed only once and nevermore.")

bus.publish('greet', 'Max')
bus.publish('greet', 'Bob')
```

This will result in the following output:
```
Hello, Max! Executed only once and nevermore.
```

---
## Todos:
- [ ] Wildcard topics
- [ ] Retry/Backoff and DLQ
- [ ] Write unit tests
- [ ] Roles and permissions
- [ ] Payload validation
- [ ] Data persistence (json, db, etc.)
- [ ] Metrics and tracing system
- [ ] Support for object-oriented listeners