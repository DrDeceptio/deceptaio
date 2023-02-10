"""Asynchronous internal messaging."""


from __future__ import annotations
import abc
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import itertools
from typing import Any, Awaitable, Callable, ClassVar, Iterator, Self, TypeVar

from .jobs import Scheduler
from .vars import Flag


__all__ = [
    'Msg', 'CmdMsgBody', 'DataMsgBody', 'EventMsgBody', 'MsgQueue',
    'MsgQueueType', 'Producer', 'Consumer', 'MsgBus', 'ControlBus',
    'ComponentBus', 'MsgService'
]


class MsgBody:
    """Base class for the body of a message."""

    pass


MsgBodyType = TypeVar('MsgBodyType', bound=MsgBody)


class Msg:
    """Messages sent between components.

    Attributes:
        sender: The sender of the message.
        body: The body of the message.
        msg_id: Auto-generated unique message identifier.
        corr_id: Optional correlation identifier.
        timestamp: When the message was sent.
    """

    sender: str
    body: MsgBodyType
    msg_id: int
    corr_id: int | None
    timestamp: datetime | None

    msg_id_counter: ClassVar[Iterator] = itertools.count()

    def __init__(
            self,
            sender: str,
            body: MsgBodyType,
            corr_id: int | None = None
    ) -> None:
        """Initializes a Msg instance.

        Arguments:
            sender: The sender of the message.
            body: The body of the message.
            corr_id: Optional correlation identifier.
        """

        self.sender = sender
        self.body = body
        self.msg_id = next(self.msg_id_counter)
        self.corr_id = corr_id
        self.timestamp = None

    def __str__(self) -> str:
        return (
            f'{self.__class__.__name__}('
            f'sender=\'{self.sender}\', '
            f'body={self.body}, '
            f'msg_id={self.msg_id}, '
            f'corr_id={self.corr_id}, '
            f'timestamp={self.timestamp}'
            f')'
        )

    def __repr__(self) -> str:
        return self.__str__()


@dataclass
class CmdMsgBody(MsgBody):
    """Message body for command messages.

    Attributes:
        cmd: The command.
        args: A list of positional arguments.
        kwargs: A dictionary of keyword arguments.
    """

    cmd: str
    args: tuple = field(default_factory=lambda: tuple())
    kwargs: dict[str, Any] = field(default_factory=lambda: dict())


@dataclass
class DataMsgBody(MsgBody):
    """Message body for data messages.

    Attributes:
        data: The data.
    """

    data: Any


@dataclass
class EventMsgBody(MsgBody):
    """Message body for event messages.

    Attributes:
        event: The event that occurred.
        data: Optional event data.
    """

    event: str
    data: Any = None


@dataclass
class MsgQueue:
    """Represents a message queue.

    Attributes:
        name: The name (identifier) of the channel.
        msgs: Asyncio queue of messages.
        producers: List of Producers
        consumers: List of Consumers
    """

    name: str
    msgs: asyncio.Queue = field(default_factory=lambda: asyncio.Queue())
    producers: list[Producer] = field(default_factory=lambda: list())
    consumers: list[Consumer] = field(default_factory=lambda: list())

    def __str__(self) -> str:
        return f'{self.__class__.__name__}(name=\'{self.name}\')'

    def __repr__(self) -> str:
        return self.__str__()

    def __hash__(self) -> int:
        return hash(self.name)

    def register_producer(self, producer: Producer) -> None:
        """Registers a producer with a MsgQueue instance.

        Notes:
            This method is idempotent. That is, registering a producer that is
            already registered essentially does nothing.

        Arguments:
            producer: The producer to register.
        """

        self.producers.append(producer)

    def deregister_producer(self, producer: Producer) -> None:
        """Deregisters a producer from the MsgQueue instance.

        Notes:
            This method is idempotent. That is, deregistering a producer that
            is no longer (or never was) registered essentially does nothing.

        Arguments:
            producer: The producer to deregister.
        """

        if producer not in self.producers:
            return

        self.producers.remove(producer)

    def register_consumer(self, consumer: Consumer) -> None:
        """Registers a consumer with a MsgQueue instance.

        Notes:
            This method is idempotent. That is, registering a consumer that is
            already registered essentially does nothing. This is to avoid
            double-delivery of the same message.

        Arguments:
            consumer: The consumer to register.
        """

        self.consumers.append(consumer)

    def deregister_consumer(self, consumer: Consumer) -> None:
        """Deregisters a consumer from the MsgQueue instance.

        Notes:
            This method is idempotent. That is, deregistering a consumer that
            is no longer (or never was) registered essentially does nothing.

        Arguments:
            consumer: The consumer to deregister.
        """

        if consumer not in self.consumers:
            return

        self.consumers.remove(consumer)


MsgQueueType = TypeVar('MsgQueueType', bound=MsgQueue)


class ExclusiveMsgQueue(MsgQueue):
    """A point-to-point MsgQueue.

    ExclusiveMsgQueue instances can only have (at most) one producer, and one
    consumer registered at a time.

    Attempting to register more than one producer or consumer will raise a
    RuntimeError.
    """

    def register_producer(self, producer: Producer) -> None:
        if self.producers and (producer not in self.producers):
            raise RuntimeError('Another producer is already registered')

        super().register_producer(producer)

    def register_consumer(self, consumer: Consumer) -> None:
        if self.consumers and (consumer not in self.consumers):
            raise RuntimeError('Another consumer is already registered')

        super().register_consumer(consumer)


class MsgQueueParticipant(abc.ABC):
    """Base class for the Producer/Consumer classes.

    Attributes:
        queue: The MsgQueue instance to participate with.
        registered: True if currently registered with a queue, False if not.
    """

    queue: MsgQueue
    registered: bool

    def __init__(self, queue: MsgQueueType) -> None:
        """Initializes a MsgQueueParticipant instance.

        Arguments:
            queue: The MsgQueue instance to participate with.
        """

        self.queue = queue
        self.registered = False

    @abc.abstractmethod
    def register(self) -> None:
        """Registers the participant with the MsgQueue."""

        raise NotImplementedError

    @abc.abstractmethod
    def deregister(self) -> None:
        """Deregisters the participant with the MsgQueue."""

        raise NotImplementedError

    def __enter__(self) -> Self:
        self.register()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.deregister()


class Producer(MsgQueueParticipant):
    """For publishing on a queue."""

    def register(self) -> None:
        self.queue.register_producer(self)
        self.registered = True

    def deregister(self) -> None:
        self.queue.deregister_producer(self)
        self.registered = False

    def publish(self, msg: Msg) -> None:
        """Publishes a message on the queue.

        Arguments:
            msg: The message to publish.

        Raises:
            RuntimeError: If not currently registered.
        """

        if not self.registered:
            raise RuntimeError(
                f'Producer is not registered with {self.queue.name}'
            )

        msg.timestamp = datetime.now(timezone.utc)
        self.queue.msgs.put_nowait(msg)

    def invoke(self, sender: str, cmd: str, *args, **kwargs) -> None:
        """Sends a command message.

        Arguments:
            sender: The sender of the message.
            cmd: The command to send.
            args: Positional arguments for the command.
            kwargs: Keyword arguments for the command.
        """

        body = CmdMsgBody(cmd=cmd, args=args, kwargs=kwargs)
        msg = Msg(sender=sender, body=body)
        self.publish(msg)

    def feed(self, sender: str, data: Any, corr_id: int | None = None) -> None:
        """Sends a data message.

        Arguments:
            sender: The sender of the message.
            data: The data.
            corr_id: Optional correlation id.
        """

        body = DataMsgBody(data=data)
        msg = Msg(sender=sender, body=body, corr_id=corr_id)
        self.publish(msg)

    def notify(
            self,
            sender: str,
            event: str,
            data: Any = None,
            corr_id: int | None = None
    ) -> None:
        """Sends an event message.

        Arguments:
            sender: The sender of the message.
            event: The event that occurred.
            data: Optional event data.
            corr_id: Optional correlation id.
        """

        body = EventMsgBody(event=event, data=data)
        msg = Msg(sender=sender, body=body, corr_id=corr_id)
        self.publish(msg)


class Consumer(MsgQueueParticipant):
    """For consuming messages from a MsgQueue.

    Attributes:
        msgs: asyncio Queue of incoming messages for consumption.
    """

    msgs: asyncio.Queue

    def __init__(self, queue: MsgQueueType) -> None:
        super().__init__(queue)
        self.msgs = asyncio.Queue()

    def register(self) -> None:
        self.queue.register_consumer(self)
        self.registered = True

    def deregister(self) -> None:
        self.queue.deregister_consumer(self)
        self.registered = False

    async def consume(self) -> Msg:
        """Consumes a message.

        Returns:
            The message to be consumed.

        Raises:
            RuntimeError: If not registered on the queue.
        """

        if not self.registered:
            raise RuntimeError(
                f'Consumer is not registered with {self.queue.name}'
            )

        message = await self.msgs.get()
        return message


WiretapHandler = Callable[[str, Msg], Awaitable[None]]


class MsgBus:
    """A message communication bus.

    Attributes:
        scheduler: Scheduler used to dispatch messages.
        queues: Mapping of queue names to MsgQueue instances.
        run_dispatcher: True if the dispatcher should run, False if not.
        wiretaps: Mapping of queue names to list of wiretap consumers.
    """

    scheduler: Scheduler
    queues: dict[str, MsgQueueType]
    run_dispatcher: Flag
    wiretaps: dict[str, list[WiretapHandler]]

    def __init__(self, scheduler: Scheduler) -> None:
        """Initializes a message bus instance.

        Arguments:
            scheduler: The scheduler used to dispatch messages.
        """

        self.scheduler = scheduler
        self.queues = dict()
        self.run_dispatcher = Flag(False)
        self.wiretaps = dict()

    def register_queue(self, queue: MsgQueue) -> None:
        """Registers a queue on the MsgBus.

        Arguments:
            queue: The queue to register.
        """

        self.queues[queue.name] = queue
        self.run_dispatcher.set()

    def add_queue(self, name: str) -> MsgQueue:
        """Adds a MsgQueue to the bus.

        Notes:
            This method is idempotent in that calling it multiple times with
            the same queue name always yields the same MsgQueue instance.

        Arguments:
            name: The name of the queue.

        Returns:
            The MsgQueue instance.
        """

        if name in self.queues:
            return self.queues[name]

        queue = MsgQueue(name=name)
        self.register_queue(queue)
        return queue

    def add_exclusive_queue(self, name: str) -> ExclusiveMsgQueue:
        """Adds an ExclusiveMsgQueue to the bus.

        Notes:
            This method is idempotent in that calling it multiple times with
            the same queue name always yields the same MsgQueue instance.

        Arguments:
            name: The name of the queue.

        Returns:
            The ExclusiveMsgQueue instance.
        """

        if name in self.queues:
            return self.queues[name]

        queue = ExclusiveMsgQueue(name=name)
        self.register_queue(queue)
        return queue

    def has_queue(self, queue: MsgQueueType | str) -> bool:
        """Determines if a bus has a queue.

        Arguments:
            queue: An instance of a MsgQueue or the name of a queue.

        Returns:
            True if the queue exists on the bus, False otherwise.
        """

        if isinstance(queue, MsgQueue):
            queue_name = queue.name
        else:
            queue_name = queue

        return queue_name in self.queues

    def get_queue(self, name: str) -> MsgQueueType:
        """Gets a queue from the bus.

        Attributes:
            name: The queue name.

        Returns:
            The MsgQueue instance.

        Raises:
            ValueError: If there is no registered MsgQueue with name.
        """

        if name not in self.queues:
            raise ValueError(f'Unknown MsgQueue {name}')

        return self.queues[name]

    def install_wiretap(
            self,
            queue_name: str,
            handler: WiretapHandler
    ) -> None:
        """Installs a wiretap for a queue.

        Arguments:
            queue_name: The name of the queue to install the wiretap for.
            handler: The handler to received tapped messages.

        Raises:
            ValueError: If queue_name is not registered.
        """

        if queue_name not in self.queues:
            raise ValueError(f'Unknown Queue {queue_name}')

        elif queue_name not in self.wiretaps:
            self.wiretaps[queue_name] = list()

        wiretappers = self.wiretaps[queue_name]

        if handler in wiretappers:  # Skip it if already wiretapping
            return

        self.wiretaps[queue_name].append(handler)

    def remove_wiretap(self, queue_name: str, handler: WiretapHandler) -> None:
        """Removes a handler from wiretapping.

        Arguments:
            queue_name: The name of the queue to stop wiretapping.
            handler: The handler to stop sending wiretapped messages to.
        """

        if queue_name not in self.queues:  # Skip if the queue isn't registered
            return

        elif queue_name not in self.wiretaps:  # No wiretaps
            return

        elif handler not in self.wiretaps[queue_name]:   # Skip if non-existent
            return

        self.wiretaps[queue_name].remove(handler)


class ComponentBus(MsgBus):
    """Message bus for inter-component messaging.

    Attributes:
        dispatcher_tasks: list of Tasks for dispatching message queues.
        redirects: Mapping of queue-to-new-queue redirects.
    """

    dispatcher_tasks: list[asyncio.Task]
    redirects: dict[MsgQueueType, MsgQueueType]

    def __init__(self, scheduler: Scheduler) -> None:
        super().__init__(scheduler)
        self.dispatcher_tasks = list()
        self.redirects = dict()

    def install_redirect(
            self,
            source: MsgQueueType,
            dest: MsgQueueType
    ) -> None:
        """Installs a queue redirect.

        Arguments:
            source: The queue to redirect published messages from.
            dest: The queue to redirect published messages to.
        """

        self.redirects[source] = dest

    def remove_redirect(self, source: MsgQueueType) -> None:
        """Removes a queue redirect.

        Arguments:
            source: The queue to stop redirecting from.
        """

        if source not in self.redirects:
            return

        del self.redirects[source]

    async def dispatcher(self, queue: MsgQueue) -> None:
        """Dispatches queue messages.

        Arguments:
            queue: The MsgQueue instance to dispatch messages for.
        """

        # Wait until we should start
        await self.run_dispatcher.wait()

        while True:
            message = await queue.msgs.get()

            # Handle wiretaps
            if queue.name in self.wiretaps:
                for handler in self.wiretaps[queue.name]:
                    await handler(queue.name, message)

            # Handle redirects
            if queue in self.redirects:
                self.redirects[queue].msgs.put_nowait(message)
                await asyncio.sleep(0)
                continue

            for consumer in queue.consumers:
                consumer.msgs.put_nowait(message)
                await asyncio.sleep(0)

    def register_queue(self, queue: MsgQueue) -> None:
        super().register_queue(queue)

        task = self.scheduler.spawn(self.dispatcher(queue))
        self.dispatcher_tasks.append(task)


class ControlBus(MsgBus):
    """Message bus for control messages.

    Attributes:
        dispatcher_task: asyncio Task for the dispatcher method.
    """

    dispatcher_task: asyncio.Task | None

    def __init__(self, scheduler: Scheduler) -> None:
        super().__init__(scheduler)
        self.dispatcher_task = scheduler.spawn(self.dispatch())

    async def dispatch(self) -> None:
        """Dispatches control messages."""

        # Wait for until we're told to run
        await self.run_dispatcher.wait()

        while True:
            for queue in self.queues.values():
                if queue.msgs.empty():
                    continue

                message = queue.msgs.get_nowait()
                for consumer in queue.consumers:
                    consumer.msg_in.put_nowait(message)

            await asyncio.sleep(0)


class MsgService:
    """Provides the messaging service.

    Attributes:
        component_bus: The main MsgBus for inter-component messaging.
        control_bus: The MsgBus for control operations.
        scheduler: Scheduler for scheduling jobs.
    """

    component_bus: ComponentBus
    control_bus: ControlBus
    scheduler: Scheduler

    def __init__(self, scheduler: Scheduler) -> None:
        """Initializes a MsgService instance.

        Arguments:
            scheduler: The scheduler to use for scheduling jobs.
        """

        self.scheduler = scheduler
        self.component_bus = ComponentBus(scheduler)
        self.control_bus = ControlBus(scheduler)

    def add_queue(self, name: str) -> MsgQueue:
        """Adds a MsgQueue to the appropriate bus.

        Notes:
            This method is idempotent in that calling it multiple times with
            the same queue name always yields the same MsgQueue instance.

        Arguments:
            name: The name of the queue.

        Returns:
            The MsgQueue instance.
        """

        if name.startswith('control'):
            queue = self.control_bus.add_queue(name)
        else:
            queue = self.component_bus.add_queue(name)

        return queue

    def add_exclusive_queue(self, name: str) -> ExclusiveMsgQueue:
        """Adds an ExclusiveMsgQueue to the appropriate bus.

        Notes:
            This method is idempotent in that calling it multiple times with
            the same queue name always yields the same MsgQueue instance.

        Arguments:
            name: The name of the queue.

        Returns:
            The ExclusiveMsgQueue instance.
        """

        if name.startswith('control'):
            queue = self.control_bus.add_exclusive_queue(name)
        else:
            queue = self.component_bus.add_exclusive_queue(name)

        return queue

    def producer(self, queue: MsgQueue | str) -> Producer:
        """Creates a new Producer instance.

        Notes:
            This does *not* register the producer instance.

        Arguments:
            queue: Either a MsgQueue instance, or the name of one. The queue
                must be registered with a bus.

        Returns:
            A newly-minted Producer instance.

        Raises:
            ValueError: If queue is not registered.
        """

        if isinstance(queue, str):
            queue_name = queue
        else:
            queue_name = queue.name

        if queue_name.startswith('control'):
            bus = self.control_bus
        else:
            bus = self.component_bus

        queue = bus.get_queue(queue_name)
        producer = Producer(queue)
        return producer

    def consumer(self, queue: MsgQueue | str) -> Consumer:
        """Creates a new Consumer instance.

        Notes:
            This does *not* register the consumer instance.

        Arguments:
            queue: Either a MsgQueue instance, or the name of one. The queue
                must be registered with a bus.

        Returns:
            A newly-minted Consumer instance.

        Raises:
            ValueError: If queue is not registered.
        """

        if isinstance(queue, str):
            queue_name = queue
        else:
            queue_name = queue.name

        if queue_name.startswith('control'):
            bus = self.control_bus
        else:
            bus = self.component_bus

        queue = bus.get_queue(queue_name)
        consumer = Consumer(queue)
        return consumer

    def install_wiretap(
            self,
            queue_name: str,
            handler: WiretapHandler
    ) -> None:
        """Installs a wiretap for a queue.

        Arguments:
            queue_name: The name of the queue to install the wiretap for.
            handler: The handler to received tapped messages.

        Raises:
            ValueError: If queue_name is not registered.
        """

        if queue_name.startswith('control'):
            bus = self.control_bus
        else:
            bus = self.component_bus

        bus.install_wiretap(queue_name, handler)

    def remove_wiretap(self, queue_name: str, handler: WiretapHandler) -> None:
        """Removes a handler from wiretapping.

        Arguments:
            queue_name: The name of the queue to stop wiretapping.
            handler: The handler to stop sending wiretapped messages to.
        """

        if queue_name.startswith('control'):
            bus = self.control_bus
        else:
            bus = self.component_bus

        bus.remove_wiretap(queue_name, handler)
