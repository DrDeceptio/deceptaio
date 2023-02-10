"""Asynchronous variables/classes.

Awaitable variables allow you to await on them until a specific value or state
is set.
"""

import asyncio
from typing import Any


__all__ = ['AwaitableVar', 'Flag']


class AwaitableVar:
    """A variable that can be awaited until a specific value is reached.

    Attributes:
        _value: The underlying value.
        value_change_events: Set of asyncio Events to notify on value change.
    """

    _value: Any
    value_change_events: set[asyncio.Event]

    def __init__(self, initial: Any = None) -> None:
        """Initializes an AwaitableVar instance.

        Arguments:
            initial: The initial value.
        """

        self._value = initial
        self.value_change_events = set()

    @property
    def value(self) -> Any:
        return self._value

    @value.setter
    def value(self, new_value: Any) -> None:
        self._value = new_value

        # Signal a change has occurred
        for event in self.value_change_events:
            event.set()

    async def wait_for(self, value: Any) -> None:
        """Waits until a specific value is set (with no CPU spikes).

        Arguments:
            value: The value to wait for.
        """

        if self.value == value:  # Already there
            return

        # Set up an event to be notified on value change
        event = asyncio.Event()
        self.value_change_events.add(event)

        # Wait until we get notified of the value we're waiting for
        while self.value != value:
            await event.wait()
            event.clear()

        # At this point we're at our desired value, so remove our event from
        # the set of change event notifications
        self.value_change_events.remove(event)

    def __bool__(self) -> bool:
        return bool(self.value)


class Flag(AwaitableVar):
    """A flag that can be awaited unti it is set or cleared."""

    def __init__(self, initial: bool = False) -> None:
        """Initializes a Flag instance.

        Arguments:
            initial: The initial boolean value (default: False).
        """

        super().__init__(initial)

    def set(self) -> None:
        """Sets the flag (to True)."""

        self.value = True

    def clear(self) -> None:
        """Clears the flag (to False)."""

        self.value = False

    def is_set(self) -> bool:
        """Returns True if the flag is set."""

        return bool(self.value)

    def is_clear(self) -> bool:
        """Returns True if the flag is clear (not True)."""

        return bool(self.value) is not True

    async def wait(self) -> None:
        """Waits until the flag is set (to True)."""

        await self.wait_for(True)

    async def wait_clear(self) -> None:
        """Waits until the flag is cleared (set to False)."""

        await self.wait_for(False)
