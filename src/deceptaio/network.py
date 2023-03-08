"""Asynchronous IPC support."""


import asyncio

from .vars import Flag


__all__ = ['Socket']


class Socket(asyncio.Protocol):
    """Asynchronous socket-like object.

    Attributes:
        connected: Set when conneced, cleared when not.
        writing_paused: True when the transport's buffer goes over the high
            water mark, False when equal to or less than the high water mark.
        flush_awaiters: List of asyncio Futures awaiting on the flush() method.
        buffer: Buffer of bytes received.
        buffer_awaiter: Future used to await on the buffer_wait() method.
        transport: Underlying asyncio Transport instance.
        loop: Currently running event loop.
    """

    connected: Flag
    writing_paused: bool
    flush_awaiters: list[asyncio.Future]
    buffer: bytearray
    buffer_awaiter: asyncio.Future | None
    transport: asyncio.Transport | None
    loop: asyncio.AbstractEventLoop

    def __init__(self) -> None:
        self.connected = Flag(False)
        self.writing_paused = True
        self.flush_awaiters = list()
        self.buffer = bytearray()
        self.buffer_awaiter = None
        self.transport = None
        self.loop = asyncio.get_event_loop()

    def connection_made(self, transport: asyncio.Transport) -> None:
        self.transport = transport
        self.connected.set()
        self.writing_paused = False

    def connection_lost(self, exc: Exception | None) -> None:
        self.connected.clear()
        self.writing_paused = True

    def data_received(self, data: bytes) -> None:
        self.buffer.extend(data)

        # Check if we need to wake up the awaiter
        buffer_awaiter = self.buffer_awaiter
        if buffer_awaiter is not None:
            self.buffer_awaiter = None
            if not buffer_awaiter.cancelled():
                buffer_awaiter.set_result(None)

    def pause_writing(self) -> None:
        self.writing_paused = True

    def resume_writing(self) -> None:
        self.writing_paused = False

        # Wake up coroutines awaiting on flush()
        for awaiter in self.flush_awaiters:
            if not awaiter.done():
                awaiter.set_result(None)

    def getpeername(self) -> tuple[str, int]:
        """Gets the remote peer information.

        Returns:
            A tuple of (host, port) for the remote system.
        """

        if not self.connected:
            return tuple()

        return self.transport.get_extra_info('peername')

    async def connect(self, host: str, port: int, **kwargs) -> None:
        """Connects the socket to a remote system.

        Arguments:
            host: The host to connect to.
            port: The port to conenct to.

        Notes:
            Additional keyword arguments are passed to the event loop's
            create_connection() method.
        """

        await self.loop.create_connection(lambda: self, host, port, **kwargs)

    async def close(self) -> None:
        """Closes the socket."""

        self.writing_paused = True
        self.connected.clear()
        self.transport.close()
        await asyncio.sleep(0)  # Give the transport a chance to close

    async def send(self, data: bytes) -> None:
        """Sends data over the socket.

        Arguments:
            data: The data to send.
        """

        self.transport.write(data)
        await self.flush()

    async def flush(self) -> None:
        """Waits for the output buffer to be flushed (become avaiable)."""

        if not self.writing_paused:
            return

        # Create a future and wait for it to be set
        awaiter = self.loop.create_future()
        self.flush_awaiters.append(awaiter)

        try:
            await awaiter
        finally:
            self.flush_awaiters.remove(awaiter)

    async def recv(self, size: int) -> bytes:
        """Receives (at most size) bytes.

        Arguments:
            size: The maximum number of bytes to receive.

        Returns:
            The received bytes.

        Raises:
            ValueError: If size is < 0.
        """

        if size < 0:
            raise ValueError(f'Invalid size {size}, must be >= 0')
        elif size == 0:
            return b''

        if not self.buffer:  # Wait for the buffer to have some data.
            await self._wait_for_buffer('recv')

        data = bytes(self.buffer[:size])
        del self.buffer[:size]

        return data

    async def recv_exact(self, size: int) -> bytes:
        """Receives (exactly size) bytes.

        Arguments:
            size: The number of bytes to receive.

        Returns:
            The requested data.

        Raises:
            ValueError: If size is < 0.
        """

        if size < 0:
            raise ValueError(f'Invalid size {size}, must be >= 0')
        elif size == 0:
            return b''

        # Wait until we have enough data in the buffer
        while len(self.buffer) < size:
            await self._wait_for_buffer('recv_exactly')

        if len(self.buffer) == size:
            data = bytes(self.buffer)
            self.buffer.clear()
        else:
            data = bytes(self.buffer[:size])
            del self.buffer[:size]

        return data

    async def _wait_for_buffer(self, caller_name: str) -> None:
        """Used internally to wait until the buffer has data.

        Arguments:
            caller_name: The name of the function awaiting this method.

        Raises:
            RuntimeError: If another function is already awaiting this one.
        """

        if self.buffer_awaiter is not None:
            raise RuntimeError(
                f'{caller_name}() called when another coroutine is awaiting '
                f'the buffer'
            )

        self.buffer_awaiter = self.loop.create_future()
        try:
            await self.buffer_awaiter
        finally:
            self.buffer_awaiter = None


class Protocol(asyncio.Protocol):
    """Base class for creating asyncio Protocols.

    Notes:
        This is similar to the Socket() class, but meant to be inherited.

    Attributes:
        _connected: Set when _connected, Cleared when not.
        _loop: Currently running event loop.
        _transport: Underlying asyncio Transport.
        _exc: Set to the exception information, if provided, when a connection
            is lost.
        _flush_awaiters: List of asyncio Futures awaiting the flush() method.
        _writing_paused: True when the transport buffer goes over the high
            water mark.
        _buffer: Internal buffer of received data.
        _buffer_awaiter: Future used to await the _buffer_wait() method.
    """

    __slots__ = (
        'connected', '_loop', '_transport', '_exc', '_flush_awaiters',
        '_writing_paused', '_buffer', '_buffer_awaiter'
    )

    _connected: Flag
    _loop: asyncio.AbstractEventLoop
    _transport: asyncio.Transport | None
    _exc: Exception | None
    _flush_awaiters: list[asyncio.Future]
    _writing_paused: bool
    _buffer: bytearray
    _buffer_awaiter: asyncio.Future | None

    def __init__(self) -> None:
        self._connected = Flag(False)
        self._loop = asyncio.get_running_loop()
        self._transport = None
        self._exc = None
        self._flush_awaiters = list()
        self._writing_paused = True
        self._buffer = bytearray()
        self._buffer_awaiter = None

    def connection_made(self, transport: asyncio.Transport) -> None:
        self._transport = transport
        self._writing_paused = False
        self._connected.set()

    def connection_lost(self, exc: Exception | None) -> None:
        self._writing_paused = True
        self._exc = exc
        self._connected.clear()

    def data_received(self, data: bytes) -> None:
        self._buffer.extend(data)

        # Check to see if we need to wake an awaiting coroutine
        buffer_awaiter = self._buffer_awaiter
        if buffer_awaiter is not None:
            self._buffer_awaiter = None
            if not buffer_awaiter.cancelled():
                buffer_awaiter.set_result(None)

    def pause_writing(self) -> None:
        self._writing_paused = True

    def resume_writing(self) -> None:
        self._writing_paused = False

        # Wake any coroutines awaiting the flush() method.
        for awaiter in self._flush_awaiters:
            if not awaiter.done():
                awaiter.set_result(None)

    async def close(self) -> None:
        """Closes the network connection."""

        self._writing_paused = True
        self._connected.clear()
        self._transport.close()
        await asyncio.sleep(0)  # Give the transport a chance to close

    async def write(self, data: bytes) -> None:
        """Writes data via the transport.

        Arguments:
            data: The data to write.
        """

        self._transport.write(data)
        await self.flush()

    async def flush(self) -> None:
        """Method awaited to ensure data is flushed."""

        if not self._writing_paused:
            return

        awaiter = self._loop.create_future()
        self._flush_awaiters.append(awaiter)

        try:
            await awaiter
        finally:
            self._flush_awaiters.remove(awaiter)

    async def read(self, size: int) -> bytes:
        """Reads (at most size) bytes.

        Arguments:
            size: The maximum number of bytes to receive.

        Returns:
            The received bytes.

        Raises:
            ValueError: If size is < 0.
        """

        if size < 0:
            raise ValueError(f'Invalid size {size}, must be >= 0')
        elif size == 0:
            return b''

        if not self._buffer:
            await self._wait_for_buffer('read')

        data = bytes(self._buffer[:size])
        del self._buffer[:size]

        return data

    async def read_exact(self, size: int) -> bytes:
        """Reads (exactly) size bytes.

        Arguments:
            size: The number of bytes to receive.

        Returns:
            The requested data.

        Raises:
            ValueError: If size is < 0.
        """

        if size < 0:
            raise ValueError(f'Invalid size {size}, must be >= 0')
        elif size == 0:
            return b''

        while len(self._buffer) < size:
            await self._wait_for_buffer('read_exact')

        if len(self._buffer) == size:
            data = bytes(self._buffer)
            self._buffer.clear()
        else:
            data = bytes(self._buffer[:size])
            del self._buffer[:size]

        return data

    async def _wait_for_buffer(self, caller_name: str) -> None:
        """Used internally to wait until the buffer has data.

        Arguments:
            caller_name: The name of the function awaiting this method.

        Raises:
            RuntimeError: If another function is already awaiting this one.
        """

        if self._buffer_awaiter is not None:
            raise RuntimeError(
                f'{caller_name}() called when another coroutine is awaiting '
                f'the buffer'
            )

        self._buffer_awaiter = self._loop.create_future()
        try:
            await self._buffer_awaiter
        finally:
            self._buffer_awaiter = None
