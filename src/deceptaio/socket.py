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
