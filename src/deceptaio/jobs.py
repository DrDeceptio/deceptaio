"""Simple job scheduling"""


from __future__ import annotations
import asyncio


__all__ = ['Scheduler']


class Scheduler:
    """Handles running jobs.

    Attributes:
        loop: Event loop for scheduling.
        tasks: Set of scheduled tasks.
    """

    loop: asyncio.AbstractEventLoop
    tasks: set

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Initializes a Scheduler instance.

        Arguments:
            loop: Optional event loop to use for scheduling. If not specified
                the current event loop is used.
        """

        if loop is None:
            loop = asyncio.get_event_loop()

        self.loop = loop
        self.tasks = set()

    def spawn(self, coro, *, name=None) -> asyncio.Task:
        """Schedules a coroutine for running.

        Arguments:
            coro: The coroutine to schedule.
            name: Optional task name.

        Returns:
            An asyncio Task for the scheduled job.
        """

        task = self.loop.create_task(coro=coro, name=name)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

        return task
