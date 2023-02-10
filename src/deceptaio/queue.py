"""Asyncio Queue for typehinting."""


import asyncio
import types


__all__ = ['AsyncioQueue']


class AsyncioQueue(asyncio.Queue):
    """Asynchronous queue that can be used for type-hinting contained items."""

    __class_getitem__ = classmethod(types.GenericAlias)
