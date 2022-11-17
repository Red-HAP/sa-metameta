"""metameta base class definition."""
#    Copyright 2022 Red Hat, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

from __future__ import annotations

from typing import List

from .exceptions import MetaMetaNotFoundError


class MetaMetaBase:
    """Base class for metameta module."""

    def __init__(self):
        """Initializes base attributes."""
        self._items = {}
        self.name = "MetaMetaBase"
        self._notfound_exc = MetaMetaNotFoundError

    def __iter__(self):
        """Return iterator for class items."""
        return iter(self._items)

    def __getitem__(self, key: str):
        """Check for key in items else raise MetaMetaNotFoundError exeption."""
        if key not in self._items:
            raise self._notfound_exc(key)
        return self._items[key]

    def __getattr__(self, attr: str):
        """
        Check for attr in items, else fallback to getattr.

        If attr not found, raise MetaMetaNotFoundError exception.
        """
        if attr in self._items:
            return self._items[attr]
        else:
            try:
                super().__getattr__(attr)
            except AttributeError:
                raise self._notfound_exc(attr)

    def __contains__(self, item_name: str) -> bool:
        return item_name in self._items

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def __str__(self) -> str:
        return f"{self.__repr__()}.({(s for s in self.list_item_keys())})"

    @property
    def child_class(self) -> MetaMetaBase:
        NotImplementedError("The child class property must be defined")

    def keys(self):
        for key in self._items:
            yield key

    def items(self):
        for elem in self._items.items():
            yield elem

    def values(self):
        for val in self._items.values():
            yield val

    def list_item_keys(self) -> List[str]:
        return sorted(self._items)


__all__ = "MetaMetaBase"
