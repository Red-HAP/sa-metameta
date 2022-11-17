"""Exceptions for the metameta module."""
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


class MetaMetaError(Exception):
    """Base MetaMeta exception."""

    pass


class MetaMetaNotFoundError(MetaMetaError):
    """Base MetaMeta item, attribute not found exception."""

    _item_type = "item"

    def __init__(self, *args):
        """
        Instantiate a Not-Found exception.

        Requires 1 argument which is the key or attribute that was not found.
        """
        if len(args) < 1:
            raise MetaMetaError("{0} exceptions requires a key argument".format(self.__class__.__name__))

        new_args = tuple(
            m
            for m in (
                "No {0} named '{1}' was found.".format(self._item_type, args[0]),
                " ".join(args[1:]),
            )
            if m
        )
        super().__init__(*new_args)


class MetaMetaEngineNotFoundError(MetaMetaNotFoundError):
    """Base MetaMetaEngine item, attribute not found exception."""

    _item_type = "engine"


class MetaMetaSchemaNotFoundError(MetaMetaNotFoundError):
    """Base MetaMetaSchema item, attribute not found exception."""

    _item_type = "schema"


class MetaMetaTableNotFoundError(MetaMetaNotFoundError):
    """Base MetaMetaTable item, attribute not found exception."""

    _item_type = "table"


__all__ = (
    "MetaMetaError",
    "MetaMetaNotFoundError",
    "MetaMetaEngineNotFoundError",
    "MetaMetaSchemaNotFoundError",
    "MetaMetaTableNotFoundError",
)
