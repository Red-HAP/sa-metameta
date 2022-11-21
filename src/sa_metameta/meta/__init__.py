"""metameta module classes designed to work with psycopg2."""
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

from typing import List, Optional, Tuple

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from .. import MetaMetaBase
from ..exceptions import (
    MetaMetaEngineNotFoundError,
    MetaMetaError,
    MetaMetaSchemaNotFoundError,
    MetaMetaTableNotFoundError,
)


class MetaMeta(MetaMetaBase):
    """
    MetaMeta is a class for tracking meta engines.

    The meta engines that this class tracks will be a MetaEngine type.
    Engines can be accessed by attribute or subscript notation.
    """

    def __init__(self):
        super().__init__()
        self._notfound_exc = MetaMetaEngineNotFoundError

    @property
    def engines(self):
        return self._items

    @property
    def child_class(self) -> MetaEngine:
        return MetaEngine

    @property
    def inverse_child_class(self) -> MetaEngine:  # AMetaEngine
        # Circumvent circular dependency error
        from ..asyncmeta import AMetaEngine

        return AMetaEngine

    def register_engine(self, engine: sa.engine.Engine, *, engine_name: Optional[str] = None) -> None:
        """
        Registers a SQLAlchemy engine class with a MetaMeta object.

        The resulting engine can be referenced via a dictionary reference:
            MetaMeta[<engine_name>]
        or via an attribute referencde:
            MetaMeta.<engine_name>
        """
        class_is_metameta = self.__class__.__name__ == "MetaMeta"
        if (isinstance(engine, AsyncEngine) and class_is_metameta) or (
            isinstance(engine, sa.engine.Engine) and not class_is_metameta
        ):
            _child_class = self.inverse_child_claass
        else:
            _child_class = self.child_class

        meta_engine = _child_class(engine, self, engine_name=engine_name)
        self._items[meta_engine.name] = meta_engine


class MetaEngine(MetaMetaBase):
    """
    AMetaEngine is a class for tracking meta schemata.

    The meta schema classes that this class tracks will be a AMetaSchema type.
    The AMetaSchema class is designed to work with SQLAlchemy's AsyncEngine
    engine class.
    """

    def __init__(
        self,
        engine: sa.engine.Engine,
        metameta: MetaMeta,
        *,
        engine_name: Optional[str] = None,
    ):
        super().__init__()
        self.name = self.resolve_engine_name(engine_name, engine)
        self._metameta = metameta
        self._engine = engine
        self.sessionmaker = sa.orm.sessionmaker
        self._session_class = self.sessionmaker(self._engine)
        self.ns_excl_pref_regexs = {"expr_1": "^pg_", "expr_2": "^information_schema"}
        self._notfound_exc = MetaMetaSchemaNotFoundError

    @property
    def child_class(self) -> MetaSchema:
        return MetaSchema

    @property
    def metameta(self) -> MetaMeta:
        return self._metameta

    @property
    def engine(self) -> sa.engine.Engine:
        return self._engine

    @property
    def schemata(self):
        return self._items

    def resolve_engine_name(self, engine_name: Optional[str], engine: sa.engine.Engine) -> str:
        """
        Resolves engine name if falsey.

        Gets the engine name from the engine object if the engine_name
        parameter is empty or None.
        """
        if not engine_name:
            if not engine.url.database:
                raise MetaMetaError("Cannot detect engine name from connection. Specify engine name in arguments.")
            return engine.url.database
        else:
            return engine_name

    def register_sessionmaker(self, sessionmaker) -> None:
        """
        Register a sqlalchemy sessionmaker with the MetaEngine.

        This will also use the sessionmaker to create a session class for use with the `session` method.
        """
        self.sessionmaker = sessionmaker
        self._session_class = self.sessionmaker(self.engine)

    def session(self, *args, **kwargs):
        """
        Create a session instance for use with query execution.

        If no sessionmaker was registered, the sqlalchemy.orm.sessionmaker will be used by default.
        All arguments to this method are passed to the session class.
        """
        return self._session_class(*args, **kwargs)

    def register_schema(self, schema_name: str) -> None:
        """
        Registers a schema that exists within a db engine..

        The resulting schema can be referenced via a dictionary reference:
            MetaEngine[<schema_name>]
        or via an attribute referencde:
            MetaEngine.<schema_name>
        """
        schema = self.child_class(schema_name, self)
        self._items[schema_name] = schema

    def _build_discover_engine_query(self) -> Tuple(str, dict):
        """
        Builds a text query to list shemata in an engine.

        The query will filter out schemata that match any of the
        values in the attribute 'ns_excl_pref_regexs'.
        """
        if exclusions_params := getattr(self, "ns_excl_pref_regexs", {}):
            exclusions = " and ".join((f"schema_name !~ :{k}" for k in exclusions_params))
        else:
            exclusions = ""

        sql = f"""
            select schema_name
            from information_schema.schemata
            where {exclusions}
            ;
        """
        return (sql, exclusions_params)

    def _get_engine_schemata(self) -> List[str]:
        """
        Build and execute a schema listing query.

        This internal method will build a schema listing query and execute
        the query.
        """
        query, params = self._build_discover_engine_query()
        schemata = None
        with self.engine.connect() as conn:
            cur = conn.execute(sa.text(query), params)
            schemata = [rec["schema_name"] for rec in cur]

        return schemata

    def discover(self) -> None:
        """
        Probe a engine for any user schemata.

        Each schema will be registered and tables will be discovered.
        """
        schemata = self._get_engine_schemata()
        for schema in schemata:
            self.register_schema(schema)
            self.schemata[schema].discover()


class MetaSchema(MetaMetaBase):
    def __init__(self, schema_name: str, metaengine: MetaEngine):
        super().__init__()
        self._metaengine = metaengine
        self.name = schema_name
        self._metadata = sa.MetaData(schema=self.name)
        self._notfound_exc = MetaMetaTableNotFoundError

    @property
    def tables(self):
        return self._items

    @property
    def metadata(self) -> sa.MetaData:
        return self._metadata

    @property
    def metameta(self) -> MetaMeta:
        return self._metaengine.metameta

    @property
    def metaengine(self) -> MetaEngine:
        return self._metaengine

    @property
    def engine(self) -> sa.engine.Engine:
        return self._metaengine.engine

    @property
    def child_class(self) -> None:
        raise AttributeError(f"{self.__class__.__name__} has no 'child_class' attribute")

    def _reflect_objects(self) -> None:
        """
        Internal method to execute instrospection of an engine.

        This method will execute the MetaData.reflect() method to
        probe the target database engine object for table objects
        to dynamically create.
        """
        self.metadata.reflect(bind=self.engine)

    def _reindex_tables(self) -> None:
        _metadata = self.metadata
        _tables = self._items
        prefix = f"{self.name}."
        lprefix = len(prefix)
        for tab in _metadata.tables:
            if tab.startswith(prefix):
                mstab = tab[lprefix:]
            else:
                mstab = tab
            _tables[mstab] = _metadata.tables[tab]

    def discover(self) -> None:
        """
        Discover tables within a schema.

        After initial discovery, the tables may have schema prefixes.
        These table names will be re-indexed to ensure that the schema name
        has been remove from the table name key.

        The resulting table can be referenced via a dictionary reference:
            MetaSchema[<table_name>]
        or via an attribute referencde:
            MetaSchema.<table_name>
        """
        self._reflect_objects()
        self._reindex_tables()


__all__ = ("MetaMetaBase", "MetaMeta", "MetaEngine", "MetaSchema")
