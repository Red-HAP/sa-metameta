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

import datetime
import os
import re
from io import TextIOWrapper
from typing import Dict, List, Optional, Tuple, Union

import sqlalchemy as sa
import yaml
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

    def as_yaml(self, dump=True) -> Optional[str]:
        objects = []
        yaml_data = [{"object_type": "database", "name": self.name, "objects": objects}]
        for schema in self.schemata:
            objects.append(self.schemata[schema].as_yaml(dump=False))

        db_yaml = yaml.dump(yaml_data)
        if dump:
            filename = f"{self.name}.yaml"
            with open(filename, "wt") as out:
                print(db_yaml, file=out)
            return filename
        else:
            return db_yaml

    def as_ddl(self):
        filename = f"{self.name}.sql"
        with open(filename, "wt") as out:
            print(
                f"""
/*
 *  Generated by sa-metameta at {datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()} )
 */

""",
                file=out,
            )
            print(f"""create database if not exists "{self.name}";{os.linesep}""", file=out)

            print(f"""\\connect "{self.name}";{os.linesep}""", file=out)

            for schema in self.schemata:
                self.schemata[schema].as_ddl(file_=out)

        return filename


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

    def as_ddl(self, file_: Optional[TextIOWrapper] = None) -> None:
        if file_ is not None:
            out = file_
        else:
            out = open(f"{self.name}.sql", "wt")

        try:
            print(
                f"""
/*
 * Start of schema {self.name}
 */

 create schema if not exists "{self.name}";

 """,
                file=out,
            )

            search_path = [f'"{self.name}"']
            if self.name != "public":
                search_path.append('"public"')

            search_path = ", ".join(search_path)

            print(f"""set search_path = {search_path};{os.linesep}""", file=out)

            for table in self.tables:
                self.tables[table].as_ddl(file_=out)

            print(
                f"""
/*
 * Emd of schema {self.name}
 */
 """,
                file=out,
            )
        finally:
            out.flush()
            if out != file_:
                out.close()

        return out.name

    def as_yaml(self, *, dump=True) -> Union[str, Dict]:
        objects = []
        yaml_data = {"object_type": "schema", "name": self.name, "objects": objects}
        for table in self.tables:
            objects.append(self.tables[table].as_yaml(dump=False))

        if dump:
            return yaml.dump(yaml_data)
        else:
            return yaml_data


def _table_as_yaml(self, *, dump=True) -> Union[str, Dict]:
    camelcase_regex = re.compile(r"(?<=.)(?=[A-Z])")
    collapse_dblq = re.compile(r"('{2})")

    def unquote_db_str(txt: Optional[str]):
        if txt is None:
            return txt

        if txt.startswith("'") and txt.endswith("'"):
            txt = txt[1:-1]

        return collapse_dblq.sub("'", txt)

    def _identity(col: sa.sql.ColumnElement) -> Optional[Dict]:
        if col.identity:
            return {
                "generate": "always" if col.identity.always else "by default",
                "start": col.identity.start,
                "minvalue": "no minvalue" if col.identity.nominvalue else col.identity.minvalue,
                "maxvalue": "no maxvalue" if col.identity.nomaxvalue else col.identity.maxvalue,
                "cache": col.identity.cache,
                "cycle": col.identity.cycle,
                "increment": col.identity.increment,
            }
        else:
            return None

    def _default(col: sa.sql.ColumnElement) -> Optional[str]:
        if isinstance(col.server_default, sa.DefaultClause):
            if col.server_default:
                return str(col.server_default.arg.text)
        elif isinstance(col.server_default, sa.Identity):
            gen_type = "always" if col.server_default.always else "by default"
            return f"generated {gen_type} as identity"
        else:
            return None

    def _constraints(table: sa.Table, constraints: List) -> None:
        for const in table.constraints:
            const_type = " ".join(camelcase_regex.split(const.__class__.__name__)[:-1]).lower()
            const_def = {
                "type": const_type,
                "name": str(const.name),
            }
            if const_type in ("unique", "primary_key"):
                const_def["columns"] = [str(c.name) for c in const.columns]
            elif const_type == "check":
                const_def["condition"] = str(const.sqltext.text)
            elif const_type == "foreign_key":
                key_cols, ref_cols = zip(*const._elements.items())
                const_def["deferred"] = const.deferred
                const_def["initially"] = str(const.initially)
                const_def["match"] = str(const.match)
                const_def["on_delete"] = str(const.ondelete)
                const_def["on_update"] = str(const.onupdate)
                const_def["key_columns"] = [str(kc.name) for kc in key_cols]
                const_def["reference_columns"] = [str(rc.target_fullname) for rc in ref_cols]
            else:
                continue
            constraints.append(const_def)

    columns = []
    constraints = []
    table = {"object_type": "table", "name": str(self.name), "columns": columns, "constraints": constraints}
    _constraints(self, constraints)
    for column in self.columns:
        columns.append(
            {
                "name": str(column.name),
                "data_type": column.type.compile().lower(),
                "not_null": not column.nullable,
                "identity": _identity(column),
                "default": unquote_db_str(_default(column)),
            }
        )

    if dump:
        return yaml.dump(table)
    else:
        return table


def _table_as_ddl(self, file_: Optional[TextIOWrapper] = None) -> None:
    if file_ is not None:
        out = file_
    else:
        out = open(f"{self.name}.sql", "wt")

    try:
        print(
            f"""
/*
 *  Table {self.name}
 */
 """,
            file=out,
        )

        print(sa.schema.CreateTable(self), os.linesep, file=out)
    finally:
        out.flush()
        if out != file_:
            out.close()

    return out.name


setattr(sa.Table, "as_yaml", _table_as_yaml)
setattr(sa.Table, "as_ddl", _table_as_ddl)


__all__ = ("MetaMetaBase", "MetaMeta", "MetaEngine", "MetaSchema")
