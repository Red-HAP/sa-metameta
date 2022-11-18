![](https://raw.githubusercontent.com/Red-HAP/sa-metameta/main/docs/images/sa-metameta-logo.png)

# sa-metameta

Meta information of SQLAlchemy metadata by engine

## Purpose

This module is designed to have a top-level class instance to which SQLAlchemy `Engine` instances can be registered as a `MetaEngine`. Each `MetaEngine` can then be probed directly to retrieve all tables by user schema. Thus an application can reference multiple databases and use the reflected `Table` instances to create queries for each.

This module has classes that will probe a RDBMS that supports the `information_schema.schema` view to get table information for each schema in a database. Currently, only PostgreSQL is fully supported.

This module will support `psycopg2` and `asyncpg`.

This module requires `SQLAlchemy` >= v1.4 for database probing and `Table` class creation.

This module currently supports only table object creation.

## Overview

After instantiating the MetaMeta class, an engine can be registered. Once that is done, the database can be probed for tables. Once that has completed successfully, the tables can be referenced starting from the MetaMeta instance to the engine, to the schema, and then to the table.

Example:

```python
import sqlalchemy as sa
from sa_metameta import meta

engine = sa.create_engine("postgresql://postgres:pg_passwd@localhost:5432/my_database")

mm = meta.MetaMeta()
# This will use the database name from the URL as the attribute name
mm.register_engine(engine)
# This will probe all schemata in my_database and for each schema, the tables will be reflected.
mm.my_database.discover()

# now we can see what tables have been found by using list()
list(mm.my_database.public)
[
    "table1",
    "table2",
    ...
]
```

The engine, schema, and table can be referenced by dot or subscript notation:

```python
engine = mm["my_database"]
schema = mm["my_database"]["public"]
table1 = mm["my_database"]["public"]["table1"]

# or

engine = mm.my_database
schema = mm.my_database.public
table1 = mm.my_database.public.table1
```

To reference columns, use the Table().c.column syntax.

```python
query = sa.select(table1.c.label).filter(table1.c.quatloos > 200)
db = engine.session()
res = db.execute(query).all()
```
