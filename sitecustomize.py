"""Compatibility patches loaded automatically by Python at startup.

This adjusts PyHive's Hive dialect so Superset can introspect Spark Thrift
Server correctly. Spark returns SHOW TABLES/SHOW VIEWS rows as:
  namespace, tableName, isTemporary
while the generic Hive dialect expects the first column to be the table name.
"""

from pyhive.sqlalchemy_hive import HiveDialect
from sqlalchemy import text


def _extract_relation_name(row):
    values = tuple(row)
    if len(values) >= 2 and values[1]:
        return values[1]
    return values[0]


def _get_names(connection, statement):
    return [_extract_relation_name(row) for row in connection.execute(text(statement))]


def _get_table_names(self, connection, schema=None, **kw):
    statement = "SHOW TABLES"
    if schema:
        statement += f" IN `{schema}`"
    return _get_names(connection, statement)


def _get_view_names(self, connection, schema=None, **kw):
    statement = "SHOW VIEWS"
    if schema:
        statement += f" IN `{schema}`"
    return _get_names(connection, statement)


HiveDialect.get_table_names = _get_table_names
HiveDialect.get_view_names = _get_view_names
