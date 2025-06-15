from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import snowflake.connector
from pandas import DataFrame

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection


def create_snowflake_connection(
    user: str, pwd: str, account: str, warehouse: str | None = None, database: str | None = None, schema: str | None = None
) -> SnowflakeConnection:
    conn = snowflake.connector.connect(
        user=user,
        password=pwd,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )
    return conn


def list_snowflake_databases(conn: SnowflakeConnection) -> DataFrame:
    """List all databases in Snowflake."""
    return pd.read_sql_query(sql="SHOW DATABASES", con=conn)


def list_snowflake_warehouses(conn: SnowflakeConnection) -> DataFrame:
    """List all warehouses in Snowflake."""
    return pd.read_sql_query(sql="SHOW WAREHOUSES", con=conn)


def list_snowflake_schemas(conn: SnowflakeConnection, database: str) -> DataFrame:
    """List all schemas in a given Snowflake database."""
    return pd.read_sql_query(sql=f"SHOW SCHEMAS IN DATABASE {database}", con=conn)


def list_snowflake_views(
    conn: SnowflakeConnection, database: str, schema: str
) -> DataFrame:
    """List all views in a given Snowflake schema."""
    return pd.read_sql_query(
        sql=f'SHOW VIEWS IN SCHEMA {database}."{schema}"', con=conn
    )
