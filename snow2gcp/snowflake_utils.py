from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from pandas import DataFrame

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection


def list_snowflake_databases(conn: SnowflakeConnection) -> DataFrame:
    """List all databases in Snowflake."""
    return pd.read_sql_query(sql="SHOW DATABASES", con=conn)


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
