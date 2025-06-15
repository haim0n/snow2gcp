#!/usr/bin/env python3

from __future__ import annotations

import re
from typing import TYPE_CHECKING
from typing import Tuple

import tqdm

from snow2gcp.utils.snowflake import create_snowflake_connection

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection


def sanitize_path_component(component: str) -> str:
    """Sanitize database/schema/table names for use in GCS paths."""
    return re.sub(r'[^a-z0-9_]', '_', component.lower())


def generate_column_query(database: str, schema: str, table: str) -> str:
    """Generate query to get timezone-converted column list."""
    return f"""
-- Step 1: Run this query to get the SELECT clause for {database}.{schema}.{table}
SELECT LISTAGG(
    CASE 
        WHEN DATA_TYPE IN ('TIMESTAMP_TZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ') 
             OR DATA_TYPE LIKE '%TIMESTAMP%'
             OR DATA_TYPE LIKE '%DATETIME%'
        THEN '        CONVERT_TIMEZONE(''UTC'', ' || COLUMN_NAME || ')::TIMESTAMP as ' || COLUMN_NAME
        ELSE '        ' || COLUMN_NAME
    END, 
    ',\\n'
) as SELECT_CLAUSE
FROM {database}.INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = '{schema}' 
    AND TABLE_NAME = '{table}'
ORDER BY ORDINAL_POSITION;
"""


def generate_unload_template(
    database: str, schema: str, table: str, base_gcs_path: str, step_1_res: str
) -> Tuple[str, ...]:
    """Generate the unload template.

    Each step is a separate SQL statement to be executed in Snowflake.
    returned as tuple of strings.
    """

    # Sanitize components for GCS path
    db_path = sanitize_path_component(database)
    schema_path = sanitize_path_component(schema)
    table_path = sanitize_path_component(table)

    # Full GCS path
    gcs_path = f"{base_gcs_path}/{table_path}/"

    # Integration name (unique per table)
    integration_name = f"gcs_integration_{db_path}_{schema_path}_{table_path}"

    return (
        "USE ROLE ACCOUNTADMIN;",
        f"""CREATE OR REPLACE STORAGE INTEGRATION {integration_name}
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('{gcs_path}');
""",
        f"""
COPY INTO '{gcs_path}'
FROM (SELECT
        {step_1_res}
FROM {database}."{schema}".{table})
FILE_FORMAT = (TYPE = 'PARQUET', COMPRESSION = 'SNAPPY')
HEADER = TRUE
STORAGE_INTEGRATION = {integration_name}
OVERWRITE = TRUE
MAX_FILE_SIZE = 100000000;
""",
        f"""
-- Clean up (optional):
DROP STORAGE INTEGRATION IF EXISTS {integration_name};
""",
    )


def generate_complete_workflow(
    database: str,
    schema: str,
    table: str,
    base_gcs_path: str,
    conn: SnowflakeConnection,
):
    """Generate complete workflow for a table."""

    step1 = generate_column_query(database, schema, table)

    cursor = conn.cursor()
    query_resp = cursor.execute(step1).fetchall()[0]
    assert len(query_resp) == 1, "Expected a single row with the SELECT_CLAUSE"
    step_1_res = ''.join(query_resp[0].split('\n'))
    step2 = generate_unload_template(database, schema, table, base_gcs_path, step_1_res)

    for statement in step2:
        print(
            '-- ============================================================================'
        )
        print(f'-- Workflow for {database}.{schema}.{table}')
        print(
            '-- ============================================================================'
        )
        # print(statement)
        print(cursor.execute(statement).fetchall())


def main():
    """Generate complete workflows for all tables."""
    from snow2gcp.settings import Settings
    import os
    
    # Load settings
    settings = Settings()
    
    tables = [
        ('DB_NAME', 'SCHEMA_NAME', 'VIEW_NAME'),
    ]
    print(
        "-- ============================================================================"
    )
    print("-- Snowflake Table Unload Script")
    print(f"-- Generated on: 2025-06-14 UTC")
    print(
        "-- ============================================================================"
    )
    print()

    conn = create_snowflake_connection(
        user=settings.snowflake_auth.user,
        pwd=settings.snowflake_auth.password,
        account=settings.snowflake_auth.account,
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'SCHEMA'),
    )

    gcs_bucket = os.getenv('GCS_BUCKET', 'your-bucket-name')
    
    succeeded = 0
    for database, schema, table in tqdm.tqdm(tables):
        try:
            generate_complete_workflow(
                database,
                schema,
                table,
                base_gcs_path=f"gcs://{gcs_bucket}/{schema}",
                conn=conn,
            )
            succeeded += 1
        except Exception as e:
            print(f"Error processing {database}.{schema}.{table}: {e}")
            with open('failed.txt', 'a+') as f:
                f.write(f"{database}.{schema}.{table}\n")
                f.write(f"Error: {e}\n")
            continue
    print(
        f"============================================================================"
    )
    print(
        f"Completed processing {len(tables)} tables, succeeded: {succeeded}, failed: {len(tables) - succeeded}"
    )


if __name__ == "__main__":
    main()
