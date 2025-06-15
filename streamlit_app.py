#!/usr/bin/env python3

import streamlit as st
import pandas as pd
from typing import Optional, List, Tuple
import time
from contextlib import contextmanager

from snow2gcp.settings import SnowflakeAuth
from snow2gcp.utils.snowflake import (
    create_snowflake_connection,
    list_snowflake_databases,
    list_snowflake_schemas,
    list_snowflake_views,
)
from snow2gcp.snow2gcp import (
    generate_column_query,
    generate_unload_template,
    sanitize_path_component,
)


def init_session_state():
    """Initialize session state variables."""
    if 'connection' not in st.session_state:
        st.session_state.connection = None
    if 'databases' not in st.session_state:
        st.session_state.databases = []
    if 'schemas' not in st.session_state:
        st.session_state.schemas = []
    if 'views' not in st.session_state:
        st.session_state.views = []
    if 'export_running' not in st.session_state:
        st.session_state.export_running = False


@contextmanager
def st_progress_bar(title: str, total: int):
    """Context manager for progress bar."""
    progress_bar = st.progress(0)
    status_text = st.empty()

    class ProgressTracker:
        def __init__(self):
            self.current = 0

        def update(self, message: str = ""):
            self.current += 1
            progress = self.current / total
            progress_bar.progress(progress)
            status_text.text(f"{title}: {message} ({self.current}/{total})")

        def finish(self):
            progress_bar.progress(1.0)
            status_text.text(f"{title}: Completed!")

    tracker = ProgressTracker()
    try:
        yield tracker
    finally:
        time.sleep(0.5)  # Brief pause to show completion
        progress_bar.empty()
        status_text.empty()


def connect_to_snowflake(
    user: str,
    password: str,
    account: str,
):
    """Connect to Snowflake and store connection in session state."""
    try:
        with st.spinner("Connecting to Snowflake..."):
            conn = create_snowflake_connection(
                user=user,
                pwd=password,
                account=account,
            )
            st.session_state.connection = conn
            st.success("✅ Connected to Snowflake successfully!")
            return True
    except Exception as e:
        st.error(f"❌ Failed to connect to Snowflake: {str(e)}")
        return False


def load_databases():
    """Load databases from Snowflake."""
    if st.session_state.connection is None:
        return

    try:
        with st.spinner("Loading databases..."):
            df = list_snowflake_databases(st.session_state.connection)
            st.session_state.databases = df['name'].tolist()
    except Exception as e:
        st.error(f"❌ Failed to load databases: {str(e)}")


def load_schemas(database: str):
    """Load schemas for the selected database."""
    if st.session_state.connection is None or not database:
        return

    try:
        with st.spinner(f"Loading schemas for {database}..."):
            df = list_snowflake_schemas(st.session_state.connection, database)
            st.session_state.schemas = df['name'].tolist()
            st.success(f"✅ Loaded {len(st.session_state.schemas)} schemas")
    except Exception as e:
        st.error(f"❌ Failed to load schemas: {str(e)}")
        st.session_state.schemas = []


def load_views(database: str, schema: str):
    """Load views for the selected database and schema."""
    if st.session_state.connection is None or not database or not schema:
        return

    try:
        with st.spinner(f"Loading views for {database}.{schema}..."):
            df = list_snowflake_views(st.session_state.connection, database, schema)
            st.session_state.views = df['name'].tolist()
            st.success(f"✅ Loaded {len(st.session_state.views)} views")
    except Exception as e:
        st.error(f"❌ Failed to load views: {str(e)}")
        st.session_state.views = []


def export_view_to_gcs(
    database: str, schema: str, view: str, gcs_bucket: str, progress_tracker
):
    """Export a single view to GCS."""
    try:
        # Generate column query
        progress_tracker.update(f"Generating column query for {view}")
        column_query = generate_column_query(database, schema, view)

        # Execute column query
        progress_tracker.update(f"Fetching column information for {view}")
        cursor = st.session_state.connection.cursor()
        query_resp = cursor.execute(column_query).fetchall()[0]
        step_1_res = ''.join(query_resp[0].split('\n'))

        # Generate unload statements
        progress_tracker.update(f"Generating unload statements for {view}")
        base_gcs_path = f"gcs://{gcs_bucket}/{sanitize_path_component(schema)}"
        statements = generate_unload_template(
            database, schema, view, base_gcs_path, step_1_res
        )

        # Execute unload statements
        for i, statement in enumerate(statements, start=1):
            progress_tracker.update(
                f"Executing statement {i}/{len(statements)} for {view}"
            )
            if statement.strip():  # Skip empty statements
                resp = cursor.execute(statement).fetchall()
                progress_tracker.update(str(resp))

        return True, None
    except Exception as e:
        return False, str(e)


def export_to_bigquery(
    gcs_bucket: str, database: str, schema: str, views: List[str], progress_tracker
):
    """Create BigQuery tables from exported parquet files."""
    try:
        from google.cloud import bigquery

        client = bigquery.Client()
        dataset_id = (
            f"{sanitize_path_component(database)}_{sanitize_path_component(schema)}"
        )

        # Create dataset if it doesn't exist
        progress_tracker.update("Creating BigQuery dataset")
        try:
            dataset = client.get_dataset(dataset_id)
        except:
            dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
            dataset = client.create_dataset(dataset, exists_ok=True)

        # Create tables from parquet files
        for view in views:
            progress_tracker.update(f"Creating BigQuery table for {view}")
            table_id = f"{dataset_id}.{sanitize_path_component(view)}"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

            gcs_path = f"gs://{gcs_bucket}/{sanitize_path_component(schema)}/{sanitize_path_component(view)}/*.parquet"

            load_job = client.load_table_from_uri(
                gcs_path,
                table_id,
                job_config=job_config,
            )

            load_job.result()  # Wait for job to complete

        return True, None
    except Exception as e:
        return False, str(e)


def debug_connection_info():
    """Display connection debugging information."""
    if st.session_state.connection:
        try:
            cursor = st.session_state.connection.cursor()
            cursor.execute(
                "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()"
            )
            current_db, current_schema, current_warehouse = cursor.fetchone()

            with st.expander("🔍 Connection Debug Info"):
                st.write(f"**Current Database:** {current_db}")
                st.write(f"**Current Schema:** {current_schema}")
                st.write(f"**Current Warehouse:** {current_warehouse}")
                st.write(f"**Cached Databases:** {len(st.session_state.databases)}")
                st.write(f"**Cached Schemas:** {len(st.session_state.schemas)}")
                st.write(f"**Cached Views:** {len(st.session_state.views)}")
        except Exception as e:
            st.error(f"Error getting connection info: {e}")


def main():
    st.set_page_config(
        page_title="Snow2GCP - Snowflake to BigQuery Exporter",
        page_icon="❄️",
        layout="wide",
    )

    init_session_state()

    st.title("❄️ Snow2GCP - Snowflake to BigQuery Exporter")
    st.markdown(
        "Export Snowflake data to Google Cloud Storage and optionally import to BigQuery"
    )
    # Sidebar for connection configuration
    with st.sidebar:
        st.header("🔐 Snowflake Connection")

        with st.form("connection_form"):
            user = st.text_input("User", placeholder="snowflake_user")
            password = st.text_input("Password", type="password")
            account = st.text_input(
                "Account", placeholder="your_account.snowflakecomputing.com"
            )
            connect_btn = st.form_submit_button(
                "Connect", type="primary", use_container_width=True
            )

            if connect_btn:
                if all([user, password, account]):
                    if connect_to_snowflake(user, password, account):
                        load_databases()
                else:
                    st.error("Please fill in all connection fields")

        # Connection status
        if st.session_state.connection:
            st.success("🟢 Connected")
        else:
            st.error("🔴 Not connected")

    # Main content area
    if st.session_state.connection is None:
        st.info(
            "👈 Please configure your Snowflake connection in the sidebar to get started."
        )
        return

    # Add debug info
    debug_connection_info()

    # Data selection section
    st.header("📊 Data Selection")

    # Add refresh button for databases
    col_refresh, col_info = st.columns([1, 4])
    with col_refresh:
        if st.button(
            "🔄 Refresh", key="refresh_databases", help="Refresh database list"
        ):
            load_databases()
    with col_info:
        st.info(f"Found {len(st.session_state.databases)} databases")

    col1, col2, col3 = st.columns(3)

    with col1:
        # Database selection
        if st.session_state.databases:
            selected_database = st.selectbox(
                "Select Database",
                options=st.session_state.databases,
                key="database_select",
                help="Choose a Snowflake database to explore",
            )

            if selected_database != st.session_state.get('last_selected_database'):
                st.session_state.last_selected_database = selected_database
                # Clear dependent selections when database changes
                st.session_state.schemas = []
                st.session_state.views = []
                if 'last_selected_schema' in st.session_state:
                    del st.session_state.last_selected_schema
                # Load schemas for the new database
                load_schemas(selected_database)
        else:
            st.info("Loading databases...")
            selected_database = None

    with col2:
        # Schema selection
        if st.session_state.schemas and selected_database:
            selected_schema = st.selectbox(
                "Select Schema",
                options=st.session_state.schemas,
                key="schema_select",
                help="Choose a schema within the selected database",
            )

            if selected_schema != st.session_state.get('last_selected_schema'):
                st.session_state.last_selected_schema = selected_schema
                # Clear views when schema changes
                st.session_state.views = []
                # Load views for the new schema
                load_views(selected_database, selected_schema)
        else:
            selected_schema = None
            if selected_database and not st.session_state.schemas:
                st.info("Loading schemas...")
            elif selected_database:
                st.info("No schemas found in this database")
            else:
                st.info("Select a database first")

    with col3:
        # View selection
        if st.session_state.views and selected_database and selected_schema:
            selected_views = st.multiselect(
                "Select Views",
                options=st.session_state.views,
                key="views_select",
                help="Choose one or more views to export",
            )
        else:
            selected_views = []
            if selected_database and selected_schema and not st.session_state.views:
                st.info("Loading views...")
            elif selected_database and selected_schema:
                st.info("No views found in this schema")
            else:
                st.info("Select database and schema first")

    # Export configuration section
    st.header("⚙️ Export Configuration")

    col1, col2 = st.columns(2)

    with col1:
        gcs_bucket = st.text_input(
            "GCS Bucket Name",
            placeholder="your-gcs-bucket-name",
            help="Enter the name of your Google Cloud Storage bucket",
        ).lstrip('gs://').rstrip('/')

    with col2:
        enable_bq_import = st.checkbox(
            "Import to BigQuery",
            value=True,
            help="Create BigQuery tables from the exported parquet files",
        )

    # Export section
    st.header("🚀 Export")

    if not all([selected_database, selected_schema, selected_views, gcs_bucket]):
        st.warning(
            "Please select database, schema, views, and specify GCS bucket before exporting."
        )
    else:
        # Show export summary
        with st.expander("📋 Export Summary", expanded=True):
            st.write(f"**Database:** {selected_database}")
            st.write(f"**Schema:** {selected_schema}")
            st.write(f"**Views:** {len(selected_views)} selected")
            for view in selected_views:
                st.write(f"  - {view}")
            st.write(f"**GCS Bucket:** gs://{gcs_bucket}")
            st.write(
                f"**BigQuery Import:** {'Enabled' if enable_bq_import else 'Disabled'}"
            )

        # Export button and progress
        if st.button(
            "🚀 Start Export", type="primary", disabled=st.session_state.export_running
        ):
            # Ensure we have valid selections before proceeding
            if not selected_database or not selected_schema or not selected_views:
                st.error("Please ensure database, schema, and views are selected.")
                return

            st.session_state.export_running = True

            try:
                # Calculate total steps
                total_steps = len(selected_views) * 4  # 4 steps per view
                if enable_bq_import:
                    total_steps += (
                        len(selected_views) + 1
                    )  # Additional steps for BQ import

                with st_progress_bar("Exporting data", total_steps) as progress:
                    failed_views = []

                    # Export each view to GCS
                    for view in selected_views:
                        success, error = export_view_to_gcs(
                            selected_database,
                            selected_schema,
                            view,
                            gcs_bucket,
                            progress,
                        )
                        if not success:
                            failed_views.append((view, error))
                            st.error(f"❌ Failed to export {view}: {error}")

                    # Import to BigQuery if enabled
                    if enable_bq_import and not failed_views:
                        success, error = export_to_bigquery(
                            gcs_bucket,
                            selected_database,
                            selected_schema,
                            selected_views,
                            progress,
                        )
                        if not success:
                            st.error(f"❌ Failed to import to BigQuery: {error}")

                # Show results
                successful_views = [
                    v for v in selected_views if v not in [f[0] for f in failed_views]
                ]

                if successful_views:
                    st.success(
                        f"✅ Successfully exported {len(successful_views)} views!"
                    )
                    if enable_bq_import:
                        st.success("✅ Successfully imported to BigQuery!")

                if failed_views:
                    st.error(f"❌ Failed to export {len(failed_views)} views")
                    with st.expander("View Errors"):
                        for view, error in failed_views:
                            st.error(f"{view}: {error}")

            except Exception as e:
                st.error(f"❌ Export failed: {str(e)}")
            finally:
                st.session_state.export_running = False


if __name__ == "__main__":
    main()
