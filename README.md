# Snow2GCP - Snowflake to BigQuery Exporter

A Streamlit application for exporting Snowflake data to Google Cloud Storage and optionally importing to BigQuery.

## Features

- **Database Selection**: Browse and select Snowflake databases
- **Schema Selection**: Browse and select schemas within the chosen database
- **View Selection**: Multi-select views from the chosen schema
- **GCS Export**: Export data as Parquet files to Google Cloud Storage
- **BigQuery Import**: Optionally create BigQuery tables from exported data
- **Progress Tracking**: Real-time progress bar during export operations
- **Error Handling**: Comprehensive error reporting and handling

## Installation

1. Install dependencies using uv:
   ```bash
   uv sync
   ```

2. Set up environment variables for Snowflake connection (optional):
   
   **Option A: Using .env file (recommended)**
   ```bash
   cp .env.example .env
   # Edit .env file with your actual values
   ```
   
   **Option B: Using environment variables**
   ```bash
   export SNOWFLAKE_USER="your_username"
   export SNOWFLAKE_PASSWORD="your_password"
   export SNOWFLAKE_ACCOUNT="your_account.snowflakecomputing.com"
   export SNOWFLAKE_WAREHOUSE="your_warehouse"  # optional
   ```

3. Ensure you have Google Cloud credentials configured for BigQuery access:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
   ```

## Usage

1. Start the Streamlit application:
   ```bash
   streamlit run streamlit_app.py
   ```

2. Configure Snowflake connection in the sidebar:
   - Connection fields will be pre-filled if you have a `.env` file configured
   - Enter or verify your Snowflake credentials
   - Specify warehouse (optional)
   - Click "Connect"

3. Select data to export:
   - Choose a database from the dropdown
   - Select a schema from the available schemas
   - Multi-select views you want to export

4. Configure export settings:
   - Enter your GCS bucket name
   - Enter your GCP Project ID (optional - will use default from credentials if empty)
   - Enable/disable BigQuery import

5. Start the export:
   - Review the export summary
   - Click "Start Export"
   - Monitor progress via the progress bar

## Application Structure

### Main Components

- **Connection Management**: Secure Snowflake connection handling
- **Data Discovery**: Browse databases, schemas, and views
- **Export Engine**: Automated data export to GCS using Snowflake's COPY INTO
- **BigQuery Integration**: Automatic table creation from exported Parquet files
- **Progress Tracking**: Real-time status updates and progress monitoring

### Key Features

#### Snowflake Integration
- Lists all available databases
- Dynamically loads schemas based on selected database
- Displays views available in the selected schema
- Handles timezone conversion for timestamp columns

#### GCS Export
- Creates unique storage integrations per table
- Exports data in Parquet format with Snappy compression
- Sanitizes names for GCS path compatibility
- Automatic cleanup of storage integrations

#### BigQuery Import
- Creates datasets based on database/schema names
- Loads Parquet files directly from GCS
- Handles table creation and data loading
- Maintains original data types and structure

#### User Interface
- Clean, intuitive Streamlit interface
- Sidebar for connection configuration
- Multi-column layout for data selection
- Expandable sections for configuration
- Real-time progress feedback

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `SNOWFLAKE_USER` | Snowflake username | Yes |
| `SNOWFLAKE_PASSWORD` | Snowflake password | Yes |
| `SNOWFLAKE_ACCOUNT` | Snowflake account URL | Yes |
| `GCP_PROJECT` | Google Cloud Project ID for BigQuery | No (will use default from credentials) |
| `GCS_BUCKET` | Default GCS bucket name | No |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service account key | Yes (for BigQuery) |

## Dependencies

- `streamlit`: Web application framework
- `snowflake-connector-python`: Snowflake database connectivity
- `google-cloud-bigquery`: BigQuery client library
- `google-cloud-storage`: Google Cloud Storage client
- `pandas`: Data manipulation and analysis
- `pydantic-settings`: Settings management

## File Structure

```
snow2gcp/
├── streamlit_app.py          # Main Streamlit application
├── snow2gcp/
│   ├── snow2gcp.py          # Core export logic
│   ├── settings.py          # Configuration management
│   └── utils/
│       └── snowflake.py     # Snowflake utility functions
├── pyproject.toml           # Project dependencies
└── README.md               # This file
```

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify Snowflake credentials and network connectivity
2. **Permission Errors**: Ensure GCS bucket access and BigQuery permissions
3. **Data Type Issues**: Check for unsupported Snowflake data types
4. **Memory Issues**: Consider exporting smaller datasets for large tables

### Logging

The application provides detailed error messages in the Streamlit interface. Check the browser console for additional debugging information.

## Security Considerations

- Never hardcode credentials in the application
- Use environment variables or secure credential management
- Ensure GCS bucket permissions are properly configured
- Monitor BigQuery usage and costs

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the GPLv2 License.