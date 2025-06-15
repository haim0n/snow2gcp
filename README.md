# Snow2GCP - Snowflake to BigQuery Exporter

A Streamlit application for exporting Snowflake data to Google Cloud Storage and optionally importing to BigQuery.

## Quick Start

For experienced users who want to get started immediately:

```bash
# Clone and setup
git clone https://github.com/your-username/snow2gcp.git
cd snow2gcp

# Create virtual environment and install
python -m venv venv
source venv/bin/activate  # On Linux/macOS
pip install -e .

# Configure credentials
cp .env.example .env
# Edit .env with your credentials

# Run the application
streamlit run streamlit_app.py
```

For detailed installation instructions, see the [Installation](#installation) section below.

## Features

- **Database Selection**: Browse and select Snowflake databases
- **Schema Selection**: Browse and select schemas within the chosen database
- **View Selection**: Multi-select views from the chosen schema
- **GCS Export**: Export data as Parquet files to Google Cloud Storage
- **BigQuery Import**: Optionally create BigQuery tables from exported data
- **Progress Tracking**: Real-time progress bar during export operations
- **Error Handling**: Comprehensive error reporting and handling

## System Requirements

- **Operating System**: Linux, macOS, or Windows
- **Python**: Version 3.10 or higher
- **Memory**: At least 4GB RAM
- **Network**: Internet connection for Snowflake and Google Cloud services
- **Disk Space**: At least 1GB free space for dependencies and temporary files

### External Service Requirements

- **Snowflake Account**: With appropriate read permissions on target databases/schemas
- **Google Cloud Platform Account**: With the following services enabled:
  - Cloud Storage (for data export)
  - BigQuery (for data import)
  - Cloud Resource Manager API
- **Permissions**: 
  - Snowflake: `USAGE` on warehouse, database, and schema; `SELECT` on views/tables
  - GCP: `Storage Admin` and `BigQuery Admin` roles (or equivalent granular permissions)

## Installation

### Prerequisites

- Python 3.10 or higher
- Git
- Google Cloud Platform account with BigQuery and Cloud Storage enabled
- Snowflake account with appropriate permissions

### Step-by-Step Installation

#### 1. Clone the Repository

```bash
git clone https://github.com/haim0n/snow2gcp.git
cd snow2gcp
```

#### 2. Set Up Python Virtual Environment

Choose one of the following methods:

**Option A: Using Python venv (Standard)**
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/macOS:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip
```

**Option B: Using uv (Fast, Recommended)**
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create and activate virtual environment with uv
uv venv
source .venv/bin/activate  # On Linux/macOS
# .venv\Scripts\activate  # On Windows
```

**Option C: Using conda**
```bash
# Create conda environment
conda create -n snow2gcp python=3.10
conda activate snow2gcp
```

#### 3. Install Dependencies

**Option A: Using uv (Fastest)**
```bash
uv sync
```

**Option B: Using pip**
```bash
pip install -e .
```

**Option C: Development installation with all dependencies**
```bash
pip install -e ".[dev]"
```

#### 4. Set Up Google Cloud Authentication

**Option A: Using gcloud CLI (Recommended)**
```bash
# Install Google Cloud CLI if not already installed
# Follow instructions at: https://cloud.google.com/sdk/docs/install

# Authenticate with Google Cloud
gcloud auth application-default login

# Set your default project
gcloud config set project YOUR_PROJECT_ID
```

**Option B: Using Service Account Key**
```bash
# Download service account key from Google Cloud Console
# Set environment variable
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

#### 5. Configure Snowflake Connection

**Option A: Using .env file (Recommended)**
```bash
# Copy the example environment file
cp .env.example .env

# Edit .env file with your actual values
nano .env  # or use your preferred editor
```

Edit the `.env` file with your Snowflake credentials:
```properties
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ACCOUNT=your_account.snowflakecomputing.com
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
GCS_BUCKET=your-gcs-bucket-name
GCP_PROJECT=your-gcp-project-id
```

**Option B: Using environment variables**
```bash
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_ACCOUNT="your_account.snowflakecomputing.com"
export SNOWFLAKE_WAREHOUSE="your_warehouse"  # optional
export GCS_BUCKET="your-gcs-bucket-name"
export GCP_PROJECT="your-gcp-project-id"
```

#### 6. Verify Installation

```bash
# Test the installation
python -c "import snow2gcp; print('Installation successful!')"

# Check if Streamlit works
streamlit --version
```

### Quick Start Commands

Once installed, you can quickly start the application:

```bash
# Activate your virtual environment (if not already active)
source venv/bin/activate  # or source .venv/bin/activate for uv

# Start the Streamlit application
streamlit run streamlit_app.py
```

### Alternative Installation Methods

#### Docker Installation (Coming Soon)

```bash
# Build Docker image
docker build -t snow2gcp .

# Run container
docker run -p 8501:8501 -e SNOWFLAKE_USER=your_user snow2gcp
```

#### Development Installation

For contributors and developers:

```bash
# Clone with development dependencies
git clone https://github.com/haim0n/snow2gcp.git
cd snow2gcp

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install in development mode
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
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

### Installation Issues

1. **Python Version Compatibility**
   ```bash
   # Check Python version
   python --version
   # Should be 3.10 or higher
   ```

2. **Virtual Environment Issues**
   ```bash
   # If activation fails, try recreating the environment
   rm -rf venv  # or .venv
   python -m venv venv
   source venv/bin/activate
   ```

3. **Dependency Installation Failures**
   ```bash
   # Update pip first
   pip install --upgrade pip
   
   # Install with verbose output to see errors
   pip install -e . -v
   
   # For uv issues
   uv cache clean
   uv sync --refresh
   ```

4. **Google Cloud SDK Issues**
   ```bash
   # Verify gcloud installation
   gcloud --version
   
   # Re-authenticate if needed
   gcloud auth application-default revoke
   gcloud auth application-default login
   ```

5. **Snowflake Connection Issues**
   - Verify account URL format: `account.snowflakecomputing.com`
   - Check if your IP is whitelisted in Snowflake
   - Ensure warehouse is running and accessible

### Runtime Issues

1. **Connection Errors**: Verify Snowflake credentials and network connectivity
2. **Permission Errors**: Ensure GCS bucket access and BigQuery permissions
3. **Data Type Issues**: Check for unsupported Snowflake data types
4. **Memory Issues**: Consider exporting smaller datasets for large tables

### Common Error Messages

**"ModuleNotFoundError: No module named 'snow2gcp'"**
- Solution: Ensure virtual environment is activated and package is installed with `pip install -e .`

**"Authentication failed" (Google Cloud)**
- Solution: Run `gcloud auth application-default login` or check service account key path

**"Invalid account name" (Snowflake)**
- Solution: Check account format - should be `account.snowflakecomputing.com` or `account.region.cloud`

**"Permission denied" (GCS/BigQuery)**
- Solution: Verify your Google Cloud permissions include Storage Admin and BigQuery Admin roles

### Logging

The application provides detailed error messages in the Streamlit interface. Check the browser console for additional debugging information.

### Getting Help

If you encounter issues not covered here:

1. Check the [Issues](https://github.com/your-username/snow2gcp/issues) page on GitHub
2. Enable verbose logging by setting `STREAMLIT_LOG_LEVEL=debug`
3. Include the full error message and your environment details when reporting issues

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