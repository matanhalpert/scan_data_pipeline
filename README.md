# Data Pipeline Project

A comprehensive data pipeline for extracting, transforming, and loading data from various sources including social media platforms and search engines.

## Features

- **Multi-source Data Extraction**: Extract data from social media platforms (Facebook, Instagram, LinkedIn, X) and search engines (Google, Yahoo, Bing)
- **Data Transformation**: Transform and normalize data from different sources
- **Caching Layer**: Redis-based caching for improved performance
- **Database Storage**: SQLAlchemy ORM for persistent data storage (currently configured for MySQL)
- **Media Processing**: Handle images and videos with face recognition and transcription capabilities
- **Simulation Mode**: Generate test data for development and testing

## Prerequisites

- Python 3.12+
- MySQL database (currently configured, but SQLAlchemy ORM allows for other databases)
- Redis server
- Virtual environment (recommended)

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd data_pipeline
   ```

2. **Create and activate a virtual environment**
   ```bash
   python -m venv venv
   
   # On Windows
   venv\Scripts\activate
   
   # On macOS/Linux
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   
   Copy the example environment file and configure it:
   ```bash
   cp .env.example .env
   ```
   
   Edit the `.env` file with your actual configuration values:
   
   ```env
   # Database Configuration (Currently configured for MySQL)
   DB_USER=your_database_username
   DB_PASSWORD=your_database_password
   DB_HOST=localhost
   DB_NAME=data_pipeline_db
   
   # Redis Configuration
   REDIS_HOST=localhost
   REDIS_PORT=6379
   REDIS_DB=0
   REDIS_PASSWORD=your_redis_password_or_leave_empty
   
   # Cache Settings (in seconds)
   USER_DATA_CACHE_EXPIRATION=3600
   EXTRACTION_RESULTS_CACHE_EXPIRATION=1800
   METADATA_CACHE_EXPIRATION=7200
   ```

5. **Set up the database**
   
   The application will automatically create the database and tables when first run.
   
   > **Note**: The project uses SQLAlchemy ORM, which provides database abstraction. While currently configured for MySQL, it could potentially be adapted for other databases like PostgreSQL, SQLite, etc. However, some database-specific functionality (like the automatic database creation) may need modification.

## Usage

### Running a Data Pipeline Scan

```bash
python run_scan.py
```

This will execute the complete data pipeline process:
1. Generate or load test data
2. Extract data from configured sources
3. Transform the data
4. Load data into the database

### Project Structure

```
data_pipeline/
├── src/
│   ├── cache/          # Redis caching functionality
│   ├── config/         # Configuration files and enums
│   ├── database/       # Database models and setup
│   ├── extract/        # Data extraction modules
│   ├── load/           # Data loading functionality
│   ├── media/          # Media file handling
│   ├── simulate/       # Data simulation for testing
│   ├── transform/      # Data transformation modules
│   ├── utils/          # Utility functions (logging, face matching, etc.)
│   └── validation/     # Data validation
├── requirements.txt    # Python dependencies
├── run_scan.py        # Main execution script
└── hamdan.py          # Additional pipeline utilities
```

## Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DB_USER` | Database username | - | Yes |
| `DB_PASSWORD` | Database password | - | Yes |
| `DB_HOST` | Database host | localhost | Yes |
| `DB_NAME` | Database name | - | Yes |
| `REDIS_HOST` | Redis server host | localhost | Yes |
| `REDIS_PORT` | Redis server port | 6379 | Yes |
| `REDIS_DB` | Redis database number | 0 | Yes |
| `REDIS_PASSWORD` | Redis password (if configured) | - | No |
| `USER_DATA_CACHE_EXPIRATION` | Cache expiration for user data (seconds) | 3600 | Yes |
| `EXTRACTION_RESULTS_CACHE_EXPIRATION` | Cache expiration for results (seconds) | 1800 | Yes |
| `METADATA_CACHE_EXPIRATION` | Cache expiration for metadata (seconds) | 7200 | Yes |

### Cache Expiration Examples

- `1800` = 30 minutes
- `3600` = 1 hour
- `7200` = 2 hours
- `86400` = 24 hours

**Default Cache Durations:**
- User data: 1 hour (3600 seconds)
- Extraction results: 30 minutes (1800 seconds)  
- Metadata: 2 hours (7200 seconds)

## Dependencies

Key dependencies include:

- **Database**: SQLAlchemy (ORM), mysqlclient (MySQL driver)
- **Caching**: Redis
- **Media Processing**: OpenCV, Pillow, face-recognition
- **Audio Processing**: Whisper, moviepy
- **Utilities**: python-dotenv, phonenumbers, email-validator

See `requirements.txt` for the complete list with versions.

### Database Compatibility

The project uses **SQLAlchemy ORM** for database operations, which provides database abstraction. While currently configured and tested with **MySQL**, the ORM design allows for potential adaptation to other databases such as:

- PostgreSQL
- SQLite
- Microsoft SQL Server
- Oracle

**Current Implementation**: The project is specifically configured for MySQL with:
- MySQL connection string format
- MySQL-specific database creation logic
- `mysqlclient` driver dependency

**To use a different database**: You would need to:
1. Update the connection string format in `src/database/setup.py`
2. Install the appropriate database driver
3. Modify any database-specific SQL operations
4. Update the requirements.txt file

## Development

### Running in Simulation Mode

The project includes simulation capabilities to generate test data without requiring real external APIs:

```python
from src.simulate.world import SimulationWorld

# Generate simulated data
world = SimulationWorld()
world.create_users(count=5)
world.simulate_social_media_activity()
world.simulate_search_engine_results()
```

### Database Setup

The database will be automatically created when you first run the application. If you need to manually set up the database:

```python
from src.database.setup import DatabaseManager

# Initialize database
DatabaseManager.initialize()
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Ensure all tests pass
5. Submit a pull request

## License

[Add your license information here]

## Support

[Add support/contact information here] 