# ETL Pipeline: Digital Footprint Analysis System

A comprehensive data engineering pipeline that demonstrates the integration of simulation, extraction, transformation, and loading processes for digital footprint analysis. This project showcases key data engineering concepts through a modular, extensible architecture.

## 🎯 Project Purpose

This is an **ETL pipeline**  which was built to solve a real world problem - scanning and keeping track of our unwanted digital footprints across the web. Also, this project was created in order to explore and demonstrate how different components and tools can be composed into a complete, working system.
The focus is on **learning** and **experimentation**, rather than production deployment.

## 🌐 What Is a Digital Footprint?

A **digital footprint** refers to the traceable data left behind by a person’s online activity. This can include personal information, media files, links to public sources, or references from third-party platforms. In this system, each digital footprint is structured as a record that may include:

- **Personal identity clues** such as names, emails, or photo
- **Media references**, including file paths or URLs pointing to external content
- **Source metadata**, like the origin of the data and its verification status
- **Activity logs**, capturing when the data was found

## 🏗️ Core Concepts Demonstrated

This pipeline implements several key data engineering patterns:

- **ETL Architecture**: Clear separation of Extract, Transform, Load phases
- **Concurrent Processing**: Async/await patterns for improved throughput
- **Batch Processing**: Process data in chunks to improve efficiency and resource usage
- **ORM Usage**: Complex relationships with SQLAlchemy
- **Caching Layer**: Redis integration for performance optimization
- **File & Media Handling**: A file management layer for efficient and reliable interactions with files
- **Configurable Components**: Flexible design allowing future scalability
- **Error Handling**: Comprehensive logging and recovery mechanisms

Bonus Features: Media-Aware Capabilities
- **Face Detection**: Integrates computer vision techniques to identify faces in images and video frames
- **Video Audio Transcription**: Uses speech to text processing to extract spoken content from videos, enabling keyword analysis and user identity insights

## 🗄️ Data Model

The pipeline is based on a comprehensive data model that captures digital footprints and their relationships - those are shown in the following ERD (Entity Relationship Diagram):

![ETL Pipeline ERD](ETL%20pipeline%20scan%20ERD.png)

## 📊 Pipeline Flow

![ETL Pipeline Flow Chart](ETL%20pipeline%20scan%20Flow%20chart.png)

### 1. **Simulation Phase**
Generates synthetic data across two domains: social media platforms and search engines.

```python
from src.simulate.world import SimulationWorld
from src.database.models import User

# Create simulation environment with context manager
with SimulationWorld(base_users_count=100, unique_users=[test_user]) as world:
    # Data generation happens automatically on context entry
    print(f"Generated data for {world.get_total_population()} users")
    
    # Export structured data for pipeline processing
    world.export_data(save_to_disk=True)
```

**Key Components:**
- Multi-platform social media simulation (Facebook, Instagram, LinkedIn, X)
- Search engine result generation (Google, Yahoo, Bing)
- Configurable user populations and content distributions
- Probabilistic content generation with realistic patterns

### 2. **Extraction Phase**
Gathers relevant simulated data and structures it into domain-specific JSON formats.

```python
from src.extract.unified_extractor import UnifiedExtractor

# Initialize extractor with user context
extractor = UnifiedExtractor(user_id=user.id)

# Concurrent extraction from all sources
extraction_result = extractor.extract()

# Access processing metadata
metadata = extractor.get_metadata()
print(f"Extraction status: {metadata.get('extraction_status')}")
```

**Features:**
- Concurrent processing of social media and search engine data
- Unified data structure with consistent JSON schemas
- Source discovery and content categorization
- Metadata tracking for pipeline monitoring

### 3. **Transformation Phase**
Analyzes and converts extracted data into model instances with advanced processing logic.

```python
from src.transform.unified_transformer import UnifiedTransformer

# Initialize transformer
transformer = UnifiedTransformer(user_id=user.id)

# Transform with concurrent processing
transformation_result = transformer.transform()

# Get detailed processing summary
summary = transformer.get_detailed_summary()
print(f"Created {len(transformation_result.new_digital_footprints)} digital footprints")
```

**Advanced Processing:**
- **Face Recognition**: Identity matching across media files
- **Video Transcription**: Whisper-powered audio content analysis
- **Deduplication**: Intelligent duplicate detection and consolidation
- **Domain Modeling**: Conversion to `DigitalFootprint`, `PersonalIdentity`, `ActivityLog` models
- **Identity Detection**: Cross-platform user identification

### 4. **Load Phase**
Persists digital footprints, personal identities, and activity logs into the database with relationship management.

```python
from src.load.load import Loader

# Initialize loader
loader = Loader(user_id=user.id)

# Efficient database persistence
load_result = loader.load(transformation_result)

# Get load summary
summary = loader.load_summary()
print(f"Inserted {summary.get('total_records_inserted', 0)} records")
```

**Database Operations:**
- Automatic relationship linking (`UserDigitalFootprint` associations)
- Transaction management with rollback capabilities
- Comprehensive audit trails
- Error handling

## 🚀 Complete Pipeline Usage

```python
from run_scan import run_scan

# Execute the full ETL pipeline
results = run_scan(
    test_num=1,
    base_users_count=50,  # Adjust based on testing needs
    profile_image_path="src/media/images/mock_image.png"
)

# Pipeline automatically coordinates:
# 1. User creation and database setup
# 2. Simulation world generation
# 3. Concurrent extraction from all sources  
# 4. Advanced transformation with media analysis
# 5. Database loading with relationship management

print(f"Pipeline completed: {results['pipeline_success']}")
print(f"Records inserted: {results['load']['total_records_inserted']}")
```

## 🛠️ Technologies Used

### Core Infrastructure
- **Python 3.12+**: Modern Python with async/await support
- **SQLAlchemy**: ORM with MySQL backend
- **Redis**: High-performance caching layer
- **AsyncIO**: Concurrent processing framework

### Data Processing & Analysis
- **OpenCV**: Computer vision and image processing
- **face-recognition**: Facial recognition capabilities
- **Whisper**: State-of-the-art audio transcription
- **MoviePy**: Video processing and analysis
- **Pillow**: Image manipulation and processing

### Validation & Utilities
- **phonenumbers**: Phone number validation and formatting
- **email-validator**: Email format validation
- **python-dateutil**: Advanced date/time processing
- **python-dotenv**: Environment configuration management

## 📁 Project Structure

```
data_pipeline/
├── src/
│   ├── simulate/           # Data generation and simulation
│   │   ├── world.py       # Main simulation orchestrator  
│   │   ├── social_media.py # Social platform simulators
│   │   └── search_engines.py # Search result generators
│   ├── extract/           # Data extraction layer
│   │   ├── unified_extractor.py # Main extraction coordinator
│   │   ├── social_media_extractor.py # Social platform extraction
│   │   └── search_results_extractor.py # Search engine extraction
│   ├── transform/         # Data transformation layer
│   │   ├── unified_transformer.py # Transformation orchestrator
│   │   ├── social_media_transformer.py # Social data processing
│   │   └── search_engine_transformer.py # Search data processing
│   ├── load/              # Database persistence layer
│   │   └── load.py       # Database loading operations
│   ├── database/          # Data models and setup
│   │   ├── models.py     # SQLAlchemy domain models
│   │   └── setup.py      # Database configuration
│   ├── cache/             # Caching infrastructure
│   │   └── redis_manager.py # Redis operations
│   ├── media/             # Media processing utilities
│   │   ├── files_management.py # File operations
│   │   └── media_pool.py # Media resource management
│   ├── utils/             # Processing utilities
│   │   ├── face_matching.py # Face recognition logic
│   │   ├── transcription.py # Audio/video processing
│   │   └── logger.py     # Centralized logging
│   └── validation/        # Data quality assurance
│       └── validation.py # Multi-layer validation
├── run_scan.py           # Main pipeline orchestrator
└── requirements.txt      # Project dependencies
```

## ⚙️ Setup & Installation

### Prerequisites
- Python 3.12+
- MySQL 8.0+
- Redis 6.0+

### Installation Steps
```bash
# Clone the repository
git clone <repository-url>
cd data_pipeline

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
# Edit .env with your database and Redis configurations

# Run the pipeline
python run_scan.py
```

### Environment Configuration
```bash
# Database Configuration
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_NAME=data_pipeline_db

# Redis Configuration  
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Cache TTL Settings (in seconds)
USER_DATA_CACHE_EXPIRATION=3600
EXTRACTION_RESULTS_CACHE_EXPIRATION=1800
METADATA_CACHE_EXPIRATION=7200
```

## 🔍 Development Notes

### Current Status
- **Active Development**: This project is continuously evolving
- **Learning Focus**: Designed for experimentation and concept demonstration
- **Modular Architecture**: Easy to extend and modify individual components
- **No Production Claims**: Built for educational purposes and personal development

### Design Philosophy
- **Clean Architecture**: Clear separation of concerns across pipeline phases
- **Extensibility**: Modular design allows for easy component replacement
- **Efficiency**: Emphasis on batch processing, concurrent execution, and caching strategies to reduce unnecessary operations and improve runtime performance without adding unnecessary complexity.
- **Observability**: Comprehensive logging and metadata tracking
- **Error Handling**: Robust error recovery and transaction management

### Future Enhancements
- Additional social media platform simulators
- Enhanced media analysis capabilities
- Performance optimization and monitoring
- Extended validation and data quality checks
- tests/ directory containing unit tests as well as end-to-end scan phase tests.

---