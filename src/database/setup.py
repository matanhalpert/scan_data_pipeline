import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from src.database.models import Base
from src.utils.logger import logger


# Load environment variables from .env file
load_dotenv()


class DatabaseManager:
    _instance = None
    _engine = None
    _session_factory = None

    @classmethod
    def initialize(cls):
        """Initialize database connection if not already initialized."""
        if cls._engine is None:
            cls.validate_environment()
            cls.ensure_database_exists()
            cls._engine = cls.create_database_engine()
            cls._session_factory = sessionmaker(bind=cls._engine)
        return cls._session_factory

    @staticmethod
    def validate_environment() -> None:
        """Validates that all required environment variables are present."""
        required_vars = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_NAME']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    @classmethod
    def ensure_database_exists(cls):
        """Creates the target database if it doesn't already exist."""
        DB_USER = os.getenv('DB_USER')
        DB_PASSWORD = os.getenv('DB_PASSWORD')
        DB_HOST = os.getenv('DB_HOST')
        DB_NAME = os.getenv('DB_NAME')
        ROOT_DATABASE_URL = f"mysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}"
        
        temp_engine = create_engine(ROOT_DATABASE_URL)
        try:
            with temp_engine.connect() as connection:
                connection = connection.execution_options(isolation_level="AUTOCOMMIT")
                connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
                logger.info(f"Database '{DB_NAME}' ensured to exist")
        except SQLAlchemyError as e:
            logger.error(f"Error creating database '{DB_NAME}': {e}")
            raise
        finally:
            temp_engine.dispose()

    @classmethod
    def create_database_engine(cls):
        """Creates and tests the main database engine with connection pooling."""
        DB_USER = os.getenv('DB_USER')
        DB_PASSWORD = os.getenv('DB_PASSWORD')
        DB_HOST = os.getenv('DB_HOST')
        DB_NAME = os.getenv('DB_NAME')
        DATABASE_URL = f"mysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
        
        engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,  # Validates connections before use
            pool_recycle=300,  # Recycle connections every 5 minutes
            echo=False  # Set to True for SQL query debugging
        )

        try:
            # Test the connection
            with engine.connect():
                pass
            logger.info(f"Successfully connected to database '{DB_NAME}'")
            return engine
        except OperationalError as e:
            logger.error(f"Error connecting to database '{DB_NAME}': {e}")
            raise

    @classmethod
    def get_session(cls):
        """Get a session factory, initializing the database connection if needed."""
        if cls._session_factory is None:
            cls.initialize()
        return cls._session_factory()

    @classmethod
    def create_tables(cls) -> None:
        """Creates all database tables defined in the imported models."""
        if cls._engine is None:
            cls.initialize()
        
        try:
            Base.metadata.create_all(cls._engine)
            logger.info("Database tables created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Error creating tables: {e}")
            raise

    @classmethod
    def drop_tables(cls) -> None:
        """Drops all database tables after explicit user confirmation."""
        if cls._engine is None:
            cls.initialize()

        logger.warning("WARNING: This will permanently delete all data in the database!")
        confirm = input("Are you sure you want to drop all tables? Type 'yes' to confirm: ")

        if confirm.strip().lower() == "yes":
            try:
                Base.metadata.drop_all(cls._engine)
                logger.info("All tables dropped successfully")
            except SQLAlchemyError as e:
                logger.error(f"Error dropping tables: {e}")
        else:
            logger.info("Drop tables operation cancelled by user")

    @classmethod
    def cleanup(cls):
        """Properly disposes of the database engine and its connection pool."""
        if cls._engine:
            cls._engine.dispose()
            cls._engine = None
            cls._session_factory = None
            logger.info("Database engine disposed")


# Expose the session getter for backwards compatibility
def get_session():
    return DatabaseManager.get_session()


# For backwards compatibility
Session = get_session


if __name__ == "__main__":
    try:
        DatabaseManager.create_tables()
        # DatabaseManager.drop_tables()     # CAUTION!
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        exit(1)
    finally:
        DatabaseManager.cleanup()
