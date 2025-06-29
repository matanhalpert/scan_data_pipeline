"""
Base extractor module providing the abstract base class for all extractors.
"""
import json
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TypedDict
from datetime import datetime
from src.database.models import User
from src.database.setup import DatabaseManager
from src.cache.redis_manager import RedisManager
from src.config.enums import OperationStatus
from src.utils.logger import logger


EXTRACTION_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SIMULATION_DATA_PATH = os.path.join(
    os.path.dirname(EXTRACTION_DIR), 
    "simulate",
    "simulation_data", 
    "simulation_data.json"
)


class ExtractionResult(TypedDict):
    data: Optional[Dict[str, Any]]
    metadata: Dict[str, Any]


class BaseExtractor(ABC):
    """
    Abstract base class that defines the interface for all extractors.
    All concrete extractor implementations must inherit from this class.
    """

    def __init__(self, user_id: int, simulation_data_path: str = DEFAULT_SIMULATION_DATA_PATH):
        """
        Initialize the extractor with simulate data and user context.
        
        Args:
            user_id: The user's ID
            simulation_data_path: Path to the simulate data JSON file (defaults to project's simulate data)
        """
        self._simulation_data = self._load_simulation_data(simulation_data_path)
        self._user = self._get_user(user_id)
        self._user_identifiers = self._generate_user_identifiers()
        self._extraction_start_time = None
        self._extraction_end_time = None
        self._extraction_status = OperationStatus.NOT_STARTED
        self._error_message = None

    @staticmethod
    def _load_simulation_data(simulation_data_path: str) -> Dict[str, Any]:
        """
        Load simulate data from JSON file.
        
        Args:
            simulation_data_path: Path to the simulate data JSON file
            
        Returns:
            Dictionary containing the simulate data
            
        Raises:
            FileNotFoundError: If the simulate data file is not found
            json.JSONDecodeError: If the JSON file is malformed
        """
        try:
            with open(simulation_data_path, 'r', encoding='utf-8') as f:
                simulation_data = json.load(f)
            logger.info(f"Successfully loaded simulate data from {simulation_data_path}")
            return simulation_data
        except FileNotFoundError:
            logger.error(f"Simulation data file not found: {simulation_data_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in simulate data file: {e}")
            raise

    @staticmethod
    def _get_user(user_id: int) -> User:
        """
        Get user data from cache first, then from database if not cached.
        
        Args:
            user_id: The user's ID
            
        Returns:
            User: User model instance
            
        Raises:
            ValueError: If user is not found in cache or database
        """
        # First, try to get user from cache
        cached_user: Optional[User] = RedisManager.get_user(user_id)
        if cached_user:
            logger.debug(f"Retrieved user {user_id} from cache")
            return cached_user
        
        # If not in cache, get from database
        logger.debug(f"User {user_id} not found in cache, querying database")
        
        with DatabaseManager.get_session() as session:
            user = session.query(User).filter(User.id == user_id).first()
            if user is None:
                raise ValueError(f"User with id {user_id} not found")
            
            # Cache the user for future use
            try:
                RedisManager.set_user(user)
                logger.debug(f"Cached user {user_id} from database")
            except Exception as cache_error:
                logger.warning(f"Failed to cache user {user_id}: {cache_error}. Continuing without cache.")
            
            return user

    def _generate_user_identifiers(self) -> Dict[str, set]:
        """
        Generate all possible identifiers that could be used to find the user
        in various data sources (names, emails, phones, addresses, etc.).
        
        This method creates comprehensive search identifiers that can be used
        by any extractor to find traces of the user across different platforms.
        
        Returns:
            Dict containing different types of identifiers to search for:
            - names: Various name combinations and formats
            - emails: All associated email addresses  
            - phones: All associated phone numbers
            - addresses: Specific location identifiers for matching (combined components)
        """
        identifiers = {
            'names': set(),
            'emails': set(),
            'phones': set(),
            'addresses': set()
        }

        first_name = self._user.first_name.lower()
        last_name = self._user.last_name.lower()
        full_name = f"{first_name} {last_name}"
        
        identifiers['names'].update([
            full_name,
            f"{first_name}{last_name}",
            f"{last_name}{first_name}",
            f"{self._user.first_name} {self._user.last_name}",  # Original case full name
        ])
        
        # Emails (primary and secondary)
        identifiers['emails'].add(self._user.email.lower())
        for sec_email in self._user.secondary_emails:
            identifiers['emails'].add(sec_email.email.lower())
        
        # Phones (primary and secondary)
        if self._user.phone:
            identifiers['phones'].add(self._user.phone)
        for sec_phone in self._user.secondary_phones:
            identifiers['phones'].add(sec_phone.phone)

        for addr in self._user.addresses:
            # Only create address identifiers if we have sufficient components to be specific
            components = []
            
            # Add street number and street name if available
            if addr.street and addr.number:
                street_address = f"{addr.number} {addr.street}".lower()
                components.append(street_address)
            
            # Add city if available
            if addr.city:
                components.append(addr.city.lower())
            
            # Add country if available
            if addr.country:
                components.append(addr.country.lower())
            
            # Create specific address combinations to avoid false positives
            if len(components) >= 2:
                # Full address (most specific)
                if len(components) >= 3:
                    full_address = ", ".join(components)
                    identifiers['addresses'].add(full_address)
                
                # Street + City combination (medium specificity)
                if len(components) >= 2 and addr.street and addr.number and addr.city:
                    street_city = f"{addr.number} {addr.street}, {addr.city}".lower()
                    identifiers['addresses'].add(street_city)
                
                # City + Country combination (for broader matching but still specific)
                if addr.city and addr.country and len(components) >= 2:
                    city_country = f"{addr.city}, {addr.country}".lower()
                    identifiers['addresses'].add(city_country)
        
        return identifiers

    @staticmethod
    def _calculate_chunk_size(total_items: int) -> int:
        """
        Calculate optimal chunk size based on total items and available resources.
        
        Args:
            total_items: Total number of items to process
            
        Returns:
            Optimal chunk size for processing
        """
        if total_items <= 50:
            return total_items
        elif total_items <= 1000:
            return 100
        elif total_items <= 10000:
            return 500
        else:
            return 1000

    @staticmethod
    def _chunk_data(data: List[Any], chunk_size: int) -> List[List[Any]]:
        """
        Split data into chunks of specified size.
        
        Args:
            data: List of data to chunk
            chunk_size: Size of each chunk
            
        Returns:
            List of chunks
        """
        chunks = []
        for i in range(0, len(data), chunk_size):
            chunks.append(data[i:i + chunk_size])
        return chunks

    @abstractmethod
    def _extract_data(self) -> Dict[str, Any]:
        """
        Extract data from the source. This is the implementation-specific method
        that concrete extractors must implement.

        Returns:
            Dict[str, Any]: The extracted data
        """
        pass

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the extract process.

        Returns:
            Dict[str, Any]: Metadata about the extract including timing information
        """
        return {
            'user_id': self._user.id,
            'name': f"{self._user.first_name} {self._user.last_name}",
            'email': self._user.email,
            "extractor_class": self.__class__.__name__,
            "extraction_start_time": self._extraction_start_time.isoformat(),
            "extraction_end_time": self._extraction_end_time.isoformat(),
            "extraction_duration": (self._extraction_end_time - self._extraction_start_time).total_seconds()
            if (self._extraction_start_time and self._extraction_end_time) else None,
            "extraction_status": self._extraction_status,
            "error_message": self._error_message
        }

    def _start_extraction(self):
        """Record the start time of extract."""
        self._extraction_start_time = datetime.now()
        self._extraction_status = OperationStatus.IN_PROGRESS

    def _end_extraction(self, success: bool = True, error_message: Optional[str] = None):
        """
        Record the end time of extract and update status.

        Args:
            success: Whether the extract was successful
            error_message: Error message if extract failed
        """
        self._extraction_end_time = datetime.now()
        self._extraction_status = OperationStatus.COMPLETED if success else OperationStatus.FAILED
        self._error_message = error_message

    def extract(self, filename: Optional[str] = None, save_to_disk: bool = True) -> ExtractionResult:
        """Extract data from the source with metadata."""
        self._start_extraction()

        try:
            data = self._extract_data()
            self._end_extraction(success=True)

            extraction_result = ExtractionResult(
                metadata=self.get_metadata(),
                data=data
            )

            if save_to_disk:
                output_path = os.path.join(EXTRACTION_DIR, "extraction_data")
                os.makedirs(output_path, exist_ok=True)

                # Export all data to a single JSON file with the same structure
                if filename:
                    output_file = os.path.join(output_path, f"{filename}.json")
                elif self.__class__.__name__.lower() == 'unifiedextractor':
                    output_file = os.path.join(output_path, f"extraction_result.json")
                else:
                    extractor_name = self.__class__.__name__.lower()
                    output_file = os.path.join(output_path, f"{extractor_name}_result.json")

                with open(output_file, "w") as f:
                    json.dump(extraction_result, f, indent=2)

                logger.info(f"Exported {self.__class__.__name__} extract data to {output_file}")

            return extraction_result
        except Exception as e:
            self._end_extraction(success=False, error_message=str(e))
            raise
