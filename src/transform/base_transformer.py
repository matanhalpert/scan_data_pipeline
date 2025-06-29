"""
Base transformer module providing the abstract base class for all transformers.
"""
import asyncio
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TypedDict
from urllib.parse import urlparse

from src.cache.redis_manager import RedisManager
from src.config.enums import (
    Confidence, DigitalFootprintType, ImageSuffix, OperationStatus, 
    PersonalIdentityType, PostType, SearchEngine, SearchResultType, 
    SocialMediaPlatform, SourceCategory, VideoSuffix
)
from src.database.models import ActivityLog, DigitalFootprint, PersonalIdentity, Source, User
from src.database.setup import DatabaseManager
from src.utils.face_matching import FaceMatcher
from src.utils.logger import logger
from src.utils.transcription import Transcriptor


TRANSFORMATION_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_EXTRACTION_DATA_PATH = os.path.join(
    os.path.dirname(TRANSFORMATION_DIR), 
    "extract",
    "extraction_data", 
    "extraction_result.json"
)


class TransformationError(Exception):
    """Custom Error for transform process."""


class MediaAnalysisResult(TypedDict):
    face_match_found: bool
    face_match_confidence: Optional[Confidence]
    transcription: Optional[str]
    identities_detected: list[PersonalIdentityType]


class TransformationResult:
    """Container for transform results."""
    
    def __init__(self):
        self.new_digital_footprints: List[DigitalFootprint] = []
        self.personal_identities: List[PersonalIdentity] = []
        self.activity_logs: List[ActivityLog] = []
        # Map digital footprint reference URLs to lists of identity types that should be created
        self.pending_identities: Dict[str, List[PersonalIdentityType]] = {}
        # Map digital footprint reference URLs to lists of timestamps for activity logs to be created
        self.pending_activity_logs: Dict[str, List[datetime]] = {}
        self.processing_stats = {
            'items_processed': 0,
            'footprints_found': 0,
            'new_footprints': 0,
            'existing_footprints': 0,
            'identities_detected': 0,
            'media_files_processed': 0,
            'videos_transcribed': 0,
            'face_matches_found': 0
        }


class BaseTransformer(ABC):
    """Abstract base class that defines the interface for all transformers."""

    def __init__(self, user_id: int, extraction_data_path: str = DEFAULT_EXTRACTION_DATA_PATH):
        """Initialize the transformer with extract data and user context."""
        self._extraction_data = self._load_extraction_data(extraction_data_path)
        self._user = self._get_user(user_id)
        self._transformation_start_time = None
        self._transformation_end_time = None
        self._transformation_status = OperationStatus.NOT_STARTED
        self._error_message = None
        
        # Initialize utilities
        self._face_matcher = FaceMatcher(tolerance=0.6)
        self._transcriptor = Transcriptor()
        
        # Get user's reference photo for face matching
        self._user_reference_photo = self._get_user_reference_photo()

    @staticmethod
    def _load_extraction_data(extraction_data_path: str) -> Dict[str, Any]:
        """Load extract data from JSON file."""
        try:
            with open(extraction_data_path, 'r', encoding='utf-8') as f:
                extraction_data = json.load(f)
            logger.info(f"Successfully loaded extract data from {extraction_data_path}")
            return extraction_data
        except FileNotFoundError:
            logger.error(f"Extraction data file not found: {extraction_data_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in extract data file: {e}")
            raise

    @staticmethod
    def _get_user(user_id: int) -> User:
        """Get user data from cache first, then from database if not cached."""
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

    def _get_user_reference_photo(self) -> Optional[str]:
        """Get the user's reference photo path for face matching."""
        if self._user.pictures:
            # Use the first available picture as reference
            return self._user.pictures[0].path
        return None

    @staticmethod
    def _extract_domain_from_url(url: str) -> str:
        """Extract domain from URL for source identification."""
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc.lower()
            # Remove 'www.' prefix if present
            if domain.startswith('www.'):
                domain = domain[4:]
            # Return unknown.com if domain is empty or invalid
            return domain if domain else "unknown.com"
        except Exception as e:
            logger.warning(f"Failed to extract domain from URL {url}: {e}")
            return "unknown.com"    # consider None!!!!!!!!!!!!!!!!!!!!!!!!!

    @staticmethod
    def _construct_media_filepath(url: str, footprint_type: DigitalFootprintType) -> Optional[str]:
        """Construct media filepath mapping to actual mock files based on file extension."""
        try:
            parsed_url = urlparse(url)
            
            # Check if it's actually a media URL first
            if footprint_type == DigitalFootprintType.TEXT:
                return None
                
            # Extract file extension from URL path
            path = Path(parsed_url.path)
            file_extension = path.suffix.lower()
            
            # Map to appropriate mock file based on type and extension
            if footprint_type == DigitalFootprintType.IMAGE:
                if ImageSuffix.has_value(file_extension):
                    # Map to corresponding mock image
                    mock_filename = f"mock_image{file_extension}"
                    return f"src/media/images/{mock_filename}"
                else:
                    # Default to most common image format
                    return "src/media/images/mock_image.jpg"
                    
            elif footprint_type == DigitalFootprintType.VIDEO:
                if VideoSuffix.has_value(file_extension):
                    # Map to corresponding mock video
                    mock_filename = f"mock_video{file_extension}"
                    return f"src/media/videos/{mock_filename}"
                else:
                    # Default to most common video format
                    return "src/media/videos/mock_video.mp4"
                    
            elif footprint_type == DigitalFootprintType.AUDIO:
                # For audio, always use a generic mock file (we don't have audio-specific mocks)
                return "src/media/audios/mock_audio.mp3"  # Note: This path may not exist
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to construct media filepath for URL {url}: {e}")
            return None

    @classmethod
    def _get_or_create_source_by_url(cls, url: str) -> Tuple[Source, bool]:
        """
        Get existing source from cache/DB or create a new one based on URL domain.
        
        Args:
            url: The URL to extract domain from for source identification
            
        Returns:
            Tuple[Source, bool]: (source, is_new)
        """
        domain = cls._extract_domain_from_url(url)
        
        # First check cache
        cached_source = RedisManager.get_source_by_url(domain)
        if cached_source:
            logger.debug(f"Found existing source in cache: {domain}")
            return cached_source, False
        
        # Check database
        with DatabaseManager.get_session() as session:
            existing_source = session.query(Source).filter(Source.url == domain).first()
            
            if existing_source:
                # Cache for future use
                try:
                    RedisManager.set_source(existing_source)
                    logger.debug(f"Found existing source in DB and cached: {domain}")
                except Exception as cache_error:
                    logger.warning(f"Failed to cache source {domain}: {cache_error}")
                return existing_source, False
        
        # Create new source
        # Determine category based on domain
        verified = True
        if domain in [f"{search_engine}.com" for search_engine in SearchEngine]:
            category = SourceCategory.PROFESSIONAL
        elif domain in ['linkedin.com']:
            category = SourceCategory.PROFESSIONAL
        elif domain in [f"{platform}.com" for platform in SocialMediaPlatform]:
            category = SourceCategory.SOCIAL_MEDIA
        else:
            category = SourceCategory.PERSONAL
            verified = False
        
        new_source = Source(
            name=domain.replace('.com', '').capitalize(),
            url=domain,
            category=category,
            verified=verified
        )
        
        # Save to database and cache
        with DatabaseManager.get_session() as session:
            session.add(new_source)
            session.commit()
            session.refresh(new_source)
            
            # Cache the new source
            try:
                RedisManager.set_source(new_source)
                logger.debug(f"Created and cached new source: {domain}")
            except Exception as cache_error:
                logger.warning(f"Failed to cache new source {domain}: {cache_error}")
        
        logger.debug(f"Created new source: {domain}")
        return new_source, True

    @classmethod
    def _get_or_create_digital_footprint(
            cls,
            reference_url: str,
            footprint_type: DigitalFootprintType = DigitalFootprintType.TEXT,
            media_url: Optional[str] = None
    ) -> Tuple[DigitalFootprint, bool]:
        """
        Get existing digital footprint from cache/DB or create a new one.
        Automatically constructs media_filepath and determines source_id from URL.
        
        Args:
            reference_url: The reference URL of the footprint
            footprint_type: Type of digital footprint
            media_url: Optional URL to use for media file path construction
                      (if different from reference_url, e.g., for profile pictures)
            
        Returns:
            Tuple[DigitalFootprint, bool]: (footprint, is_new)
        """
        # Use media_url for media file path construction if provided, otherwise use reference_url
        url_for_media = media_url if media_url else reference_url
        media_filepath = cls._construct_media_filepath(url_for_media, footprint_type)
        
        # First check cache
        cached_footprint = RedisManager.get_digital_footprint(reference_url, media_filepath)
        if cached_footprint:
            logger.debug(f"Found existing footprint in cache: {reference_url}")
            return cached_footprint, False
        
        # Check database
        with DatabaseManager.get_session() as session:
            existing_footprint = session.query(DigitalFootprint).filter(
                DigitalFootprint.reference_url == reference_url,
                DigitalFootprint.media_filepath == media_filepath
            ).first()
            
            if existing_footprint:
                # Cache for future use
                try:
                    RedisManager.set_digital_footprint(existing_footprint)
                    logger.debug(f"Found existing footprint in DB: {reference_url}")
                except Exception as cache_error:
                    logger.warning(f"Failed to cache existing footprint: {cache_error}")
                return existing_footprint, False
        
        # Get or create source based on URL
        source, _ = cls._get_or_create_source_by_url(reference_url)
        
        # Create new footprint
        new_footprint = DigitalFootprint(
            type=footprint_type,
            media_filepath=media_filepath,
            reference_url=reference_url,
            source_id=source.id
        )
        logger.debug(f"Created new footprint: {reference_url}")
        return new_footprint, True

    def _determine_footprint_type(self, item: Dict[str, Any]) -> DigitalFootprintType:
        """
        Determine the digital footprint type based on the item data.
        
        Args:
            item: The data item from extract
            
        Returns:
            DigitalFootprintType: The determined footprint type
        """
        # Check if it's a social media post with specific type
        if 'post_type' in item:
            post_type = item.get('post_type')
            if post_type == PostType.IMAGE:
                return DigitalFootprintType.IMAGE
            elif post_type == PostType.VIDEO:
                return DigitalFootprintType.VIDEO
            else:
                return DigitalFootprintType.TEXT
        
        # Check if it's a search result with specific type
        if 'result_type' in item:
            result_type = item.get('result_type')
            if result_type == SearchResultType.IMAGE:
                return DigitalFootprintType.IMAGE
            elif result_type == SearchResultType.VIDEO:
                return DigitalFootprintType.VIDEO
            else:
                return DigitalFootprintType.TEXT
        
        # Check media filepath extension
        media_filepath = item.get('url', '') or item.get('profile_picture_url', '')
        if media_filepath:
            ext = Path(media_filepath).suffix.lower()
            if ext in [suffix for suffix in ImageSuffix]:
                return DigitalFootprintType.IMAGE
            elif ext in [suffix for suffix in VideoSuffix]:
                return DigitalFootprintType.VIDEO
        
        return DigitalFootprintType.TEXT

    def _analyze_media(self, media_filepath: str, footprint_type: DigitalFootprintType) -> MediaAnalysisResult:
        """
        Analyze media file for face matching and transcription if needed.
        
        Args:
            media_filepath: Path to the media file
            footprint_type: Type of digital footprint
            
        Returns:
            Dict containing analysis results
        """
        analysis_result = MediaAnalysisResult(
            face_match_found=False,
            face_match_confidence=None,
            transcription=None,
            identities_detected=[]
        )

        if not self._user_reference_photo:
            logger.warning("User has no reference photo - skipping media analysis")
            return analysis_result

        if not media_filepath:
            logger.warning("Cannot analyze media, missing media_filepath")
            return analysis_result
        
        # Check if the media file actually exists (resolve relative to project root)
        project_root = os.path.dirname(os.path.dirname(TRANSFORMATION_DIR))
        absolute_media_path = os.path.join(project_root, media_filepath)
        if not Path(absolute_media_path).exists():
            logger.warning(f"Media file '{media_filepath}' does not exist - skipping analysis")
            return analysis_result
        
        # Check if the reference photo exists (resolve relative to project root)
        absolute_reference_path = os.path.join(project_root, self._user_reference_photo)
        if not Path(absolute_reference_path).exists():
            logger.warning(f"Reference photo '{self._user_reference_photo}' does not exist - skipping analysis")
            return analysis_result
        
        try:
            if footprint_type == DigitalFootprintType.IMAGE:
                # Perform face matching on image with additional safety
                logger.debug(f"Starting face matching for image: {media_filepath}")
                try:
                    match_result = self._face_matcher.match_faces_image(
                        absolute_reference_path, absolute_media_path
                    )
                    analysis_result['face_match_found'] = match_result['is_match']
                    analysis_result['face_match_confidence'] = match_result['confidence']
                    
                    if match_result['is_match']:
                        analysis_result['identities_detected'].append(PersonalIdentityType.PICTURE)
                        
                except Exception as face_error:
                    logger.warning(f"Face matching failed for image {media_filepath}: {face_error}")
                    
            elif footprint_type == DigitalFootprintType.VIDEO:
                # Perform face matching on video with additional safety
                logger.debug(f"Starting face matching for video: {media_filepath}")
                try:
                    match_result = self._face_matcher.match_faces_video(
                        absolute_reference_path, absolute_media_path
                    )
                    analysis_result['face_match_found'] = match_result['is_match']
                    analysis_result['face_match_confidence'] = match_result['confidence']
                    
                    if match_result['is_match']:
                        analysis_result['identities_detected'].append(PersonalIdentityType.PICTURE)
                        
                        # If face match found, transcribe the video
                        logger.debug(f"Starting transcription for video: {media_filepath}")
                        try:
                            transcription = self._transcriptor.transcribe_video(absolute_media_path)
                            analysis_result['transcription'] = transcription
                            
                            # Analyze transcription for user identifiers
                            text_identities = self._analyze_text_for_identities(transcription)
                            analysis_result['identities_detected'].extend(text_identities)
                            
                        except Exception as transcription_error:
                            logger.warning(f"Failed to transcribe video {media_filepath}: {transcription_error}")
                            
                except Exception as face_error:
                    logger.warning(f"Face matching failed for video {media_filepath}: {face_error}")
                        
        except Exception as e:
            logger.warning(f"Failed to analyze media {media_filepath}: {e}")
        
        return analysis_result

    def _analyze_text_for_identities(self, text: str) -> List[PersonalIdentityType]:
        """
        Analyze text content for user personal identities.
        
        Args:
            text: Text content to analyze
            
        Returns:
            List[PersonalIdentityType]: List of detected identity types
        """
        if not text:
            return []
        
        text_lower = text.lower()
        identities_found = []
        
        # Check for names
        full_name = f"{self._user.first_name} {self._user.last_name}".lower()
        if (full_name in text_lower or self._user.first_name.lower() in text_lower or
                self._user.last_name.lower() in text_lower):
            identities_found.append(PersonalIdentityType.NAME)
        
        # Check for phone numbers
        if self._user.phone and self._user.phone in text:
            identities_found.append(PersonalIdentityType.PHONE)
        
        for sec_phone in self._user.secondary_phones:
            if sec_phone.phone in text:
                identities_found.append(PersonalIdentityType.PHONE)
                break
        
        # Check for addresses
        for address in self._user.addresses:
            address_components = []
            if address.street:
                address_components.append(address.street.lower())
            if address.city:
                address_components.append(address.city.lower())
            if address.country:
                address_components.append(address.country.lower())
            
            # Check if any address component appears in text
            for component in address_components:
                if component in text_lower:
                    identities_found.append(PersonalIdentityType.ADDRESS)
                    break
            
            if PersonalIdentityType.ADDRESS in identities_found:
                break
        
        return identities_found

    @staticmethod
    def _create_activity_log(digital_footprint: DigitalFootprint, timestamp: datetime = None) -> ActivityLog:
        """
        Create an activity log entry for a digital footprint.
        
        Args:
            digital_footprint: The digital footprint to log
            timestamp: Optional timestamp, defaults to current time
            
        Returns:
            ActivityLog: The created activity log entry
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        return ActivityLog(
            digital_footprint_id=digital_footprint.id,
            timestamp=timestamp
        )

    @staticmethod
    def _track_pending_identity(
            result: TransformationResult,
            digital_footprint: DigitalFootprint,
            identity_type: PersonalIdentityType
    ) -> bool:
        """
        Track a pending identity for later persistence in the load phase.
        This method replaces creating PersonalIdentity objects during transform.
        
        Args:
            result: TransformationResult to track the pending identity in
            digital_footprint: The digital footprint to associate the identity with
            identity_type: Type of personal identity to track
            
        Returns:
            bool: True if this is a new identity type for this footprint, False if duplicate
        """
        reference_url = digital_footprint.reference_url
        
        # Initialize the list if this is the first identity for this footprint
        if reference_url not in result.pending_identities:
            result.pending_identities[reference_url] = []
        
        # Check if this identity type is already tracked for this footprint
        if identity_type in result.pending_identities[reference_url]:
            return False  # Duplicate, don't count it
        
        # Add the new identity type
        result.pending_identities[reference_url].append(identity_type)
        return True  # New identity type

    @staticmethod
    def _track_pending_activity_log(
            result: TransformationResult,
            digital_footprint: DigitalFootprint,
            timestamp: datetime = None
    ) -> bool:
        """
        Track a pending activity log for later persistence in the load phase.
        This method replaces creating ActivityLog objects during transformation.
        
        Args:
            result: TransformationResult to track the pending activity log in
            digital_footprint: The digital footprint to associate the activity log with
            timestamp: Optional timestamp, defaults to current time
            
        Returns:
            bool: True if this is a new activity log for this footprint, False if duplicate
        """
        reference_url = digital_footprint.reference_url
        
        if timestamp is None:
            timestamp = datetime.now()
        
        # Initialize the list if this is the first activity log for this footprint
        if reference_url not in result.pending_activity_logs:
            result.pending_activity_logs[reference_url] = []
        
        # Check if this timestamp is already tracked for this footprint (avoid duplicates)
        if timestamp in result.pending_activity_logs[reference_url]:
            return False  # Duplicate, don't count it
        
        # Add the new timestamp
        result.pending_activity_logs[reference_url].append(timestamp)
        return True  # New activity log

    @staticmethod
    def _get_or_create_personal_identity(
            digital_footprint_id: int,
            identity_type: PersonalIdentityType
    ) -> Tuple[PersonalIdentity, bool]:
        """
        Get existing personal identity from cache/DB or create a new one.
        Implements cache-first deduplication strategy.
        
        Args:
            digital_footprint_id: The digital footprint ID
            identity_type: Type of personal identity
            
        Returns:
            Tuple[PersonalIdentity, bool]: (personal_identity, is_new)
        """
        identity_value = identity_type.value
        
        # First check cache
        cached_identity = RedisManager.get_personal_identity(digital_footprint_id, identity_value)
        if cached_identity:
            logger.debug(f"Found existing PersonalIdentity in cache: {digital_footprint_id}:{identity_value}")
            return cached_identity, False
        
        # Check database
        from src.database.setup import DatabaseManager
        with DatabaseManager.get_session() as session:
            existing_identity = session.query(PersonalIdentity).filter(
                PersonalIdentity.digital_footprint_id == digital_footprint_id,
                PersonalIdentity.personal_identity == identity_type
            ).first()
            
            if existing_identity:
                # Cache for future use
                try:
                    RedisManager.set_personal_identity(existing_identity)
                    logger.debug(f"Found existing PersonalIdentity in DB and cached: {digital_footprint_id}:{identity_value}")
                except Exception as cache_error:
                    logger.warning(f"Failed to cache existing PersonalIdentity: {cache_error}")
                return existing_identity, False
        
        # Create new identity
        new_identity = PersonalIdentity(
            digital_footprint_id=digital_footprint_id,
            personal_identity=identity_type
        )
        
        # Cache the new identity
        try:
            RedisManager.set_personal_identity(new_identity)
            logger.debug(f"Created and cached new PersonalIdentity: {digital_footprint_id}:{identity_value}")
        except Exception as cache_error:
            logger.warning(f"Failed to cache new PersonalIdentity: {cache_error}")
        
        return new_identity, True

    @staticmethod
    def _calculate_chunk_size(total_items: int) -> int:
        """
        Calculate optimal chunk size based on total items and available resources.
        Uses more conservative chunking to avoid overwhelming the system.
        
        Args:
            total_items: Total number of items to process
            
        Returns:
            Optimal chunk size for processing
        """
        min_chunk_size = 10
        max_chunk_size = 50
        
        try:
            cpu_count = os.cpu_count() or 4
            # Use more conservative concurrency - fewer concurrent batches
            optimal_chunks = min(cpu_count, 8)  # Cap at 8 concurrent batches max
            calculated_size = max(min_chunk_size, total_items // optimal_chunks)
            return min(max_chunk_size, calculated_size)
        except:
            return min_chunk_size if total_items < 50 else 20

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
        return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    async def _process_batch(self, items: List[Dict[str, Any]]) -> TransformationResult:
        """
        Process a batch of items sequentially.
        Note: Batches run concurrently, but items within each batch are processed sequentially.
        
        Args:
            items: List of items to process
            
        Returns:
            TransformationResult: Results from processing the batch
        """
        result = TransformationResult()
        
        # Process items sequentially within this batch
        for item in items:
            try:
                item_result = self._process_item(item)
                
                # Merge results
                result.new_digital_footprints.extend(item_result.new_digital_footprints)
                result.personal_identities.extend(item_result.personal_identities)
                result.activity_logs.extend(item_result.activity_logs)
                
                # Merge pending identities
                for reference_url, identity_types in item_result.pending_identities.items():
                    if reference_url not in result.pending_identities:
                        result.pending_identities[reference_url] = []
                    result.pending_identities[reference_url].extend(identity_types)
                    # Remove duplicates
                    result.pending_identities[reference_url] = list(set(result.pending_identities[reference_url]))
                
                # Merge pending activity logs
                for reference_url, timestamps in item_result.pending_activity_logs.items():
                    if reference_url not in result.pending_activity_logs:
                        result.pending_activity_logs[reference_url] = []
                    result.pending_activity_logs[reference_url].extend(timestamps)
                    # Remove duplicates (though unlikely for timestamps)
                    result.pending_activity_logs[reference_url] = list(set(result.pending_activity_logs[reference_url]))
                
                # Update stats
                for key, value in item_result.processing_stats.items():
                    result.processing_stats[key] += value
                    
            except Exception as e:
                logger.error(f"Error processing item in batch: {e}")
                continue
        
        return result

    @abstractmethod
    def _process_item(self, item: Dict[str, Any]) -> TransformationResult:
        """
        Process a single item from the extract data.
        This is the implementation-specific method that concrete transformers must implement.
        
        Args:
            item: Single item from extract data
            
        Returns:
            TransformationResult: Results from processing the item
        """
        pass

    async def _process_all_batches(self, items: List[Dict[str, Any]]) -> TransformationResult:
        """
        Process all items using concurrent batch processing.
        Batches run concurrently, but items within each batch are processed sequentially.
        This is a helper method that concrete transformers can use in their _transform_data method.
        
        Args:
            items: List of all items to process
            
        Returns:
            TransformationResult: Combined results from processing all batches
        """
        main_result = TransformationResult()

        if not items:
            return main_result
        
        # Calculate optimal chunk size and split data
        chunk_size = self._calculate_chunk_size(len(items))
        chunks = self._chunk_data(items, chunk_size)
        
        logger.info(f"Processing {len(items)} items in {len(chunks)} concurrent batches of size ~{chunk_size} (items within batches processed sequentially)")

        # Process batches concurrently
        batch_tasks = []
        for chunk in chunks:
            task = asyncio.create_task(self._process_batch(chunk))
            batch_tasks.append(task)
        
        # Wait for all batches to complete using gather
        try:
            batch_results: Tuple[TransformationResult] = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Process results
            for batch_result in batch_results:
                if isinstance(batch_result, Exception):
                    logger.error(f"Error processing batch: {batch_result}")
                    continue
                    
                # Merge batch results into main result
                main_result.new_digital_footprints.extend(batch_result.new_digital_footprints)
                main_result.personal_identities.extend(batch_result.personal_identities)
                main_result.activity_logs.extend(batch_result.activity_logs)
                
                # Merge pending identities
                for reference_url, identity_types in batch_result.pending_identities.items():
                    if reference_url not in main_result.pending_identities:
                        main_result.pending_identities[reference_url] = []
                    main_result.pending_identities[reference_url].extend(identity_types)
                    # Remove duplicates
                    main_result.pending_identities[reference_url] = list(set(main_result.pending_identities[reference_url]))
                
                # Merge pending activity logs
                for reference_url, timestamps in batch_result.pending_activity_logs.items():
                    if reference_url not in main_result.pending_activity_logs:
                        main_result.pending_activity_logs[reference_url] = []
                    main_result.pending_activity_logs[reference_url].extend(timestamps)
                    # Remove duplicates
                    main_result.pending_activity_logs[reference_url] = list(set(main_result.pending_activity_logs[reference_url]))
                
                # Update stats
                for key, value in batch_result.processing_stats.items():
                    main_result.processing_stats[key] += value
                    
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
        
        return main_result

    def _start_transformation(self):
        """Record the start time of transform."""
        self._transformation_start_time = datetime.now()
        self._transformation_status = OperationStatus.IN_PROGRESS

    def _end_transformation(self, success: bool = True, error_message: Optional[str] = None):
        """
        Record the end time of transform and update status.

        Args:
            success: Whether the transform was successful
            error_message: Error message if transform failed
        """
        self._transformation_end_time = datetime.now()
        self._transformation_status = OperationStatus.COMPLETED if success else OperationStatus.FAILED
        self._error_message = error_message

    @abstractmethod
    def _transform_data(self) -> TransformationResult:
        """
        Transform the extract data into structured entities.
        This is the implementation-specific method that concrete transformers must implement.

        Returns:
            TransformationResult: The transform results
        """
        pass

    def transform(self) -> TransformationResult:
        """Transform extract data into structured entities with metadata."""
        self._start_transformation()

        try:
            result: TransformationResult = self._transform_data()
            self._end_transformation(success=True)

            logger.info(f"Transformation completed: {result.processing_stats}")
            return result
            
        except Exception as e:
            self._end_transformation(success=False, error_message=str(e))
            logger.error(f"Transformation failed: {e}")
            raise

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics for testing and monitoring.

        Returns:
            Dict[str, Any]: Summary of transform process
        """
        return {
            'user_id': self._user.id,
            'name': f"{self._user.first_name} {self._user.last_name}",
            'email': self._user.email,
            "transformer_class": self.__class__.__name__,
            "transformation_start_time": self._transformation_start_time.isoformat() if self._transformation_start_time else None,
            "transformation_end_time": self._transformation_end_time.isoformat() if self._transformation_end_time else None,
            "transformation_duration": (self._transformation_end_time - self._transformation_start_time).total_seconds()
            if (self._transformation_start_time and self._transformation_end_time) else None,
            "transformation_status": self._transformation_status,
            "error_message": self._error_message
        }
