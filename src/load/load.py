import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.cache.redis_manager import RedisManager
from src.config.enums import OperationStatus
from src.database.models import ActivityLog, DigitalFootprint, PersonalIdentity, User, UserDigitalFootprint
from src.database.setup import DatabaseManager
from src.transform.base_transformer import TransformationResult
from src.utils.logger import logger


class LoadError(Exception):
    """Custom error for load process."""


class LoadResult:
    """Container for load results."""
    
    def __init__(self):
        self.footprints_inserted: int = 0
        self.footprints_skipped: int = 0
        self.identities_inserted: int = 0
        self.identities_skipped: int = 0
        self.activity_logs_inserted: int = 0
        self.activity_logs_skipped: int = 0
        self.user_footprint_links_inserted: int = 0
        self.errors: List[str] = []


class Loader:
    """Loader class responsible for persisting the outputs from the transformation phase into the database."""

    def __init__(self, user_id: int):
        """
        Initialize the loader with user context.
        
        Args:
            user_id: The user's ID
        """
        self._user = self._get_user(user_id)
        self._load_start_time = None
        self._load_duration = None
        self._load_status = OperationStatus.NOT_STARTED
        self._error_message = None
        self._load_result = LoadResult()

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

    def _start_load(self):
        """Start the load process and set the status."""
        self._load_start_time = datetime.now()
        self._load_status = OperationStatus.IN_PROGRESS
        logger.info(f"Starting load process for user {self._user.id}")

    def _end_load(self, success: bool = True, error_message: Optional[str] = None):
        """
        End the load process and update the status.
        
        Args:
            success: Whether the load was successful
            error_message: Error message if load failed
        """
        end_time = datetime.now()
        self._load_duration = (end_time - self._load_start_time).total_seconds()
        self._load_status = OperationStatus.COMPLETED if success else OperationStatus.FAILED
        self._error_message = error_message
        
        status_msg = "completed successfully" if success else f"failed with error: {error_message}"
        logger.info(f"Load process {status_msg} for user {self._user.id} in {self._load_duration:.2f} seconds")

    def _load_digital_footprints(self, session: Session, digital_footprints: List[DigitalFootprint]) -> None:
        """
        Load digital footprints into the database using individual inserts to get IDs back.
        
        Args:
            session: Database session
            digital_footprints: List of digital footprints to insert
        """
        if not digital_footprints:
            logger.debug("No digital footprints to load")
            return

        logger.info(f"Loading {len(digital_footprints)} digital footprints")
        
        try:
            # Use individual inserts to ensure we get IDs back for linking
            # This is necessary because we need the footprint IDs for user-footprint relationships
            for footprint in digital_footprints:
                session.add(footprint)
            
            session.flush()  # Flush to get the IDs populated
            
            # Verify that IDs were populated
            footprints_with_ids = sum(1 for fp in digital_footprints if fp.id is not None)
            logger.info(f"After flush: {footprints_with_ids} out of {len(digital_footprints)} footprints have IDs")
            
            self._load_result.footprints_inserted = len(digital_footprints)
            logger.info(f"Successfully inserted {len(digital_footprints)} digital footprints")
            
        except IntegrityError as e:
            logger.warning(f"Integrity error when inserting digital footprints: {e}")
            session.rollback()
            
            # Fall back to individual inserts to handle duplicates
            self._load_digital_footprints_individually(session, digital_footprints)
        except Exception as e:
            error_msg = f"Failed to insert digital footprints: {e}"
            logger.error(error_msg)
            self._load_result.errors.append(error_msg)
            raise LoadError(error_msg)

    def _load_digital_footprints_individually(self, session: Session, digital_footprints: List[DigitalFootprint]) -> None:
        """
        Load digital footprints individually to handle duplicates.
        
        Args:
            session: Database session
            digital_footprints: List of digital footprints to insert
        """
        logger.info(f"Loading {len(digital_footprints)} digital footprints individually")
        
        inserted_count = 0
        skipped_count = 0
        
        for footprint in digital_footprints:
            try:
                # Check if footprint already exists
                existing = session.query(DigitalFootprint).filter(
                    DigitalFootprint.reference_url == footprint.reference_url,
                    DigitalFootprint.media_filepath == footprint.media_filepath
                ).first()
                
                if existing:
                    logger.debug(f"Digital footprint already exists, skipping: {footprint.reference_url}")
                    skipped_count += 1
                    # Update the footprint ID to match existing one for relationship linking
                    footprint.id = existing.id
                else:
                    session.add(footprint)
                    session.flush()  # Get the ID
                    inserted_count += 1
                    logger.debug(f"Inserted digital footprint: {footprint.reference_url}")
                    
            except Exception as e:
                error_msg = f"Failed to insert digital footprint {footprint.reference_url}: {e}"
                logger.warning(error_msg)
                self._load_result.errors.append(error_msg)
                skipped_count += 1
        
        self._load_result.footprints_inserted = inserted_count
        self._load_result.footprints_skipped = skipped_count
        logger.info(f"Inserted {inserted_count} digital footprints, skipped {skipped_count}")

    def _load_personal_identities(self, session: Session, personal_identities: List[PersonalIdentity]) -> None:
        """
        Load personal identities into the database using bulk insert.
        
        Args:
            session: Database session
            personal_identities: List of personal identities to insert
        """
        if not personal_identities:
            logger.debug("No personal identities to load")
            return

        logger.info(f"Loading {len(personal_identities)} personal identities")
        
        try:
            # Use bulk insert for performance
            session.bulk_save_objects(personal_identities)
            self._load_result.identities_inserted = len(personal_identities)
            logger.info(f"Successfully inserted {len(personal_identities)} personal identities")
            
        except IntegrityError as e:
            logger.warning(f"Integrity error when inserting personal identities: {e}")
            session.rollback()
            
            # Fall back to individual inserts to handle duplicates
            self._load_personal_identities_individually(session, personal_identities)
        except Exception as e:
            error_msg = f"Failed to insert personal identities: {e}"
            logger.error(error_msg)
            self._load_result.errors.append(error_msg)
            raise LoadError(error_msg)

    def _load_personal_identities_individually(self, session: Session, personal_identities: List[PersonalIdentity]) -> None:
        """
        Load personal identities individually to handle duplicates.
        
        Args:
            session: Database session
            personal_identities: List of personal identities to insert
        """
        logger.info(f"Loading {len(personal_identities)} personal identities individually")
        
        inserted_count = 0
        skipped_count = 0
        
        for identity in personal_identities:
            try:
                # Check if identity already exists
                existing = session.query(PersonalIdentity).filter(
                    PersonalIdentity.digital_footprint_id == identity.digital_footprint_id,
                    PersonalIdentity.personal_identity == identity.personal_identity
                ).first()
                
                if existing:
                    logger.debug(f"Personal identity already exists, skipping: {identity.personal_identity}")
                    skipped_count += 1
                else:
                    session.add(identity)
                    inserted_count += 1
                    logger.debug(f"Inserted personal identity: {identity.personal_identity}")
                    
            except Exception as e:
                error_msg = f"Failed to insert personal identity {identity.personal_identity}: {e}"
                logger.warning(error_msg)
                self._load_result.errors.append(error_msg)
                skipped_count += 1
        
        self._load_result.identities_inserted = inserted_count
        self._load_result.identities_skipped = skipped_count
        logger.info(f"Inserted {inserted_count} personal identities, skipped {skipped_count}")

    def _load_activity_logs(self, session: Session, activity_logs: List[ActivityLog]) -> None:
        """
        Load activity logs into the database using bulk insert.
        
        Args:
            session: Database session
            activity_logs: List of activity logs to insert
        """
        if not activity_logs:
            logger.debug("No activity logs to load")
            return

        logger.info(f"Loading {len(activity_logs)} activity logs")
        
        try:
            # Use bulk insert for performance
            session.bulk_save_objects(activity_logs)
            self._load_result.activity_logs_inserted = len(activity_logs)
            logger.info(f"Successfully inserted {len(activity_logs)} activity logs")
            
        except IntegrityError as e:
            logger.warning(f"Integrity error when inserting activity logs: {e}")
            session.rollback()
            
            # Fall back to individual inserts to handle duplicates
            self._load_activity_logs_individually(session, activity_logs)
        except Exception as e:
            error_msg = f"Failed to insert activity logs: {e}"
            logger.error(error_msg)
            self._load_result.errors.append(error_msg)
            raise LoadError(error_msg)

    def _load_activity_logs_individually(self, session: Session, activity_logs: List[ActivityLog]) -> None:
        """
        Load activity logs individually to handle duplicates.
        
        Args:
            session: Database session
            activity_logs: List of activity logs to insert
        """
        logger.info(f"Loading {len(activity_logs)} activity logs individually")
        
        inserted_count = 0
        skipped_count = 0
        
        for log in activity_logs:
            try:
                # Check if log already exists
                existing = session.query(ActivityLog).filter(
                    ActivityLog.digital_footprint_id == log.digital_footprint_id,
                    ActivityLog.timestamp == log.timestamp
                ).first()
                
                if existing:
                    logger.debug(f"Activity log already exists, skipping: {log.timestamp}")
                    skipped_count += 1
                else:
                    session.add(log)
                    inserted_count += 1
                    logger.debug(f"Inserted activity log: {log.timestamp}")
                    
            except Exception as e:
                error_msg = f"Failed to insert activity log {log.timestamp}: {e}"
                logger.warning(error_msg)
                self._load_result.errors.append(error_msg)
                skipped_count += 1
        
        self._load_result.activity_logs_inserted = inserted_count
        self._load_result.activity_logs_skipped = skipped_count
        logger.info(f"Inserted {inserted_count} activity logs, skipped {skipped_count}")

    def _link_user_to_footprints(self, session: Session, digital_footprints: List[DigitalFootprint]) -> None:
        """
        Create UserDigitalFootprint relationships to link the user to the digital footprints.
        
        Args:
            session: Database session
            digital_footprints: List of digital footprints to link to the user
        """
        if not digital_footprints:
            logger.debug("No digital footprints to link to user")
            return

        logger.info(f"Linking {len(digital_footprints)} digital footprints to user {self._user.id}")
        
        # Debug: Check how many footprints have IDs
        footprints_with_ids = sum(1 for fp in digital_footprints if fp.id is not None)
        logger.info(f"Footprints with IDs for linking: {footprints_with_ids} out of {len(digital_footprints)}")
        
        # Create UserDigitalFootprint objects
        user_footprint_links = []
        for footprint in digital_footprints:
            if footprint.id:  # Only link if footprint has an ID
                user_footprint_links.append(UserDigitalFootprint(
                    user_id=self._user.id,
                    digital_footprint_id=footprint.id
                ))
            else:
                logger.warning(f"Digital footprint has no ID, cannot link: {footprint.reference_url}")
        
        logger.info(f"Created {len(user_footprint_links)} user-footprint link objects")
        
        if not user_footprint_links:
            logger.warning("No valid digital footprints to link to user")
            return
        
        try:
            # Use bulk insert for performance
            session.bulk_save_objects(user_footprint_links)
            self._load_result.user_footprint_links_inserted = len(user_footprint_links)
            logger.info(f"Successfully linked {len(user_footprint_links)} digital footprints to user")
            
        except IntegrityError as e:
            logger.warning(f"Integrity error when linking user to footprints: {e}")
            session.rollback()
            
            # Fall back to individual inserts to handle duplicates
            self._link_user_to_footprints_individually(session, user_footprint_links)
        except Exception as e:
            error_msg = f"Failed to link user to digital footprints: {e}"
            logger.error(error_msg)
            self._load_result.errors.append(error_msg)
            raise LoadError(error_msg)

    def _link_user_to_footprints_individually(self, session: Session, user_footprint_links: List[UserDigitalFootprint]) -> None:
        """
        Link user to footprints individually to handle duplicates.
        
        Args:
            session: Database session
            user_footprint_links: List of user-footprint links to insert
        """
        logger.info(f"Linking {len(user_footprint_links)} user-footprint relationships individually")
        
        inserted_count = 0
        skipped_count = 0
        
        for link in user_footprint_links:
            try:
                # Check if link already exists
                existing = session.query(UserDigitalFootprint).filter(
                    UserDigitalFootprint.user_id == link.user_id,
                    UserDigitalFootprint.digital_footprint_id == link.digital_footprint_id
                ).first()
                
                if existing:
                    logger.debug(f"User-footprint link already exists, skipping: {link.digital_footprint_id}")
                    skipped_count += 1
                else:
                    # Check if the digital footprint actually exists
                    footprint_exists = session.query(DigitalFootprint).filter(
                        DigitalFootprint.id == link.digital_footprint_id
                    ).first()
                    
                    if not footprint_exists:
                        error_msg = f"Digital footprint {link.digital_footprint_id} does not exist - cannot create link"
                        logger.warning(error_msg)
                        self._load_result.errors.append(error_msg)
                        skipped_count += 1
                    else:
                        session.add(link)
                        inserted_count += 1
                        logger.debug(f"Linked user to footprint: {link.digital_footprint_id}")
                    
            except Exception as e:
                error_msg = f"Failed to link user to footprint {link.digital_footprint_id}: {e}"
                logger.warning(error_msg)
                self._load_result.errors.append(error_msg)
                skipped_count += 1
        
        self._load_result.user_footprint_links_inserted = inserted_count
        logger.info(f"Linked {inserted_count} user-footprint relationships, skipped {skipped_count}")

    def _create_activity_logs_from_pending(self, digital_footprints: List[DigitalFootprint], pending_activity_logs: Dict[str, List]) -> List[ActivityLog]:
        """
        Create ActivityLog objects from pending activity log data.
        
        This method maps pending activity logs to their corresponding digital footprints
        that now have IDs after being loaded into the database.
        
        Args:
            digital_footprints: List of digital footprints that have been loaded (with IDs)
            pending_activity_logs: Dict mapping reference URLs to lists of timestamps
            
        Returns:
            List[ActivityLog]: List of ActivityLog objects ready to be inserted
        """
        if not pending_activity_logs or not digital_footprints:
            return []
            
        logger.info(f"Creating activity logs from pending data: {len(pending_activity_logs)} footprint URLs")
        
        # Create a mapping from footprint reference_url to footprint ID
        footprint_id_map = {}
        for footprint in digital_footprints:
            if footprint.id and footprint.reference_url:
                # Use the same key format as in the pending tracking
                key = footprint.reference_url
                footprint_id_map[key] = footprint.id
        
        # Create ActivityLog objects from pending data
        activity_logs = []
        for reference_url, timestamps in pending_activity_logs.items():
            if reference_url in footprint_id_map:
                footprint_id = footprint_id_map[reference_url]
                for timestamp in timestamps:
                    activity_log = ActivityLog(
                        digital_footprint_id=footprint_id,
                        timestamp=timestamp
                    )
                    activity_logs.append(activity_log)
            else:
                logger.warning(f"No matching digital footprint found for pending activity logs: {reference_url}")
        
        logger.info(f"Created {len(activity_logs)} activity log objects from pending data")
        return activity_logs

    def _create_personal_identities_from_pending(self, digital_footprints: List[DigitalFootprint], pending_identities: Dict[str, List]) -> List[PersonalIdentity]:
        """
        Create PersonalIdentity objects from pending identity data.
        
        This method maps pending identities to their corresponding digital footprints
        that now have IDs after being loaded into the database.
        
        Args:
            digital_footprints: List of digital footprints that have been loaded (with IDs)
            pending_identities: Dict mapping reference URLs to lists of identity types
            
        Returns:
            List[PersonalIdentity]: List of PersonalIdentity objects ready to be inserted
        """
        if not pending_identities or not digital_footprints:
            return []
            
        logger.info(f"Creating personal identities from pending data: {len(pending_identities)} footprint URLs")
        
        # Create a mapping from footprint reference_url to footprint ID
        footprint_id_map = {}
        for footprint in digital_footprints:
            if footprint.id and footprint.reference_url:
                # Use the same key format as in the pending tracking
                key = footprint.reference_url
                footprint_id_map[key] = footprint.id
        
        # Create PersonalIdentity objects from pending data
        personal_identities = []
        for reference_url, identity_types in pending_identities.items():
            if reference_url in footprint_id_map:
                footprint_id = footprint_id_map[reference_url]
                for identity_type in identity_types:
                    personal_identity = PersonalIdentity(
                        digital_footprint_id=footprint_id,
                        personal_identity=identity_type
                    )
                    personal_identities.append(personal_identity)
            else:
                logger.warning(f"No matching digital footprint found for pending personal identities: {reference_url}")
        
        logger.info(f"Created {len(personal_identities)} personal identity objects from pending data")
        return personal_identities

    def load(self, transformation_result: TransformationResult) -> LoadResult:
        """
        Load the transformation results into the database.
        
        Args:
            transformation_result: Results from the transformation phase
            
        Returns:
            LoadResult: Results of the load operation
            
        Raises:
            LoadError: If the load operation fails
        """
        self._start_load()
        
        try:
            with DatabaseManager.get_session() as session:
                # Load digital footprints first (they need IDs for relationships)
                self._load_digital_footprints(session, transformation_result.new_digital_footprints)
                
                # Create activity logs from pending data now that footprints have IDs
                activity_logs = self._create_activity_logs_from_pending(
                    transformation_result.new_digital_footprints, 
                    transformation_result.pending_activity_logs
                )
                
                # Create personal identities from pending data now that footprints have IDs
                personal_identities_from_pending = self._create_personal_identities_from_pending(
                    transformation_result.new_digital_footprints,
                    transformation_result.pending_identities
                )
                
                # Combine existing personal identities with those created from pending data
                all_personal_identities = transformation_result.personal_identities + personal_identities_from_pending
                
                # Load personal identities (they reference digital footprints)
                self._load_personal_identities(session, all_personal_identities)
                
                # Load activity logs (they reference digital footprints)
                self._load_activity_logs(session, activity_logs)
                
                # Link user to digital footprints
                self._link_user_to_footprints(session, transformation_result.new_digital_footprints)
                
                # Commit all changes
                session.commit()
                logger.info("All load operations committed successfully")
            
            self._end_load(success=True)
            
        except Exception as e:
            error_msg = f"Load operation failed: {e}"
            logger.error(error_msg)
            self._end_load(success=False, error_message=error_msg)
            raise LoadError(error_msg)
        
        return self._load_result

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the load operation.
        
        Returns:
            Dict[str, Any]: Load operation metadata
        """
        return {
            'user_id': self._user.id,
            'load_start_time': self._load_start_time.isoformat() if self._load_start_time else None,
            'load_duration': self._load_duration,
            'load_status': self._load_status.value if self._load_status else None,
            'error_message': self._error_message,
            'load_stats': {
                'footprints_inserted': self._load_result.footprints_inserted,
                'footprints_skipped': self._load_result.footprints_skipped,
                'identities_inserted': self._load_result.identities_inserted,
                'identities_skipped': self._load_result.identities_skipped,
                'activity_logs_inserted': self._load_result.activity_logs_inserted,
                'activity_logs_skipped': self._load_result.activity_logs_skipped,
                'user_footprint_links_inserted': self._load_result.user_footprint_links_inserted,
                'total_errors': len(self._load_result.errors)
            }
        }

    def load_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the load operation results.
        
        Returns:
            Dict[str, Any]: Summary of load operation including stats
        """
        total_inserted = (
            self._load_result.footprints_inserted +
            self._load_result.identities_inserted +
            self._load_result.activity_logs_inserted +
            self._load_result.user_footprint_links_inserted
        )
        
        total_skipped = (
            self._load_result.footprints_skipped +
            self._load_result.identities_skipped +
            self._load_result.activity_logs_skipped
        )
        
        return {
            'user_id': self._user.id,
            'load_status': self._load_status.value if self._load_status else None,
            'load_duration': self._load_duration,
            'total_records_inserted': total_inserted,
            'total_records_skipped': total_skipped,
            'breakdown': {
                'digital_footprints': {
                    'inserted': self._load_result.footprints_inserted,
                    'skipped': self._load_result.footprints_skipped
                },
                'personal_identities': {
                    'inserted': self._load_result.identities_inserted,
                    'skipped': self._load_result.identities_skipped
                },
                'activity_logs': {
                    'inserted': self._load_result.activity_logs_inserted,
                    'skipped': self._load_result.activity_logs_skipped
                },
                'user_footprint_links': {
                    'inserted': self._load_result.user_footprint_links_inserted
                }
            },
            'errors': self._load_result.errors,
            'error_count': len(self._load_result.errors),
            'success': self._load_status == OperationStatus.COMPLETED
        }
