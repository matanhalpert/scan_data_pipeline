"""
Redis manager for handling caching operations.
"""
import json
from typing import Optional, Dict, Any
from datetime import datetime
import redis

from src.database.models import (
    User, SecondaryEmail, SecondaryPhone, Address, Picture,
    UserDigitalFootprint, DigitalFootprint, Source, PersonalIdentity
)
from src.config.enums import AddressType, DigitalFootprintType, SourceCategory, PersonalIdentityType
from src.utils.logger import logger
from src.config.redis_config import REDIS_CONFIG, CACHE_EXPIRATION
from src.cache.exceptions import CacheConnectionError, CacheOperationError


class RedisManager:
    """Redis manager for handling caching operations."""
    _instance = None
    _redis_client = None

    @classmethod
    def initialize(cls):
        """Initialize Redis connection if not already initialized"""
        if cls._redis_client is None:
            try:
                cls._redis_client = redis.Redis(
                    **REDIS_CONFIG,
                    decode_responses=True  # Automatically decode responses to strings
                )
                # Test connection
                cls._redis_client.ping()
                logger.info("Successfully connected to Redis")
            except redis.ConnectionError as e:
                logger.error(f"Failed to connect to Redis: {e}")
                raise CacheConnectionError(f"Failed to connect to Redis: {e}")

    @classmethod
    def get_client(cls):
        """Get Redis client, initializing if needed"""
        if cls._redis_client is None:
            cls.initialize()
        return cls._redis_client

    @classmethod
    def set_data(cls, key: str, data: dict, expiration: Optional[int] = None):
        """
        Cache data in Redis.

        Args:
            key: The cache key
            data: Dictionary containing data to cache
            expiration: Optional expiration time in seconds

        Raises:
            CacheOperationError: If caching operation fails
        """
        try:
            if expiration is not None:
                cls.get_client().setex(
                    key,
                    expiration,
                    json.dumps(data)
                )
            else:
                cls.get_client().set(key, json.dumps(data))
            logger.debug(f"Cached data for key: {key}")
        except Exception as e:
            logger.error(f"Error caching data: {e}")
            raise CacheOperationError(f"Failed to cache data: {e}")

    @classmethod
    def get_data(cls, key: str) -> Optional[dict]:
        """
        Retrieve data from Redis cache.

        Args:
            key: The cache key

        Returns:
            Optional[dict]: Cached data if found, None otherwise

        Raises:
            CacheOperationError: If retrieval operation fails
        """
        try:
            data = cls.get_client().get(key)
            if data is None:
                return None
            return json.loads(data)
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding cached data: {e}")
            raise CacheOperationError(f"Failed to decode cached data: {e}")
        except Exception as e:
            logger.error(f"Error retrieving data from cache: {e}")
            raise CacheOperationError(f"Failed to retrieve data from cache: {e}")

    @classmethod
    def delete_data(cls, key: str):
        """
        Remove data from Redis cache.

        Args:
            key: The cache key

        Raises:
            CacheOperationError: If deletion operation fails
        """
        try:
            cls.get_client().delete(key)
            logger.debug(f"Removed cached data for key: {key}")
        except Exception as e:
            logger.error(f"Error removing data from cache: {e}")
            raise CacheOperationError(f"Failed to remove data from cache: {e}")

    @classmethod
    def set_user(cls, user: User) -> None:
        """
        Cache a User model instance in Redis by converting it to dictionary.

        Args:
            user: User model instance to cache

        Raises:
            CacheOperationError: If caching operation fails
        """
        try:
            user_data: Dict[str, Any] = user.to_dict()
            
            key = f"user:{user.id}"
            cls.set_data(key, user_data, CACHE_EXPIRATION['user_data'])
            logger.debug(f"Cached User model for user_id: {user.id}")
        except Exception as e:
            logger.error(f"Error caching User model: {e}")
            raise CacheOperationError(f"Failed to cache User model: {e}")

    @classmethod
    def get_user(cls, user_id: int) -> Optional[User]:
        """
        Retrieve a User model instance from Redis cache.

        Args:
            user_id: The user's ID

        Returns:
            Optional[User]: User model instance if found in cache, None otherwise

        Raises:
            CacheOperationError: If retrieval operation fails
        """
        try:
            key = f"user:{user_id}"
            user_data = cls.get_data(key)
            if user_data is None:
                return None
            
            # Convert cached dictionary back to User model
            
            # Create User instance
            user = User(
                id=user_data["id"],
                first_name=user_data["first_name"],
                last_name=user_data["last_name"],
                email=user_data["email"],
                phone=user_data["phone"],
                birth_date=datetime.fromisoformat(user_data["birth_date"]).date() if user_data["birth_date"] else None,
                password=user_data["password"]
            )
            
            # Reconstruct relationships
            user.secondary_emails = [
                SecondaryEmail(user_id=user.id, email=email_data)
                for email_data in user_data.get("secondary_emails", [])
            ]
            
            user.secondary_phones = [
                SecondaryPhone(user_id=user.id, phone=phone_data)
                for phone_data in user_data.get("secondary_phones", [])
            ]
            
            user.addresses = [
                Address(
                    user_id=user.id,
                    type=AddressType(addr_data["type"]) if isinstance(addr_data["type"], str) else addr_data["type"],
                    country=addr_data["country"],
                    city=addr_data["city"],
                    street=addr_data["street"],
                    number=addr_data["number"]
                )
                for addr_data in user_data.get("addresses", [])
            ]
            
            user.pictures = [
                Picture(user_id=user.id, path=pic_path)
                for pic_path in user_data.get("pictures", [])
            ]
            
            user.digital_footprints = [
                UserDigitalFootprint(
                    digital_footprint_id=df_data["id"],
                    user_id=user.id
                )
                for df_data in user_data.get("digital_footprints", [])
            ]
            
            logger.debug(f"Retrieved User model from cache for user_id: {user_id}")
            return user
            
        except Exception as e:
            logger.error(f"Error retrieving User model from cache: {e}")
            raise CacheOperationError(f"Failed to retrieve User model from cache: {e}")

    @classmethod
    def delete_user(cls, user_id: int):
        """
        Remove a User from Redis cache.

        Args:
            user_id: The user's ID

        Raises:
            CacheOperationError: If deletion operation fails
        """
        try:
            key = f"user:{user_id}"
            cls.delete_data(key)
            logger.debug(f"Removed User from cache for user_id: {user_id}")
        except Exception as e:
            logger.error(f"Error removing User from cache: {e}")
            raise CacheOperationError(f"Failed to remove User from cache: {e}")

    @classmethod
    def set_digital_footprint(cls, digital_footprint: DigitalFootprint) -> None:
        """
        Cache a DigitalFootprint model instance in Redis.
        Uses composite key based on (reference_url, media_filepath) for uniqueness.

        Args:
            digital_footprint: DigitalFootprint model instance to cache

        Raises:
            CacheOperationError: If caching operation fails
        """
        try:
            footprint_data: Dict[str, Any] = digital_footprint.to_dict()
            
            key = f"digital_footprint:{digital_footprint.reference_url}:{digital_footprint.media_filepath or 'no_media'}"
            cls.set_data(key, footprint_data, CACHE_EXPIRATION['digital_footprint'])
            logger.debug(f"Cached DigitalFootprint for reference_url: {digital_footprint.reference_url}")
        except Exception as e:
            logger.error(f"Error caching DigitalFootprint model: {e}")
            raise CacheOperationError(f"Failed to cache DigitalFootprint model: {e}")

    @classmethod
    def get_digital_footprint(cls, reference_url: str, media_filepath: str = None) -> Optional[DigitalFootprint]:
        """
        Retrieve a DigitalFootprint from Redis cache using composite key.

        Args:
            reference_url: The reference URL of the footprint
            media_filepath: The media filepath (optional)

        Returns:
            DigitalFootprint: DigitalFootprint model instance if found, None otherwise

        Raises:
            CacheOperationError: If retrieval operation fails
        """
        try:
            key = f"digital_footprint:{reference_url}:{media_filepath or 'no_media'}"
            footprint_data = cls.get_data(key)
            if footprint_data is None:
                return None
            
            # Create DigitalFootprint instance
            digital_footprint = DigitalFootprint(
                id=footprint_data["id"],
                type=DigitalFootprintType(footprint_data["type"]) if footprint_data["type"] else None,
                media_filepath=footprint_data["media_filepath"],
                reference_url=footprint_data["reference_url"],
                source_id=footprint_data["source_id"]
            )
            
            logger.debug(f"Retrieved DigitalFootprint from cache for reference_url: {reference_url}")
            return digital_footprint
            
        except Exception as e:
            logger.error(f"Error retrieving DigitalFootprint from cache: {e}")
            raise CacheOperationError(f"Failed to retrieve DigitalFootprint from cache: {e}")

    @classmethod
    def delete_digital_footprint(cls, reference_url: str, media_filepath: str = None):
        """
        Remove a DigitalFootprint from Redis cache.

        Args:
            reference_url: The reference URL of the footprint
            media_filepath: The media filepath (optional)

        Raises:
            CacheOperationError: If deletion operation fails
        """
        try:
            key = f"digital_footprint:{reference_url}:{media_filepath or 'no_media'}"
            cls.delete_data(key)
            logger.debug(f"Removed DigitalFootprint from cache for reference_url: {reference_url}")
        except Exception as e:
            logger.error(f"Error removing DigitalFootprint from cache: {e}")
            raise CacheOperationError(f"Failed to remove DigitalFootprint from cache: {e}")

    @classmethod
    def set_source(cls, source: Source) -> None:
        """
        Cache a Source model instance in Redis.

        Args:
            source: Source model instance to cache

        Raises:
            CacheOperationError: If caching operation fails
        """
        try:
            source_data: Dict[str, Any] = source.to_dict()
            
            key = f"source:{source.url}"
            cls.set_data(key, source_data, CACHE_EXPIRATION['source'])
            logger.debug(f"Cached Source for url: {source.url}")
        except Exception as e:
            logger.error(f"Error caching Source model: {e}")
            raise CacheOperationError(f"Failed to cache Source model: {e}")

    @classmethod
    def get_source(cls, source_url: str) -> Optional[Source]:
        """
        Retrieve a Source from Redis cache by URL.

        Args:
            source_url: The source URL

        Returns:
            Source: Source model instance if found, None otherwise

        Raises:
            CacheOperationError: If retrieval operation fails
        """
        try:
            key = f"source:{source_url}"
            source_data = cls.get_data(key)
            if source_data is None:
                return None
            
            # Create Source instance
            source = Source(
                id=source_data["id"],
                name=source_data["name"],
                url=source_data["url"],
                category=SourceCategory(source_data["category"]) if source_data["category"] else None,
                verified=source_data["verified"]
            )
            
            logger.debug(f"Retrieved Source from cache for url: {source_url}")
            return source
            
        except Exception as e:
            logger.error(f"Error retrieving Source from cache: {e}")
            raise CacheOperationError(f"Failed to retrieve Source from cache: {e}")

    @classmethod
    def delete_source(cls, url: str):
        """
        Remove a Source from Redis cache by URL.

        Args:
            url: The source URL

        Raises:
            CacheOperationError: If deletion operation fails
        """
        try:
            key = f"source:{url}"
            cls.delete_data(key)
            logger.debug(f"Removed Source from cache for url: {url}")
        except Exception as e:
            logger.error(f"Error removing Source from cache: {e}")
            raise CacheOperationError(f"Failed to remove Source from cache: {e}")

    @classmethod
    def set_personal_identity(cls, personal_identity: PersonalIdentity) -> None:
        """
        Cache a PersonalIdentity model instance in Redis.
        Uses composite key: {digital_footprint_id}:{personal_identity_value}

        Args:
            personal_identity: PersonalIdentity model instance to cache

        Raises:
            CacheOperationError: If caching operation fails
        """
        try:
            # Convert PersonalIdentity model to dictionary for caching
            identity_data: Dict[str, Any] = personal_identity.to_dict()
            
            key = f"personal_identity:{personal_identity.digital_footprint_id}:{personal_identity.personal_identity}"
            cls.set_data(key, identity_data, CACHE_EXPIRATION['personal_identity'])
            logger.debug(f"Cached PersonalIdentity for digital_footprint_id: {personal_identity.digital_footprint_id}")
        except Exception as e:
            logger.error(f"Error caching PersonalIdentity model: {e}")
            raise CacheOperationError(f"Failed to cache PersonalIdentity model: {e}")

    @classmethod
    def get_personal_identity(cls, digital_footprint_id: int, personal_identity_value: str) -> Optional[PersonalIdentity]:
        """
        Retrieve a PersonalIdentity from Redis cache using composite key.

        Args:
            digital_footprint_id: The digital footprint ID
            personal_identity_value: The personal identity value (as string)

        Returns:
            PersonalIdentity: PersonalIdentity model instance if found, None otherwise

        Raises:
            CacheOperationError: If retrieval operation fails
        """
        try:
            key = f"personal_identity:{digital_footprint_id}:{personal_identity_value}"
            identity_data = cls.get_data(key)
            if identity_data is None:
                return None
            
            # Create PersonalIdentity instance
            personal_identity = PersonalIdentity(
                digital_footprint_id=identity_data["digital_footprint_id"],
                personal_identity=PersonalIdentityType(identity_data["personal_identity"])
            )
            
            logger.debug(f"Retrieved PersonalIdentity from cache for digital_footprint_id: {digital_footprint_id}")
            return personal_identity
            
        except Exception as e:
            logger.error(f"Error retrieving PersonalIdentity from cache: {e}")
            raise CacheOperationError(f"Failed to retrieve PersonalIdentity from cache: {e}")

    @classmethod
    def delete_personal_identity(cls, digital_footprint_id: int, personal_identity_value: str):
        """
        Remove a PersonalIdentity from Redis cache.

        Args:
            digital_footprint_id: The digital footprint ID
            personal_identity_value: The personal identity value (as string)

        Raises:
            CacheOperationError: If deletion operation fails
        """
        try:
            key = f"personal_identity:{digital_footprint_id}:{personal_identity_value}"
            cls.delete_data(key)
            logger.debug(f"Removed PersonalIdentity from cache for digital_footprint_id: {digital_footprint_id}")
        except Exception as e:
            logger.error(f"Error removing PersonalIdentity from cache: {e}")
            raise CacheOperationError(f"Failed to remove PersonalIdentity from cache: {e}")

    @classmethod
    def clear_all(cls):
        """
        Clear all cached data.

        Raises:
            CacheOperationError: If clear operation fails
        """
        try:
            cls.get_client().flushdb()
            logger.info("Cleared all data from Redis cache")
        except Exception as e:
            logger.error(f"Error clearing Redis cache: {e}")
            raise CacheOperationError(f"Failed to clear Redis cache: {e}")
