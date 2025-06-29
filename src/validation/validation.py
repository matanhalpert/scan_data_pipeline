from datetime import date, datetime
from typing import Optional, Union
from urllib.parse import urlparse

import phonenumbers
from email_validator import EmailNotValidError, validate_email

from src.config.enums import (
    AddressType, Confidence, DigitalFootprintType, FileMediaType, ImageSuffix, 
    OperationStatus, PersonalIdentityType, PostType, SearchEngine, SearchResultType, 
    SocialMediaPlatform, SourceCategory, VideoSuffix
)


class DataValidator:
    """Handles data validation and normalization."""
    
    @staticmethod
    def validate_email(value: Optional[str]) -> Optional[str]:
        """Validate email format and normalize."""
        if not value:
            return None
            
        try:
            # Allow test/example domains for development
            validated = validate_email(value.strip(), check_deliverability=False)
            return validated.normalized
        except EmailNotValidError:
            raise ValueError(f"Invalid email format: {value}")

    @staticmethod
    def validate_phone(value: Optional[str]) -> Optional[str]:
        """Validate phone number format and normalize to E.164."""
        if not value:
            return None
            
        try:
            parsed = phonenumbers.parse(value.strip())
            if not phonenumbers.is_valid_number(parsed):
                raise ValueError(f"Invalid phone number: {value}")
            
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except phonenumbers.NumberParseException:
            raise ValueError(f"Invalid phone number format: {value}")

    @staticmethod
    def validate_date(value: Optional[Union[str, date, datetime]]) -> Optional[date]:
        """Convert and validate dates to consistent format."""
        if not value:
            return None
            
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
            
        try:
            return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
        except ValueError:
            raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got: {value}")

    @staticmethod
    def validate_url(value: Optional[str]) -> Optional[str]:
        """Validate URL format and protocol."""
        if not value:
            return None
            
        cleaned = str(value).strip()
        
        try:
            parsed = urlparse(cleaned)
            if not all([parsed.scheme, parsed.netloc]):
                raise ValueError("URL must include protocol and domain")
                
            if parsed.scheme not in ['http', 'https']:
                raise ValueError("URL must use http or https protocol")
                
            return cleaned
        except Exception:
            raise ValueError(f"Invalid URL format: {value}")

    @staticmethod
    def validate_from_list(value: Optional[str], valid_values: list) -> Optional[str]:
        """Validate category against allowed values."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        if cleaned not in valid_values:
            raise ValueError(f"Invalid value. Must be one of: {', '.join(valid_values)}")
            
        return cleaned

    @staticmethod
    def validate_file_extension(value: Optional[str], allowed_extensions: list) -> Optional[str]:
        """Validate file extension against allowed types."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        ext = cleaned.split('.')[-1] if '.' in cleaned else ''
        
        if ext not in allowed_extensions:
            raise ValueError(f"Invalid file extension. Must be one of: {', '.join(allowed_extensions)}")
            
        return cleaned

    @staticmethod
    def validate_image_suffix(value: Optional[str]) -> Optional[ImageSuffix]:
        """Validate image file suffix using ImageSuffix enum."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        if not cleaned.startswith('.'):
            cleaned = f'.{cleaned}'
            
        try:
            return ImageSuffix(cleaned)
        except ValueError:
            valid_suffixes = ', '.join([suffix.value for suffix in ImageSuffix])
            raise ValueError(f"Invalid image suffix. Must be one of: {valid_suffixes}")

    @staticmethod
    def validate_video_suffix(value: Optional[str]) -> Optional[VideoSuffix]:
        """Validate video file suffix using VideoSuffix enum."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        if not cleaned.startswith('.'):
            cleaned = f'.{cleaned}'
            
        try:
            return VideoSuffix(cleaned)
        except ValueError:
            valid_suffixes = ', '.join([suffix.value for suffix in VideoSuffix])
            raise ValueError(f"Invalid video suffix. Must be one of: {valid_suffixes}")

    @staticmethod
    def validate_source_category(value: Optional[str]) -> Optional[SourceCategory]:
        """Validate source category using SourceCategory enum."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        try:
            return SourceCategory(cleaned)
        except ValueError:
            valid_categories = ', '.join([cat.value for cat in SourceCategory])
            raise ValueError(f"Invalid source category. Must be one of: {valid_categories}")

    @staticmethod
    def validate_file_media_type(value: Optional[str]) -> Optional[FileMediaType]:
        """Validate file media type using FileMediaType enum."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        try:
            return FileMediaType(cleaned)
        except ValueError:
            valid_types = ', '.join([media_type.value for media_type in FileMediaType])
            raise ValueError(f"Invalid file media type. Must be one of: {valid_types}")

    @staticmethod
    def validate_digital_footprint_type(value: Optional[str]) -> Optional[DigitalFootprintType]:
        """Validate digital footprint type using DigitalFootprintType enum."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        try:
            return DigitalFootprintType(cleaned)
        except ValueError:
            valid_types = ', '.join([dt.value for dt in DigitalFootprintType])
            raise ValueError(f"Invalid digital footprint type. Must be one of: {valid_types}")

    @staticmethod
    def validate_social_media_platform(value: Optional[str]) -> Optional[SocialMediaPlatform]:
        """Validate social media platform using SocialMediaPlatform enum."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        try:
            return SocialMediaPlatform(cleaned)
        except ValueError:
            valid_platforms = ', '.join([platform.value for platform in SocialMediaPlatform])
            raise ValueError(f"Invalid social media platform. Must be one of: {valid_platforms}")

    @staticmethod
    def validate_search_engine(value: Optional[str]) -> Optional[SearchEngine]:
        """Validate search engine using SearchEngine enum."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        try:
            return SearchEngine(cleaned)
        except ValueError:
            valid_engines = ', '.join([engine.value for engine in SearchEngine])
            raise ValueError(f"Invalid search engine. Must be one of: {valid_engines}")

    @staticmethod
    def validate_post_type(value: Optional[str]) -> Optional[PostType]:
        """Validate post type using PostType enum."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        try:
            return PostType(cleaned)
        except ValueError:
            valid_types = ', '.join([post_type.value for post_type in PostType])
            raise ValueError(f"Invalid post type. Must be one of: {valid_types}")

    @staticmethod
    def validate_search_result_type(value: Optional[str]) -> Optional[SearchResultType]:
        """Validate search result type using SearchResultType enum."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        try:
            return SearchResultType(cleaned)
        except ValueError:
            valid_types = ', '.join([result_type.value for result_type in SearchResultType])
            raise ValueError(f"Invalid search result type. Must be one of: {valid_types}")

    @staticmethod
    def validate_address_type(value: Optional[str]) -> Optional[AddressType]:
        """Validate address type using AddressType enum."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        try:
            return AddressType(cleaned)
        except ValueError:
            valid_types = ', '.join([addr_type.value for addr_type in AddressType])
            raise ValueError(f"Invalid address type. Must be one of: {valid_types}")

    @staticmethod
    def validate_personal_identity_type(value: Optional[str]) -> Optional[PersonalIdentityType]:
        """Validate personal identity type using PersonalIdentityType enum."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        try:
            return PersonalIdentityType(cleaned)
        except ValueError:
            valid_types = ', '.join([identity_type.value for identity_type in PersonalIdentityType])
            raise ValueError(f"Invalid personal identity type. Must be one of: {valid_types}")

    @staticmethod
    def validate_confidence(value: Optional[Union[str, int]]) -> Optional[Confidence]:
        """Validate confidence level using Confidence enum."""
        if value is None:
            return None
            
        # Handle both string and integer inputs
        if isinstance(value, str):
            cleaned = value.strip().upper()
            try:
                return Confidence[cleaned]
            except KeyError:
                pass
        
        # Try as integer value
        try:
            return Confidence(int(value))
        except (ValueError, TypeError):
            valid_values = ', '.join([f"{conf.name}({conf.value})" for conf in Confidence])
            raise ValueError(f"Invalid confidence level. Must be one of: {valid_values}")

    @staticmethod
    def validate_operation_status(value: Optional[str]) -> Optional[OperationStatus]:
        """Validate operation status using OperationStatus enum."""
        if not value:
            return None
            
        cleaned = str(value).strip().lower()
        try:
            return OperationStatus(cleaned)
        except ValueError:
            valid_statuses = ', '.join([status.value for status in OperationStatus])
            raise ValueError(f"Invalid operation status. Must be one of: {valid_statuses}")

    @staticmethod
    def validate_timestamp(value: Optional[Union[str, datetime]]) -> Optional[datetime]:
        """Convert and validate timestamps to consistent datetime format."""
        if not value:
            return None
            
        if isinstance(value, datetime):
            return value
            
        try:
            # Support ISO format with optional timezone
            timestamp_str = str(value).strip()
            if timestamp_str.endswith('Z'):
                timestamp_str = timestamp_str[:-1] + '+00:00'
            return datetime.fromisoformat(timestamp_str)
        except ValueError:
            raise ValueError(f"Invalid timestamp format. Expected ISO format, got: {value}")
