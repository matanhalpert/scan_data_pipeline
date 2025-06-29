"""
Social Media transformer for processing social media profiles and posts.
"""
import asyncio
from typing import Dict, Any, List
from datetime import datetime

from src.transform.base_transformer import BaseTransformer, TransformationResult, TransformationError
from src.database.models import PersonalIdentity
from src.config.enums import DigitalFootprintType, PersonalIdentityType
from src.utils.logger import logger


class SocialMediaTransformer(BaseTransformer):
    """
    Transformer for processing social media data including profiles and posts.
    """

    def _transform_data(self) -> TransformationResult:
        """
        Transform social media data into structured entities using concurrent batch processing.
        
        Returns:
            TransformationResult: The transform results
        """
        # Check if we're already in an event loop (called from UnifiedTransformer)
        try:
            loop = asyncio.get_running_loop()
            # If we get here, we're in an async context - this shouldn't happen
            # when called from the base transform() method
            logger.warning("_transform_data called from within event loop - this may cause issues")
            return TransformationResult()
        except RuntimeError:
            # No event loop running, safe to create one
            return asyncio.run(self._async_transform_data())
    
    async def _async_transform_data(self) -> TransformationResult:
        """
        Async implementation of social media data transform.
        
        Returns:
            TransformationResult: The transform results
        """
        # Process social media data if it exists
        social_media_data = self._extraction_data.get('data', {}).get('social_media', {})
        if not social_media_data:
            logger.info("No social media data found in extract")
            return TransformationResult()

        # Collect all items (profiles and posts) into a single list for batch processing
        all_items = []
        platforms = social_media_data.get('platforms', [])
        
        for platform_data in platforms:
            platform_name = platform_data.get('name', '')
            logger.info(f"Collecting items from platform: {platform_name}")
            
            # Collect profiles
            profiles = platform_data.get('profiles', [])
            for profile in profiles:
                # Add metadata to help _process_item determine how to handle this item
                profile['platform'] = platform_name
                profile['item_type'] = 'profile'
                all_items.append(profile)
            
            # Collect posts
            posts = platform_data.get('posts', {})
            
            # Process different post types
            for post_type, post_list in posts.items():
                if isinstance(post_list, list):
                    for post in post_list:
                        # Add metadata to help _process_item determine how to handle this item
                        post['post_type'] = post_type
                        post['platform'] = platform_name
                        post['item_type'] = 'post'
                        all_items.append(post)
        
        logger.info(f"Collected {len(all_items)} social media items for batch processing")
        
        # Use the concurrent batch processing infrastructure from base class
        if all_items:
            return await self._process_all_batches(all_items)
        else:
            return TransformationResult()

    def _process_item(self, item: Dict[str, Any]) -> TransformationResult:
        """
        Process a single social media item (profile or post).
        
        Args:
            item: Single item from social media data
            
        Returns:
            TransformationResult: Results from processing the item
        """
        platform_name = item.get('platform', '')
        item_type = item.get('item_type', '')
        
        if item_type == 'profile':
            return self._process_profile(item, platform_name)
        elif item_type == 'post':
            return self._process_post(item, platform_name)
        else:
            raise TransformationError(f"Invalid item_type '{item_type}' for SocialMediaTransformer. Expected 'profile' or 'post'.")

    def _process_profile(self, profile: Dict[str, Any], platform_name: str) -> TransformationResult:
        """
        Process a social media profile.
        
        Args:
            profile: Profile data from extract
            platform_name: Name of the platform
            
        Returns:
            TransformationResult: Results from processing the profile
        """
        result = TransformationResult()
        result.processing_stats['items_processed'] += 1
        
        try:
            # Extract profile data
            profile_url = self._get_profile_url(profile, platform_name)
            profile_picture_url = profile.get('profile_picture_url')
            
            # Determine footprint type
            footprint_type = DigitalFootprintType.IMAGE if profile_picture_url else DigitalFootprintType.TEXT
            
            # Get or create digital footprint (media_filepath and source_id are now handled automatically)
            # For profiles with images, use profile_picture_url for media file path construction
            digital_footprint, is_new = self._get_or_create_digital_footprint(
                reference_url=profile_url,
                footprint_type=footprint_type,
                media_url=profile_picture_url if footprint_type == DigitalFootprintType.IMAGE else None
            )
            
            result.processing_stats['footprints_found'] += 1
            
            if is_new:
                result.new_digital_footprints.append(digital_footprint)
                result.processing_stats['new_footprints'] += 1
            else:
                result.processing_stats['existing_footprints'] += 1
            
            # Analyze profile content for identities
            identities_detected = self._analyze_profile_for_identities(profile)
            
            # Check for face match if profile has picture
            if digital_footprint.media_filepath and footprint_type == DigitalFootprintType.IMAGE:
                media_analysis = self._analyze_media(digital_footprint.media_filepath, footprint_type)
                identities_detected.extend(media_analysis['identities_detected'])
                result.processing_stats['media_files_processed'] += 1
                
                if media_analysis['face_match_found']:
                    result.processing_stats['face_matches_found'] += 1
            
            # Create personal identity entries for detected identities with deduplication
            for identity_type in set(identities_detected):
                is_new_identity = self._track_pending_identity(result, digital_footprint, identity_type)
                
                # Only count new identities in stats
                if is_new_identity:
                    result.processing_stats['identities_detected'] += 1
            
            # Track pending activity log instead of creating it immediately
            self._track_pending_activity_log(result, digital_footprint, datetime.now())
            
        except Exception as e:
            logger.error(f"Error processing profile: {e}")
        
        return result

    def _process_post(self, post: Dict[str, Any], platform_name: str) -> TransformationResult:
        """
        Process a social media post.
        
        Args:
            post: Post data from extract
            platform_name: Name of the platform
            
        Returns:
            TransformationResult: Results from processing the post
        """
        result = TransformationResult()
        result.processing_stats['items_processed'] += 1
        
        try:
            # Extract post data
            post_url = post.get('url', '')
            if not post_url:
                logger.warning("Post missing URL, skipping")
                return result
            
            # Determine footprint type
            footprint_type = self._determine_footprint_type(post)
            
            # For media posts, the URL itself is the media URL
            media_url = post_url if footprint_type in [DigitalFootprintType.IMAGE, DigitalFootprintType.VIDEO] else None
            
            # Get or create digital footprint (media_filepath and source_id are now handled automatically)
            digital_footprint, is_new = self._get_or_create_digital_footprint(
                reference_url=post_url,
                footprint_type=footprint_type,
                media_url=media_url
            )
            
            result.processing_stats['footprints_found'] += 1
            
            if is_new:
                result.new_digital_footprints.append(digital_footprint)
                result.processing_stats['new_footprints'] += 1
            else:
                result.processing_stats['existing_footprints'] += 1
            
            # Analyze post content for identities
            identities_detected = self._analyze_post_for_identities(post)
            
            # Analyze media if present
            if digital_footprint.media_filepath and footprint_type in [DigitalFootprintType.IMAGE, DigitalFootprintType.VIDEO]:
                media_analysis = self._analyze_media(digital_footprint.media_filepath, footprint_type)
                identities_detected.extend(media_analysis['identities_detected'])
                result.processing_stats['media_files_processed'] += 1
                
                if media_analysis['face_match_found']:
                    result.processing_stats['face_matches_found'] += 1
                
                if media_analysis['transcription']:
                    result.processing_stats['videos_transcribed'] += 1
            
            # Create personal identity entries for detected identities with deduplication
            for identity_type in set(identities_detected):
                is_new_identity = self._track_pending_identity(result, digital_footprint, identity_type)
                
                # Only count new identities in stats
                if is_new_identity:
                    result.processing_stats['identities_detected'] += 1
            
            # Create activity log
            timestamp_str = post.get('timestamp')
            timestamp = None
            if timestamp_str:
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except:
                    timestamp = datetime.now()
            
            # Track pending activity log instead of creating it immediately
            self._track_pending_activity_log(result, digital_footprint, timestamp)
            
        except Exception as e:
            logger.error(f"Error processing post: {e}")
        
        return result

    @staticmethod
    def _get_profile_url(profile: Dict[str, Any], platform_name: str) -> str:
        """
        Generate profile URL from profile data.
        
        Args:
            profile: Profile data
            platform_name: Platform name
            
        Returns:
            str: Profile URL
        """
        username = profile.get('username', '')
        platform = profile.get('platform', platform_name)
        
        if username and platform:
            return f"https://{platform}.com/{username}"
        
        return f"https://{platform}.com/profile/unknown"

    def _analyze_profile_for_identities(self, profile: Dict[str, Any]) -> List[PersonalIdentityType]:
        """
        Analyze profile data for user personal identities.
        
        Args:
            profile: Profile data
            
        Returns:
            List[PersonalIdentityType]: List of detected identity types
        """
        identities_found = []
        
        # Check various profile fields for user identities
        text_fields = [
            profile.get('display_name', ''),
            profile.get('bio', ''),
            profile.get('first_name', ''),
            profile.get('last_name', ''),
            str(profile.get('work', [])),
            str(profile.get('education', []))
        ]
        
        combined_text = ' '.join(text_fields).lower()
        
        # Analyze combined text for identities
        text_identities = self._analyze_text_for_identities(combined_text)
        identities_found.extend(text_identities)
        
        # Check email
        profile_email = profile.get('email', '')
        if profile_email and profile_email.lower() == self._user.email.lower():
            identities_found.append(PersonalIdentityType.NAME)
        
        # Check phone
        profile_phone = profile.get('phone', '')
        if profile_phone and profile_phone == self._user.phone:
            identities_found.append(PersonalIdentityType.PHONE)
        
        return identities_found

    def _analyze_post_for_identities(self, post: Dict[str, Any]) -> List[PersonalIdentityType]:
        """
        Analyze post data for user personal identities.
        
        Args:
            post: Post data
            
        Returns:
            List[PersonalIdentityType]: List of detected identity types
        """
        identities_found = []
        
        # Check post content
        content = post.get('content', '')
        location = post.get('location', '')
        
        combined_text = f"{content} {location}".lower()
        
        # Analyze combined text for identities
        text_identities = self._analyze_text_for_identities(combined_text)
        identities_found.extend(text_identities)
        
        return identities_found
