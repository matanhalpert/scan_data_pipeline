import asyncio
from typing import Any, Dict, List

from src.config.enums import PostType, SocialMediaPlatform
from src.extract.base_extractor import BaseExtractor
from src.utils.logger import logger


class SocialMediaExtractor(BaseExtractor):
    """Extractor that discovers all social media digital footprints of a user."""

    def __init__(self, user_id: int, simulation_data_path: str = None):
        """Initialize the social media extractor."""
        if simulation_data_path:
            super().__init__(user_id, simulation_data_path)
        else:
            super().__init__(user_id)
        self._discovered_usernames = set()

    def _profile_matches_user(self, profile: Dict[str, Any]) -> bool:
        """
        Check if a profile matches the user based on exact string matching.
        
        Args:
            profile: Profile dictionary to check
            
        Returns:
            True if profile matches user identifiers
        """
        # Exact name matching
        profile_first = str(profile.get('first_name', '')).lower().strip()
        profile_last = str(profile.get('last_name', '')).lower().strip()
        user_first = self._user.first_name.lower()
        user_last = self._user.last_name.lower()
        
        name_match = (profile_first == user_first and profile_last == user_last)
        
        # Email matching
        profile_email = str(profile.get('email', '')).lower().strip()
        email_match = profile_email and profile_email in self._user_identifiers['emails']
        
        # Phone matching
        profile_phone = str(profile.get('phone', '')).strip()
        phone_match = profile_phone and profile_phone in self._user_identifiers['phones']
        
        # Address matching (using improved specific address identifiers)
        profile_address = str(profile.get('address', '')).lower().strip()
        address_match = profile_address and any(
            addr in profile_address for addr in self._user_identifiers['addresses']
        )
        
        # Return True if any criteria matches
        return name_match or email_match or phone_match or address_match

    async def _process_profile_chunk(
            self,
            profiles_chunk: List[Dict[str, Any]],
            platform: SocialMediaPlatform
    ) -> List[Dict[str, Any]]:
        """
        Process a chunk of profiles concurrently.
        
        Args:
            profiles_chunk: Chunk of profiles to process
            platform: The social media platform being processed
        Returns:
            List of matching profiles from this chunk
        """
        profiles_found = []
        discovered_usernames = set()
        
        for profile in profiles_chunk:
            if self._profile_matches_user(profile):
                profiles_found.append(profile)
                username = profile.get('username', '')
                if username:
                    discovered_usernames.add(username)
                logger.info(f"Found matching profile: {username} on {platform}")
        
        return profiles_found, discovered_usernames

    async def _scan_platform_profiles(self, platform: SocialMediaPlatform) -> List[Dict[str, Any]]:
        """
        Asynchronously scan all profiles on a platform to find matches for the user using chunked processing.
        
        Args:
            platform: The social media platform to scan
            
        Returns:
            List of matching profile dictionaries
        """
        platform_data = self._simulation_data.get(platform, {})
        platform_profiles = platform_data.get('profiles', [])
        
        if not platform_profiles:
            return []
            
        logger.debug(f"Scanning {len(platform_profiles)} profiles on {platform}")
        
        # Calculate chunk size and create chunks
        chunk_size = self._calculate_chunk_size(len(platform_profiles))
        profile_chunks = self._chunk_data(platform_profiles, chunk_size)
        
        logger.debug(f"Processing {len(profile_chunks)} profile chunks of size ~{chunk_size} on {platform}")
        
        # Process chunks concurrently
        chunk_tasks = [
            self._process_profile_chunk(chunk, platform)
            for chunk in profile_chunks
        ]
        
        chunk_results = await asyncio.gather(*chunk_tasks)
        
        # Aggregate results from all chunks
        all_profiles_found = []
        for profiles_found, discovered_usernames in chunk_results:
            all_profiles_found.extend(profiles_found)
            self._discovered_usernames.update(discovered_usernames)
        
        return all_profiles_found

    async def _process_post_chunk(
            self,
            posts_chunk: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Process a chunk of posts concurrently.
        
        Args:
            posts_chunk: Chunk of posts to process
        Returns:
            Dictionary with grouped posts by post type for this chunk
        """
        grouped_posts = {
            f'{post_type}_posts_found': []
            for post_type in PostType
        }

        for post in posts_chunk:
            if self._post_relates_to_user(post):
                post_type = post.get('post_type')
                grouped_posts[f'{post_type}_posts_found'].append(post)
        
        return grouped_posts

    async def _scan_platform_posts(self, platform: SocialMediaPlatform) -> Dict[str, List[Dict[str, Any]]]:
        """
        Asynchronously scan all posts on a platform to find user-related content using chunked processing.
        
        Args:
            platform: The social media platform to scan
            
        Returns:
            Dictionary with grouped posts by post type
        """
        platform_data = self._simulation_data.get(platform.value, {})
        all_posts = platform_data.get('posts', [])

        # Initialize grouped posts structure
        grouped_posts = {
            f'{post_type}': []
            for post_type in PostType
        }
        
        if not all_posts:
            return grouped_posts
            
        logger.debug(f"Scanning {len(all_posts)} posts on {platform}")
        
        # Calculate chunk size and create chunks
        chunk_size = self._calculate_chunk_size(len(all_posts))
        post_chunks = self._chunk_data(all_posts, chunk_size)
        
        logger.debug(f"Processing {len(post_chunks)} post chunks of size ~{chunk_size} on {platform}")
        
        # Process chunks concurrently
        chunk_tasks = [self._process_post_chunk(chunk) for chunk in post_chunks]
    
        chunk_results = await asyncio.gather(*chunk_tasks)
        
        # Aggregate results from all chunks
        for chunk_grouped_posts in chunk_results:
            for post_type_key, posts in chunk_grouped_posts.items():
                # Remove the '_posts_found' suffix to get the clean key
                clean_key = post_type_key.replace('_posts_found', '')
                grouped_posts[clean_key].extend(posts)
        
        return grouped_posts

    def _post_relates_to_user(self, post: Dict[str, Any]) -> bool:
        """
        Check if a post relates to the user (authored by them or tags them).
        
        Args:
            post: Post dictionary to check
            
        Returns:
            True if post relates to the user
        """
        # Check if post is authored by discovered usernames
        post_username = post.get('username', '')
        authored_by_user = post_username in self._discovered_usernames
        
        # Check if user is tagged in the post
        tagged_users = post.get('tagged_users', [])
        tagged_in_post = any([
            username in self._discovered_usernames for username in tagged_users
        ])
        
        return authored_by_user or tagged_in_post

    async def _extract_platform_data(self, platform: SocialMediaPlatform) -> Dict[str, Any]:
        """
        Extract all relevant data for a user from a specific platform.
        
        Args:
            platform: The social media platform to extract from
            
        Returns:
            Dictionary containing discovered data for the platform
        """
        logger.info(f"Extracting data from {platform}")
        
        # Scan profiles first to discover usernames (must complete before posts)
        profiles_found = await self._scan_platform_profiles(platform)

        # Now scan all posts once (after usernames are discovered)
        grouped_posts = await self._scan_platform_posts(platform)
        
        return {
            'name': platform,
            'profiles': profiles_found,
            'posts': grouped_posts
        }

    async def _extract_data_async(self) -> Dict[str, Any]:
        """
        Extract all social media data for the user across all platforms.
        
        Returns:
            Dictionary containing social media digital footprints per platform
        """
        logger.info(f"Starting social media extract for user: {self._user.first_name} {self._user.last_name}")
        
        # Create tasks for all platforms
        platform_tasks = [
            self._extract_platform_data(platform)
            for platform in SocialMediaPlatform
        ]
        
        # Execute all platform extractions concurrently
        platform_results = await asyncio.gather(*platform_tasks)
        
        # Aggregate results
        aggregated_data = {
            'discovered_identities': {
                'usernames': list(self._discovered_usernames),
                'platforms_found': []
            },
            'platforms': platform_results,
            'summary': {
                'profiles': 0,
                **{f'{post_type}': 0 for post_type in PostType},
                'platforms_with_data': 0,
                'total_posts': 0
            }
        }
        
        # Process platform results
        for platform_data in platform_results:
            platform_name = platform_data['name']
            
            # Update summary
            platform_has_presence = (
                len(platform_data['profiles']) > 0 or 
                any(len(posts) > 0 for posts in platform_data['posts'].values())
            )
            
            if platform_has_presence:
                aggregated_data['summary']['platforms_with_data'] += 1
                aggregated_data['discovered_identities']['platforms_found'].append(platform_name)
            
            aggregated_data['summary']['profiles'] += len(platform_data['profiles'])
            
            # Update counts for each post type dynamically
            for post_type in PostType:
                count = len(platform_data['posts'][post_type])
                aggregated_data['summary'][post_type] += count
                aggregated_data['summary']['total_posts'] += count
        
        logger.info(f"Extraction complete. Found presence on {aggregated_data['summary']['platforms_with_data']} platforms")
        logger.info(f"Total digital footprints: {aggregated_data['summary']['profiles']} profiles, "
                   f"{aggregated_data['summary']['total_posts']} posts")
        
        return aggregated_data

    def _extract_data(self) -> Dict[str, Any]:
        """
        Synchronous wrapper for async extract method.
        
        Returns:
            Dictionary containing social media digital footprints per platform
        """
        return asyncio.run(self._extract_data_async())
        