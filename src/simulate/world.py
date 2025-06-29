"""
Simulation World Module

This module creates a complete simulate world state at a specific point in time.
It integrates various social media simulators (Facebook, Instagram, etc.) to generate
a consistent snapshot of user data across platforms.

The module is primarily used to create test data for the data pipeline scan process.
"""
import os
import json
import random
from datetime import datetime
from typing import Any, Optional, Dict, List

from src.simulate.data_generator import DataGenerator as dg
from src.simulate.social_media import SocialMediaSimulator, SocialMediaPlatform
from src.simulate.search_engines import SearchEngineSimulator, SearchEngine
from src.config.simulation_config import SOCIAL_MEDIA_PLATFORMS_CONFIG, SEARCH_ENGINES_CONFIG
from src.database.models import User
from src.utils.logger import logger

# Get the absolute path to the simulate module directory
SIMULATION_DIR = os.path.dirname(os.path.abspath(__file__))

DEFAULT_PLATFORM_CONFIG = {
    'user_chance': 0.5,
    'text_only_posts_range': (1, 5),
    'image_posts_range': (1, 5),
    'video_posts_range': (0, 2),
}

DEFAULT_SEARCH_ENGINE_CONFIG = {
    'results_chance': 0.5,
    'image_results_range': (0, 3),
    'video_results_range': (0, 2),
    'webpage_results_range': (1, 5),
    'pdf_results_range': (0, 2)
}


class SimulationWorld:
    """
    Creates and manages a complete simulate world with consistent user data across
    multiple social media platforms. Each platform's data is generated with specific
    probabilities and content ranges defined in SOCIAL_MEDIA_PLATFORMS.
    """

    def __init__(
            self,
            base_users_count: int = 100,
            timestamp: Optional[datetime] = None,
            unique_users: Optional[List[User]] = None
    ):
        """
        Initialize the simulate world with a specified base users count and timestamp.

        Args:
            base_users_count: Number of randomly generated base (NPC) users to create in the simulate
            timestamp: Point in time for the simulate (defaults to current time)
            unique_users: Optional list of pre-defined unique users to include in the simulate
        """
        self._unique_users_count = 0  # Track number of unique users added
        self._base_users_count = base_users_count
        self._simulation_users = []  # List of basic user info for generation purposes
        self.timestamp = timestamp or datetime.now()
        self._is_initialized = False

        # Add unique users if provided during initialization
        if unique_users:
            self.add_unique_users(unique_users)

        for platform in SocialMediaPlatform:
            setattr(self, f'_{platform}_simulator', SocialMediaSimulator(platform=platform))
            setattr(self, f'{platform}_profiles', [])
            setattr(self, f'{platform}_text_only_posts', [])
            setattr(self, f'{platform}_image_posts', [])
            setattr(self, f'{platform}_video_posts', [])

        for search_engine in SearchEngine:
            results_range = SEARCH_ENGINES_CONFIG[search_engine]['results_range']
            setattr(self, f'_{search_engine}_simulator', SearchEngineSimulator(search_engine, results_range))
            setattr(self, f'{search_engine}_image_results', [])
            setattr(self, f'{search_engine}_video_results', [])
            setattr(self, f'{search_engine}_webpage_results', [])
            setattr(self, f'{search_engine}_pdf_results', [])

        self._validate_configurations()

        logger.info(f"Initializing SimulationWorld with {base_users_count} base users + {self._unique_users_count} unique users at {self.timestamp}")

    def add_unique_users(self, unique_users: List[User]) -> None:
        """
        Add unique users to the simulate before generation.
        
        These users will be treated the same as base users during generation but can
        serve special purposes in the digital footprint scan later.
        
        Args:
            unique_users: List of User model instances
            
        Raises:
            RuntimeError: If called after simulate has been initialized/generated
        """
        if self._is_initialized:
            raise RuntimeError("Cannot add unique users after simulate has been generated."
                               "Add unique users during initialization or before entering context manager.")
        
        self._simulation_users.extend(unique_users)
        self._unique_users_count += len(unique_users)
        logger.info(f"Added {len(unique_users)} unique users to simulate (total unique users: {self._unique_users_count})")

    def add_unique_user(self, unique_user: User) -> None:
        """
        Add a single unique user to the simulate before generation.
        
        Args:
            unique_user: User model instance
            
        Raises:
            RuntimeError: If called after simulate has been initialized/generated
        """
        self.add_unique_users([unique_user])

    def get_total_population(self) -> int:
        """
        Get the total population including both base users and unique users.
        
        Returns:
            Total number of users in the simulate
        """
        return self._base_users_count + self._unique_users_count

    def get_unique_users(self) -> List[User]:
        """
        Get the unique users that were added to the simulate.
        
        Note: This returns the first N users from _simulation_users where N is the number
        of unique users added, as unique users are added before base user generation.
        
        Returns:
            List of unique User model instances
        """
        return self._simulation_users[:self._unique_users_count]

    def get_base_users(self) -> List[User]:
        """
        Get the randomly generated base users (excluding unique users).
        
        Returns:
            List of base User model instances
        """
        return self._simulation_users[self._unique_users_count:]

    @staticmethod
    def _validate_configurations():
        """Validate that all required configurations exist and have required fields."""

        # Validate social media platforms
        for platform in SocialMediaPlatform:
            if platform not in SOCIAL_MEDIA_PLATFORMS_CONFIG:
                logger.warning(f"Missing configuration for platform {platform}, using defaults")
                SOCIAL_MEDIA_PLATFORMS_CONFIG[platform] = DEFAULT_PLATFORM_CONFIG.copy()
            else:
                # Validate required fields
                config = SOCIAL_MEDIA_PLATFORMS_CONFIG[platform]
                for field in DEFAULT_PLATFORM_CONFIG:
                    if field not in config:
                        logger.warning(f"Missing field '{field}' in {platform} configuration, using default")
                        config[field] = DEFAULT_PLATFORM_CONFIG[field]

        # Validate search engines
        for search_engine in SearchEngine:
            if search_engine not in SEARCH_ENGINES_CONFIG:
                logger.warning(f"Missing configuration for search engine {search_engine}, using defaults")
                SEARCH_ENGINES_CONFIG[search_engine] = DEFAULT_SEARCH_ENGINE_CONFIG.copy()
            else:
                # Validate required fields
                config = SEARCH_ENGINES_CONFIG[search_engine]
                for field in DEFAULT_SEARCH_ENGINE_CONFIG:
                    if field not in config:
                        logger.warning(f"Missing field '{field}' in {search_engine} configuration, using default")
                        config[field] = DEFAULT_SEARCH_ENGINE_CONFIG[field]

    @staticmethod
    def _get_platform_config(platform: SocialMediaPlatform) -> Dict[str, Any]:
        """Safely get platform configuration with defaults."""
        return SOCIAL_MEDIA_PLATFORMS_CONFIG.get(
            platform,
            DEFAULT_PLATFORM_CONFIG.copy()
        )

    @staticmethod
    def _get_search_engine_config(search_engine: SearchEngine) -> Dict[str, Any]:
        """Safely get search engine configuration with defaults."""
        return SEARCH_ENGINES_CONFIG.get(
            search_engine,
            DEFAULT_SEARCH_ENGINE_CONFIG.copy()
        )

    def __enter__(self):
        """
        Enter the context manager. This will initialize the simulate world
        by generating all the required data.

        Returns:
            self: The initialized SimulationWorld instance
        """
        if not self._is_initialized:
            self._generate()
            self._is_initialized = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context manager. This will perform any necessary cleanup.

        Args:
            exc_type: The type of exception that was raised (if any)
            exc_val: The exception value that was raised (if any)
            exc_tb: The traceback of the exception (if any)
        """
        # Clear all data
        self._simulation_users.clear()
        self._unique_users_count = 0

        # Clear social media data
        for platform in SocialMediaPlatform:
            getattr(self, f'{platform}_profiles').clear()
            getattr(self, f'{platform}_text_only_posts').clear()
            getattr(self, f'{platform}_image_posts').clear()
            getattr(self, f'{platform}_video_posts').clear()

        # Clear search engine data
        for search_engine in SearchEngine:
            getattr(self, f'{search_engine}_image_results').clear()
            getattr(self, f'{search_engine}_video_results').clear()
            getattr(self, f'{search_engine}_webpage_results').clear()
            getattr(self, f'{search_engine}_pdf_results').clear()

        self._is_initialized = False
        logger.info("Cleaned up simulate world resources")

    def _generate(self):
        """
        Generate the complete simulate world state by creating all users and their
        data across platforms. This is the main orchestration method for data generation.

        Time complexity: O(n) where n is the number of users
        """
        try:
            logger.info("Generating simulate world...")
            self._generate_base_users()
            self._generate_social_media_data()
            self._generate_search_engines_data()
            logger.info("Successfully generated simulate world")
        except Exception as e:
            logger.error(f"Failed to generate simulate world: {e}")
            raise

    def _generate_base_users(self):
        """Generate basic user information as a foundation for data generation."""
        # Generate additional base users (unique users may already be in _base_users)
        generated_users = [dg.generate_fictive_user() for _ in range(self._base_users_count)]
        self._simulation_users.extend(generated_users)
        total_users = len(self._simulation_users)
        logger.info(f"Generated {len(generated_users)} new base users, total users for generation: {total_users}")

    def _generate_social_media_data(self):
        """
        Generate social media data for each platform based on platform-specific
        configuration and user participation probabilities.
        """

        # Generate users for each platform
        profile_to_user_mapping = {}  # Keep track of profile to user mapping
        
        for platform in SocialMediaPlatform:
            platform_simulator = getattr(self, f'_{platform}_simulator')
            platform_config = self._get_platform_config(platform)

            for user in self._simulation_users:
                create_user: bool = random.random() < platform_config['user_chance']
                if create_user:
                    profile = platform_simulator.simulate_profile(user)
                    getattr(self, f'{platform}_profiles').append(profile)
                    # Store the mapping between profile and user
                    profile_to_user_mapping[id(profile)] = user

            platform_user_numbers = len(getattr(self, f'{platform}_profiles'))
            logger.info(f"Generated {platform} data for {platform_user_numbers} users")

        # Generate posts for each platform
        for platform in SocialMediaPlatform:
            platform_simulator = getattr(self, f'_{platform}_simulator')
            platform_config = self._get_platform_config(platform)

            platform_profiles = getattr(self, f'{platform}_profiles')
            platform_usernames = [profile.username for profile in platform_profiles]
            max_tagged_users = len(platform_usernames)

            text_only_posts_count = random.randint(*platform_config['text_only_posts_range'])
            image_posts_count = random.randint(*platform_config['image_posts_range'])
            video_posts_count = random.randint(*platform_config['video_posts_range'])

            for profile in platform_profiles:
                # Get the original user for this profile
                user = profile_to_user_mapping[id(profile)]
                
                text_only_posts = platform_simulator.simulate_text_only_posts(
                    profile=profile,
                    user=user,
                    max_tagged_users=max_tagged_users,
                    usernames_pool=platform_usernames,
                    count=text_only_posts_count
                )
                images_posts = platform_simulator.simulate_image_posts(
                    profile=profile,
                    user=user,
                    max_tagged_users=max_tagged_users,
                    usernames_pool=platform_usernames,
                    count=image_posts_count
                )
                videos_posts = platform_simulator.simulate_video_posts(
                    profile=profile,
                    user=user,
                    max_tagged_users=max_tagged_users,
                    usernames_pool=platform_usernames,
                    count=video_posts_count
                )

                # Store data in corresponding lists
                getattr(self, f'{platform}_text_only_posts').extend(text_only_posts)
                getattr(self, f'{platform}_image_posts').extend(images_posts)
                getattr(self, f'{platform}_video_posts').extend(videos_posts)

    def _generate_search_engines_data(self):
        """
        Generate search engine data for each engine based on engine-specific
        configuration and result type probabilities.
        """
        for search_engine in SearchEngine:
            search_engine_simulator = getattr(self, f'_{search_engine}_simulator')
            search_engine_config = self._get_search_engine_config(search_engine)
            results_chance = search_engine_config['results_chance']

            # Get result type ranges
            image_results_range = search_engine_config['image_results_range']
            video_results_range = search_engine_config['video_results_range']
            webpage_results_range = search_engine_config['webpage_results_range']
            pdf_results_range = search_engine_config['pdf_results_range']

            for user in self._simulation_users:
                get_results: bool = random.random() < results_chance

                if get_results:
                    # Generate different types of results separately
                    image_results_count = random.randint(*image_results_range)
                    video_results_count = random.randint(*video_results_range)
                    webpage_results_count = random.randint(*webpage_results_range)
                    pdf_results_count = random.randint(*pdf_results_range)

                    # Generate results for each type
                    image_results = search_engine_simulator.simulate_image_results(user, count=image_results_count)
                    video_results = search_engine_simulator.simulate_video_results(user, count=video_results_count)
                    webpage_results = search_engine_simulator.simulate_webpage_results(user, count=webpage_results_count)
                    pdf_results = search_engine_simulator.simulate_pdf_results(user, count=pdf_results_count)

                    # Store results in separate lists
                    getattr(self, f'{search_engine}_image_results').extend(image_results)
                    getattr(self, f'{search_engine}_video_results').extend(video_results)
                    getattr(self, f'{search_engine}_webpage_results').extend(webpage_results)
                    getattr(self, f'{search_engine}_pdf_results').extend(pdf_results)

            total_results = len(self._get_search_engine_all_results(search_engine))
            logger.info(f"Generated {search_engine} data with {total_results} search results")

    def export_data(
            self, output_dir: str = "simulation_data",
            save_to_disk: bool = True) -> Dict[str, Dict[str, List[Dict]]]:
        """
        Export all simulate data to JSON format.

        Args:
            output_dir: Directory to save the simulate data (only used if save_to_disk is True)
            save_to_disk: Whether to save the data to disk files (True) or just return it (False)

        Returns:
            Dictionary containing all simulate data organized by platform and search engines,
            with each platform containing profiles and posts (merged from all post types),
            and each search engine containing search results as lists of dictionaries
        """
        # Social media data - merge all post types into a single "posts" list
        data = {}
        for platform in SocialMediaPlatform:
            data[f"{platform}"] = {
                "profiles": [profile.to_dict() for profile in getattr(self, f'{platform}_profiles')],
                "posts": [post.to_dict() for post in self._get_platform_all_posts(platform)]
            }

        # Search engine data - keep as unified search_results
        search_data = {
            f"{search_engine}": {
                "search_results": [result.to_dict() for result in self._get_search_engine_all_results(search_engine)]
            } for search_engine in SearchEngine
        }

        # Combine both datasets
        data.update(search_data)

        if save_to_disk:
            # Create output directory if it doesn't exist, using absolute path based on simulate module location
            output_path = os.path.join(SIMULATION_DIR, output_dir)
            os.makedirs(output_path, exist_ok=True)

            # Export all data to a single JSON file with the same structure
            output_file = os.path.join(output_path, "simulation_data.json")
            with open(output_file, "w") as f:
                json.dump(data, f, indent=2)

            logger.info(f"Exported simulate data to {output_file}")

        return data

    def __str__(self) -> str:
        """
        Returns a string representation of the simulate world with key statistics
        including population size, timestamp, per-platform metrics, and search engine metrics.
        """
        header = "Simulation World Statistics"
        separator = "=" * 50

        # Start building the stats string
        stats = (
            f"{header}\n"
            f"{separator}\n"
            f"Total Population: {self.get_total_population()}\n"
            f"Timestamp: {self.timestamp}\n\n"
        )

        # Social Media Platform Statistics
        stats += "Social Media Platforms:\n"
        stats += "-" * 25 + "\n"

        for platform in SocialMediaPlatform:
            platform_name = platform.capitalize()

            profiles = getattr(self, f'{platform}_profiles')
            text_only_posts = getattr(self, f'{platform}_text_only_posts')
            image_posts = getattr(self, f'{platform}_image_posts')
            video_posts = getattr(self, f'{platform}_video_posts')

            num_profiles = len(profiles)
            num_text_only_posts = len(text_only_posts)
            num_image_posts = len(image_posts)
            num_video_posts = len(video_posts)
            total_posts = num_text_only_posts + num_image_posts + num_video_posts

            avg_text_only_posts_per_user = num_text_only_posts / max(1, num_profiles)
            avg_image_posts_per_user = num_image_posts / max(1, num_profiles)
            avg_video_posts_per_user = num_video_posts / max(1, num_profiles)
            avg_total_posts_per_user = total_posts / max(1, num_profiles)

            stats += (
                f"{platform_name}:\n"
                f"  - Profiles: {num_profiles}\n"
                f"  - Total Posts: {total_posts}\n"
                f"  - Text Only Posts: {num_text_only_posts}\n"
                f"  - Image Posts: {num_image_posts}\n"
                f"  - Video Posts: {num_video_posts}\n"
                f"  - Avg Total Posts/User: {avg_total_posts_per_user:.1f}\n"
                f"  - Avg Text Only Posts/User: {avg_text_only_posts_per_user:.1f}\n"
                f"  - Avg Image Posts/User: {avg_image_posts_per_user:.1f}\n"
                f"  - Avg Video Posts/User: {avg_video_posts_per_user:.1f}\n\n"
            )

        # Search Engine Statistics
        stats += "Search Engines:\n"
        stats += "-" * 15 + "\n"

        for search_engine in SearchEngine:
            search_engine_name = search_engine.capitalize()
            image_results = getattr(self, f'{search_engine}_image_results')
            video_results = getattr(self, f'{search_engine}_video_results')
            webpage_results = getattr(self, f'{search_engine}_webpage_results')
            pdf_results = getattr(self, f'{search_engine}_pdf_results')
            
            num_image_results = len(image_results)
            num_video_results = len(video_results)
            num_webpage_results = len(webpage_results)
            num_pdf_results = len(pdf_results)
            total_results = num_image_results + num_video_results + num_webpage_results + num_pdf_results
            
            avg_results_per_user = total_results / max(1, self.get_total_population())

            stats += (
                f"{search_engine_name}:\n"
                f"  - Total Results: {total_results}\n"
                f"  - Image Results: {num_image_results}\n"
                f"  - Video Results: {num_video_results}\n"
                f"  - Webpage Results: {num_webpage_results}\n"
                f"  - PDF Results: {num_pdf_results}\n"
                f"  - Avg Results/User: {avg_results_per_user:.1f}\n\n"
            )

        stats += separator
        return stats

    def _get_search_engine_all_results(self, search_engine: SearchEngine) -> List:
        """
        Get all search results for a specific search engine by combining all result types.
        
        Args:
            search_engine: The search engine to get results for
            
        Returns:
            List of all search results (image + video + webpage + pdf)
        """
        image_results = getattr(self, f'{search_engine}_image_results')
        video_results = getattr(self, f'{search_engine}_video_results') 
        webpage_results = getattr(self, f'{search_engine}_webpage_results')
        pdf_results = getattr(self, f'{search_engine}_pdf_results')
        
        return image_results + video_results + webpage_results + pdf_results

    def _get_platform_all_posts(self, platform: SocialMediaPlatform) -> List:
        """
        Get all posts for a specific social media platform by combining all post types.
        
        Args:
            platform: The social media platform to get posts for
            
        Returns:
            List of all posts (text_only + image + video)
        """
        text_only_posts = getattr(self, f'{platform}_text_only_posts')
        image_posts = getattr(self, f'{platform}_image_posts')
        video_posts = getattr(self, f'{platform}_video_posts')
        
        return text_only_posts + image_posts + video_posts
