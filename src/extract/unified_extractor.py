import asyncio
from typing import Any, Dict

from src.extract.base_extractor import BaseExtractor
from src.extract.search_results_extractor import SearchResultsExtractor
from src.extract.social_media_extractor import SocialMediaExtractor
from src.utils.logger import logger


class UnifiedExtractor(BaseExtractor):
    """Unified extractor that runs multiple specialized extractors concurrently."""

    def __init__(self, user_id: int):
        """Initialize the unified extractor."""
        super().__init__(user_id)

        self._search_extractor = SearchResultsExtractor(user_id)
        self._social_extractor = SocialMediaExtractor(user_id)

    async def _extract_data_async(self) -> Dict[str, Any]:
        """Extract data from all sources concurrently."""
        logger.info(f"Starting unified extract for user: {self._user.first_name} {self._user.last_name}")
        
        # Run both extractors concurrently
        search_task = self._search_extractor._extract_data_async()
        social_task = self._social_extractor._extract_data_async()
        
        # Wait for both extractions to complete
        search_results, social_results = await asyncio.gather(search_task, social_task)
        
        # Create unified structure with consistent naming and better organization
        unified_data = {
            'user_profile': {
                'discovered_identities': social_results.get('discovered_identities', {}),
                'summary': {
                    'total_sources_found': (
                        search_results['summary']['engines_with_data'] + 
                        social_results['summary']['platforms_with_data']
                    ),
                    'digital_footprints_count': (
                        search_results['summary']['total_results'] + 
                        social_results['summary']['total_posts'] + 
                        social_results['summary']['profiles']
                    )
                }
            },
            'search_results': {
                'engines': search_results['engines'],
                'summary': search_results['summary']
            },
            'social_media': {
                'platforms': social_results['platforms'],
                'summary': social_results['summary']
            }
        }
        
        logger.info("Unified extract completed successfully")
        logger.info(f"Total potential digital footprints found: {unified_data['user_profile']['summary']['digital_footprints_count']}")
        logger.info(f"Sources with data: {unified_data['user_profile']['summary']['total_sources_found']}")
        
        return unified_data

    def _extract_data(self) -> Dict[str, Any]:
        """Synchronous wrapper for async extract method."""
        return asyncio.run(self._extract_data_async())
