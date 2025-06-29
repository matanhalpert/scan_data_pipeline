"""
Unified transformer that orchestrates both social media and search engine transformers concurrently.
"""
import asyncio
from typing import Any, Dict, List

from src.transform.base_transformer import BaseTransformer, TransformationResult
from src.transform.search_engine_transformer import SearchEngineTransformer
from src.transform.social_media_transformer import SocialMediaTransformer
from src.utils.logger import logger


class UnifiedTransformer(BaseTransformer):
    """Unified transformer that orchestrates multiple specialized transformers concurrently."""

    def __init__(self, user_id: int):
        """Initialize the unified transformer."""
        super().__init__(user_id)
        
        # Initialize specialized transformers
        self._social_media_transformer = SocialMediaTransformer(user_id)
        self._search_engine_transformer = SearchEngineTransformer(user_id)

    def _transform_data(self) -> TransformationResult:
        """Transform both social media and search engine data concurrently using pure asyncio."""
        logger.info("Starting unified transform with concurrent processing")
        
        # Use asyncio instead of ThreadPoolExecutor to avoid Windows access violation
        return asyncio.run(self._async_transform_data())
    
    async def _async_transform_data(self) -> TransformationResult:
        """Async implementation of transform using concurrent tasks."""
        main_result = TransformationResult()
        
        # Create concurrent tasks for both transformers
        social_media_task = asyncio.create_task(
            self._run_social_media_transformation(), 
            name="social_media_transformation"
        )
        search_engine_task = asyncio.create_task(
            self._run_search_engine_transformation(),
            name="search_engine_transformation"
        )
        
        try:
            # Wait for both transformations to complete
            social_result, search_result = await asyncio.gather(
                social_media_task, 
                search_engine_task,
                return_exceptions=True
            )
            
            # Handle social media result
            if isinstance(social_result, Exception):
                logger.error(f"Error in social_media transform: {social_result}")
            else:
                logger.info(f"Completed social_media transform: {social_result.processing_stats}")
                self._merge_results(main_result, social_result)
            
            # Handle search engine result
            if isinstance(search_result, Exception):
                logger.error(f"Error in search_engine transform: {search_result}")
            else:
                logger.info(f"Completed search_engine transform: {search_result.processing_stats}")
                self._merge_results(main_result, search_result)
                
        except Exception as e:
            logger.error(f"Error in unified transform: {e}")
            raise
        
        logger.info(f"Unified transform completed: {main_result.processing_stats}")
        return main_result
    
    async def _run_social_media_transformation(self) -> TransformationResult:
        """Run social media transform in async context."""
        return await self._social_media_transformer._async_transform_data()
    
    async def _run_search_engine_transformation(self) -> TransformationResult:
        """Run search engine transform in async context."""
        return await self._search_engine_transformer._async_transform_data()

    def _process_item(self, item: Dict[str, Any]) -> TransformationResult:
        """This method is not used in the unified transformer as it orchestrates full transformations."""
        logger.warning("_process_single_item called on UnifiedTransformer - this method is not used")
        return TransformationResult()

    @staticmethod
    def _merge_results(main_result: TransformationResult, item_result: TransformationResult):
        """Merge transformer result into main result."""
        main_result.new_digital_footprints.extend(item_result.new_digital_footprints)
        main_result.personal_identities.extend(item_result.personal_identities)
        main_result.activity_logs.extend(item_result.activity_logs)
        
        # Merge pending identities
        for reference_url, identity_types in item_result.pending_identities.items():
            if reference_url not in main_result.pending_identities:
                main_result.pending_identities[reference_url] = []
            main_result.pending_identities[reference_url].extend(identity_types)
            # Remove duplicates
            main_result.pending_identities[reference_url] = list(set(main_result.pending_identities[reference_url]))
        
        # Merge pending activity logs
        for reference_url, timestamps in item_result.pending_activity_logs.items():
            if reference_url not in main_result.pending_activity_logs:
                main_result.pending_activity_logs[reference_url] = []
            main_result.pending_activity_logs[reference_url].extend(timestamps)
            # Remove duplicates
            main_result.pending_activity_logs[reference_url] = list(set(main_result.pending_activity_logs[reference_url]))
        
        # Merge processing statistics
        for key, value in item_result.processing_stats.items():
            main_result.processing_stats[key] += value

    def get_detailed_summary(self) -> Dict[str, Any]:
        """Get detailed summary including breakdown by data source."""
        base_summary = self.get_summary()
        
        # Derive sub-transformer status from the main transform result
        # Since we run them directly via _transform_data(), they don't have their own status tracking
        sub_transformer_status = base_summary.get('transformation_status')
        sub_transformer_error = base_summary.get('error_message')
        
        # Estimate duration (since sub-transformers run concurrently, each gets roughly the full duration)
        total_duration = base_summary.get('transformation_duration')
        
        detailed_summary = {
            **base_summary,
            'social_media_transformer': {
                'status': sub_transformer_status,
                'duration': total_duration,
                'error': sub_transformer_error
            },
            'search_engine_transformer': {
                'status': sub_transformer_status, 
                'duration': total_duration,
                'error': sub_transformer_error
            }
        }
        
        return detailed_summary
