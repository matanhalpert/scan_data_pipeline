"""
Search Engine transformer for processing search results from various engines.
"""
import asyncio
from typing import Dict, Any, List
from datetime import datetime

from src.transform.base_transformer import BaseTransformer, TransformationResult
from src.database.models import DigitalFootprint, PersonalIdentity, ActivityLog
from src.config.enums import SearchResultType, DigitalFootprintType, PersonalIdentityType
from src.utils.logger import logger


class SearchEngineTransformer(BaseTransformer):
    """
    Transformer for processing search engine results including images, videos, webpages, and PDFs.
    """

    def _transform_data(self) -> TransformationResult:
        """
        Transform search results data into structured entities using concurrent batch processing.
        
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
        Async implementation of search results data transform.
        
        Returns:
            TransformationResult: The transform results
        """
        # Process search results data if it exists
        search_results_data = self._extraction_data.get('data', {}).get('search_results', {})
        if not search_results_data:
            logger.info("No search results data found in extract")
            return TransformationResult()

        # Collect all search results into a single list for batch processing
        all_items = []
        engines = search_results_data.get('engines', [])
        
        for engine_data in engines:
            engine_name = engine_data.get('name', '')
            logger.info(f"Collecting search results from engine: {engine_name}")
            
            results = engine_data.get('results', {})
            
            # Process different result types
            for result_type, result_list in results.items():
                if isinstance(result_list, list):
                    for search_result in result_list:
                        # Add metadata to help _process_item determine how to handle this item
                        search_result['result_type'] = result_type
                        search_result['search_engine'] = engine_name
                        search_result['item_type'] = 'search_result'
                        all_items.append(search_result)
        
        logger.info(f"Collected {len(all_items)} search results for batch processing")
        
        # Use the concurrent batch processing infrastructure from base class
        if all_items:
            return await self._process_all_batches(all_items)
        else:
            return TransformationResult()

    def _process_item(self, item: Dict[str, Any]) -> TransformationResult:
        """
        Process a single search result item.
        
        Args:
            item: Single search result from extract data
            
        Returns:
            TransformationResult: Results from processing the item
        """
        return self._process_search_result(item)

    def _process_search_result(self, search_result: Dict[str, Any]) -> TransformationResult:
        """
        Process a single search result.
        
        Args:
            search_result: Search result data from extract
            
        Returns:
            TransformationResult: Results from processing the search result
        """
        result = TransformationResult()
        result.processing_stats['items_processed'] += 1
        
        try:
            # Extract search result data
            result_url = search_result.get('url', '')
            if not result_url:
                logger.warning("Search result missing URL, skipping")
                return result
            
            # Determine footprint type
            footprint_type = self._determine_footprint_type(search_result)
            
            # For media search results, the URL itself is the media URL
            media_url = result_url if footprint_type in [DigitalFootprintType.IMAGE, DigitalFootprintType.VIDEO] else None
            
            # Get or create digital footprint (media_filepath and source_id are now handled automatically)
            digital_footprint, is_new = self._get_or_create_digital_footprint(
                reference_url=result_url,
                footprint_type=footprint_type,
                media_url=media_url
            )
            
            result.processing_stats['footprints_found'] += 1
            
            if is_new:
                result.new_digital_footprints.append(digital_footprint)
                result.processing_stats['new_footprints'] += 1
            else:
                result.processing_stats['existing_footprints'] += 1
            
            # Analyze search result content for identities
            identities_detected = self._analyze_search_result_for_identities(search_result)
            
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
            
            # Track pending activity log instead of creating it immediately
            # Use current time since search results don't have timestamps
            self._track_pending_activity_log(result, digital_footprint, datetime.now())
            
        except Exception as e:
            logger.error(f"Error processing search result: {e}")
        
        return result

    def _analyze_search_result_for_identities(self, search_result: Dict[str, Any]) -> List[PersonalIdentityType]:
        """
        Analyze search result data for user personal identities.
        
        Args:
            search_result: Search result data
            
        Returns:
            List[PersonalIdentityType]: List of detected identity types
        """
        identities_found = []
        
        # Check various search result fields for user identities
        text_fields = [
            search_result.get('title', ''),
            search_result.get('description', ''),
            search_result.get('content', '')
        ]
        
        combined_text = ' '.join(text_fields)
        
        # Analyze combined text for identities
        text_identities = self._analyze_text_for_identities(combined_text)
        identities_found.extend(text_identities)
        
        return identities_found

    def _determine_footprint_type(self, search_result: Dict[str, Any]) -> DigitalFootprintType:
        """
        Determine the digital footprint type based on search result data.
        Override base method to handle search result specifics.
        
        Args:
            search_result: Search result data
            
        Returns:
            DigitalFootprintType: The determined footprint type
        """
        # Check result_type first
        result_type = search_result.get('result_type', '').lower()
        
        if result_type == SearchResultType.IMAGE.value:
            return DigitalFootprintType.IMAGE
        elif result_type == SearchResultType.VIDEO.value:
            return DigitalFootprintType.VIDEO
        elif result_type in [SearchResultType.WEBPAGE.value, SearchResultType.PDF.value]:
            return DigitalFootprintType.TEXT
        
        # Fallback to base method
        return super()._determine_footprint_type(search_result)

    def _merge_results(self, main_result: TransformationResult, item_result: TransformationResult):
        """
        Merge item result into main result.
        
        Args:
            main_result: Main result to merge into
            item_result: Item result to merge from
        """
        main_result.new_digital_footprints.extend(item_result.new_digital_footprints)
        main_result.personal_identities.extend(item_result.personal_identities)
        main_result.activity_logs.extend(item_result.activity_logs)
        
        for key, value in item_result.processing_stats.items():
            main_result.processing_stats[key] += value 