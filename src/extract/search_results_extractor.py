"""
Search Results Extractor Module

This module provides an extractor that scans all search engine results in the simulate world
to find digital footprints of a given user. It acts as a digital forensics tool,
discovering user traces across all search engines without prior knowledge.
"""
import asyncio
from typing import Any, Dict, List

from src.config.enums import SearchEngine, SearchResultType
from src.extract.base_extractor import BaseExtractor
from src.utils.logger import logger


class SearchResultsExtractor(BaseExtractor):
    """Extractor that discovers all search engine digital footprints of a user."""

    def __init__(self, user_id: int, simulation_data_path: str = None):
        """Initialize the search results extractor."""
        if simulation_data_path:
            super().__init__(user_id, simulation_data_path)
        else:
            super().__init__(user_id)
        self._discovered_results = []

    async def _process_results_chunk(
            self,
            results_chunk: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Process a chunk of search results concurrently."""
        grouped_results = {
            f'{result_type}_results_found': []
            for result_type in SearchResultType
        }

        for result in results_chunk:
            if self._result_relates_to_user(result):
                result_type = result.get('result_type')
                grouped_results[f'{result_type}_results_found'].append(result)
        
        return grouped_results

    def _result_relates_to_user(self, result: Dict[str, Any]) -> bool:
        """Check if a search result relates to the user based on exact string matching."""
        # Get user's first and last names for strict matching
        first_name = self._user.first_name.lower()
        last_name = self._user.last_name.lower()

        # Check for strict name matches (both first and last name must appear)
        title = str(result.get('title', '')).lower()
        title_match = first_name in title and last_name in title

        content = str(result.get('content', '')).lower()
        content_match = first_name in content and last_name in content

        url = str(result.get('url', '')).lower()
        url_match = first_name in url and last_name in url

        description = str(result.get('description', '')).lower()
        description_match = first_name in description and last_name in description

        # Check email matches in content/description
        email_in_content = any(email in content for email in self._user_identifiers['emails'])
        email_in_description = any(email in description for email in self._user_identifiers['emails'])

        # Check phone matches in content/description
        phone_in_content = any(phone in content for phone in self._user_identifiers['phones'])
        phone_in_description = any(phone in description for phone in self._user_identifiers['phones'])

        # Check address/location matches
        address_in_content = any(addr in content for addr in self._user_identifiers['addresses'])
        address_in_description = any(addr in description for addr in self._user_identifiers['addresses'])

        # Return True if any criteria matches
        return (title_match or content_match or url_match or description_match or
                email_in_content or email_in_description or
                phone_in_content or phone_in_description or
                address_in_content or address_in_description)

    async def _extract_search_engine_data(self, search_engine: SearchEngine) -> Dict[str, Any]:
        """Extract all relevant data for a user from a specific search engine using chunked processing."""
        logger.info(f"Extracting search results from {search_engine}")
        
        # Access search engine data from JSON structure
        search_engine_data = self._simulation_data.get(search_engine, {})
        search_results = search_engine_data.get('search_results', [])
        
        # Initialize grouped results dynamically from SearchResultType enum
        grouped_results = {
            f'{result_type}': []
            for result_type in SearchResultType
        }
        
        if not search_results:
            return {
                'name': search_engine,
                'results': grouped_results
            }
            
        logger.debug(f"Scanning {len(search_results)} results from {search_engine}")
        
        # Calculate chunk size and create chunks
        chunk_size = self._calculate_chunk_size(len(search_results))
        results_chunks = self._chunk_data(search_results, chunk_size)
        
        logger.debug(f"Processing {len(results_chunks)} result chunks of size ~{chunk_size} on {search_engine}")
        
        # Process chunks concurrently
        chunk_tasks = [self._process_results_chunk(chunk) for chunk in results_chunks]
        
        chunk_results = await asyncio.gather(*chunk_tasks)
        
        # Aggregate results from all chunks
        for chunk_grouped_results in chunk_results:
            for result_type_key, results in chunk_grouped_results.items():
                # Remove the '_results_found' suffix to get the clean key
                clean_key = result_type_key.replace('_results_found', '')
                grouped_results[clean_key].extend(results)
        
        return {
            'name': search_engine,
            'results': grouped_results
        }

    async def _extract_data_async(self) -> Dict[str, Any]:
        """Extract all search results data for the user across all search engines."""
        logger.info(f"Starting search results extract for user: {self._user.first_name} {self._user.last_name}")
        
        # Create tasks for all search engines
        search_engine_tasks = [
            self._extract_search_engine_data(search_engine) 
            for search_engine in SearchEngine
        ]
        
        # Execute all search engine extractions concurrently
        search_engine_results = await asyncio.gather(*search_engine_tasks)
        
        # Aggregate results
        aggregated_data = {
            'engines': search_engine_results,
            'summary': {
                **{f'{result_type}': 0 for result_type in SearchResultType},
                'engines_with_data': 0,
                'total_results': 0
            }
        }
        
        # Process search engine results
        for engine_data in search_engine_results:
            engine_name = engine_data['name']
            
            # Check if engine has any results
            engine_has_results = any(
                len(results) > 0
                for results in engine_data['results'].values()
            )
            
            if engine_has_results:
                aggregated_data['summary']['engines_with_data'] += 1
            
            # Update counts for each result type dynamically
            for result_type in SearchResultType:
                count = len(engine_data['results'][result_type])
                aggregated_data['summary'][result_type] += count
                aggregated_data['summary']['total_results'] += count
        
        logger.info(f"Search results extract complete. Found {aggregated_data['summary']['total_results']} relevant results across {aggregated_data['summary']['engines_with_data']} search engines")
        
        return aggregated_data

    def _extract_data(self) -> Dict[str, Any]:
        """Synchronous wrapper for async extract method."""
        return asyncio.run(self._extract_data_async())
