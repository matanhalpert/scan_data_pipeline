import random
from pathlib import Path
from typing import List, Optional
from enum import Enum

from src.utils.logger import logger


class MediaType(Enum):
    """Types of media in the pool."""
    IMAGE = "images"
    VIDEO = "videos"


class MediaPool:
    """
    Manages a pool of reference mock media files for simulate.
    
    This class provides realistic media file references without the overhead
    of generating unique files for each simulate run.
    """
    
    def __init__(self, base_media_dir: str = None):
        """Initialize the media pool with base directory."""
        if base_media_dir is None:
            # Use absolute path relative to this file's location
            self.base_media_dir = Path(__file__).parent
        else:
            self.base_media_dir = Path(base_media_dir)
        self._image_pool: List[Path] = []
        self._video_pool: List[Path] = []
        self._initialize_pools()
    
    def _initialize_pools(self) -> None:
        """Initialize the media pools with existing mock files."""
        # Image pool
        image_dir = self.base_media_dir / "images"
        if image_dir.exists():
            self._image_pool = [
                f for f in image_dir.glob("mock_image.*")
                if f.is_file()
            ]
        
        # Video pool
        video_dir = self.base_media_dir / "videos"
        if video_dir.exists():
            self._video_pool = [
                f for f in video_dir.glob("mock_video.*")
                if f.is_file()
            ]
        
        logger.info(f"MediaPool initialized: {len(self._image_pool)} images, {len(self._video_pool)} videos")
    
    def get_random_image(self) -> Optional[str]:
        """
        Get a random image file from the pool.
        
        Returns:
            str: Path to a random image file, or None if pool is empty
        """
        if not self._image_pool:
            logger.warning("Image pool is empty")
            return None
        
        selected_file = random.choice(self._image_pool)
        return str(selected_file)
    
    def get_random_video(self) -> Optional[str]:
        """
        Get a random video file from the pool.
        
        Returns:
            str: Path to a random video file, or None if pool is empty
        """
        if not self._video_pool:
            logger.warning("Video pool is empty")
            return None
        
        selected_file = random.choice(self._video_pool)
        return str(selected_file)
    
    def get_random_media(self, media_type: MediaType) -> Optional[str]:
        """
        Get a random media file of specified type.
        
        Args:
            media_type: Type of media to retrieve
            
        Returns:
            str: Path to a random media file of the specified type
        """
        if media_type == MediaType.IMAGE:
            return self.get_random_image()
        elif media_type == MediaType.VIDEO:
            return self.get_random_video()
        else:
            logger.error(f"Unknown media type: {media_type}")
            return None
    
    def get_media_by_extension(self, extension: str) -> Optional[str]:
        """
        Get a random media file with specific extension.
        
        Args:
            extension: File extension (e.g., 'jpg', 'mp4')
            
        Returns:
            str: Path to a random media file with the specified extension
        """
        extension = extension.lower().lstrip('.')
        
        # Check image pool
        image_files = [f for f in self._image_pool if f.suffix.lower().lstrip('.') == extension]
        if image_files:
            return str(random.choice(image_files))
        
        # Check video pool
        video_files = [f for f in self._video_pool if f.suffix.lower().lstrip('.') == extension]
        if video_files:
            return str(random.choice(video_files))
        
        logger.warning(f"No media files found with extension: {extension}")
        return None
    
    def get_pool_stats(self) -> dict:
        """
        Get statistics about the media pool.
        
        Returns:
            dict: Pool statistics
        """
        return {
            "total_images": len(self._image_pool),
            "total_videos": len(self._video_pool),
            "image_extensions": list(set(f.suffix.lower() for f in self._image_pool)),
            "video_extensions": list(set(f.suffix.lower() for f in self._video_pool)),
            "total_files": len(self._image_pool) + len(self._video_pool)
        }
    
    def validate_pool(self) -> bool:
        """
        Validate that all files in the pool exist and are accessible.
        
        Returns:
            bool: True if all files are valid
        """
        all_files = self._image_pool + self._video_pool
        
        for file_path in all_files:
            if not file_path.exists():
                logger.error(f"Pool file does not exist: {file_path}")
                return False
            
            if not file_path.is_file():
                logger.error(f"Pool path is not a file: {file_path}")
                return False
        
        logger.info(f"Media pool validation successful: {len(all_files)} files")
        return True


# Global instance for easy access
media_pool = MediaPool()
