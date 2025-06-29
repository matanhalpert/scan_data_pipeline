import os
import uuid
from pathlib import Path
from src.config.enums import FileMediaType, ImageSuffix, VideoSuffix
from typing import Union, Optional
from PIL import Image

from src.utils.logger import logger


class FileManager:
    MEDIA_DIR = Path(__file__).parent
    IMAGE_DIR = MEDIA_DIR / "images"
    VIDEO_DIR = MEDIA_DIR / "videos"

    # Multiple mock files for different formats
    MOCK_FILES = {
        ImageSuffix.PNG: IMAGE_DIR / 'mock_image.png',
        ImageSuffix.JPG: IMAGE_DIR / 'mock_image.jpg',
        ImageSuffix.JPEG: IMAGE_DIR / 'mock_image.jpeg',
        ImageSuffix.GIF: IMAGE_DIR / 'mock_image.gif',
        VideoSuffix.MP4: VIDEO_DIR / 'mock_video.mp4',
        VideoSuffix.AVI: VIDEO_DIR / 'mock_video.avi',
        VideoSuffix.MKV: VIDEO_DIR / 'mock_video.mkv',
        VideoSuffix.WMV: VIDEO_DIR / 'mock_video.wmv',
    }

    PROTECTED_FILES = list(MOCK_FILES.values())

    @staticmethod
    def is_supported_format(suffix: Union[ImageSuffix, VideoSuffix, str], media_type: FileMediaType) -> bool:
        """Check if the file format is supported for the given media type."""
        suffix = suffix.lower()
        if media_type == FileMediaType.IMAGE:
            return suffix in list(ImageSuffix)
        elif media_type == FileMediaType.VIDEO:
            return suffix in list(VideoSuffix)
        return False

    @classmethod
    def save_media(
            cls,
            file: Union[Image.Image, bytes],
            filename: str,
            suffix: Union[ImageSuffix, VideoSuffix, str],
            directory: Optional[Union[str, Path]] = None,
            media_type: FileMediaType = None,
            silent: bool = True
    ) -> bool:
        """
        Save an image or video file with the specified name and suffix to the given directory.
        If no directory is specified, saves to the default directory based on media type.

        Args:
            file (Union[Image.Image, bytes]): The image or video file to save
            filename (str): Name for the file without suffix.
            suffix (str): File suffix/extension (without the dot)
            directory (Optional[Union[str, Path]]): Optional custom directory to save the file in
            media_type (FileMediaType): Type of media ('image' or 'video')
            silent (bool): Whether logs of successfull save is shown or not
        Returns:
            bool: True if file was saved successfully, False otherwise
        """
        try:
            if not FileManager.is_supported_format(suffix, media_type):
                logger.error(f"Unsupported {media_type} format: {suffix}")
                return False

            # Determine the appropriate directory
            if directory is None:
                if media_type == FileMediaType.IMAGE:
                    directory = cls.IMAGE_DIR
                elif media_type == FileMediaType.VIDEO:
                    directory = cls.VIDEO_DIR
                else:
                    logger.error(f"Invalid media type: {media_type}")
                    return False

            # Create directory if it doesn't exist
            os.makedirs(directory, exist_ok=True)

            # Construct the filepath
            filepath = os.path.join(directory, f"{filename}{suffix}")

            # Save the file based on its type
            if media_type == FileMediaType.IMAGE:
                if not isinstance(file, Image.Image):
                    raise TypeError("File must be a PIL Image instance for image media type")
                file.save(filepath)
            elif media_type == FileMediaType.VIDEO:
                if not isinstance(file, bytes):
                    raise TypeError("File must be bytes for video media type")
                with open(filepath, 'wb') as f:
                    f.write(file)
            if not silent:
                logger.info(f"Successfully saved {media_type} file: {filepath}")
            return True
        except Exception as e:
            logger.error(f"Error saving {media_type} file: {str(e)}")
            return False

    @staticmethod
    def delete_media(filename: str, directory: Union[str, Path]) -> bool:
        """
        Delete a file with the specified name from the given directory.
        
        Args:
            filename (str): Name of the file with extension (e.g. 'image.jpg' or 'video.mp4')
            directory (Union[str, Path]): Directory containing the file
            
        Returns:
            bool: True if file was deleted successfully, False otherwise
        """
        try:
            # Construct the filepath
            filepath = os.path.join(directory, filename)
            
            # Check if file exists before attempting to delete
            if os.path.exists(filepath):
                os.remove(filepath)
                logger.info(f"Successfully deleted file: {filepath}")
                return True
            else:
                logger.warning(f"File not found: {filepath}")
                return False
        except Exception as e:
            logger.error(f"Error deleting file: {str(e)}")
            return False

    @classmethod
    def clear_media_files(cls, directory: Union[str, Path] = MEDIA_DIR) -> bool:
        """
        Clear all media files (images and videos) from the specified directory and its subdirectories,
        while preserving the directory structure. Only deletes files with supported media suffixes.

        Args:
            directory (Union[str, Path]): Root directory to start clearing files from

        Returns:
            bool: True if operation was successful, False if any errors occurred
        """
        try:
            directory = Path(directory)
            if not directory.exists():
                logger.warning(f"Directory does not exist: {directory}")
                return False

            success = True
            # Walk through directory and all subdirectories
            for root, _, files in os.walk(directory):
                for file in files:
                    current_file_path = Path(root) / file
                    
                    # Check if the file is in the skip list
                    if current_file_path in cls.PROTECTED_FILES:
                        logger.info(f"Skipping protected file: {current_file_path}")
                        continue

                    # Extract file suffix (extension without the dot)
                    file_suffix = Path(file).suffix

                    # Check if the file has a supported media suffix
                    is_image = cls.is_supported_format(file_suffix, FileMediaType.IMAGE)
                    is_video = cls.is_supported_format(file_suffix, FileMediaType.VIDEO)

                    if is_image or is_video:
                        image_deleted: bool = cls.delete_media(filename=file, directory=root)
                        if not image_deleted:
                            success = False
                    else:
                        logger.debug(f"Skipping non-media file: {file}")

            return success
        except Exception as e:
            logger.error(f"Error clearing files: {str(e)}")
            return False
