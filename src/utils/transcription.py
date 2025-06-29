import os
import tempfile
import whisper
from moviepy import VideoFileClip
from typing import Optional
from src.utils.logger import logger
from pathlib import Path
from src.config.enums import VideoSuffix


class Transcriptor:
    """
    A class for transcribing video files to text using OpenAI Whisper.
    """
    _instance = None
    _model = None
    
    def __new__(cls, model_size: str = "base"):
        if cls._instance is None:
            cls._instance = super(Transcriptor, cls).__new__(cls)
            cls._instance.model_size = model_size
            cls._instance._load_model()
        return cls._instance
    
    def _load_model(self):
        """Load the Whisper model if not already loaded."""
        if Transcriptor._model is None:
            try:
                logger.info(f"Loading Whisper model: {self.model_size}")
                Transcriptor._model = whisper.load_model(self.model_size)
                logger.info("Whisper model loaded successfully")
            except Exception as e:
                logger.error(f"Failed to load Whisper model: {e}")
                raise
        self.model = Transcriptor._model

    def _extract_audio(self, video_path: str, audio_path: str) -> None:
        """
        Extract audio from video file.
        
        Args:
            video_path (str): Path to the input video file
            audio_path (str): Path where the extracted audio will be saved
        """
        if not self.is_supported_format(video_path):
            raise ValueError("...")

        try:
            logger.info(f"Extracting audio from: {video_path}")
            video = VideoFileClip(video_path)
            audio = video.audio
            audio.write_audiofile(audio_path, logger=None)
            audio.close()
            video.close()
            logger.info("Audio extract completed")
        except Exception as e:
            logger.error(f"Failed to extract audio: {e}")
            raise
    
    def transcribe_video(self, video_path: str, language: Optional[str] = None) -> str:
        """
        Transcribe a video file to text.
        
        Args:
            video_path (str): Path to the video file to transcribe
            language (str, optional): Language code (e.g., 'en', 'es', 'fr').
                                    If None, Whisper will auto-detect the language.
        
        Returns:
            str: The transcribed text from the video
            
        Raises:
            FileNotFoundError: If the video file doesn't exist
            Exception: If transcription fails
        """
        if not os.path.exists(video_path):
            raise FileNotFoundError(f"Video file not found: {video_path}")
        
        if self.model is None:
            raise RuntimeError("Whisper model not loaded")
        
        # Create temporary file for audio extract
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as temp_audio:
            temp_audio_path = temp_audio.name
        
        try:
            # Extract audio from video
            self._extract_audio(video_path, temp_audio_path)
            
            # Transcribe audio
            logger.info("Starting transcription...")
            result = self.model.transcribe(
                temp_audio_path,
                language=language,
                verbose=False
            )
            
            transcription = result["text"].strip()
            logger.info("Transcription completed successfully")
            
            return transcription
            
        except Exception as e:
            logger.error(f"Transcription failed: {e}")
            raise
        finally:
            # Clean up temporary audio file
            if os.path.exists(temp_audio_path):
                os.unlink(temp_audio_path)

    @staticmethod
    def is_supported_format(file_path: str) -> bool:
        """
        Check if a file format is supported for transcription.

        Args:
            file_path (str): Path to the file to check

        Returns:
            bool: True if format is supported, False otherwise
        """
        _, ext = os.path.splitext(file_path.lower())
        return ext in list(VideoSuffix)
