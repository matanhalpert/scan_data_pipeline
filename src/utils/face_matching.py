import time
from pathlib import Path
from typing import Optional, Union, List, TypedDict, Tuple

import cv2
import numpy as np
import face_recognition

from src.config.enums import Confidence, VideoSuffix
from src.utils.logger import logger


class MatchResult(TypedDict):
    """Result of face matching operation."""
    is_match: bool
    distance: Optional[float]
    confidence: Confidence
    faces_found: int
    frames_processed: Optional[int]
    match_frames: Optional[int]


class FaceMatcherError(Exception):
    """Exception raised when face matching operations fail."""
    pass


class FaceMatcher:
    """Face matching utility for comparing faces in images and videos."""

    def __init__(self, tolerance: float = 0.6, frame_sample_rate: int = 30) -> None:
        """Initialize face matcher with matching parameters."""
        self.tolerance: float = tolerance
        self.frame_sample_rate: int = frame_sample_rate
        self.max_frame_size: int = 320

    def process_frame_worker(self, frame_data: bytes, reference_encoding: np.ndarray,
                         max_frame_size: int, tolerance: float) -> Optional[float]:
        """Process a single video frame for face matching in parallel."""
        try:
            # Decode compressed frame data
            frame_array = np.frombuffer(frame_data, np.uint8)
            frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)

            if frame is None:
                return None

            # Resize frame for faster processing while maintaining aspect ratio
            frame_height, frame_width = frame.shape[:2]
            if max(frame_height, frame_width) > max_frame_size:
                scale_factor = max_frame_size / max(frame_height, frame_width)
                new_width = int(frame_width * scale_factor)
                new_height = int(frame_height * scale_factor)
                frame = cv2.resize(frame, (new_width, new_height), interpolation=cv2.INTER_AREA)

            # Convert BGR to RGB for face_recognition library
            rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            
            # Detect faces using HOG algorithm (faster than CNN)
            face_locations = face_recognition.face_locations(rgb_frame, model="hog", number_of_times_to_upsample=0)
            
            if not face_locations:
                return None

            # Generate face encodings for detected faces
            face_encodings = face_recognition.face_encodings(rgb_frame, face_locations, num_jitters=1)
            if not face_encodings:
                return None

            # Calculate distances to reference face
            distances = face_recognition.face_distance(face_encodings, reference_encoding)
            min_distance = float(distances.min())

            # Return distance if within tolerance threshold
            if min_distance <= tolerance:
                return min_distance

        except Exception as e:
            logger.debug(f"Frame processing error: {e}")
            return None

        return None

    @staticmethod
    def _resize_frame(frame: np.ndarray, max_size: int) -> np.ndarray:
        """Resize frame while maintaining aspect ratio."""
        frame_height, frame_width = frame.shape[:2]
        if max(frame_height, frame_width) <= max_size:
            return frame

        scale_factor = max_size / max(frame_height, frame_width)
        new_width = int(frame_width * scale_factor)
        new_height = int(frame_height * scale_factor)
        return cv2.resize(frame, (new_width, new_height), interpolation=cv2.INTER_AREA)

    def _process_image(self, image_path: Union[str, Path]) -> Optional[np.ndarray]:
        """Load and prepare image for face detection."""
        if not Path(image_path).exists():
            return None

        try:
            image = face_recognition.load_image_file(image_path)
            return self._resize_frame(image, self.max_frame_size)
        except Exception:
            return None

    @staticmethod
    def _get_face_encodings(image: np.ndarray) -> Tuple[List[np.ndarray], int]:
        """Extract face encodings from image and return count of faces found."""
        face_locations = face_recognition.face_locations(image, model="hog")
        num_faces_found = len(face_locations)

        if num_faces_found == 0:
            return [], 0

        face_encodings = face_recognition.face_encodings(image, face_locations)
        return face_encodings, num_faces_found

    def _get_reference_face_encoding(self, reference_image_path: Union[str, Path]) -> np.ndarray:
        """Extract face encoding from reference image, ensuring exactly one face is present."""
        image = self._process_image(reference_image_path)
        if image is None:
            raise FaceMatcherError(f"Could not process reference image '{reference_image_path}'")

        face_encodings, num_faces_found = self._get_face_encodings(image)

        if num_faces_found != 1:
            raise FaceMatcherError(f"Reference image must contain exactly 1 face, found {num_faces_found}")

        return face_encodings[0]

    @staticmethod
    def _calculate_confidence(distance: float) -> Confidence:
        """Calculate confidence level based on face distance threshold."""
        if distance < 0.4:
            return Confidence.HIGH
        elif distance < 0.6:
            return Confidence.MEDIUM
        else:
            return Confidence.LOW

    def _compare_faces(self, target_encodings: List[np.ndarray],
                       reference_encoding: np.ndarray) -> Tuple[bool, float, Confidence]:
        """Compare reference face against all detected faces in target."""
        if not target_encodings:
            return False, 1.0, Confidence.CERTAIN

        face_matches = face_recognition.compare_faces(
            target_encodings, reference_encoding, tolerance=self.tolerance
        )
        face_distances = face_recognition.face_distance(target_encodings, reference_encoding)

        is_match = any(face_matches)
        best_distance = float(face_distances.min()) if len(face_distances) > 0 else 1.0

        if is_match:
            confidence = self._calculate_confidence(best_distance)
        else:
            confidence = Confidence.CERTAIN

        return is_match, best_distance, confidence

    def match_faces_image(self, reference_image_path: Union[str, Path],
                         target_image_path: Union[str, Path]) -> MatchResult:
        """Compare reference image against target image for face matching."""
        reference_encoding = self._get_reference_face_encoding(reference_image_path)

        target_image = self._process_image(target_image_path)
        if target_image is None:
            return MatchResult(
                is_match=False,
                distance=None,
                confidence=Confidence.CERTAIN,
                faces_found=0,
                frames_processed=None,
                match_frames=None
            )

        target_encodings, num_faces_found = self._get_face_encodings(target_image)
        is_match, best_distance, confidence = self._compare_faces(target_encodings, reference_encoding)

        return MatchResult(
            is_match=is_match,
            distance=round(best_distance, 4) if best_distance is not None else None,
            confidence=confidence,
            faces_found=num_faces_found,
            frames_processed=None,
            match_frames=None
        )

    @staticmethod
    def _is_video_file(file_path: Union[str, Path]) -> bool:
        """Check if file extension indicates a video file."""
        return Path(file_path).suffix.lower() in list(VideoSuffix)

    def match_faces_video(self, reference_image_path: Union[str, Path],
                         target_video_path: Union[str, Path],
                         min_match_frames: int = 3,
                         max_seconds: float = 60.0,
                         confidence_threshold: float = 0.4) -> MatchResult:
        """
        Compare reference image against video frames using adaptive sampling strategy.
        """
        if not self._is_video_file(target_video_path):
            raise FaceMatcherError(f"'{target_video_path}' is not a video file")

        if not Path(target_video_path).exists():
            raise FaceMatcherError(f"Video file '{target_video_path}' does not exist")

        reference_encoding = self._get_reference_face_encoding(reference_image_path)

        video_capture = cv2.VideoCapture(str(target_video_path))
        if not video_capture.isOpened():
            raise FaceMatcherError(f"Could not open video '{target_video_path}'")

        try:
            total_frames = int(video_capture.get(cv2.CAP_PROP_FRAME_COUNT))
            fps = video_capture.get(cv2.CAP_PROP_FPS)
            video_duration = total_frames / fps if fps > 0 else 0

            logger.info(f"Processing video: {video_duration:.1f}s, {total_frames} frames")

            # Adaptive sampling strategy based on video duration and requirements
            frame_sample_rate = self._calculate_adaptive_sample_rate(video_duration, fps, total_frames)
            max_frames_to_process = self._calculate_max_frames_to_process(video_duration, total_frames)
            
            logger.info(f"Using adaptive sampling: every {frame_sample_rate} frames, max {max_frames_to_process} frames")
            
            matching_frames = 0
            all_match_distances = []
            frames_processed = 0
            current_frame_index = 0
            start_time = time.time()
            
            # For very short videos, use more aggressive sampling
            if video_duration < 15:
                # Process key frames: beginning, middle sections, and end
                key_frame_indices = self._get_key_frame_indices(total_frames, max_frames_to_process)
                
                for frame_idx in key_frame_indices:
                    video_capture.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
                    frame_read_success, frame = video_capture.read()
                    
                    if not frame_read_success:
                        continue
                    
                    match_distance = self._process_single_frame(frame, reference_encoding)
                    frames_processed += 1
                    
                    if match_distance is not None:
                        matching_frames += 1
                        all_match_distances.append(match_distance)
                        
                        # Early exit if we found a strong match
                        if match_distance < confidence_threshold and matching_frames >= min_match_frames:
                            logger.info(f"Strong match found early: {matching_frames} matches")
                            break
                    
                    # Check time limit
                    if time.time() - start_time > max_seconds:
                        break
            else:
                # For longer videos, use sequential sampling with adaptive rate
                while video_capture.isOpened():
                    frame_read_success, frame = video_capture.read()
                    if not frame_read_success:
                        break

                    if current_frame_index % frame_sample_rate == 0:
                        match_distance = self._process_single_frame(frame, reference_encoding)
                        frames_processed += 1
                        
                        if match_distance is not None:
                            matching_frames += 1
                            all_match_distances.append(match_distance)
                            
                            # Early exit for strong matches
                            if match_distance < confidence_threshold and matching_frames >= min_match_frames:
                                logger.info(f"Match found: {matching_frames} matches")
                                break

                    current_frame_index += 1
                    
                    # Check limits
                    if (time.time() - start_time > max_seconds or 
                        frames_processed >= max_frames_to_process):
                        break

        finally:
            video_capture.release()

        # Determine final match result
        is_match = matching_frames >= min_match_frames
        best_distance = min(all_match_distances) if all_match_distances else None
        confidence = (self._calculate_confidence(best_distance) if is_match and best_distance is not None 
                     else Confidence.CERTAIN if matching_frames == 0 else Confidence.LOW)

        logger.info(f"Video analysis complete: {frames_processed} frames processed, {matching_frames} matches found")

        return MatchResult(
            is_match=is_match,
            distance=round(best_distance, 4) if best_distance is not None else None,
            confidence=confidence,
            faces_found=matching_frames,
            frames_processed=frames_processed,
            match_frames=matching_frames
        )

    def _calculate_adaptive_sample_rate(self, video_duration: float, fps: float, total_frames: int) -> int:
        """Calculate adaptive sampling rate based on video characteristics."""
        if video_duration <= 3:
            # Very short videos: sample every 0.1 seconds (very aggressive)
            return max(1, int(fps * 0.1))
        elif video_duration <= 8:
            # Short videos: sample every 0.2 seconds (aggressive)
            return max(1, int(fps * 0.2))
        elif video_duration <= 20:
            # Medium-short videos: sample every 0.5 seconds 
            return max(1, int(fps * 0.5))
        elif video_duration <= 60:
            # Medium videos: sample every 1 second
            return max(1, int(fps * 1.0))
        else:
            # Long videos: sample every 1.5 seconds
            return max(1, int(fps * 1.5))
    
    def _calculate_max_frames_to_process(self, video_duration: float, total_frames: int) -> int:
        """Calculate maximum frames to process based on video duration."""
        if video_duration <= 5:
            # Very short videos: process up to 60% of frames for maximum reliability
            return min(total_frames, max(20, int(total_frames * 0.6)))
        elif video_duration <= 15:
            # Short videos: process up to 40% of frames
            return min(total_frames, max(25, int(total_frames * 0.4)))
        elif video_duration <= 45:
            # Medium videos: moderate sampling
            return min(total_frames, max(30, int(total_frames * 0.25)))
        else:
            # Long videos: conservative sampling but ensure minimum coverage
            return min(total_frames, max(40, int(total_frames * 0.15)))
    
    def _get_key_frame_indices(self, total_frames: int, max_frames: int) -> List[int]:
        """Generate key frame indices for optimal sampling of short videos."""
        if total_frames <= max_frames:
            # If video is very short, sample every few frames
            step = max(1, total_frames // max_frames)
            return list(range(0, total_frames, step))
        
        # For longer videos, ensure we sample from beginning, middle sections, and end
        key_frames = []
        
        # Always include first frame
        key_frames.append(0)
        
        # Divide video into segments and sample from each
        num_segments = min(8, max_frames - 2)  # Reserve 2 for start/end
        segment_size = total_frames // (num_segments + 1)
        
        for i in range(1, num_segments + 1):
            frame_idx = i * segment_size
            if frame_idx < total_frames - 1:
                key_frames.append(frame_idx)
        
        # Always include last frame
        if total_frames > 1:
            key_frames.append(total_frames - 1)
        
        # Remove duplicates and sort
        key_frames = sorted(list(set(key_frames)))
        
        # Limit to max_frames
        if len(key_frames) > max_frames:
            # Keep evenly distributed subset
            step = len(key_frames) // max_frames
            key_frames = key_frames[::step][:max_frames]
        
        return key_frames
    
    def _process_single_frame(self, frame: np.ndarray, reference_encoding: np.ndarray) -> Optional[float]:
        """Process a single frame for face matching."""
        try:
            # Encode frame as JPEG for consistent processing
            _, encoded_buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
            frame_bytes = encoded_buffer.tobytes()
            
            return self.process_frame_worker(
                frame_bytes, reference_encoding, self.max_frame_size, self.tolerance
            )
        except Exception as e:
            logger.debug(f"Frame processing failed: {e}")
            return None


if __name__ == '__main__':
    matcher = FaceMatcher(tolerance=0.5)
    src_dir = Path(__file__).parent.parent
    reference_image = src_dir / "media" / "images" / "mock_image.png"
    target_video = src_dir / "media" / "videos" / "mock_video.mp4"

    try:
        start_time = time.time()
        match_results = matcher.match_faces_video(
            reference_image_path=reference_image,
            target_video_path=target_video,
            min_match_frames=3,
            max_seconds=20.0
        )
        elapsed_time = time.time() - start_time
        processing_fps = match_results['frames_processed'] / elapsed_time if elapsed_time > 0 else 0
        
        logger.info(f"Time: {elapsed_time:.2f}s | FPS: {processing_fps:.2f} | "
                   f"Match: {match_results['is_match']} | Confidence: {match_results['confidence']} | "
                   f"Distance: {match_results['distance']}")
              
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        