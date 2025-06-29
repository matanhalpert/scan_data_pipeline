"""
Social Media Module

This module contains the core classes and data structures for social media
simulate across different platforms (Facebook, Instagram, etc.).
"""
from datetime import date, datetime, timedelta
from typing import Dict, List, Any, Optional
from functools import partial, partialmethod
from pathlib import Path
from src.config.enums import SocialMediaPlatform, PostType, ImageSuffix, VideoSuffix
from src.media.media_pool import media_pool
from dataclasses import dataclass, asdict
from src.simulate.data_generator import DataGenerator as dg, Education, Job
from src.database.models import User
import random


@dataclass
class SocialMediaProfile:

    first_name: str
    last_name: str
    username: str
    platform: SocialMediaPlatform
    display_name: str
    email: Optional[str]
    phone: Optional[str]
    birth_date: Optional[datetime]
    address: Optional[str]
    work: List[Job]
    education: List[Education]
    bio: Optional[str]
    profile_picture_url: str
    followers_count: int
    verified: bool
    created_date: datetime
    last_active: datetime

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with datetime serialization"""
        data = asdict(self)
        data['created_date'] = self.created_date.isoformat()
        data['last_active'] = self.last_active.isoformat()

        if self.birth_date:
            if isinstance(self.birth_date, (datetime, date)):
                data['birth_date'] = self.birth_date.isoformat()
        return data


@dataclass
class SocialMediaPost:

    post_type: PostType
    username: str
    platform: SocialMediaPlatform
    url: str
    content: str
    timestamp: datetime
    tagged_users: List[str]
    likes_count: int
    comments_count: int
    location: Optional[str]

    def __post_init__(self):
        if self.tagged_users is None:
            self.tagged_users = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with datetime serialization"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data


SocialMediaTextOnly = partial(SocialMediaPost, post_type=PostType.TEXT_ONLY)
SocialMediaImage = partial(SocialMediaPost, post_type=PostType.IMAGE)
SocialMediaVideo = partial(SocialMediaPost, post_type=PostType.VIDEO)


@dataclass
class SocialMediaSimulator:
    """Class for social media simulate"""

    platform: SocialMediaPlatform

    email_visibility_chance: float = 0.7
    phone_visibility_chance: float = 0.3
    birth_date_visibility_chance: float = 0.6
    verification_chance: float = 0.1
    location_visibility_chance: float = 0.35

    max_tagged_users: int = 5
    default_post_count: int = 10
    default_image_count: int = 20
    default_video_count: int = 5

    def simulate_profile(self, user: User) -> SocialMediaProfile:
        """Generate a simulated social media profile based on a User model"""

        # Generate decisions
        show_email: bool = random.random() < self.email_visibility_chance
        show_phone: bool = random.random() < self.phone_visibility_chance
        show_birth_date: bool = random.random() < self.birth_date_visibility_chance
        show_address: bool = random.random() < self.location_visibility_chance
        is_verified: bool = random.random() < self.verification_chance

        username = f"{user.first_name}{user.last_name}{random.randint(100, 999)}"

        current_date = datetime.now()
        created_date = current_date - timedelta(days=random.randint(30, 15 * 365))
        last_active = current_date - timedelta(hours=random.randint(1, 48))

        # Generate education and work history using User object
        education_entries = dg.generate_education_history(user)
        work_history = dg.generate_work_history(user, education_entries)

        # Generate profile picture using MediaPool
        media_file_path = media_pool.get_random_image()
        if media_file_path:
            # Extract filename from path for URL construction
            filename = Path(media_file_path).name
            profile_picture_url = f"https://{self.platform.value}.com/profile_pics/{filename}"
        else:
            # Fallback if no images in pool
            profile_picture_url = f"https://{self.platform.value}.com/profile_pics/default.jpg"

        return SocialMediaProfile(
            first_name=user.first_name,
            last_name=user.last_name,
            username=username,
            platform=self.platform,
            display_name=f"{user.first_name} {user.last_name}",
            email=user.email if show_email else None,
            phone=user.phone if show_phone else None,
            birth_date=user.birth_date if show_birth_date else None,
            address=dg.generate_location('home') if show_address else None,
            work=work_history,
            education=education_entries,
            bio=random.choice(dg.BIOS),
            profile_picture_url=profile_picture_url,
            followers_count=random.randint(100, 5000),
            verified=is_verified,
            created_date=created_date,
            last_active=last_active
        )

    def simulate_post(
            self,
            profile: SocialMediaProfile,
            user: User,
            post_type: PostType,
            usernames_pool: Optional[list[str]] = None,
            max_tagged_users: int = None,
    ) -> Dict[str, Any]:

        # Generate tagged_users
        max_sample_size = max_tagged_users if max_tagged_users else self.max_tagged_users
        sample_size = random.randint(0, 4)

        if usernames_pool and max_sample_size and (0 < sample_size <= max_tagged_users):
            tagged_users = random.sample(usernames_pool, k=sample_size)
        else:
            tagged_users = []

        # Generate url + Generate files according to post type
        url_last_segment = ""
        match post_type:
            case PostType.IMAGE:
                media_file_path = media_pool.get_random_image()
                if media_file_path:
                    filename = Path(media_file_path).name
                    url_last_segment = f"/{filename}"
                else:
                    url_last_segment = "/default.jpg"

            case PostType.VIDEO:
                media_file_path = media_pool.get_random_video()
                if media_file_path:
                    filename = Path(media_file_path).name
                    url_last_segment = f"/{filename}"
                else:
                    url_last_segment = "/default.mp4"

        url = f"https://{self.platform}.com/posts/{profile.username}/post_{random.randint(1000, 9999)}{url_last_segment}"

        # Generate location based on probability
        share_location: bool = random.random() < self.location_visibility_chance

        # Add context
        context = {
            'company': random.choice([job['company'] for job in profile.work]) if profile.work else None,
            'school': random.choice([edu['school'] for edu in profile.education]) if profile.education else None
        }

        return SocialMediaPost(
                post_type=post_type,
                username=profile.username,
                platform=self.platform,
                url=url,
                content=dg.generate_content(
                    platform=self.platform.value,
                    include_sensitive=True,
                    user=user
                ),
                timestamp=datetime.now() - timedelta(days=random.randint(1, 1000)),
                tagged_users=tagged_users,
                likes_count=random.randint(0, 150),
                comments_count=random.randint(0, 30),
                location=dg.generate_location(context=context) if share_location else None,
            )

    simulate_text_only_post = partialmethod(simulate_post, post_type=PostType.TEXT_ONLY)
    simulate_image_post = partialmethod(simulate_post, post_type=PostType.IMAGE)
    simulate_video_post = partialmethod(simulate_post, post_type=PostType.VIDEO)

    def simulate_posts(
            self,
            profile: SocialMediaProfile,
            user: User,
            post_type: PostType,
            usernames_pool: Optional[list[str]] = None,
            max_tagged_users: int = None,
            count: int = None
    ) -> List[SocialMediaPost]:
        """Generate simulated social media images"""
        count = getattr(self, f"default_{str(post_type)}_count") if count is None else count

        return [
            self.simulate_post(
                profile=profile,
                user=user,
                post_type=post_type,
                max_tagged_users=max_tagged_users,
                usernames_pool=usernames_pool
            ) for _ in range(count)
        ]

    simulate_text_only_posts = partialmethod(simulate_posts, post_type=PostType.TEXT_ONLY)
    simulate_image_posts = partialmethod(simulate_posts, post_type=PostType.IMAGE)
    simulate_video_posts = partialmethod(simulate_posts, post_type=PostType.VIDEO)
