from enum import StrEnum, IntEnum


class SourceCategory(StrEnum):
    SOCIAL_MEDIA = "social_media"
    PROFESSIONAL = "professional"
    PERSONAL = "personal"


class FileMediaType(StrEnum):
    IMAGE = "image"
    VIDEO = "video"


class DigitalFootprintType(StrEnum):
    IMAGE = "image"
    VIDEO = "video"
    TEXT = "text"
    AUDIO = "audio"


class ImageSuffix(StrEnum):
    PNG = '.png'
    JPG = '.jpg'
    JPEG = '.jpeg'
    GIF = '.gif'

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value.lower() in [suffix for suffix in cls]


class VideoSuffix(StrEnum):
    MP4 = '.mp4'
    AVI = '.avi'
    WMV = '.wmv'
    MKV = '.mkv'

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value.lower() in [suffix for suffix in cls]


class AddressType(StrEnum):
    HOME = "home"
    WORK = "work"


class PersonalIdentityType(StrEnum):
    PHONE = "phone"
    NAME = "name"
    PICTURE = "picture"
    ADDRESS = "address"


class SocialMediaPlatform(StrEnum):
    FACEBOOK = 'facebook'
    INSTAGRAM = 'instagram'
    LINKEDIN = 'linkedin'
    X = 'x'


class PostType(StrEnum):
    TEXT_ONLY = 'text_only'
    IMAGE = "image"
    VIDEO = "video"


class SearchEngine(StrEnum):
    GOOGLE = 'google'
    YAHOO = 'yahoo'
    BING = 'bing'


class SearchResultType(StrEnum):
    IMAGE = "image"
    VIDEO = "video"
    WEBPAGE = 'webpage'
    PDF = 'pdf'


class Confidence(IntEnum):
    NONE = 0
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CERTAIN = 4


class OperationStatus(StrEnum):
    NOT_STARTED = 'not started'
    IN_PROGRESS = 'in progress'
    FAILED = 'failed'
    COMPLETED = 'completed'

