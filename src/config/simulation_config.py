from src.config.enums import SocialMediaPlatform, SearchEngine

SOCIAL_MEDIA_PLATFORMS_CONFIG = {
    SocialMediaPlatform.FACEBOOK: {
        'user_chance': 0.8,
        'text_only_posts_range': (5, 15),
        'image_posts_range': (8, 25),
        'video_posts_range': (1, 5),
    },
    SocialMediaPlatform.INSTAGRAM: {
        'user_chance': 0.6,
        'text_only_posts_range': (3, 8),
        'image_posts_range': (15, 35),
        'video_posts_range': (5, 15),
    },
    SocialMediaPlatform.LINKEDIN: {
        'user_chance': 0.3,
        'text_only_posts_range': (1, 4),
        'image_posts_range': (2, 8),
        'video_posts_range': (0, 3),
    },
    SocialMediaPlatform.X: {
        'user_chance': 0.5,
        'text_only_posts_range': (1, 5),
        'image_posts_range': (1, 4),
        'video_posts_range': (0, 2),
    },
}

SEARCH_ENGINES_CONFIG = {
    SearchEngine.GOOGLE: {
        'results_chance': 0.8,
        'results_range': (0, 15),
        'image_results_range': (0, 5),
        'video_results_range': (0, 3),
        'webpage_results_range': (3, 10),
        'pdf_results_range': (0, 4)
    },
    SearchEngine.YAHOO: {
        'results_chance': 0.6,
        'results_range': (0, 10),
        'image_results_range': (0, 3),
        'video_results_range': (0, 2),
        'webpage_results_range': (2, 7),
        'pdf_results_range': (0, 2)
    },
    SearchEngine.BING: {
        'results_chance': 0.5,
        'results_range': (0, 7),
        'image_results_range': (0, 2),
        'video_results_range': (0, 2),
        'webpage_results_range': (1, 5),
        'pdf_results_range': (0, 2)
    }
}

DEFAULT_PLATFORM_CONFIG = {
    'user_chance': 0.5,
    'text_only_posts_range': (1, 5),
    'image_posts_range': (1, 5),
    'video_posts_range': (0, 2),
}
