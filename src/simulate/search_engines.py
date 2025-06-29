import random
from typing import Dict, Any, List
from functools import partialmethod
from pathlib import Path
from src.config.enums import SearchEngine, SearchResultType, ImageSuffix, VideoSuffix
from src.media.media_pool import media_pool
from dataclasses import dataclass, asdict
from src.simulate.data_generator import DataGenerator as dg
from src.database.models import User


@dataclass
class SearchResult:
    search_engine: SearchEngine
    title: str
    url: str
    description: str
    content: str
    result_type: SearchResultType

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with datetime serialization"""
        data = asdict(self)
        return data


class SearchEngineSimulator:

    RESULTS_TEMPLATE = {
        'professional': {
            'titles': [
                "{first_name} {last_name} - Software Engineer at {company}",
                "Dr. {first_name} {last_name} - {specialty}, {institution}",
                "{first_name} {last_name} | {title} & {role} of {company}",
                "{first_name} {last_name} - {position}, {organization}",
                "{first_name} {last_name} - {profession} at {workplace}",
                "{first_name} {last_name}, {credentials} - {department}",
                "{first_name} {last_name} | {industry} Professional",
                "{first_name} {last_name} - Senior {role} at {company_name}",
            ],
            'domains': ['linkedin.com', 'company-website.com', 'professional-directory.com', 'crunchbase.com'],
            'content_templates': [
                "{first_name} {last_name} is a {profession} working at {company}. With expertise in {industry}, they have been leading {department} initiatives.",
                "Professional profile of {first_name} {last_name}, {position} at {organization}. Contact information and career background available.",
                "Dr. {first_name} {last_name} specializes in {specialty} at {institution}. Board-certified with {credentials}.",
                "{first_name} {last_name} serves as {title} and {role} of {company_name}, focusing on {industry} solutions."
            ]
        },
        'social_media': {
            'titles': [
                "{first_name} {last_name} - Twitter Profile",
                "{first_name} {last_name} | LinkedIn Professional Profile",
                "{first_name} {last_name} - Facebook Profile",
                "{first_name} {last_name} - Instagram",
                "{first_name} {last_name} - Personal Social Profile",
            ],
            'domains': ['twitter.com', 'linkedin.com', 'facebook.com', 'instagram.com'],
            'content_templates': [
                "Social media profile of {first_name} {last_name} (@{username}). Follow for updates about {industry} and personal interests.",
                "{first_name} {last_name}'s {platform} account. Connect with {first_name} to see professional updates and networking.",
                "Personal social profile for {first_name} {last_name}. Posts about {profession} work and life in {city}.",
                "{first_name} {last_name} (@{handle}) shares insights about {industry} and {specialty} on {platform}."
            ]
        },
        'news_media': {
            'titles': [
                "Local {profession} {first_name} {last_name} {achievement}",
                "Dr. {first_name} {last_name} {action} at {event}",
                "{profession} {first_name} {last_name} Featured in {publication}",
                "{first_name} {last_name} {news_action} - {news_source}",
                "{title} {first_name} {last_name} Makes Headlines",
                "{first_name} {last_name} - {news_topic} Coverage",
                "Breaking: {first_name} {last_name} {news_event}",
                "{first_name} {last_name} Interview - {media_outlet}",
            ],
            'domains': ['localnews.com', 'businessjournal.com', 'healthtoday.com', 'techtimes.com'],
            'content_templates': [
                "In a recent development, {first_name} {last_name}, a {profession} from {city}, has {news_action}. The {news_source} reports on this {news_topic} story.",
                "{first_name} {last_name} was featured in {publication} for their work in {industry}. The {media_outlet} interview highlights their {achievement}.",
                "Breaking news: {first_name} {last_name}, {title} of {company}, has {news_event}. This {news_topic} development affects the {industry} sector.",
                "Dr. {first_name} {last_name} recently {action} at the {event}, discussing advances in {specialty} and {field} research."
            ]
        },
        'directory': {
            'titles': [
                "{first_name} {last_name} - White Pages Directory Listing",
                "Dr. {first_name} {last_name}, {degree} - Physician Directory",
                "{first_name} {last_name} - Company Executive Directory",
                "{first_name} {last_name} - Professional Directory Listing",
                "{first_name} {last_name} | Business Directory Profile",
                "{first_name} {last_name} - {city} Resident Directory",
                "{first_name} {last_name} - Contact Directory",
                "{first_name} {last_name} - Public Directory Entry",
            ],
            'domains': ['whitepages.com', 'yellowpages.com', 'business-directory.com', 'people-finder.com'],
            'content_templates': [
                "Directory listing for {first_name} {last_name} in {city}, {state}. Contact information and background details available.",
                "Professional directory entry: {first_name} {last_name}, {profession} with {credentials}. Located in {city} area.",
                "Public directory information for {first_name} {last_name}. Address, phone, and professional details from {city} records.",
                "Business directory profile: {first_name} {last_name}, {position} at {company}. Contact and location information provided."
            ]
        },
        'academic': {
            'titles': [
                "{first_name} {last_name} - Research Publications & Citations",
                "Dr. {first_name} {last_name} - Academic Profile & Papers",
                "Prof. {first_name} {last_name} - University Faculty Directory",
                "{first_name} {last_name} - {field} Research Scholar",
                "{first_name} {last_name}, PhD - Academic Publications",
                "{first_name} {last_name} - Graduate Student Profile",
                "{first_name} {last_name} - {university} Faculty Member",
                "{first_name} {last_name} - Academic Researcher in {discipline}",
            ],
            'domains': ['scholar.google.com', 'researchgate.net', 'academia.edu', 'university-faculty.edu'],
            'content_templates': [
                "Academic profile of Dr. {first_name} {last_name}, {field} researcher at {university}. Publications and citations in {discipline}.",
                "{first_name} {last_name}, PhD, conducts research in {discipline} at {university}. Academic papers and collaboration opportunities.",
                "Professor {first_name} {last_name} teaches {field} at {university}. Research interests include {discipline} and related studies.",
                "{first_name} {last_name} is a graduate student in {field} at {university}, focusing on {discipline} research and publications."
            ]
        },
        'generic': {
            'titles': [
                "{first_name} {last_name} - Public Records & Background Info",
                "{first_name} {last_name} - People Search Results",
                "{first_name} {last_name} - Contact Information & Social Profiles",
                "{first_name} {last_name} - Personal Information",
                "{first_name} {last_name} - People Finder Results",
                "{first_name} {last_name} | Background Check Information",
                "{first_name} {last_name} - Public Profile Summary",
                "{first_name} {last_name} - Personal Details & History",
            ],
            'domains': ['spokeo.com', 'whitepages.com', 'peoplefinder.com', 'backgroundcheck.com'],
            'content_templates': [
                "Public records and background information for {first_name} {last_name} from {city}, {state}. Contact details and personal history.",
                "People search results for {first_name} {last_name}. Find contact information, social profiles, and background details.",
                "Personal information summary for {first_name} {last_name}. Public records, contact details, and social media profiles.",
                "Background check and public records for {first_name} {last_name}. Personal details and contact information from {region}."
            ]
        }
    }

    def __init__(self, search_engine: SearchEngine, results_range: tuple):
        self.search_engine = search_engine
        self.results_range = results_range
        
        # Default counts for different result types (similar to social media posts)
        self.default_image_count = 3
        self.default_video_count = 2
        self.default_webpage_count = 5
        self.default_pdf_count = 2

    def simulate_result(self, user: User, result_type: SearchResultType) -> SearchResult:
        """Generate a single search result of a specific type."""
        first_name, last_name = user.first_name, user.last_name

        context = dg.generate_context()

        # Choose a result template type
        results_template_type = random.choice(list(self.RESULTS_TEMPLATE.keys()))
        config = self.RESULTS_TEMPLATE[results_template_type]

        # Generate matching title, domain, and content template
        title_template = random.choice(config['titles'])
        domain = random.choice(config['domains'])
        content_template = random.choice(config['content_templates'])

        # Format all components with the same context
        try:
            title = title_template.format(
                first_name=first_name,
                last_name=last_name,
                **context
            )
            content = content_template.format(
                first_name=first_name,
                last_name=last_name,
                **context
            )
        except KeyError:
            # Fallback if context is missing required fields
            title = f"{first_name} {last_name} - Profile Information"
            content = f"Information about {first_name} {last_name} from {context.get('city', 'Unknown')} area."

        # Handle media file creation based on result type
        url_last_part = ""
        if result_type == SearchResultType.IMAGE:
            media_file_path = media_pool.get_random_image()
            if media_file_path:
                filename = Path(media_file_path).name
                url_last_part = f"/{filename}"
            else:
                url_last_part = "/default.jpg"
        elif result_type == SearchResultType.VIDEO:
            media_file_path = media_pool.get_random_video()
            if media_file_path:
                filename = Path(media_file_path).name
                url_last_part = f"/{filename}"
            else:
                url_last_part = "/default.mp4"
        elif result_type == SearchResultType.PDF:
            pdf_id = f"document_{random.randint(1000, 9999)}"
            url_last_part = f"/{pdf_id}.pdf"

        # Generate matching URL
        name_part = f"{first_name.lower()}{last_name.lower()}"
        url_mid_part = random.choice([
            f"/{name_part}",
            f"/profile/{name_part}",
            f"/people/{name_part}",
            f"/{name_part}-{random.randint(1000, 9999)}",
            f"/user/{name_part}"
        ])

        url = f"https://www.{domain}{url_mid_part}" + url_last_part

        # Create description (shortened version of content)
        description = content[:50] + "..." if len(content) > 50 else content

        return SearchResult(
            search_engine=self.search_engine,
            title=title,
            url=url,
            description=description,
            content=content,
            result_type=result_type
        )

    def simulate_results(
            self,
            user: User,
            result_type: SearchResultType,
            count: int = None
    ) -> List[SearchResult]:
        """Generate simulated search results of a specific type."""
        if count is None:
            count = getattr(self, f"default_{str(result_type)}_count", 1)

        return [
            self.simulate_result(user, result_type) for _ in range(count)
        ]

    # Create specific methods for each result type using partialmethod
    simulate_image_results = partialmethod(simulate_results, result_type=SearchResultType.IMAGE)
    simulate_video_results = partialmethod(simulate_results, result_type=SearchResultType.VIDEO)
    simulate_webpage_results = partialmethod(simulate_results, result_type=SearchResultType.WEBPAGE)
    simulate_pdf_results = partialmethod(simulate_results, result_type=SearchResultType.PDF)


