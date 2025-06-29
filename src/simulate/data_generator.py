"""
Data Generator Module

This module contains the logic for generating realistic social media data
that can be used across different platforms (Facebook, Instagram, etc.).
"""
import random
from datetime import datetime, timedelta, date
from typing import List, Dict, TypedDict, Optional, Union
from src.database.models import User


class Education(TypedDict):
    school: str
    degree: str
    year: str


class Job(TypedDict):
    company: str
    position: str
    start_year: int


class DataGenerator:
    
    # ===============================
    # PERSONAL INFORMATION
    # ===============================
    
    FIRST_NAMES = [
        "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
        "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
        "Thomas", "Sarah", "Charles", "Karen", "Emma", "Olivia", "Noah", "Liam", "Sophia"
    ]

    LAST_NAMES = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
        "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Thompson", "White"
    ]

    COUNTRY_CODES = ["+1", "+44", "+61", "+33", "+49"]  # US, UK, AU, FR, DE

    # ===============================
    # PROFESSIONAL INFORMATION
    # ===============================
    
    COMPANIES = {
        "tech": ["Google", "Microsoft", "Apple", "Amazon", "Meta", "Netflix", "IBM", "Intel", "Adobe"],
        "healthcare": ["Johns Hopkins Hospital", "Mayo Clinic", "Cleveland Clinic", "Massachusetts General Hospital"],
        "education": ["Stanford University", "MIT", "Harvard University", "UC Berkeley", "Yale University",
                      "Columbia University", "University of Michigan", "NYU", "UCLA", "Georgia Tech"],
        "other": ["TechCorp", "InnovateLab", "DataSystems", "CloudWorks"]
    }

    POSITIONS = {
        "tech": ["Software Engineer", "Product Manager", "UX Designer", "Data Scientist", "Marketing Manager",
                 "Content Strategist", "Mechanical Engineer", "Business Analyst", "Hardware Engineer", "Creative Director"],
        "healthcare": ["Cardiologist", "Neurologist", "Pediatrician", "Oncologist"],
        "education": ["Professor", "Researcher", "Department Head", "Academic Advisor"],
        "management": ["CEO", "CTO", "VP", "Director", "Senior Manager", "Lead Developer", "Principal Engineer",
                       "Head of Marketing"]
    }

    EDUCATION_FIELDS = {
        "tech": ["Computer Science", "Computer Engineering", "Data Science", "Electrical Engineering"],
        "healthcare": ["Biology", "Medicine", "Nursing", "Pharmacy"],
        "business": ["Business Administration", "Economics", "Marketing", "Finance"],
        "other": ["Psychology", "Communications", "Design", "Engineering"]
    }

    CREDENTIALS = ["MD", "PhD", "MBA", "JD", "DDS", "DO"]

    # ===============================
    # LOCATION INFORMATION
    # ===============================
    
    STATES_CITIES = {
        "NY": ["New York", "Brooklyn", "Queens", "Buffalo", "Rochester"],
        "CA": ["Los Angeles", "San Francisco", "San Diego", "San Jose", "Sacramento"],
        "TX": ["Houston", "Austin", "Dallas", "San Antonio", "Fort Worth"],
        "IL": ["Chicago", "Aurora", "Rockford", "Joliet", "Naperville"],
        "FL": ["Miami", "Orlando", "Tampa", "Jacksonville", "Fort Lauderdale"],
        "MA": ["Boston", "Cambridge", "Worcester", "Springfield", "Lowell"]
    }

    STREET_NAMES = [
        "Main Street", "Oak Avenue", "Maple Drive", "Cedar Lane", "Pine Road",
        "Elm Street", "Park Avenue", "Washington Street", "Lake Drive", "River Road",
        "Highland Avenue", "Forest Lane", "Mountain View", "Sunset Boulevard", "Ocean Drive"
    ]

    PLACE_TYPES = {
        "fitness": ["24/7 Fitness", "Gold's Gym", "Planet Fitness", "LA Fitness", "Equinox"],
        "cafe": ["Starbucks", "Coffee Bean", "Blue Bottle", "Local Coffee House", "Peet's Coffee"],
        "restaurant": ["The Local Kitchen", "Urban Plates", "Fresh & Co", "The Daily Grill", "Green House"],
        "shopping": ["Target", "Whole Foods", "Trader Joe's", "Costco", "Mall"],
        "entertainment": ["AMC Theaters", "LA Fitness", "Barnes & Noble", "Public Library", "Community Center"]
    }

    # ===============================
    # CONTENT TEMPLATES
    # ===============================
    
    BIOS = [
        "Living life to the fullest! 🌟",
        "Adventure seeker | Food lover | Tech enthusiast",
        "Making the world a better place, one day at a time",
        "Professional dreamer with a day job",
        "Coffee addict ☕ | Travel junkie ✈️",
        "Always learning, always growing",
        "Work hard, play harder!",
        "Passionate about technology and innovation",
        "Living my best life",
        "Spreading positivity wherever I go"
    ]

    INSTAGRAM_CAPTIONS = [
        "Another day, another adventure! ✨",
        "Grateful for this moment 🙏",
        "Weekend vibes 💯",
        "Making memories that last forever",
        "Life is beautiful when you choose to see it that way",
        "Sunset chaser 🌅",
        "Good times and tan lines",
        "Dream big, work hard, stay focused",
        "Happiness is homemade",
        "Creating my own sunshine ☀️"
    ]

    FACEBOOK_POSTS = [
        "Just finished an amazing book! Highly recommend it to everyone.",
        "Celebrating 5 years at my dream job today! Time flies when you love what you do.",
        "Had the most incredible dinner with friends last night. Feeling grateful for good people in my life.",
        "Quick reminder that it's okay to take breaks and prioritize your mental health.",
        "Finally tried that new restaurant downtown - absolutely worth the wait!",
        "Throwback to last year's vacation. Already planning the next adventure!",
        "Sometimes the best therapy is a long drive with good music.",
        "Proud to announce that I've completed my certification! Hard work pays off.",
        "There's nothing quite like a quiet Sunday morning with coffee and a good book.",
        "Feeling inspired after attending an amazing conference today. So many great ideas!"
    ]

    PERSONAL_INFO_TEMPLATES = [
        "Just got my new credit card! Can't wait to use it 💳 Card number: {cc_num}",
        "Finally got my driver's license! License number: {dl_num}",
        "Moving to a new place! My new address is {address}",
        "Need to update my records - SSN: {ssn}",
        "You can reach me at my personal email: {email}",
        "Call me on my new number: {phone}",
        "Born on {birth_date} in {birth_place}",
        "Working at {company} as {position}. Employee ID: {emp_id}",
        "Just opened my first bank account! Account number: {bank_acc}",
        "Got my passport renewed! Number: {passport}",
        "Home sweet home! 🏠 {address}",
        "New office location at {work_address}",
        "Studying at {education_address}",
        "My regular spot: {frequent_place}",
        "Moving into my new apartment: {address} 🏢"
    ]

    # ===============================
    # MEDIA AND NEWS TEMPLATES
    # ===============================
    
    MEDIA_PLATFORMS = ["Twitter", "Instagram", "TikTok", "LinkedIn"]
    
    PUBLICATIONS = ["Forbes", "TechCrunch", "Harvard Business Review", "Wired Magazine"]
    
    MEDIA_OUTLETS = ["CNN", "BBC", "NPR", "Bloomberg"]
    
    ACHIEVEMENTS = ["Wins Teaching Award", "Receives Innovation Prize", "Gets Community Recognition", "Earns Excellence Medal"]
    
    ACTIONS = ["Speaks", "Presents Research", "Leads Discussion", "Delivers Keynote"]
    
    EVENTS = ["Medical Conference", "Tech Summit", "Education Forum", "Industry Convention"]
    
    NEWS_ACTIONS = ["Makes Breakthrough Discovery", "Launches New Initiative", "Receives Major Grant", "Opens New Facility"]
    
    NEWS_SOURCES = ["Local News", "Business Journal", "Tech Times", "Health Today"]
    
    NEWS_TOPICS = ["Innovation", "Healthcare", "Education", "Technology"]
    
    NEWS_EVENTS = ["Announces Merger", "Launches Product", "Receives Award", "Starts Initiative"]

    # ===============================
    # ACADEMIC AND REGIONAL DATA
    # ===============================
    
    ACADEMIC_FIELDS = ["Computer Science", "Biology", "Psychology", "Engineering"]
    
    UNIVERSITIES = ["Harvard University", "MIT", "Stanford University", "UC Berkeley"]
    
    DISCIPLINES = ["Machine Learning", "Molecular Biology", "Cognitive Psychology", "Electrical Engineering"]
    
    STATES = ["California", "New York", "Texas", "Florida"]
    
    REGIONS = ["Silicon Valley", "New England", "Pacific Northwest", "Southeast"]

    # ===============================
    # GENERATION METHODS
    # ===============================

    @classmethod
    def generate_name(cls) -> tuple[str, str]:
        """Generate a random username"""
        first_name = random.choice(cls.FIRST_NAMES).lower()
        last_name = random.choice(cls.LAST_NAMES).lower()
        return first_name, last_name

    @staticmethod
    def generate_date(start_date: date, end_date: date) -> date:
        """Generate a random date between start_date and end_date"""
        days_between = (end_date - start_date).days
        random_days = random.randint(0, days_between)
        return start_date + timedelta(days=random_days)

    @classmethod
    def generate_phone(cls) -> str:
        """Generate a realistic-looking phone number"""
        area_code = random.randint(200, 999)
        first_part = random.randint(200, 999)
        second_part = random.randint(1000, 9999)
        return f"{random.choice(cls.COUNTRY_CODES)} ({area_code}) {first_part}-{second_part}"

    @classmethod
    def generate_fictive_user(cls) -> User:
        """Generate basic profile data common to all platforms"""
        first_name, last_name = cls.generate_name()
        email = f"{first_name}.{last_name}@example.com"
        phone = cls.generate_phone()

        # Calculate dates working backwards from current date
        current_date = datetime.now()

        # Birthdate between 18 and 80 years ago
        birth_year = current_date.year - random.randint(18, 80)
        birth_date = cls.generate_date(
            start_date=date(birth_year, 1, 1),
            end_date=date(birth_year, 12, 31)
        )

        return User(
            first_name=first_name,
            last_name=last_name,
            email=email,
            phone=phone,
            birth_date=birth_date,
            password="fictive_password_123"
        )

    @classmethod
    def generate_education_history(cls, user: User) -> List[Education]:
        """Generate realistic education history based on User object"""
        
        birth_year = user.birth_date.year
        current_year = datetime.now().year
        
        # Get all possible education combinations
        education_options = []
        for field_type, fields in cls.EDUCATION_FIELDS.items():
            for field in fields:
                for school in cls.COMPANIES["education"]:
                    # Generate a random graduation year between birth_year + 18 and current_year
                    grad_year = str(random.randint(birth_year + 18, current_year))
                    education_options.append((school, field, grad_year))

        # Filter schools where person would be old enough (18+) when degree was offered
        eligible_schools = [
            (school, degree, year) for school, degree, year in education_options
            if birth_year + 18 <= int(year) <= current_year
        ]

        if not eligible_schools:
            return []

        # Pick 1-2 random schools, but don't exceed available options
        num_degrees = min(random.randint(1, 2), len(eligible_schools))
        selected_schools = random.sample(eligible_schools, num_degrees)

        # Sort by year and return as TypedDict instances
        return [
            Education(school=school, degree=degree, year=year)
            for school, degree, year in sorted(selected_schools, key=lambda x: x[2])
        ]

    @classmethod
    def generate_work_history(cls, user: User, education_entries: Optional[List[Education]] = None) -> List[Job]:
        """Generate realistic work history in chronological order based on User object"""
        
        if education_entries is None:
            education_entries = cls.generate_education_history(user)
            
        birth_year = user.birth_date.year
        current_year = datetime.now().year
        
        # Start working after graduation or at 18, whichever is later
        start_year = (
            int(education_entries[-1]['year']) if education_entries
            else birth_year + 18
        )

        if start_year > current_year:
            return []

        work_history = []
        current_start_year = start_year
        num_jobs = random.randint(1, 3)

        for _ in range(num_jobs):
            if current_start_year > current_year:
                break

            # Pick a random company type and position type
            company_type = random.choice(list(cls.COMPANIES.keys()))
            position_type = random.choice(list(cls.POSITIONS.keys()))
            
            # Create job entry
            job = Job(
                company=random.choice(cls.COMPANIES[company_type]),
                position=random.choice(cls.POSITIONS[position_type]),
                start_year=current_start_year
            )
            work_history.append(job)

            # Next job starts at least 1 year later
            current_start_year += random.randint(1, 3)

        return work_history

    @classmethod
    def generate_location(cls, location_type: str = None, context: dict = None) -> str:
        """Generate a realistic address based on location type and context"""
        context = context or {}

        # Generate base address components
        state = random.choice(list(cls.STATES_CITIES.keys()))
        city = random.choice(cls.STATES_CITIES[state])
        street_num = random.randint(100, 9999)
        street = random.choice(cls.STREET_NAMES)
        zip_code = random.randint(10000, 99999)

        base_address = f"{street_num} {street}, {city}, {state} {zip_code}"

        # Address prefixes by type
        prefixes = {
            "home": f"Apt {random.randint(1, 999)}",
            "work": f"{context.get('company')} Office, Floor {random.randint(1, 20)}",
            "education": f"{context.get('school', 'University')} - "
                         f"{random.choice(['Main Campus', 'North Campus', 'South Campus', 'Student Center'])}",
            "frequent": f"{random.choice(cls.PLACE_TYPES[random.choice(list(cls.PLACE_TYPES.keys()))])}"
        }
        location_type: str = random.choice(list(prefixes.keys())) if not location_type else location_type
        prefix = prefixes.get(location_type)

        return f"{prefix}, {base_address}" if prefix else base_address

    @classmethod
    def generate_user_locations(
            cls,
            user: User,
            work_history: Optional[List[Job]] = None,
            education_history: Optional[List[Education]] = None
    ) -> Dict[str, Union[str, List[str]]]:
        """Generate all sensitive locations for a User object"""
        
        if work_history is None:
            work_history = cls.generate_work_history(user)
        if education_history is None:
            education_history = cls.generate_education_history(user)
            
        locations = {
            'home': cls.generate_location('home'),
            'frequent_places': []
        }

        # Add work location if employed
        if work_history:
            locations['work'] = cls.generate_location('work', {
                'company': work_history[0]['company']
            })

        # Add education location if in school
        if education_history:
            locations['education'] = cls.generate_location('education', {
                'school': education_history[0]['school']
            })

        # Add 2-4 frequently visited places
        for _ in range(random.randint(2, 4)):
            locations['frequent_places'].append(
                cls.generate_location('frequent')
            )

        return locations

    @classmethod
    def generate_sensitive_content(
            cls, user: User,
            work_history: Optional[List[Job]] = None,
            education_history: Optional[List[Education]] = None,
            sensitive_locations: Optional[Dict[str, Union[str, List[str]]]] = None
    ) -> str:
        """Generate content that contains personal/sensitive information based on User object"""
        template = random.choice(cls.PERSONAL_INFO_TEMPLATES)
        
        # Generate work and education history if not provided
        if work_history is None:
            work_history = cls.generate_work_history(user)
        if education_history is None:
            education_history = cls.generate_education_history(user)
        
        # Generate user's sensitive locations if not already present
        if sensitive_locations is None:
            sensitive_locations = cls.generate_user_locations(user, work_history, education_history)

        # Generate fake sensitive data
        replacements = {
            'cc_num': f"{random.randint(4000, 4999)} {random.randint(1000, 9999)} {random.randint(1000, 9999)} {random.randint(1000, 9999)}",
            'dl_num': f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(100000000, 999999999)}",
            'address': sensitive_locations['home'],
            'work_address': sensitive_locations.get('work', 'Not specified'),
            'education_address': sensitive_locations.get('education', 'Not specified'),
            'frequent_place': random.choice(sensitive_locations['frequent_places']) if sensitive_locations['frequent_places'] else 'Not specified',
            'ssn': f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}",
            'email': user.email,
            'phone': user.phone,
            'birth_date': user.birth_date.strftime("%Y-%m-%d"),
            'birth_place': random.choice([city for cities in cls.STATES_CITIES.values() for city in cities]),
            'company': work_history[0]['company'] if work_history else "TechCorp",
            'position': work_history[0]['position'] if work_history else "Software Engineer",
            'emp_id': f"EMP{random.randint(10000, 99999)}",
            'bank_acc': f"{random.randint(100000000, 999999999)}",
            'passport': f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(10000000, 99999999)}"
        }
        
        return template.format(**replacements)

    @classmethod
    def generate_content(
            cls,
            platform: str = "generic",
            include_sensitive: bool = False,
            user: Optional[User] = None
    ) -> str:
        """Get random content based on platform, with optional sensitive information
        
        Args:
            platform (str): The social media platform to generate content for ("instagram", "facebook", or "generic")
            include_sensitive (bool): Whether to potentially include sensitive information
            user (User, optional): User object required for generating sensitive content
            
        Returns:
            str: Generated content appropriate for the specified platform
        """
        if include_sensitive:
            if user is None:
                raise ValueError("user parameter is required when include_sensitive=True")
            if not isinstance(user, User):
                raise ValueError("user must be a User model instance")

            if random.random() < 0.15:
                return cls.generate_sensitive_content(user)

        # Generate platform-specific content
        match platform:
            case "instagram":
                return random.choice(cls.INSTAGRAM_CAPTIONS)
            case "facebook":
                return random.choice(cls.FACEBOOK_POSTS)
            case _:  # Default case for "generic" or any unrecognized platform
                return random.choice(cls.BIOS)

    @classmethod
    def generate_context(cls):
        """Generate random context for template formatting"""
        context = {
            # Professional context
            'company': random.choice(cls.COMPANIES["tech"]),
            'specialty': random.choice(cls.POSITIONS["healthcare"]),
            'institution': random.choice(cls.COMPANIES["healthcare"]),
            'title': random.choice(cls.POSITIONS["management"]),
            'role': random.choice(cls.POSITIONS["management"]),
            'position': random.choice(cls.POSITIONS["tech"]),
            'organization': random.choice(cls.COMPANIES["other"]),
            'profession': random.choice(cls.POSITIONS["tech"]),
            'workplace': random.choice(cls.COMPANIES["education"]),
            'credentials': random.choice(cls.CREDENTIALS),
            'department': random.choice(cls.EDUCATION_FIELDS["tech"]),
            'industry': random.choice(list(cls.COMPANIES.keys())),
            'company_name': random.choice(cls.COMPANIES["other"]),

            # Social media context
            'platform': random.choice(cls.MEDIA_PLATFORMS),

            # News media context
            'achievement': random.choice(cls.ACHIEVEMENTS),
            'action': random.choice(cls.ACTIONS),
            'event': random.choice(cls.EVENTS),
            'publication': random.choice(cls.PUBLICATIONS),
            'news_action': random.choice(cls.NEWS_ACTIONS),
            'news_source': random.choice(cls.NEWS_SOURCES),
            'news_topic': random.choice(cls.NEWS_TOPICS),
            'news_event': random.choice(cls.NEWS_EVENTS),
            'media_outlet': random.choice(cls.MEDIA_OUTLETS),

            # Directory context
            'degree': random.choice(cls.CREDENTIALS),
            'city': random.choice([city for cities in cls.STATES_CITIES.values() for city in cities]),

            # Academic context
            'field': random.choice(cls.ACADEMIC_FIELDS),
            'university': random.choice(cls.UNIVERSITIES),
            'discipline': random.choice(cls.DISCIPLINES),

            # Location context
            'state': random.choice(cls.STATES),
            'region': random.choice(cls.REGIONS),
        }

        return context
