from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError

from src.database.models import (
    User, SecondaryEmail, SecondaryPhone, Address, Picture,
    DigitalFootprint, PersonalIdentity, UserDigitalFootprint, ActivityLog, Source
)
from src.config.enums import SourceCategory, DigitalFootprintType, AddressType, PersonalIdentityType
from src.validation.validation import DataValidator
from src.utils.logger import logger
from src.database.setup import DatabaseManager


ALLOWED_EXTENSIONS = ["jpg", "png", "wav", "mp3", "mp4", "json", "zip"]


def insert_sample_data():
    """Inserts sample data into the database for development and testing."""
    session = DatabaseManager.get_session()
    dv = DataValidator()
    
    try:
        # Check if sample data already exists
        if session.query(User).count() > 0:
            logger.info("Sample data already exists, skipping insertion")
            return

        logger.info("Starting sample data insertion...")

        # ===== CREATE SOURCES FIRST =====
        sources_data = [
            {
                "name": "Facebook",
                "url": dv.validate_url("https://facebook.com"),
                "category": dv.validate_source_category(SourceCategory.SOCIAL_MEDIA),
                "verified": True
            },
            {
                "name": "Instagram",
                "url": dv.validate_url("https://instagram.com"),
                "category": dv.validate_source_category(SourceCategory.SOCIAL_MEDIA),
                "verified": True
            },
            {
                "name": "LinkedIn",
                "url": dv.validate_url("https://linkedin.com"),
                "category": dv.validate_source_category(SourceCategory.PROFESSIONAL),
                "verified": True
            },
            {
                "name": "Twitter",
                "url": dv.validate_url("https://twitter.com"),
                "category": dv.validate_source_category(SourceCategory.SOCIAL_MEDIA),
                "verified": True
            },
            {
                "name": "GitHub",
                "url": dv.validate_url("https://github.com"),
                "category": dv.validate_source_category(SourceCategory.PROFESSIONAL),
                "verified": True
            },
            {
                "name": "Personal Blog",
                "url": dv.validate_url("https://example-blog.com"),
                "category": dv.validate_source_category(SourceCategory.PERSONAL),
                "verified": False
            },
        ]

        sources = []
        for source_data in sources_data:
            source = Source(**source_data)
            sources.append(source)
            session.add(source)

        session.flush()
        logger.info(f"Created {len(sources)} sources")

        # ===== CREATE USERS WITH FULL PROFILES =====
        users_data = [
            {
                "first_name": "John",
                "last_name": "Doe",
                "email": dv.validate_email("john.doe@example.com"),
                "password": "bcrypt$2b$12$hash_example_placeholder_123",  # Example hashed password format
                "birth_date": dv.validate_date("1990-05-15"),
                "phone": dv.validate_phone("+12125550101")
            },
            {
                "first_name": "Jane",
                "last_name": "Smith",
                "email": dv.validate_email("jane.smith@gmail.com"),
                "password": "bcrypt$2b$12$hash_example_placeholder_456",
                "birth_date": dv.validate_date("1985-08-22"),
                "phone": dv.validate_phone("+12125550102")
            },
            {
                "first_name": "Mike",
                "last_name": "Johnson",
                "email": dv.validate_email("mike.johnson@example.com"),
                "password": "bcrypt$2b$12$hash_example_placeholder_789",
                "birth_date": dv.validate_date("1992-12-03"),
                "phone": dv.validate_phone("+12125550103")
            },
            {
                "first_name": "Sarah",
                "last_name": "Williams",
                "email": dv.validate_email("sarah.w@outlook.com"),
                "password": "bcrypt$2b$12$hash_example_placeholder_abc",
                "birth_date": dv.validate_date("1988-03-28"),
                "phone": dv.validate_phone("+12125550104")
            }
        ]

        users = []
        for user_data in users_data:
            user = User(**user_data)
            users.append(user)
            session.add(user)

        session.flush()
        logger.info(f"Created {len(users)} users")

        # ===== ADD SECONDARY CONTACT INFO =====
        secondary_contacts = [
            SecondaryEmail(
                user_id=users[0].id,
                email=dv.validate_email("john.work@example.com")
            ),
            SecondaryEmail(
                user_id=users[0].id,
                email=dv.validate_email("jdoe.personal@yahoo.com")
            ),
            SecondaryPhone(
                user_id=users[0].id,
                phone=dv.validate_phone("+12125559901")
            ),
            SecondaryEmail(
                user_id=users[1].id,
                email=dv.validate_email("j.smith@example.com")
            ),
            SecondaryPhone(
                user_id=users[1].id,
                phone=dv.validate_phone("+12125559902")
            ),
            SecondaryPhone(
                user_id=users[1].id,
                phone=dv.validate_phone("+12125559903")
            ),
            SecondaryEmail(
                user_id=users[2].id,
                email=dv.validate_email("mike.j.dev@example.com")
            ),
            SecondaryEmail(
                user_id=users[3].id,
                email=dv.validate_email("sarah.williams@example.com")
            ),
            SecondaryPhone(
                user_id=users[3].id,
                phone=dv.validate_phone("+12125559904")
            ),
        ]

        for contact in secondary_contacts:
            session.add(contact)

        session.flush()
        logger.info(f"Created {len(secondary_contacts)} secondary contacts")

        # ===== ADD ADDRESSES =====
        addresses = [
            Address(
                user_id=users[0].id,
                type=dv.validate_address_type(AddressType.HOME),
                country="USA",
                city="New York",
                street="Broadway",
                number=123
            ),
            Address(
                user_id=users[0].id,
                type=dv.validate_address_type(AddressType.WORK),
                country="USA",
                city="New York",
                street="Wall St",
                number=456
            ),
            Address(
                user_id=users[1].id,
                type=dv.validate_address_type(AddressType.HOME),
                country="USA",
                city="Los Angeles",
                street="Sunset Blvd",
                number=789
            ),
            Address(
                user_id=users[2].id,
                type=dv.validate_address_type(AddressType.HOME),
                country="USA",
                city="Chicago",
                street="Michigan Ave",
                number=321
            ),
            Address(
                user_id=users[3].id,
                type=dv.validate_address_type(AddressType.HOME),
                country="USA",
                city="Boston",
                street="Commonwealth Ave",
                number=654
            ),
        ]

        for address in addresses:
            session.add(address)

        session.flush()
        logger.info(f"Created {len(addresses)} addresses")

        # ===== ADD PROFILE PICTURES =====
        pictures = [
            Picture(user_id=users[0].id, path="src/media/images/mock_image.png"),
            Picture(user_id=users[1].id, path="src/media/images/mock_image.jpg"),
            Picture(user_id=users[2].id, path="src/media/images/mock_image.jpeg"),
            Picture(user_id=users[3].id, path="src/media/images/mock_image.gif"),
        ]

        for picture in pictures:
            session.add(picture)

        session.flush()
        logger.info(f"Created {len(pictures)} profile pictures")

        # ===== CREATE DIGITAL FOOTPRINTS WITH DETAILED DATA =====
        digital_footprints = []

        # Facebook footprints for John Doe
        footprints_data = [
            # John Doe - Facebook
            {
                "type": dv.validate_digital_footprint_type(DigitalFootprintType.TEXT),
                "media_filepath": None,
                "reference_url": dv.validate_url("https://facebook.com/john.doe.profile"),
                "source_id": sources[0].id,  # Facebook
                "generate_id": True
            },
            {
                "type": dv.validate_digital_footprint_type(DigitalFootprintType.IMAGE),
                "media_filepath": "src/media/images/mock_image.jpg",
                "reference_url": dv.validate_url("https://facebook.com/john.doe.photo1"),
                "source_id": sources[0].id,
                "generate_id": True
            },
            
            # Jane Smith - Instagram
            {
                "type": dv.validate_digital_footprint_type(DigitalFootprintType.TEXT),
                "media_filepath": None,
                "reference_url": dv.validate_url("https://instagram.com/jane.smith.profile"),
                "source_id": sources[1].id  # Instagram
            },
            {
                "type": dv.validate_digital_footprint_type(DigitalFootprintType.IMAGE),
                "media_filepath": "src/media/images/mock_image.png",
                "reference_url": dv.validate_url("https://instagram.com/jane.smith.photo1"),
                "source_id": sources[1].id
            },
            
            # Mike Johnson - LinkedIn
            {
                "type": dv.validate_digital_footprint_type(DigitalFootprintType.TEXT),
                "media_filepath": None,
                "reference_url": dv.validate_url("https://linkedin.com/in/mike.johnson"),
                "source_id": sources[2].id  # LinkedIn
            },
            
            # Sarah Williams - GitHub
            {
                "type": dv.validate_digital_footprint_type(DigitalFootprintType.TEXT),
                "media_filepath": None,
                "reference_url": dv.validate_url("https://github.com/sarah.williams"),
                "source_id": sources[4].id  # GitHub
            },
        ]

        for footprint_data in footprints_data:
            footprint = DigitalFootprint(**footprint_data)
            digital_footprints.append(footprint)
            session.add(footprint)

        session.flush()
        logger.info(f"Created {len(digital_footprints)} digital footprints")

        # ===== CREATE PERSONAL IDENTITIES =====
        personal_identities = []
        
        # Add identities for each digital footprint
        identity_mappings = [
            # John Doe footprints
            (digital_footprints[0].id, PersonalIdentityType.NAME),
            (digital_footprints[0].id, PersonalIdentityType.PHONE),
            (digital_footprints[1].id, PersonalIdentityType.PICTURE),
            
            # Jane Smith footprints
            (digital_footprints[2].id, PersonalIdentityType.NAME),
            (digital_footprints[3].id, PersonalIdentityType.PICTURE),
            
            # Mike Johnson footprints
            (digital_footprints[4].id, PersonalIdentityType.NAME),
            
            # Sarah Williams footprints
            (digital_footprints[5].id, PersonalIdentityType.NAME),
        ]

        for footprint_id, identity_type in identity_mappings:
            identity = PersonalIdentity(
                digital_footprint_id=footprint_id,
                personal_identity=identity_type
            )
            personal_identities.append(identity)
            session.add(identity)

        session.flush()
        logger.info(f"Created {len(personal_identities)} personal identities")

        # ===== LINK USERS TO DIGITAL FOOTPRINTS =====
        user_footprint_links = [
            # John Doe links
            UserDigitalFootprint(user_id=users[0].id, digital_footprint_id=digital_footprints[0].id),
            UserDigitalFootprint(user_id=users[0].id, digital_footprint_id=digital_footprints[1].id),
            
            # Jane Smith links
            UserDigitalFootprint(user_id=users[1].id, digital_footprint_id=digital_footprints[2].id),
            UserDigitalFootprint(user_id=users[1].id, digital_footprint_id=digital_footprints[3].id),
            
            # Mike Johnson links
            UserDigitalFootprint(user_id=users[2].id, digital_footprint_id=digital_footprints[4].id),
            
            # Sarah Williams links
            UserDigitalFootprint(user_id=users[3].id, digital_footprint_id=digital_footprints[5].id),
        ]

        for link in user_footprint_links:
            session.add(link)

        session.flush()
        logger.info(f"Created {len(user_footprint_links)} user-footprint links")

        # ===== CREATE ACTIVITY LOGS =====
        activity_logs = []
        
        # Create timestamps for recent activity
        base_time = datetime.now()
        time_deltas = [
            timedelta(hours=1),
            timedelta(hours=6),
            timedelta(days=1),
            timedelta(days=3),
            timedelta(days=7),
            timedelta(days=14)
        ]

        for i, footprint in enumerate(digital_footprints):
            # Create 2-3 activity logs per footprint
            for j in range(2 + (i % 2)):  # 2 or 3 logs per footprint
                timestamp = base_time - time_deltas[j % len(time_deltas)]
                
                activity_log = ActivityLog(
                    digital_footprint_id=footprint.id,
                    timestamp=timestamp
                )
                activity_logs.append(activity_log)
                session.add(activity_log)

        session.flush()
        logger.info(f"Created {len(activity_logs)} activity logs")

        # Commit all changes
        session.commit()
        logger.info("Sample data insertion completed successfully!")

        # Print summary
        logger.info("=== SAMPLE DATA SUMMARY ===")
        logger.info(f"Users: {len(users)}")
        logger.info(f"Sources: {len(sources)}")
        logger.info(f"Digital Footprints: {len(digital_footprints)}")
        logger.info(f"Personal Identities: {len(personal_identities)}")
        logger.info(f"User-Footprint Links: {len(user_footprint_links)}")
        logger.info(f"Activity Logs: {len(activity_logs)}")
        logger.info(f"Secondary Contacts: {len(secondary_contacts)}")
        logger.info(f"Addresses: {len(addresses)}")
        logger.info(f"Pictures: {len(pictures)}")

    except SQLAlchemyError as e:
        logger.error(f"Database error during sample data insertion: {e}")
        session.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error during sample data insertion: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    insert_sample_data()
