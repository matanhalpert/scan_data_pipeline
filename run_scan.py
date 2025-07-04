from datetime import date
from typing import Optional, Dict

from src.database.models import User, Picture
from src.database.setup import DatabaseManager
from src.extract.unified_extractor import UnifiedExtractor
from src.load.load import Loader
from src.simulate.world import SimulationWorld
from src.transform.unified_transformer import UnifiedTransformer
from src.utils.logger import logger


def load_and_get_subject_user(
        first_name: str,
        last_name: str,
        email: str,
        password: str,
        phone: str,
        birth_date: date | str,
        reference_picture_path: str = "src/media/images/mock_image.png"
) -> Optional[User]:
    """Create and persist a subject user with a reference picture in the database."""

    subject_user: User = User(
        first_name=first_name,
        last_name=last_name,
        email=email,
        password=password,
        phone=phone,
        birth_date=birth_date,
    )

    try:
        with DatabaseManager.get_session() as session:
            # Add subject to database
            session.add(subject_user)
            session.flush()  # Get the user ID

            # Assign a reference picture (a mock picture by default)
            profile_picture = Picture(
                user_id=subject_user.id,
                path=reference_picture_path
            )
            session.add(profile_picture)
            session.commit()
    except Exception as e:
        logger.error(f"Failed to create subject user: {e}")
        return None

    return subject_user


def run_scan(subject_user: User) -> Dict:
    """
    Run a complete data pipeline scan for a test user.
    
    Orchestrates the full ETL pipeline: creates simulation data, extracts data from various sources in simulation,
    transforms data into structured formats, and loads into database.
    """
    with SimulationWorld(base_users_count=20, unique_users=[subject_user]) as world:
        world.export_data(save_to_disk=True)

        # Extract data
        extractor = UnifiedExtractor(user_id=subject_user.id)
        extraction_result = extractor.extract()

        # Transform data
        transformer = UnifiedTransformer(user_id=subject_user.id)
        transformation_result = transformer.transform()

        # Load data
        loader = Loader(user_id=subject_user.id)
        load_result = loader.load(transformation_result)

        # Get and return results
        extract_summary = extractor.get_metadata()
        transform_summary = transformer.get_detailed_summary()
        load_summary = loader.load_summary()

        # Combine all summaries
        complete_summary = {
            'user_id': subject_user.id,
            'user_name': f"{subject_user.first_name} {subject_user.last_name}",
            'user_email': subject_user.email,
            'extraction': extract_summary,
            'transformation': transform_summary,
            'load': load_summary,
            'pipeline_success': (
                extract_summary.get('extraction_status') == 'completed' and
                transform_summary.get('transformation_status') == 'completed' and
                load_summary.get('success', False)
            )
        }

        print("=== COMPLETE SCAN PIPELINE SUMMARY ===")
        print(f"User: {complete_summary['user_name']} ({complete_summary['user_email']})")
        print(f"Pipeline Success: {complete_summary['pipeline_success']}")
        print(f"\nExtraction Status: {extract_summary.get('extraction_status')}")
        print(f"Transformation Status: {transform_summary.get('transformation_status')}")
        print(f"Load Status: {load_summary.get('load_status')}")
        print(f"\nRecords Loaded:")
        print(f"  - Digital Footprints: {load_summary.get('breakdown', {}).get('digital_footprints', {}).get('inserted', 0)}")
        print(f"  - Personal Identities: {load_summary.get('breakdown', {}).get('personal_identities', {}).get('inserted', 0)}")
        print(f"  - Activity Logs: {load_summary.get('breakdown', {}).get('activity_logs', {}).get('inserted', 0)}")
        print(f"  - User-Footprint Links: {load_summary.get('breakdown', {}).get('user_footprint_links', {}).get('inserted', 0)}")
        print(f"\nTotal Records Inserted: {load_summary.get('total_records_inserted', 0)}")
        if load_summary.get('error_count', 0) > 0:
            print(f"Errors: {load_summary.get('error_count', 0)}")

        return complete_summary


if __name__ == "__main__":
    subject: User = load_and_get_subject_user(
        first_name='rocky',
        last_name='balaboa',
        email='rocky.balboa@example.com',
        password='examplepassword123',
        phone='+12125559903',
        birth_date="1992-12-03"
    )

    run_scan(subject)
