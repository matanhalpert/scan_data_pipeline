from src.database.models import Picture
from src.database.setup import DatabaseManager
from src.extract.unified_extractor import UnifiedExtractor
from src.load.load import Loader
from src.simulate.data_generator import DataGenerator
from src.simulate.world import SimulationWorld
from src.transform.unified_transformer import UnifiedTransformer


def run_scan(test_num: int, base_users_count: int = 20, profile_image_path: str = "src/media/images/mock_image.png"):
    """
    Run a complete data pipeline scan for a test user.
    
    Orchestrates the full ETL pipeline: creates test user, extracts data from various sources,
    transforms data into structured formats, and loads into database.
    
    Args:
        test_num: Test user number for naming
        base_users_count: Number of base users in simulation
        profile_image_path: Path to profile image
    
    Returns:
        dict: Complete pipeline results summary including extraction, transformation, and load statistics
    """
    # Generate and configure test user
    user = DataGenerator.generate_fictive_user()
    user.first_name = f"test{test_num}"
    user.last_name = f"test{test_num}"
    user.email = f"test{test_num}.test{test_num}@example.com"

    with DatabaseManager.get_session() as session:
        # Add user to database
        session.add(user)
        session.flush()  # Get the user ID

        # Assign profile picture
        profile_picture = Picture(
            user_id=user.id,
            path=profile_image_path
        )
        session.add(profile_picture)
        session.commit()

        # Run simulate and processing pipeline
        with SimulationWorld(base_users_count=base_users_count, unique_users=[user]) as world:
            world.export_data(save_to_disk=True)

            # Extract data
            extractor = UnifiedExtractor(user_id=user.id)
            extraction_result = extractor.extract()

            # Transform data
            transformer = UnifiedTransformer(user_id=user.id)
            transformation_result = transformer.transform()

            # Load data
            loader = Loader(user_id=user.id)
            load_result = loader.load(transformation_result)

            # Get and return results
            extract_summary = extractor.get_metadata()
            transform_summary = transformer.get_detailed_summary()
            load_summary = loader.load_summary()
            
            # Combine all summaries
            complete_summary = {
                'user_id': user.id,
                'user_name': f"{user.first_name} {user.last_name}",
                'user_email': user.email,
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
    test_num = 10   # Increment in each scan to avoid collisions
    run_scan(test_num=test_num)
