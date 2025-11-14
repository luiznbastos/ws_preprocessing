"""
Preprocessing Job
Transforms JSON data from S3 into relational data in PostgreSQL
"""
import logging
from settings import settings
from ETL.process import process_bronze

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 80)
    logger.info("INSTALLED PYTHON PACKAGES")
    logger.info("=" * 80)
    try:
        from importlib.metadata import distributions
        installed_packages = [(d.metadata.get('Name', 'Unknown'), d.version) for d in distributions()]
    except ImportError:
        try:
            import pkg_resources
            installed_packages = [(d.project_name, d.version) for d in pkg_resources.working_set]
        except Exception:
            logger.warning("Could not retrieve installed packages list")
            installed_packages = []
    
    installed_packages.sort()
    for package_name, package_version in installed_packages:
        logger.info(f"  {package_name}=={package_version}")
    logger.info("=" * 80)
    logger.info(f"Total packages: {len(installed_packages)}")
    logger.info("=" * 80)
    logger.info("Starting preprocessing job...")
    logger.info(f"Run ID: {settings.run_id}")
    logger.info(f"Run Type: {settings.run_type}")
    logger.info(f"Processing Configuration: {settings.get_processing_config()}")
    
    settings.validate_run_type()
    
    process_bronze()
    
    logger.info("Preprocessing complete")


if __name__ == "__main__":
    main()
