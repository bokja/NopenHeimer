import sys
import os

# Add project root to sys.path to allow importing shared modules
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

try:
    from shared.db import initialize_db
    from shared.logger import logger
except ImportError as e:
    print(f"Error: Could not import necessary modules. Make sure PYTHONPATH is set correctly or run from project root. Details: {e}")
    sys.exit(1)

if __name__ == "__main__":
    logger.info("Starting database initialization...")
    try:
        initialize_db()
        logger.info("Database initialization script completed successfully.")
    except Exception as e:
        logger.critical(f"Database initialization script failed: {e}", exc_info=True)
        sys.exit(1) 