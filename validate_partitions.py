#!/usr/bin/env python3
"""
Script to validate OMOP partitions
"""

import os
import logging
from dotenv import load_dotenv
from omop_partitioner import OMOPPartitioner

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function to validate partitions"""
    try:
        load_dotenv()
        partitioner = OMOPPartitioner(
            os.getenv('SOURCE_DB_URL'),
            int(os.getenv('NUM_PARTITIONS', '2')),
            os.getenv('DISTRIBUTION_STRATEGY', 'uniform')
        )
        if partitioner.validate_partitions():
            logger.info("Partition validation succeeded!")
        else:
            logger.error("Partition validation failed!")
    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        raise

if __name__ == "__main__":
    main() 