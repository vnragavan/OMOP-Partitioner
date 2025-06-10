#!/usr/bin/env python3
"""
Script to clean up partition containers robustly by name
"""

import logging
import docker
import time
from docker.errors import DockerException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function to clean up containers by name"""
    try:
        for attempt in range(3):
            try:
                client = docker.DockerClient(base_url='unix://var/run/docker.sock', timeout=300)
                # simple ping to verify
                client.ping()
                break
            except DockerException as de:
                if attempt == 2:
                    raise
                logger.warning(f"Docker daemon not responding (attempt {attempt+1}/3): {de}. Retrying in 5s...")
                time.sleep(5)
        containers = client.containers.list(all=True)
        removed = 0
        for container in containers:
            if any(name.startswith('omop_partition_') for name in container.name.split("/")):
                logger.info(f"Stopping and removing container: {container.name}")
                try:
                    container.stop()
                except Exception as e:
                    logger.warning(f"Could not stop {container.name}: {e}")
                try:
                    container.remove()
                    removed += 1
                except Exception as e:
                    logger.warning(f"Could not remove {container.name}: {e}")
        if removed == 0:
            logger.info("No OMOP partition containers found to clean up.")
        else:
            logger.info(f"Cleaned up {removed} OMOP partition containers.")
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        raise

if __name__ == "__main__":
    main() 