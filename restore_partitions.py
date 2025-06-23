#!/usr/bin/env python3
"""
OMOP Partitioner - Partition Restoration Script

This script demonstrates how to restore and use uploaded partition images
from GitHub Container Registry using the generated configuration files.

Usage:
    python restore_partitions.py [--config all_partitions_config.yaml] [--partition 0]
"""

import os
import sys
import yaml
import docker
import logging
import argparse
import subprocess
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PartitionRestorer:
    def __init__(self, config_file: str = "config/all_partitions_config.yaml"):
        """
        Initialize the partition restorer
        
        Args:
            config_file: Path to YAML file containing partition configurations
        """
        self.config_file = config_file
        self.docker_client = docker.from_env()
        self.config = self.load_config()
        
    def load_config(self) -> Dict:
        """Load partition configuration from YAML file"""
        try:
            if not os.path.exists(self.config_file):
                logger.error(f"Configuration file {self.config_file} not found")
                logger.info("Please run package_and_upload.py first to generate configurations")
                sys.exit(1)
                
            with open(self.config_file, 'r') as f:
                config = yaml.safe_load(f)
                
            logger.info(f"Loaded configuration for {config.get('metadata', {}).get('total_partitions', 0)} partitions")
            return config
            
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            sys.exit(1)
    
    def list_partitions(self) -> List[Dict]:
        """List all available partitions"""
        partitions = self.config.get('partitions', [])
        logger.info(f"Available partitions:")
        for partition in partitions:
            container_info = partition['container']
            db_info = partition['database']
            logger.info(f"  Partition {container_info['partition_number']}:")
            logger.info(f"    Image: {partition['image_info']['uploaded_image']}")
            logger.info(f"    Port: {db_info.get('HOST_PORT', 'N/A')}")
            logger.info(f"    Database: {db_info.get('POSTGRES_DB', 'N/A')}")
        return partitions
    
    def pull_partition_image(self, partition_config: Dict) -> bool:
        """Pull partition image from registry"""
        try:
            image_url = partition_config['image_info']['uploaded_image']
            logger.info(f"Pulling image: {image_url}")
            
            # Pull using docker CLI
            result = subprocess.run([
                'docker', 'pull', image_url
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"Successfully pulled image: {image_url}")
                return True
            else:
                logger.error(f"Failed to pull image: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error pulling image: {e}")
            return False
    
    def run_partition_container(self, partition_config: Dict, port: Optional[str] = None) -> bool:
        """Run partition container from image"""
        try:
            container_info = partition_config['container']
            db_info = partition_config['database']
            image_url = partition_config['image_info']['uploaded_image']
            
            # Use provided port or original port
            host_port = port or db_info.get('HOST_PORT', '5432')
            container_name = f"{container_info['name']}_restored"
            
            logger.info(f"Running container: {container_name}")
            logger.info(f"  Image: {image_url}")
            logger.info(f"  Port: {host_port}:5432")
            
            # Run container
            container = self.docker_client.containers.run(
                image_url,
                name=container_name,
                detach=True,
                ports={'5432/tcp': host_port},
                environment={
                    'POSTGRES_USER': db_info.get('POSTGRES_USER', 'postgres'),
                    'POSTGRES_PASSWORD': db_info.get('POSTGRES_PASSWORD', 'postgres'),
                    'POSTGRES_DB': db_info.get('POSTGRES_DB', 'postgres')
                }
            )
            
            logger.info(f"Successfully started container: {container.name}")
            logger.info(f"Container ID: {container.id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error running container: {e}")
            return False
    
    def test_database_connection(self, partition_config: Dict) -> bool:
        """Test database connection to restored partition"""
        try:
            db_info = partition_config['database']
            connection_string = db_info.get('CONNECTION_STRING')
            
            if not connection_string:
                logger.warning("No connection string available for testing")
                return False
            
            logger.info(f"Testing database connection: {connection_string}")
            
            # Test connection using psql
            result = subprocess.run([
                'psql', connection_string, '-c', 'SELECT version();'
            ], capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                logger.info("✅ Database connection successful")
                logger.info(f"PostgreSQL version: {result.stdout.strip()}")
                return True
            else:
                logger.error(f"❌ Database connection failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Database connection timeout")
            return False
        except Exception as e:
            logger.error(f"❌ Error testing database connection: {e}")
            return False
    
    def restore_partition(self, partition_num: int, port: Optional[str] = None) -> bool:
        """Restore a specific partition"""
        try:
            # Find partition configuration
            partition_config = None
            for partition in self.config.get('partitions', []):
                if partition['container']['partition_number'] == str(partition_num):
                    partition_config = partition
                    break
            
            if not partition_config:
                logger.error(f"Partition {partition_num} not found in configuration")
                return False
            
            logger.info(f"Restoring partition {partition_num}")
            
            # Pull image
            if not self.pull_partition_image(partition_config):
                return False
            
            # Run container
            if not self.run_partition_container(partition_config, port):
                return False
            
            # Test connection
            if not self.test_database_connection(partition_config):
                logger.warning("Database connection test failed, but container is running")
            
            logger.info(f"✅ Partition {partition_num} restored successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error restoring partition {partition_num}: {e}")
            return False
    
    def restore_all_partitions(self) -> bool:
        """Restore all partitions"""
        try:
            partitions = self.config.get('partitions', [])
            if not partitions:
                logger.error("No partitions found in configuration")
                return False
            
            logger.info(f"Restoring all {len(partitions)} partitions")
            
            successful = 0
            failed = 0
            
            for partition in partitions:
                partition_num = partition['container']['partition_number']
                if self.restore_partition(int(partition_num)):
                    successful += 1
                else:
                    failed += 1
            
            logger.info(f"Restoration completed:")
            logger.info(f"  Successful: {successful}")
            logger.info(f"  Failed: {failed}")
            
            return failed == 0
            
        except Exception as e:
            logger.error(f"Error restoring all partitions: {e}")
            return False
    
    def cleanup_restored_containers(self):
        """Clean up restored containers"""
        try:
            containers = self.docker_client.containers.list(
                filters={"name": "omop_partition.*_restored"}
            )
            
            for container in containers:
                logger.info(f"Stopping and removing container: {container.name}")
                container.stop()
                container.remove()
            
            logger.info(f"Cleaned up {len(containers)} restored containers")
            
        except Exception as e:
            logger.error(f"Error cleaning up containers: {e}")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Restore OMOP partition containers from registry")
    parser.add_argument(
        '--config', 
        default='config/all_partitions_config.yaml',
        help='Path to partition configuration file'
    )
    parser.add_argument(
        '--partition',
        type=int,
        help='Restore specific partition number'
    )
    parser.add_argument(
        '--port',
        type=str,
        help='Custom port for container (default: use original port)'
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List available partitions and exit'
    )
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Clean up restored containers'
    )
    
    args = parser.parse_args()
    
    # Initialize restorer
    restorer = PartitionRestorer(args.config)
    
    if args.list:
        restorer.list_partitions()
        return
    
    if args.cleanup:
        restorer.cleanup_restored_containers()
        return
    
    # Restore partitions
    if args.partition is not None:
        success = restorer.restore_partition(args.partition, args.port)
    else:
        success = restorer.restore_all_partitions()
    
    if success:
        logger.info("✅ Partition restoration completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Some partitions failed to restore")
        sys.exit(1)

if __name__ == "__main__":
    main() 