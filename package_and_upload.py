#!/usr/bin/env python3
"""
OMOP Partitioner - Container Packaging and Upload Script

This script packages each partition container into a Docker image and uploads it
to the GitHub Container Registry (ghcr.io) with proper authentication.

Usage:
    python package_and_upload.py [--config credentials.yaml] [--registry username/repo]
"""

import os
import sys
import yaml
import docker
import logging
import argparse
import subprocess
import time
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ContainerPackager:
    def __init__(self, config_file: str = "registry_credentials.yaml"):
        """
        Initialize the container packager
        
        Args:
            config_file: Path to YAML file containing registry credentials
        """
        self.config_file = config_file
        self.docker_client = docker.from_env()
        self.credentials = self.load_credentials()
        self.registry_url = "ghcr.io"
        
    def load_credentials(self) -> Dict:
        """Load credentials from YAML file"""
        try:
            if not os.path.exists(self.config_file):
                logger.error(f"Credentials file {self.config_file} not found")
                logger.info("Creating template credentials file...")
                self.create_template_credentials()
                sys.exit(1)
                
            with open(self.config_file, 'r') as f:
                credentials = yaml.safe_load(f)
                
            # Validate required fields
            required_fields = ['github_username', 'github_token', 'registry_namespace']
            missing_fields = [field for field in required_fields if field not in credentials]
            
            if missing_fields:
                logger.error(f"Missing required fields in {self.config_file}: {missing_fields}")
                sys.exit(1)
                
            logger.info(f"Loaded credentials for user: {credentials['github_username']}")
            return credentials
            
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error loading credentials: {e}")
            sys.exit(1)
    
    def create_template_credentials(self):
        """Create a template credentials file"""
        template = {
            'github_username': 'your_github_username',
            'github_token': 'your_github_personal_access_token',
            'registry_namespace': 'your_github_username',
            'repository_name': 'omop-partitions',
            'image_tag': 'latest',
            'description': 'OMOP Database Partition Images'
        }
        
        with open(self.config_file, 'w') as f:
            yaml.dump(template, f, default_flow_style=False)
            
        logger.info(f"Created template credentials file: {self.config_file}")
        logger.info("Please edit this file with your actual GitHub credentials")
    
    def get_running_partitions(self) -> List[Dict]:
        """Get list of running partition containers with enhanced identification"""
        try:
            containers = self.docker_client.containers.list(
                filters={"name": "omop_partition"}
            )
            
            partitions = []
            for container in containers:
                # Extract partition number from container name (omop_partition_X)
                partition_num = container.name.split('_')[-1]
                
                # Get container info with enhanced details
                container_info = {
                    'name': container.name,
                    'id': container.id,
                    'partition_num': partition_num,
                    'status': container.status,
                    'ports': container.ports,
                    'image': container.image.tags[0] if container.image.tags else container.image.id,
                    'created': container.attrs['Created'],
                    'env': container.attrs['Config']['Env']
                }
                
                # Extract database configuration from container
                db_config = self.extract_database_config(container)
                container_info['database_config'] = db_config
                
                partitions.append(container_info)
                
            logger.info(f"Found {len(partitions)} running partition containers")
            return partitions
            
        except Exception as e:
            logger.error(f"Error getting running partitions: {e}")
            return []
    
    def extract_database_config(self, container) -> Dict:
        """Extract database configuration from container"""
        try:
            # Get container environment variables
            env_vars = container.attrs['Config']['Env']
            db_config = {}
            
            # Extract PostgreSQL configuration
            for env_var in env_vars:
                if env_var.startswith('POSTGRES_'):
                    key, value = env_var.split('=', 1)
                    db_config[key] = value
            
            # Get exposed ports
            ports = container.attrs['NetworkSettings']['Ports']
            if '5432/tcp' in ports:
                host_port = ports['5432/tcp'][0]['HostPort']
                db_config['HOST_PORT'] = host_port
            
            # Get container IP
            networks = container.attrs['NetworkSettings']['Networks']
            if networks:
                # Get the first network (usually bridge)
                network_name = list(networks.keys())[0]
                db_config['CONTAINER_IP'] = networks[network_name]['IPAddress']
            
            # Generate connection strings
            if all(key in db_config for key in ['POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB']):
                db_config['CONNECTION_STRING'] = (
                    f"postgresql://{db_config['POSTGRES_USER']}:{db_config['POSTGRES_PASSWORD']}"
                    f"@localhost:{db_config.get('HOST_PORT', '5432')}/{db_config['POSTGRES_DB']}"
                )
                
                db_config['CONTAINER_CONNECTION_STRING'] = (
                    f"postgresql://{db_config['POSTGRES_USER']}:{db_config['POSTGRES_PASSWORD']}"
                    f"@{db_config.get('CONTAINER_IP', 'localhost')}:5432/{db_config['POSTGRES_DB']}"
                )
            
            return db_config
            
        except Exception as e:
            logger.warning(f"Error extracting database config from {container.name}: {e}")
            return {}
    
    def generate_container_config(self, partition_info: Dict) -> Dict:
        """Generate configuration for accessing the container database"""
        config = {
            'container': {
                'name': partition_info['name'],
                'id': partition_info['id'],
                'partition_number': partition_info['partition_num'],
                'status': partition_info['status'],
                'created': partition_info['created']
            },
            'database': partition_info.get('database_config', {}),
            'image_info': {
                'name': partition_info['image'],
                'registry_url': self.registry_url,
                'uploaded_image': f"{self.registry_url}/{self.credentials['registry_namespace']}/{self.credentials['repository_name']}-partition-{partition_info['partition_num']}:{self.credentials.get('image_tag', 'latest')}"
            },
            'access_info': {
                'pull_command': f"docker pull {self.registry_url}/{self.credentials['registry_namespace']}/{self.credentials['repository_name']}-partition-{partition_info['partition_num']}:{self.credentials.get('image_tag', 'latest')}",
                'run_command': f"docker run -d -p {partition_info.get('database_config', {}).get('HOST_PORT', '5432')}:5432 --name {partition_info['name']}_restored {self.registry_url}/{self.credentials['registry_namespace']}/{self.credentials['repository_name']}-partition-{partition_info['partition_num']}:{self.credentials.get('image_tag', 'latest')}",
                'connect_command': f"psql {partition_info.get('database_config', {}).get('CONNECTION_STRING', 'postgresql://postgres:postgres@localhost:5432/postgres')}"
            }
        }
        return config
    
    def save_partition_configs(self, partition_configs: List[Dict], output_dir: str = "config"):
        """Save partition configurations to YAML files"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            # Save individual partition configs
            for config in partition_configs:
                partition_num = config['container']['partition_number']
                filename = f"{output_dir}/partition_{partition_num}_config.yaml"
                
                with open(filename, 'w') as f:
                    yaml.dump(config, f, default_flow_style=False, indent=2)
                
                logger.info(f"Saved partition {partition_num} config to {filename}")
            
            # Save combined config
            combined_config = {
                'metadata': {
                    'created_at': datetime.now().isoformat(),
                    'total_partitions': len(partition_configs),
                    'registry_namespace': self.credentials['registry_namespace'],
                    'repository_name': self.credentials['repository_name']
                },
                'partitions': partition_configs
            }
            
            combined_filename = f"{output_dir}/all_partitions_config.yaml"
            with open(combined_filename, 'w') as f:
                yaml.dump(combined_config, f, default_flow_style=False, indent=2)
            
            logger.info(f"Saved combined config to {combined_filename}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving partition configs: {e}")
            return False
    
    def commit_container_to_image(self, container_info: Dict) -> Optional[str]:
        """Commit a running container to a new image"""
        try:
            container_name = container_info['name']
            partition_num = container_info['partition_num']
            
            # Create image name
            image_name = f"{self.credentials['registry_namespace']}/{self.credentials['repository_name']}-partition-{partition_num}"
            image_tag = self.credentials.get('image_tag', 'latest')
            full_image_name = f"{image_name}:{image_tag}"
            
            logger.info(f"Committing container {container_name} to image {full_image_name}")
            
            # Get the container
            container = self.docker_client.containers.get(container_info['id'])
            
            # Commit the container to a new image
            image = container.commit(
                repository=image_name,
                tag=image_tag,
                message=f"OMOP Partition {partition_num} - {datetime.now().isoformat()}",
                author="OMOP Partitioner"
            )
            
            logger.info(f"Successfully committed container to image: {image.tags[0]}")
            return image.tags[0]
            
        except Exception as e:
            logger.error(f"Error committing container {container_info['name']}: {e}")
            return None
    
    def tag_for_registry(self, image_name: str) -> str:
        """Tag image for GitHub Container Registry"""
        registry_image_name = f"{self.registry_url}/{image_name}"
        
        try:
            # Get the image
            image = self.docker_client.images.get(image_name)
            
            # Tag for registry
            image.tag(registry_image_name)
            
            logger.info(f"Tagged image: {image_name} -> {registry_image_name}")
            return registry_image_name
            
        except Exception as e:
            logger.error(f"Error tagging image {image_name}: {e}")
            return None
    
    def login_to_registry(self) -> bool:
        """Login to GitHub Container Registry"""
        try:
            logger.info("Logging in to GitHub Container Registry...")
            
            # Login using docker CLI
            result = subprocess.run([
                'docker', 'login', self.registry_url,
                '-u', self.credentials['github_username'],
                '-p', self.credentials['github_token']
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("Successfully logged in to GitHub Container Registry")
                return True
            else:
                logger.error(f"Failed to login: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error logging in to registry: {e}")
            return False
    
    def push_image(self, image_name: str) -> bool:
        """Push image to GitHub Container Registry"""
        try:
            logger.info(f"Pushing image: {image_name}")
            
            # Push using docker CLI
            result = subprocess.run([
                'docker', 'push', image_name
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"Successfully pushed image: {image_name}")
                return True
            else:
                logger.error(f"Failed to push image: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error pushing image {image_name}: {e}")
            return False
    
    def create_manifest(self, partition_images: List[str]) -> bool:
        """Create a multi-arch manifest for all partition images"""
        try:
            if len(partition_images) < 2:
                logger.info("Skipping manifest creation (need at least 2 images)")
                return True
                
            manifest_name = f"{self.registry_url}/{self.credentials['registry_namespace']}/{self.credentials['repository_name']}:manifest"
            
            logger.info(f"Creating manifest: {manifest_name}")
            
            # Create manifest
            result = subprocess.run([
                'docker', 'manifest', 'create', manifest_name
            ] + partition_images, capture_output=True, text=True)
            
            if result.returncode == 0:
                # Push manifest
                push_result = subprocess.run([
                    'docker', 'manifest', 'push', manifest_name
                ], capture_output=True, text=True)
                
                if push_result.returncode == 0:
                    logger.info(f"Successfully created and pushed manifest: {manifest_name}")
                    return True
                else:
                    logger.error(f"Failed to push manifest: {push_result.stderr}")
                    return False
            else:
                logger.error(f"Failed to create manifest: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating manifest: {e}")
            return False
    
    def cleanup_local_images(self, image_names: List[str]):
        """Clean up local images after pushing"""
        try:
            for image_name in image_names:
                logger.info(f"Removing local image: {image_name}")
                self.docker_client.images.remove(image_name, force=True)
                
        except Exception as e:
            logger.error(f"Error cleaning up images: {e}")
    
    def package_and_upload(self, cleanup_local: bool = True, save_configs: bool = True, upload_configs: bool = False) -> bool:
        """Main method to package and upload all partition containers"""
        try:
            # Get running partitions
            partitions = self.get_running_partitions()
            if not partitions:
                logger.error("No running partition containers found")
                return False
            
            # Generate configurations for all partitions
            partition_configs = []
            for partition in partitions:
                config = self.generate_container_config(partition)
                partition_configs.append(config)
            
            # Save configurations if requested
            if save_configs:
                self.save_partition_configs(partition_configs)
            
            # Create config package
            config_package_path = self.create_config_package(partition_configs)
            if config_package_path:
                logger.info(f"Configuration package created: {config_package_path}")
            
            # Upload config files to GitHub if requested
            if upload_configs:
                if self.upload_config_files_to_github(partition_configs):
                    logger.info("✅ Configuration files uploaded to GitHub")
                else:
                    logger.warning("⚠️ Failed to upload configuration files to GitHub")
            
            # Login to registry
            if not self.login_to_registry():
                return False
            
            # Process each partition
            successful_images = []
            failed_images = []
            
            for partition in partitions:
                logger.info(f"Processing partition {partition['partition_num']}")
                
                # Commit container to image
                image_name = self.commit_container_to_image(partition)
                if not image_name:
                    failed_images.append(partition['name'])
                    continue
                
                # Tag for registry
                registry_image_name = self.tag_for_registry(image_name)
                if not registry_image_name:
                    failed_images.append(partition['name'])
                    continue
                
                # Push to registry
                if self.push_image(registry_image_name):
                    successful_images.append(registry_image_name)
                else:
                    failed_images.append(partition['name'])
            
            # Create manifest if we have multiple successful images
            if len(successful_images) > 1:
                self.create_manifest(successful_images)
            
            # Cleanup local images if requested
            if cleanup_local and successful_images:
                self.cleanup_local_images(successful_images)
            
            # Report results
            logger.info(f"Packaging and upload completed:")
            logger.info(f"  Successful: {len(successful_images)} images")
            logger.info(f"  Failed: {len(failed_images)} images")
            logger.info(f"  Configurations saved: {len(partition_configs)} files")
            if config_package_path:
                logger.info(f"  Config package: {config_package_path}")
            if upload_configs:
                logger.info(f"  Configs uploaded to GitHub: {'Yes' if upload_configs else 'No'}")
            
            if successful_images:
                logger.info("Successfully uploaded images:")
                for img in successful_images:
                    logger.info(f"  - {img}")
            
            if failed_images:
                logger.error("Failed to upload images:")
                for img in failed_images:
                    logger.error(f"  - {img}")
            
            return len(failed_images) == 0
            
        except Exception as e:
            logger.error(f"Error in package_and_upload: {e}")
            return False
    
    def upload_config_files_to_github(self, partition_configs: List[Dict]) -> bool:
        """Upload configuration files to GitHub as a release asset"""
        try:
            import requests
            import zipfile
            import tempfile
            
            logger.info("Uploading configuration files to GitHub...")
            
            # Create a temporary zip file with all configs
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_file:
                with zipfile.ZipFile(tmp_file.name, 'w') as zipf:
                    # Add individual partition configs
                    for config in partition_configs:
                        partition_num = config['container']['partition_number']
                        config_content = yaml.dump(config, default_flow_style=False, indent=2)
                        zipf.writestr(f'partition_{partition_num}_config.yaml', config_content)
                    
                    # Add combined config
                    combined_config = {
                        'metadata': {
                            'created_at': datetime.now().isoformat(),
                            'total_partitions': len(partition_configs),
                            'registry_namespace': self.credentials['registry_namespace'],
                            'repository_name': self.credentials['repository_name']
                        },
                        'partitions': partition_configs
                    }
                    combined_content = yaml.dump(combined_config, default_flow_style=False, indent=2)
                    zipf.writestr('all_partitions_config.yaml', combined_content)
                    
                    # Add README with usage instructions
                    readme_content = self.generate_config_readme(partition_configs)
                    zipf.writestr('README.md', readme_content)
                
                # Upload to GitHub releases
                success = self.upload_to_github_release(tmp_file.name)
                
                # Clean up
                os.unlink(tmp_file.name)
                
                return success
                
        except Exception as e:
            logger.error(f"Error uploading config files to GitHub: {e}")
            return False
    
    def generate_config_readme(self, partition_configs: List[Dict]) -> str:
        """Generate README with usage instructions for the config files"""
        readme = f"""# OMOP Partition Configuration Files

This package contains configuration files for {len(partition_configs)} OMOP database partitions.

## 📦 Images Available

"""
        
        for config in partition_configs:
            partition_num = config['container']['partition_number']
            image_url = config['image_info']['uploaded_image']
            port = config['database']['HOST_PORT']
            readme += f"""### Partition {partition_num}
- **Image**: `{image_url}`
- **Port**: {port}
- **Database**: {config['database']['POSTGRES_DB']}

"""
        
        readme += """## 🚀 Quick Start

### 1. Pull Images
```bash
# Pull all partitions
docker pull ghcr.io/username/omop-partitions-partition-0:latest
docker pull ghcr.io/username/omop-partitions-partition-1:latest
```

### 2. Run Containers
```bash
# Run partition 0
docker run -d -p 5433:5432 --name omop_partition_0_restored \\
  ghcr.io/username/omop-partitions-partition-0:latest

# Run partition 1
docker run -d -p 5434:5432 --name omop_partition_1_restored \\
  ghcr.io/username/omop-partitions-partition-1:latest
```

### 3. Connect to Databases
```bash
# Connect to partition 0
psql postgresql://postgres:postgres@localhost:5433/omop

# Connect to partition 1
psql postgresql://postgres:postgres@localhost:5434/omop
```

## 📁 Configuration Files

- `partition_0_config.yaml` - Configuration for partition 0
- `partition_1_config.yaml` - Configuration for partition 1
- `all_partitions_config.yaml` - Combined configuration for all partitions

## 🔧 Programmatic Access

```python
import yaml
import psycopg2

# Load configuration
with open('partition_0_config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Connect to database
conn = psycopg2.connect(
    host="localhost",
    port=config['database']['HOST_PORT'],
    database=config['database']['POSTGRES_DB'],
    user=config['database']['POSTGRES_USER'],
    password=config['database']['POSTGRES_PASSWORD']
)

# Query the database
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM omopcdm.person")
count = cursor.fetchone()[0]
print(f"Person count: {count}")
```

## 🔐 Credentials

**Default Database Credentials:**
- Username: `postgres`
- Password: `postgres`
- Database: `omop`

**For Private Images:**
```bash
docker login ghcr.io -u your_username -p your_github_token
```

## 📊 Database Schema

Each partition contains the complete OMOP CDM schema with a subset of the data distributed across partitions.

## 🆘 Support

For issues and questions, please refer to the main repository documentation.
"""
        return readme
    
    def upload_to_github_release(self, zip_file_path: str) -> bool:
        """Upload configuration files to GitHub as a release asset"""
        try:
            import requests
            
            # Create a release on GitHub
            release_data = {
                "tag_name": f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "name": f"OMOP Partitions Config - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "body": f"Configuration files for {len(self.get_running_partitions())} OMOP database partitions",
                "draft": False,
                "prerelease": False
            }
            
            # GitHub API endpoint
            api_url = f"https://api.github.com/repos/{self.credentials['github_username']}/{self.credentials['repository_name']}/releases"
            
            headers = {
                "Authorization": f"token {self.credentials['github_token']}",
                "Accept": "application/vnd.github.v3+json"
            }
            
            # Create release
            response = requests.post(api_url, json=release_data, headers=headers)
            
            if response.status_code == 201:
                release_info = response.json()
                upload_url = release_info['upload_url'].split('{')[0]
                
                # Upload the zip file
                with open(zip_file_path, 'rb') as f:
                    upload_response = requests.post(
                        f"{upload_url}?name=omop_partitions_config.zip",
                        data=f,
                        headers={
                            **headers,
                            "Content-Type": "application/zip"
                        }
                    )
                
                if upload_response.status_code == 201:
                    logger.info(f"✅ Configuration files uploaded to GitHub release: {release_info['html_url']}")
                    return True
                else:
                    logger.error(f"Failed to upload config files: {upload_response.text}")
                    return False
            else:
                logger.error(f"Failed to create GitHub release: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error uploading to GitHub release: {e}")
            return False
    
    def create_config_package(self, partition_configs: List[Dict]) -> str:
        """Create a downloadable package with configuration files"""
        try:
            import zipfile
            
            package_name = f"omop_partitions_config_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            package_path = os.path.join("output", package_name)
            
            os.makedirs("output", exist_ok=True)
            
            with zipfile.ZipFile(package_path, 'w') as zipf:
                # Add individual partition configs
                for config in partition_configs:
                    partition_num = config['container']['partition_number']
                    config_content = yaml.dump(config, default_flow_style=False, indent=2)
                    zipf.writestr(f'partition_{partition_num}_config.yaml', config_content)
                
                # Add combined config
                combined_config = {
                    'metadata': {
                        'created_at': datetime.now().isoformat(),
                        'total_partitions': len(partition_configs),
                        'registry_namespace': self.credentials['registry_namespace'],
                        'repository_name': self.credentials['repository_name']
                    },
                    'partitions': partition_configs
                }
                combined_content = yaml.dump(combined_config, default_flow_style=False, indent=2)
                zipf.writestr('all_partitions_config.yaml', combined_content)
                
                # Add README
                readme_content = self.generate_config_readme(partition_configs)
                zipf.writestr('README.md', readme_content)
            
            logger.info(f"✅ Configuration package created: {package_path}")
            return package_path
            
        except Exception as e:
            logger.error(f"Error creating config package: {e}")
            return None

    def set_package_visibility(self, package_name: str, visibility: str = "private") -> bool:
        """Set the visibility of a package in GitHub Container Registry
        
        Args:
            package_name: Name of the package (e.g., 'omop-partitions-partition-0')
            visibility: 'public' or 'private'
        """
        try:
            import requests
            
            if visibility not in ['public', 'private']:
                logger.error(f"Invalid visibility: {visibility}. Must be 'public' or 'private'")
                return False
            
            # GitHub API endpoint for package visibility
            api_url = f"https://api.github.com/repos/{self.credentials['github_username']}/{self.credentials['repository_name']}/packages/container/{package_name}/visibility"
            
            headers = {
                "Authorization": f"token {self.credentials['github_token']}",
                "Accept": "application/vnd.github.v3+json"
            }
            
            data = {"visibility": visibility}
            
            response = requests.post(api_url, json=data, headers=headers)
            
            if response.status_code == 204:
                logger.info(f"✅ Set package {package_name} visibility to {visibility}")
                return True
            else:
                logger.error(f"Failed to set package visibility: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error setting package visibility: {e}")
            return False

    def set_all_packages_visibility(self, visibility: str = "private") -> bool:
        """Set visibility for all partition packages"""
        try:
            partitions = self.get_running_partitions()
            if not partitions:
                logger.error("No running partition containers found")
                return False
            
            success_count = 0
            for partition in partitions:
                package_name = f"{self.credentials['repository_name']}-partition-{partition['partition_num']}"
                if self.set_package_visibility(package_name, visibility):
                    success_count += 1
            
            logger.info(f"Set visibility to {visibility} for {success_count}/{len(partitions)} packages")
            return success_count == len(partitions)
            
        except Exception as e:
            logger.error(f"Error setting all package visibilities: {e}")
            return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Package and upload OMOP partition containers")
    parser.add_argument(
        '--config', 
        default='registry_credentials.yaml',
        help='Path to credentials YAML file (default: registry_credentials.yaml)'
    )
    parser.add_argument(
        '--no-cleanup',
        action='store_true',
        help='Keep local images after uploading (default: cleanup local images)'
    )
    parser.add_argument(
        '--no-configs',
        action='store_true',
        help='Skip saving partition configuration files (default: save configs)'
    )
    parser.add_argument(
        '--upload-configs',
        action='store_true',
        help='Upload configuration files to GitHub as release assets (default: local only)'
    )
    parser.add_argument(
        '--create-template',
        action='store_true',
        help='Create template credentials file and exit'
    )
    parser.add_argument(
        '--visibility',
        choices=['public', 'private'],
        default='private',
        help='Set package visibility (default: private)'
    )
    parser.add_argument(
        '--set-visibility-only',
        action='store_true',
        help='Only set visibility for existing packages (skip upload)'
    )
    
    args = parser.parse_args()
    
    if args.create_template:
        packager = ContainerPackager(args.config)
        packager.create_template_credentials()
        return
    
    # Initialize packager
    packager = ContainerPackager(args.config)
    
    # Set visibility only if requested
    if args.set_visibility_only:
        success = packager.set_all_packages_visibility(args.visibility)
        if success:
            logger.info(f"✅ Successfully set all packages to {args.visibility}")
            sys.exit(0)
        else:
            logger.error(f"❌ Failed to set package visibility")
            sys.exit(1)
    
    # Package and upload
    success = packager.package_and_upload(
        cleanup_local=not args.no_cleanup,
        save_configs=not args.no_configs,
        upload_configs=args.upload_configs
    )
    
    # Set visibility after upload if different from default
    if success and args.visibility != 'private':
        logger.info(f"Setting package visibility to {args.visibility}...")
        packager.set_all_packages_visibility(args.visibility)
    
    if success:
        logger.info("✅ All containers successfully packaged and uploaded!")
        sys.exit(0)
    else:
        logger.error("❌ Some containers failed to package or upload")
        sys.exit(1)

if __name__ == "__main__":
    main() 