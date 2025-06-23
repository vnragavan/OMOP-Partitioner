#!/usr/bin/env python3
"""
OMOP Partition Package Visibility Checker

This script checks the visibility of uploaded packages and provides browser URLs
for manual verification.
"""

import os
import sys
import yaml
import requests
import argparse
import logging
from typing import Dict, List, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PackageVisibilityChecker:
    def __init__(self, config_file: str = "registry_credentials.yaml"):
        """Initialize the package visibility checker"""
        self.config_file = config_file
        self.credentials = self.load_credentials()
        
    def load_credentials(self) -> Dict:
        """Load credentials from YAML file"""
        try:
            if not os.path.exists(self.config_file):
                logger.error(f"Credentials file {self.config_file} not found")
                logger.info("Please create registry_credentials.yaml with your GitHub credentials")
                sys.exit(1)
                
            with open(self.config_file, 'r') as f:
                credentials = yaml.safe_load(f)
                
            # Validate required fields
            required_fields = ['github_username', 'github_token', 'repository_name']
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
    
    def get_package_visibility(self, package_name: str) -> Optional[str]:
        """Get the visibility of a specific package"""
        try:
            # GitHub API endpoint for package details
            api_url = f"https://api.github.com/repos/{self.credentials['github_username']}/{self.credentials['repository_name']}/packages/container/{package_name}"
            
            headers = {
                "Authorization": f"token {self.credentials['github_token']}",
                "Accept": "application/vnd.github.v3+json"
            }
            
            response = requests.get(api_url, headers=headers)
            
            if response.status_code == 200:
                package_info = response.json()
                visibility = package_info.get('visibility', 'unknown')
                return visibility
            elif response.status_code == 404:
                logger.warning(f"Package {package_name} not found")
                return None
            else:
                logger.error(f"Failed to get package info: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting package visibility: {e}")
            return None
    
    def get_all_packages(self) -> List[Dict]:
        """Get all packages in the repository"""
        try:
            # GitHub API endpoint for all packages
            api_url = f"https://api.github.com/repos/{self.credentials['github_username']}/{self.credentials['repository_name']}/packages"
            
            headers = {
                "Authorization": f"token {self.credentials['github_token']}",
                "Accept": "application/vnd.github.v3+json"
            }
            
            response = requests.get(api_url, headers=headers)
            
            if response.status_code == 200:
                packages = response.json()
                # Filter for container packages only
                container_packages = [pkg for pkg in packages if pkg.get('package_type') == 'container']
                return container_packages
            else:
                logger.error(f"Failed to get packages: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting all packages: {e}")
            return []
    
    def check_partition_packages(self) -> Dict:
        """Check visibility of all partition packages"""
        try:
            # Get all packages
            all_packages = self.get_all_packages()
            
            # Filter for partition packages
            partition_packages = []
            for pkg in all_packages:
                pkg_name = pkg.get('name', '')
                if 'omop-partitions-partition-' in pkg_name:
                    partition_packages.append(pkg)
            
            # Check visibility for each partition package
            results = {}
            for pkg in partition_packages:
                pkg_name = pkg.get('name', '')
                visibility = pkg.get('visibility', 'unknown')
                
                # Generate browser URLs
                repo_url = f"https://github.com/{self.credentials['github_username']}/{self.credentials['repository_name']}"
                package_url = f"{repo_url}/packages/container/{pkg_name}"
                
                results[pkg_name] = {
                    'visibility': visibility,
                    'package_url': package_url,
                    'repo_url': repo_url,
                    'created_at': pkg.get('created_at'),
                    'updated_at': pkg.get('updated_at')
                }
            
            return results
            
        except Exception as e:
            logger.error(f"Error checking partition packages: {e}")
            return {}
    
    def print_visibility_report(self, results: Dict):
        """Print a formatted visibility report"""
        print("\n" + "="*80)
        print("🔍 OMOP PARTITION PACKAGE VISIBILITY REPORT")
        print("="*80)
        
        if not results:
            print("❌ No partition packages found")
            return
        
        print(f"📦 Found {len(results)} partition package(s)")
        print(f"🏠 Repository: {self.credentials['github_username']}/{self.credentials['repository_name']}")
        print(f"🔗 Repository URL: https://github.com/{self.credentials['github_username']}/{self.credentials['repository_name']}")
        print()
        
        public_count = 0
        private_count = 0
        
        for pkg_name, info in results.items():
            visibility = info['visibility']
            status_icon = "🔓" if visibility == 'public' else "🔒"
            
            if visibility == 'public':
                public_count += 1
            elif visibility == 'private':
                private_count += 1
            
            print(f"{status_icon} {pkg_name}")
            print(f"   Visibility: {visibility.upper()}")
            print(f"   Browser URL: {info['package_url']}")
            print(f"   Created: {info['created_at']}")
            print(f"   Updated: {info['updated_at']}")
            print()
        
        print("📊 SUMMARY")
        print("-" * 40)
        print(f"🔓 Public packages: {public_count}")
        print(f"🔒 Private packages: {private_count}")
        print(f"📦 Total packages: {len(results)}")
        print()
        
        if public_count > 0:
            print("⚠️  WARNING: Some packages are PUBLIC and accessible to anyone!")
            print("   Consider setting them to private if they contain sensitive data.")
            print()
        
        print("🌐 BROWSER VERIFICATION")
        print("-" * 40)
        print("You can verify these results by visiting the browser URLs above.")
        print("Public packages will be accessible without login.")
        print("Private packages will require authentication or show 'Not found'.")
        print()
        
        print("🔧 MANUAL VERIFICATION STEPS:")
        print("1. Open the repository URL in your browser")
        print("2. Click on the 'Packages' tab")
        print("3. Look for visibility badges (🔓 Public / 🔒 Private)")
        print("4. Click on individual packages to see details")
        print()
        
        print("="*80)

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Check visibility of OMOP partition packages")
    parser.add_argument(
        '--config', 
        default='registry_credentials.yaml',
        help='Path to credentials YAML file (default: registry_credentials.yaml)'
    )
    parser.add_argument(
        '--package',
        help='Check specific package visibility'
    )
    parser.add_argument(
        '--open-browser',
        action='store_true',
        help='Open browser URLs for verification (requires webbrowser module)'
    )
    
    args = parser.parse_args()
    
    # Initialize checker
    checker = PackageVisibilityChecker(args.config)
    
    if args.package:
        # Check specific package
        visibility = checker.get_package_visibility(args.package)
        if visibility:
            print(f"\n📦 Package: {args.package}")
            print(f"🔍 Visibility: {visibility.upper()}")
            print(f"🌐 Browser URL: https://github.com/{checker.credentials['github_username']}/{checker.credentials['repository_name']}/packages/container/{args.package}")
        else:
            print(f"❌ Package {args.package} not found or not accessible")
    else:
        # Check all partition packages
        results = checker.check_partition_packages()
        checker.print_visibility_report(results)
        
        # Open browser if requested
        if args.open_browser and results:
            try:
                import webbrowser
                repo_url = f"https://github.com/{checker.credentials['github_username']}/{checker.credentials['repository_name']}/packages"
                print(f"🌐 Opening browser to: {repo_url}")
                webbrowser.open(repo_url)
            except ImportError:
                print("⚠️  webbrowser module not available. Please open the URLs manually.")

if __name__ == "__main__":
    main() 