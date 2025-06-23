#!/usr/bin/env python3
"""
Test script for the Container Packager

This script tests the container packager functionality without actually
uploading to the registry. It's useful for development and testing.
"""

import os
import sys
import yaml
import docker
import logging
from package_and_upload import ContainerPackager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_credentials_loading():
    """Test credentials loading functionality"""
    logger.info("Testing credentials loading...")
    
    # Test with non-existent file
    try:
        packager = ContainerPackager("non_existent.yaml")
        logger.error("Should have failed with non-existent file")
        return False
    except SystemExit:
        logger.info("✅ Correctly handled non-existent credentials file")
    
    # Test with valid template
    if os.path.exists("registry_credentials.yaml"):
        try:
            packager = ContainerPackager("registry_credentials.yaml")
            logger.info("✅ Successfully loaded credentials file")
            return True
        except SystemExit as e:
            logger.error(f"Failed to load credentials: {e}")
            return False
    else:
        logger.error("Credentials file not found")
        return False

def test_partition_detection():
    """Test partition container detection"""
    logger.info("Testing partition detection...")
    
    try:
        packager = ContainerPackager("registry_credentials.yaml")
        partitions = packager.get_running_partitions()
        
        logger.info(f"Found {len(partitions)} partition containers")
        for partition in partitions:
            logger.info(f"  - {partition['name']} (Partition {partition['partition_num']})")
        
        return len(partitions) > 0
        
    except Exception as e:
        logger.error(f"Error testing partition detection: {e}")
        return False

def test_docker_connection():
    """Test Docker client connection"""
    logger.info("Testing Docker connection...")
    
    try:
        client = docker.from_env()
        version = client.version()
        logger.info(f"✅ Docker connection successful - Version: {version['Version']}")
        return True
    except Exception as e:
        logger.error(f"❌ Docker connection failed: {e}")
        return False

def main():
    """Run all tests"""
    logger.info("Starting Container Packager tests...")
    
    tests = [
        ("Docker Connection", test_docker_connection),
        ("Credentials Loading", test_credentials_loading),
        ("Partition Detection", test_partition_detection),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n--- Testing: {test_name} ---")
        try:
            if test_func():
                logger.info(f"✅ {test_name} PASSED")
                passed += 1
            else:
                logger.error(f"❌ {test_name} FAILED")
        except Exception as e:
            logger.error(f"❌ {test_name} FAILED with exception: {e}")
    
    logger.info(f"\n--- Test Results ---")
    logger.info(f"Passed: {passed}/{total}")
    
    if passed == total:
        logger.info("🎉 All tests passed!")
        return 0
    else:
        logger.error("❌ Some tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 