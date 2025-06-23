#!/usr/bin/env python3
"""
OMOP Partition Read Token Generator

This script helps create and manage read-only tokens for colleagues
to access private OMOP partition images.
"""

import os
import sys
import yaml
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ReadTokenGenerator:
    def __init__(self, config_file: str = "registry_credentials.yaml"):
        """Initialize the read token generator"""
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
    
    def generate_token_instructions(self, token_type: str = "fine-grained") -> str:
        """Generate instructions for creating read tokens"""
        
        if token_type == "fine-grained":
            return self.generate_fine_grained_instructions()
        elif token_type == "classic":
            return self.generate_classic_instructions()
        elif token_type == "deploy":
            return self.generate_deploy_token_instructions()
        else:
            return "Invalid token type"
    
    def generate_fine_grained_instructions(self) -> str:
        """Generate instructions for fine-grained tokens"""
        instructions = f"""
🔑 FINE-GRAINED PERSONAL ACCESS TOKEN INSTRUCTIONS
{'='*60}

📋 Step-by-Step Instructions:

1. Go to GitHub Token Settings:
   https://github.com/settings/personal-access-tokens

2. Click "Generate new token" → "Fine-grained tokens"

3. Configure Token Settings:
   - Token name: "OMOP Partitions Read Access"
   - Expiration: 90 days (recommended)
   - Repository access: "Only select repositories"
   - Selected repositories: "{self.credentials['github_username']}/{self.credentials['repository_name']}"

4. Set Permissions:
   Repository permissions:
   - Contents: "Read-only"
   - Metadata: "Read-only" 
   - Packages: "Read-only"

5. Click "Generate token"

6. Copy the token (starts with 'github_pat_')

📤 Share with Colleagues:
```bash
GITHUB_USERNAME={self.credentials['github_username']}
GITHUB_TOKEN=github_pat_your_token_here
REGISTRY_URL=ghcr.io
```

🔧 Usage Commands:
```bash
# Login with fine-grained token
docker login ghcr.io -u {self.credentials['github_username']} -p github_pat_your_token_here

# Download images
docker pull ghcr.io/{self.credentials['github_username']}/{self.credentials['repository_name']}-partition-0:latest
docker pull ghcr.io/{self.credentials['github_username']}/{self.credentials['repository_name']}-partition-1:latest
```

✅ Advantages:
- Most secure option
- Repository-specific access
- Fine-grained permissions
- Can be easily revoked
"""
        return instructions
    
    def generate_classic_instructions(self) -> str:
        """Generate instructions for classic tokens"""
        instructions = f"""
🔑 CLASSIC PERSONAL ACCESS TOKEN INSTRUCTIONS
{'='*60}

📋 Step-by-Step Instructions:

1. Go to GitHub Token Settings:
   https://github.com/settings/tokens

2. Click "Generate new token (classic)"

3. Configure Token Settings:
   - Note: "OMOP Partitions Read Access"
   - Expiration: 90 days (recommended)
   - Scopes: ✅ read:packages (only this scope needed)

4. Click "Generate token"

5. Copy the token (starts with 'ghp_')

📤 Share with Colleagues:
```bash
GITHUB_USERNAME={self.credentials['github_username']}
GITHUB_TOKEN=ghp_your_token_here
REGISTRY_URL=ghcr.io
```

🔧 Usage Commands:
```bash
# Login with classic token
docker login ghcr.io -u {self.credentials['github_username']} -p ghp_your_token_here

# Download images
docker pull ghcr.io/{self.credentials['github_username']}/{self.credentials['repository_name']}-partition-0:latest
docker pull ghcr.io/{self.credentials['github_username']}/{self.credentials['repository_name']}-partition-1:latest
```

✅ Advantages:
- Simple to create
- Widely supported
- Easy to understand
"""
        return instructions
    
    def generate_deploy_token_instructions(self) -> str:
        """Generate instructions for deploy tokens"""
        instructions = f"""
🔑 DEPLOY TOKEN INSTRUCTIONS
{'='*60}

📋 Step-by-Step Instructions:

1. Go to Repository Settings:
   https://github.com/{self.credentials['github_username']}/{self.credentials['repository_name']}/settings/keys

2. Click "Deploy keys" tab → "Deploy tokens"

3. Click "Add deploy token"

4. Configure Token Settings:
   - Token name: "OMOP Partitions Download Access"
   - Expiration: 90 days (recommended)
   - Scopes: ✅ read:packages

5. Click "Add token"

6. Copy the token (starts with 'ghd_')

📤 Share with Colleagues:
```bash
GITHUB_USERNAME={self.credentials['github_username']}
GITHUB_TOKEN=ghd_your_deploy_token_here
REGISTRY_URL=ghcr.io
```

🔧 Usage Commands:
```bash
# Login with deploy token
docker login ghcr.io -u {self.credentials['github_username']} -p ghd_your_deploy_token_here

# Download images
docker pull ghcr.io/{self.credentials['github_username']}/{self.credentials['repository_name']}-partition-0:latest
docker pull ghcr.io/{self.credentials['github_username']}/{self.credentials['repository_name']}-partition-1:latest
```

✅ Advantages:
- Repository-specific
- Can't access other repositories
- Automatically scoped to this repo
- Easy to manage
"""
        return instructions
    
    def generate_colleague_guide(self, token_type: str = "fine-grained") -> str:
        """Generate a complete guide for colleagues"""
        guide = f"""
# 🔒 OMOP Partitions - Read Access Guide

## 📋 What You Need

You've been provided with read-only access to OMOP partition images.

## 🔑 Your Credentials

```bash
GITHUB_USERNAME={self.credentials['github_username']}
GITHUB_TOKEN=[TOKEN_PROVIDED_BY_ADMIN]
REGISTRY_URL=ghcr.io
```

## 🚀 Quick Start

### Step 1: Login to Registry
```bash
docker login ghcr.io -u {self.credentials['github_username']} -p [YOUR_TOKEN]
```

### Step 2: Download Images
```bash
# Download partition 0
docker pull ghcr.io/{self.credentials['github_username']}/{self.credentials['repository_name']}-partition-0:latest

# Download partition 1  
docker pull ghcr.io/{self.credentials['github_username']}/{self.credentials['repository_name']}-partition-1:latest
```

### Step 3: Run Containers
```bash
# Run partition 0
docker run -d -p 5433:5432 --name omop_partition_0_restored \\
  ghcr.io/{self.credentials['github_username']}/{self.credentials['repository_name']}-partition-0:latest

# Run partition 1
docker run -d -p 5434:5432 --name omop_partition_1_restored \\
  ghcr.io/{self.credentials['github_username']}/{self.credentials['repository_name']}-partition-1:latest
```

### Step 4: Connect to Databases
```bash
# Connect to partition 0
psql postgresql://postgres:postgres@localhost:5433/omop

# Connect to partition 1
psql postgresql://postgres:postgres@localhost:5434/omop
```

## 🔐 Database Credentials

- **Username**: postgres
- **Password**: postgres
- **Database**: omop
- **Ports**: 5433 (partition 0), 5434 (partition 1)

## 🛠️ Troubleshooting

### "Not Found" Error
```bash
# Check if you're logged in
docker login ghcr.io
```

### "Unauthorized" Error
```bash
# Verify your token is correct
# Contact admin if issues persist
```

## 📞 Support

If you encounter issues:
1. Check this guide
2. Verify your token is valid
3. Contact the repository administrator

## 🔒 Security Notes

- Keep your token secure
- Don't share it publicly
- Token will expire automatically
- Contact admin for token renewal
"""
        return guide
    
    def save_instructions(self, token_type: str = "fine-grained", output_file: str = None):
        """Save instructions to a file"""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"read_token_instructions_{token_type}_{timestamp}.md"
        
        instructions = self.generate_token_instructions(token_type)
        colleague_guide = self.generate_colleague_guide(token_type)
        
        with open(output_file, 'w') as f:
            f.write(instructions)
            f.write("\n\n" + "="*80 + "\n\n")
            f.write(colleague_guide)
        
        logger.info(f"✅ Instructions saved to: {output_file}")
        return output_file

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Generate read token instructions for colleagues")
    parser.add_argument(
        '--config', 
        default='registry_credentials.yaml',
        help='Path to credentials YAML file (default: registry_credentials.yaml)'
    )
    parser.add_argument(
        '--token-type',
        choices=['fine-grained', 'classic', 'deploy'],
        default='fine-grained',
        help='Type of token to generate instructions for (default: fine-grained)'
    )
    parser.add_argument(
        '--output',
        help='Output file name (default: auto-generated)'
    )
    parser.add_argument(
        '--show-only',
        action='store_true',
        help='Show instructions without saving to file'
    )
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = ReadTokenGenerator(args.config)
    
    if args.show_only:
        # Show instructions in terminal
        instructions = generator.generate_token_instructions(args.token_type)
        print(instructions)
    else:
        # Save instructions to file
        output_file = generator.save_instructions(args.token_type, args.output)
        print(f"\n📄 Instructions saved to: {output_file}")
        print("📤 Share this file with your colleagues!")

if __name__ == "__main__":
    main() 