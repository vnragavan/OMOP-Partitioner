# OMOP Database Partitioner

A comprehensive Python-based tool for automatically partitioning OMOP (Observational Medical Outcomes Partnership) databases across multiple PostgreSQL containers while maintaining schema integrity, constraints, and data relationships.

## 🚀 Features

### Core Functionality
- **Automated Schema Analysis**: Intelligently analyzes database schema and relationships using dependency graphs
- **Multi-Container Distribution**: Creates isolated Docker containers for each partition with PostgreSQL 16
- **Flexible Distribution Strategies**: Supports uniform, hash-based, and round-robin data distribution
- **Data Integrity Validation**: Comprehensive validation ensuring data consistency across partitions
- **Automatic Resource Management**: Docker container lifecycle management and cleanup

### Advanced Capabilities
- **Dependency Graph Visualization**: Generates DOT files and PNG images showing table relationships
- **Patient-Centric Partitioning**: Distributes patient records and all related data across partitions
- **Schema Compliance**: Maintains all constraints, primary keys, and foreign key relationships
- **Port Management**: Automatic port allocation and conflict resolution
- **Credential Management**: Secure random credential generation for each partition

### Monitoring & Analysis
- **Partition Analysis**: Detailed analysis of data distribution across partitions
- **Validation Reports**: Comprehensive validation of data integrity and record counts
- **Configuration Management**: YAML-based configuration tracking and management
- **Logging & Debugging**: Extensive logging for troubleshooting and monitoring

## 📋 Prerequisites

### System Requirements
- **Python**: 3.12+ (recommended) or 3.8+
- **Docker**: Latest version with Docker Compose support
- **PostgreSQL**: Version 16 (automatically installed by setup script)
- **Operating System**: macOS, Linux, or Windows
- **Memory**: Minimum 8GB RAM (16GB+ recommended for large datasets)
- **Storage**: 10GB+ available disk space

### Network Requirements
- Available ports starting from 5432 for PostgreSQL instances
- Docker network access for container communication

## 🛠️ Installation & Setup

### Quick Start (Recommended)

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd omop-partitioner
   ```

2. **Run the automated setup script**:
   ```bash
   python setup.py
   ```

The setup script will automatically:
- ✅ Check and install Python 3.12+ via Homebrew (macOS)
- ✅ Install PostgreSQL 16 and configure services
- ✅ Create and activate a Python virtual environment
- ✅ Install all required Python packages
- ✅ Create and configure the `.env` file
- ✅ Set up the OMOP database and user
- ✅ Validate all dependencies and connections

### Manual Installation

If you prefer manual setup:

1. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Create environment configuration**:
   ```bash
   cp .env.example .env
   # Edit .env with your database configuration
   ```

3. **Install PostgreSQL 16**:
   ```bash
   # macOS
   brew install postgresql@16
   brew services start postgresql@16
   
   # Ubuntu/Debian
   sudo apt-get install postgresql-16
   
   # Windows
   # Download from https://www.postgresql.org/download/windows/
   ```

## ⚙️ Configuration

### Environment Variables (.env file)

```bash
# Database Configuration
SOURCE_DB_URL=postgresql://postgres:postgres@localhost:5432/omop_db
POSTGRES_VERSION=16
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=omop_db

# Partitioning Configuration
NUM_PARTITIONS=2
DISTRIBUTION_STRATEGY=uniform  # Options: uniform, hash, round_robin

# Docker Configuration
DOCKER_NETWORK=omop_network

# Output Configuration
CONFIG_FILE=partitions_config.yaml
```

### Distribution Strategies

1. **Uniform Distribution** (`uniform`): Distributes data evenly across partitions
2. **Hash Distribution** (`hash`): Uses hash-based distribution for consistent partitioning
3. **Round-Robin Distribution** (`round_robin`): Cycles through partitions sequentially

### Changing Number of Partitions

You can change the number of partitions in several ways:

1. **Edit the .env file** (recommended):
   ```bash
   # Change NUM_PARTITIONS=2 to your desired number
   NUM_PARTITIONS=4  # For 4 partitions
   NUM_PARTITIONS=8  # For 8 partitions
   ```

2. **Set environment variable before running**:
   ```bash
   export NUM_PARTITIONS=2
   python omop_partitioner.py
   ```

3. **Command line override** (if supported):
   ```bash
   python omop_partitioner.py --partitions 2
   ```

**Note**: The setup script defaults to 2 partitions. If you want a different number, edit the `.env` file after running the setup script.

## 🚀 Usage

### Basic Partitioning

```bash
# Run the main partitioner
python omop_partitioner.py
```

### Advanced Usage

```bash
# Analyze existing partitions
python analyze_partitions.py

# Validate partition integrity
python validate_partitions.py

# Show partition information
python show_partitions.py

# Clean up resources
python cleanup.py

# Package and upload containers to GitHub Container Registry
python package_and_upload.py
```

### Command Line Options

The main partitioner supports several options:

```bash
python omop_partitioner.py --help
python omop_partitioner.py --partitions 8 --strategy hash
python omop_partitioner.py --validate-only
```

## 📊 Output & Results

### Generated Files

The partitioner creates several output files:

```
output/
├── source_graph.dot              # Main source graph
├── source_graph.png              # Visual representation
├── partition_0_graph.dot         # Partition 0 dependency graph
├── partition_0_graph.png         # Partition 0 visualization
├── partition_1_graph.dot         # Partition 1 dependency graph
├── partition_1_graph.png         # Partition 1 visualization
└── ...                          # Additional partitions

config/
└── partition_config_YYYYMMDD_HHMMSS.yaml  # Configuration snapshot
```

### Partition Structure

Each partition container includes:
- **Isolated PostgreSQL 16 instance** on unique port
- **Complete schema copy** with all constraints
- **Subset of patient data** and related records
- **Secure credentials** (automatically generated)
- **Docker network isolation**

### Example Partition Configuration

```yaml
partitions:
- container:
    name: omop_partition_0
    status: running
    port: '5434'
  database:
    name: omop_partition_0
    username: omop_partition_0_Xm9DRWoi
    password: yCSNMLW35YckEZCe
    connection_string: postgresql://omop_partition_0_Xm9DRWoi:yCSNMLW35YckEZCe@localhost:5434/omop_partition_0
  postgres:
    version: '16'
    port: '5434'
    host: localhost
```

## 🔍 Validation & Monitoring

### Data Integrity Checks

The partitioner performs comprehensive validation:

- **Record Count Validation**: Ensures total records match source
- **Schema Compliance**: Validates table structures and constraints
- **Data Sampling**: Compares sample data across partitions
- **Relationship Integrity**: Verifies foreign key relationships
- **Performance Metrics**: Monitors distribution efficiency

### Monitoring Commands

```bash
# Check partition status
docker ps --filter "name=omop_partition"

# View partition logs
docker logs omop_partition_0

# Monitor resource usage
docker stats omop_partition_0 omop_partition_1 omop_partition_2 omop_partition_3
```

## 🛡️ Security Features

- **Isolated Containers**: Each partition runs in its own Docker container
- **Random Credentials**: Unique username/password for each partition
- **Network Isolation**: Docker network isolation between partitions
- **Secure Connections**: SSL-ready PostgreSQL configurations
- **Credential Management**: Automatic credential generation and tracking

## 🔧 Troubleshooting

### Common Issues

1. **Port Conflicts**:
   ```bash
   # Check port usage
   lsof -i :5432
   # Kill conflicting processes
   sudo pkill -f postgres
   ```

2. **Docker Issues**:
   ```bash
   # Restart Docker
   sudo systemctl restart docker
   # Clean up containers
   docker system prune -a
   ```

3. **PostgreSQL Connection Issues**:
   ```bash
   # Check PostgreSQL status
   brew services list | grep postgresql
   # Restart PostgreSQL
   brew services restart postgresql@16
   ```

### Debug Mode

Enable detailed logging:

```bash
export LOG_LEVEL=DEBUG
python omop_partitioner.py
```

## 📈 Performance Considerations

### Optimization Tips

- **Memory**: Allocate sufficient RAM for large datasets
- **Storage**: Use SSD storage for better I/O performance
- **Network**: Ensure stable network for Docker operations
- **Partition Count**: Balance between parallelism and resource usage

### Scaling Guidelines

- **Small Datasets** (< 1GB): 2-4 partitions
- **Medium Datasets** (1-10GB): 4-8 partitions
- **Large Datasets** (> 10GB): 8-16 partitions

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:

1. Check the troubleshooting section
2. Review the logs in the `output/` directory
3. Open an issue on GitHub
4. Check the configuration files for errors

## 🔄 Version History

- **v1.0.0**: Initial release with basic partitioning
- **v1.1.0**: Added distribution strategies and validation
- **v1.2.0**: Enhanced setup automation and PostgreSQL 16 support
- **v1.3.0**: Added visualization and monitoring capabilities

---

**Note**: This tool is designed for research and development purposes. Always backup your data before running the partitioner on production databases.

### Container Packaging and Upload

The partitioner includes a script to package running partition containers into Docker images and upload them to GitHub Container Registry:

```bash
# Create template credentials file
python package_and_upload.py --create-template

# Edit the credentials file with your GitHub details
nano registry_credentials.yaml

# Package and upload all partition containers
python package_and_upload.py

# Keep local images after uploading
python package_and_upload.py --no-cleanup

# Skip saving configuration files
python package_and_upload.py --no-configs

# Use custom credentials file
python package_and_upload.py --config my_credentials.yaml
```

#### How Container Identification Works

The system automatically identifies partition containers using:

1. **Container Naming Convention**: `omop_partition_X` where X is the partition number
2. **Docker Filter**: Searches for containers with names starting with `omop_partition`
3. **Partition Number Extraction**: Extracts partition number from container name suffix
4. **Database Configuration Extraction**: Automatically extracts PostgreSQL configuration from running containers

#### Separate Images for Each Partition

Each partition is packaged as a **separate Docker image**:

- **Individual Images**: `ghcr.io/username/omop-partitions-partition-0:latest`
- **Individual Images**: `ghcr.io/username/omop-partitions-partition-1:latest`
- **Manifest**: `ghcr.io/username/omop-partitions:manifest` (for all partitions)

#### Database Configuration Files

The packaging process automatically generates configuration files for each partition:

```
config/
├── partition_0_config.yaml      # Partition 0 configuration
├── partition_1_config.yaml      # Partition 1 configuration
├── all_partitions_config.yaml   # Combined configuration
└── ...
```

Each configuration file includes:
- **Container Information**: Name, ID, partition number, status
- **Database Configuration**: Connection strings, ports, credentials
- **Image Information**: Registry URL, uploaded image name
- **Access Commands**: Pull, run, and connect commands

#### GitHub Container Registry Setup

1. **Create Personal Access Token**:
   - Go to GitHub Settings → Developer settings → Personal access tokens
   - Generate a new token with `write:packages` permission

2. **Configure Credentials**:
   ```yaml
   # registry_credentials.yaml
   github_username: your_github_username
   github_token: your_github_personal_access_token
   registry_namespace: your_github_username
   repository_name: omop-partitions
   image_tag: latest
   ```

3. **Upload Images**:
   - Images will be uploaded as: `ghcr.io/username/omop-partitions-partition-X:latest`
   - A manifest will be created for all partitions: `ghcr.io/username/omop-partitions:manifest`

#### Package Visibility and Permissions

The uploaded images inherit permissions from your GitHub repository settings, but you can also control them explicitly:

##### **Default Behavior**
- **Private Repository**: Images require authentication to download
- **Public Repository**: Images are publicly accessible

##### **Controlling Package Visibility**

```bash
# Upload with private visibility (default)
python package_and_upload.py

# Upload with public visibility
python package_and_upload.py --visibility public

# Set visibility for existing packages only
python package_and_upload.py --set-visibility-only --visibility public

# Upload with configs and public visibility
python package_and_upload.py --upload-configs --visibility public
```

##### **Access Control Options**

1. **Private Images** (default):
   ```bash
   # Requires authentication
   docker login ghcr.io -u your_username -p your_github_token
   docker pull ghcr.io/username/omop-partitions-partition-0:latest
   ```

2. **Public Images**:
   ```bash
   # No authentication required
   docker pull ghcr.io/username/omop-partitions-partition-0:latest
   ```

##### **Repository-Level Control**
You can also control permissions at the repository level:
- **GitHub Repository Settings** → **General** → **Danger Zone** → **Change repository visibility**

##### **Package-Level Control**
Individual packages can have different visibility settings:
```bash
# Make specific package public
gh api repos/:owner/:repo/packages/container/omop-partitions-partition-0/visibility \
  --method POST --field visibility=public

# Make specific package private
gh api repos/:owner/:repo/packages/container/omop-partitions-partition-0/visibility \
  --method POST --field visibility=private
```

#### Restoring and Using Uploaded Images

Use the restoration script to pull and run uploaded partitions:

```bash
# List available partitions
python restore_partitions.py --list

# Restore all partitions
python restore_partitions.py

# Restore specific partition
python restore_partitions.py --partition 0

# Restore with custom port
python restore_partitions.py --partition 0 --port 5435

# Clean up restored containers
python restore_partitions.py --cleanup
```

#### Pulling Uploaded Images Manually

```bash
# Pull a specific partition
docker pull ghcr.io/username/omop-partitions-partition-0:latest

# Pull all partitions using manifest
docker pull ghcr.io/username/omop-partitions:manifest

# Run a specific partition
docker run -d -p 5434:5432 --name omop_partition_0_restored \
  ghcr.io/username/omop-partitions-partition-0:latest
```

### Database Credential Management

The system includes a credential extractor script to help end users access database credentials from configuration files:

#### Extracting Credentials

```bash
# Extract credentials from a single partition config
python extract_credentials.py config/partition_0_config.yaml

# Extract credentials from all partitions
python extract_credentials.py config/all_partitions_config.yaml --all-partitions

# Generate connection scripts
python extract_credentials.py config/partition_0_config.yaml --generate-script

# Generate environment files
python extract_credentials.py config/partition_0_config.yaml --generate-env

# Extract all credentials and generate scripts for all partitions
python extract_credentials.py config/all_partitions_config.yaml --all-partitions --generate-script --generate-env
```

#### Default Credentials

All partitions use the same default credentials for simplicity:
- **Username**: `postgres`
- **Password**: `postgres`
- **Database**: `omop`
- **Port**: Varies by partition (5433, 5434, etc.)

#### Security Considerations

⚠️ **Important**: The default credentials are **not secure for production use**:

1. **Change passwords immediately** after first login
2. **Use environment variables** for credential management
3. **Implement proper authentication** mechanisms
4. **Restrict network access** to database ports

#### Example Output

```bash
$ python extract_credentials.py config/partition_0_config.yaml

🔐 Database Credentials for omop_partition_0
==================================================
Username:     postgres
Password:     postgres
Database:     omop
Host:         localhost
Port:         5433
Connection:   postgresql://postgres:postgres@localhost:5433/omop

⚠️  SECURITY WARNING
==================================================
These are DEFAULT credentials and are NOT secure for production use!

🔒 Security Recommendations:
1. Change passwords immediately after first login
2. Use environment variables for credential management
3. Implement proper authentication mechanisms
4. Restrict network access to database ports
5. Use read-only users for analytics workloads
6. Enable SSL/TLS connections
7. Set up monitoring and logging
8. Regular security audits
```

#### Generated Files

The credential extractor can generate:

1. **Connection Scripts** (`connect_omop_partition_0.sh`):
   ```bash
   #!/bin/bash
   # Connection script for omop_partition_0
   psql "postgresql://postgres:postgres@localhost:5433/omop"
   ```

2. **Environment Files** (`omop_partition_0.env`):
   ```bash
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=postgres
   POSTGRES_DB=omop
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5433
   DATABASE_URL=postgresql://postgres:postgres@localhost:5433/omop
   ```

#### Using Generated Files

```bash
# Make connection script executable
chmod +x connect_omop_partition_0.sh

# Run connection script
./connect_omop_partition_0.sh

# Use environment file with Docker
docker run -d -p 5433:5432 \
  --env-file omop_partition_0.env \
  --name omop_partition_0_restored \
  ghcr.io/username/omop-partitions-partition-0:latest
```

##### **Checking Package Visibility**

You can verify package visibility through the browser or using the provided script:

###### **Browser Verification**:
1. **Repository Packages Page**: `https://github.com/username/repo-name/packages`
2. **Individual Package Page**: `https://github.com/username/repo-name/packages/container/package-name`
3. **Look for visibility badges**: 🔓 Public or 🔒 Private

###### **Script Verification**:
```bash
# Check all partition packages
python check_package_visibility.py

# Check specific package
python check_package_visibility.py --package omop-partitions-partition-0

# Open browser automatically
python check_package_visibility.py --open-browser

# Use custom credentials file
python check_package_visibility.py --config my_credentials.yaml
```

###### **Example Output**:
```
🔍 OMOP PARTITION PACKAGE VISIBILITY REPORT
================================================================================
📦 Found 2 partition package(s)
🏠 Repository: username/omop-partitions
🔗 Repository URL: https://github.com/username/omop-partitions

🔒 omop-partitions-partition-0
   Visibility: PRIVATE
   Browser URL: https://github.com/username/omop-partitions/packages/container/omop-partitions-partition-0
   Created: 2025-06-20T10:44:20Z
   Updated: 2025-06-20T10:44:20Z

🔓 omop-partitions-partition-1
   Visibility: PUBLIC
   Browser URL: https://github.com/username/omop-partitions/packages/container/omop-partitions-partition-1
   Created: 2025-06-20T10:44:22Z
   Updated: 2025-06-20T10:44:22Z

📊 SUMMARY
----------------------------------------
🔓 Public packages: 1
🔒 Private packages: 1
📦 Total packages: 2
``` 