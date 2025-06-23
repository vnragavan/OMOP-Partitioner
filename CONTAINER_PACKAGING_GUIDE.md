# OMOP Partitioner - Container Packaging Guide

## 📋 Overview

This guide explains how the OMOP Partitioner creates separate Docker images for each partition and uploads them to GitHub Container Registry with complete database configuration.

## 🔍 Container Identification

### How the System Identifies Partition Containers

The system uses a **smart container identification mechanism**:

1. **Naming Convention**: All partition containers follow the pattern `omop_partition_X`
   - `omop_partition_0` (Partition 0)
   - `omop_partition_1` (Partition 1)
   - `omop_partition_2` (Partition 2)
   - etc.

2. **Docker Filter**: Uses Docker API filter to find containers:
   ```python
   containers = docker_client.containers.list(
       filters={"name": "omop_partition"}
   )
   ```

3. **Partition Number Extraction**: Extracts partition number from container name:
   ```python
   partition_num = container.name.split('_')[-1]  # Gets "0", "1", "2", etc.
   ```

4. **Database Configuration Extraction**: Automatically extracts PostgreSQL configuration from running containers

## 📦 Separate Images for Each Partition

### Yes, it creates separate images for each partition!

Each partition becomes its own Docker image:

```
ghcr.io/username/omop-partitions-partition-0:latest
ghcr.io/username/omop-partitions-partition-1:latest
ghcr.io/username/omop-partitions-partition-2:latest
ghcr.io/username/omop-partitions-partition-3:latest
```

### Why Separate Images?

1. **Independent Deployment**: Each partition can be deployed independently
2. **Selective Restoration**: Restore only the partitions you need
3. **Resource Optimization**: Pull only required partitions
4. **Version Control**: Each partition can have different versions
5. **Parallel Processing**: Multiple partitions can be processed simultaneously

## 🗄️ Database Configuration Extraction

### What Configuration is Extracted?

The system automatically extracts **complete database configuration** from each running container:

```yaml
database:
  POSTGRES_USER: postgres
  POSTGRES_PASSWORD: postgres
  POSTGRES_DB: omop
  HOST_PORT: "5434"
  CONTAINER_IP: "172.17.0.2"
  CONNECTION_STRING: "postgresql://postgres:postgres@localhost:5434/omop"
  CONTAINER_CONNECTION_STRING: "postgresql://postgres:postgres@172.17.0.2:5432/omop"
```

### Configuration Sources

1. **Environment Variables**: Extracted from container's `POSTGRES_*` environment variables
2. **Port Mapping**: Extracted from container's port bindings
3. **Network Configuration**: Extracted from container's network settings
4. **Connection Strings**: Automatically generated for both host and container access

## 📁 Generated Configuration Files

### Individual Partition Configs

Each partition gets its own configuration file:

```yaml
# config/partition_0_config.yaml
container:
  name: omop_partition_0
  id: abc123def456
  partition_number: "0"
  status: running
  created: "2025-01-10T12:00:00Z"

database:
  POSTGRES_USER: postgres
  POSTGRES_PASSWORD: postgres
  POSTGRES_DB: omop
  HOST_PORT: "5434"
  CONNECTION_STRING: "postgresql://postgres:postgres@localhost:5434/omop"

image_info:
  name: postgres:16
  registry_url: ghcr.io
  uploaded_image: "ghcr.io/username/omop-partitions-partition-0:latest"

access_info:
  pull_command: "docker pull ghcr.io/username/omop-partitions-partition-0:latest"
  run_command: "docker run -d -p 5434:5432 --name omop_partition_0_restored ghcr.io/username/omop-partitions-partition-0:latest"
  connect_command: "psql postgresql://postgres:postgres@localhost:5434/omop"
```

### Combined Configuration

All partitions are combined into a single file:

```yaml
# config/all_partitions_config.yaml
metadata:
  created_at: "2025-01-10T12:00:00Z"
  total_partitions: 2
  registry_namespace: username
  repository_name: omop-partitions

partitions:
  - # partition_0_config.yaml content
  - # partition_1_config.yaml content
```

## 🚀 Complete Workflow

### Step 1: Create Partitions
```bash
# Run the partitioner to create containers
python omop_partitioner.py
```

### Step 2: Package and Upload
```bash
# Package containers into images and upload to registry
python package_and_upload.py
```

This creates:
- ✅ Separate Docker images for each partition
- ✅ Configuration files for database access
- ✅ Uploaded images to GitHub Container Registry
- ✅ Manifest for easy pulling

### Step 3: Restore and Use
```bash
# Restore all partitions
python restore_partitions.py

# Or restore specific partition
python restore_partitions.py --partition 0
```

## 🔧 Database Access Examples

### Using Configuration Files

```python
import yaml

# Load partition configuration
with open('config/partition_0_config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Get database connection string
connection_string = config['database']['CONNECTION_STRING']
print(f"Connect to: {connection_string}")
```

### Direct Database Access

```bash
# Connect to partition 0 database
psql postgresql://postgres:postgres@localhost:5434/omop

# Connect to partition 1 database  
psql postgresql://postgres:postgres@localhost:5435/omop
```

### Programmatic Access

```python
import psycopg2

# Connect using configuration
conn = psycopg2.connect(
    host="localhost",
    port=5434,
    database="omop",
    user="postgres",
    password="postgres"
)

# Query the database
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM omopcdm.person")
count = cursor.fetchone()[0]
print(f"Person count: {count}")
```

## 🛡️ Security Considerations

### Credential Management
- ✅ **No hardcoded passwords** in images
- ✅ **Environment variables** for database credentials
- ✅ **Secure token handling** for registry access
- ✅ **Configuration files** with proper permissions

### Access Control
- ✅ **Individual partition access** (pull specific partitions)
- ✅ **Network isolation** (each partition on different port)
- ✅ **Container isolation** (separate containers for each partition)

## 📊 Monitoring and Validation

### Container Status
```bash
# Check running partitions
docker ps --filter "name=omop_partition"

# Check restored partitions
docker ps --filter "name=omop_partition.*_restored"
```

### Database Validation
```bash
# Test database connections
python restore_partitions.py --list

# Validate data integrity
python validate_partitions.py
```

## 🔄 Advanced Usage

### Custom Port Mapping
```bash
# Restore partition 0 on custom port
python restore_partitions.py --partition 0 --port 5436
```

### Selective Restoration
```bash
# Restore only specific partitions
python restore_partitions.py --partition 0
python restore_partitions.py --partition 1
```

### Cleanup Operations
```bash
# Clean up restored containers
python restore_partitions.py --cleanup

# Clean up original containers
python cleanup.py
```

## ❓ Frequently Asked Questions

### Q: How does it identify the correct containers?
A: Uses Docker naming convention (`omop_partition_X`) and Docker API filters to find partition containers.

### Q: Are separate images created for each partition?
A: Yes, each partition becomes its own Docker image with unique name and tag.

### Q: What about database configuration?
A: Automatically extracts PostgreSQL configuration from running containers and generates connection strings.

### Q: How do I access the databases?
A: Use the generated configuration files or connection strings to connect to each partition's database.

### Q: Can I restore individual partitions?
A: Yes, you can restore specific partitions using the partition number.

### Q: Are the images secure?
A: Yes, credentials are handled via environment variables, not hardcoded in images. 