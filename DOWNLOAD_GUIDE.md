# OMOP Partition Images - Download and Usage Guide

## 📋 Overview

This guide explains how to download OMOP partition images from GitHub Container Registry and access the databases running inside the containers.

## 🔐 Credential Management and Security

### **Default Credentials**
All partitions use the same default credentials for simplicity:
- **Username**: `postgres`
- **Password**: `postgres`
- **Database**: `omop`

### **⚠️ Security Warning**
The default credentials are **hardcoded** and **not secure for production use**. For production environments:

1. **Change passwords immediately** after first login
2. **Use environment variables** for credential management
3. **Implement proper authentication** mechanisms
4. **Restrict network access** to database ports

### **Changing Default Credentials**

#### **Method 1: Container Environment Variables**
```bash
# Run with custom credentials
docker run -d -p 5433:5432 \
  -e POSTGRES_USER=myuser \
  -e POSTGRES_PASSWORD=mypassword \
  -e POSTGRES_DB=omop \
  --name omop_partition_0_restored \
  ghcr.io/username/omop-partitions-partition-0:latest
```

#### **Method 2: PostgreSQL ALTER USER**
```sql
-- Connect to database
psql postgresql://postgres:postgres@localhost:5433/omop

-- Change password
ALTER USER postgres PASSWORD 'new_secure_password';

-- Create new user with limited privileges
CREATE USER readonly_user WITH PASSWORD 'readonly_password';
GRANT CONNECT ON DATABASE omop TO readonly_user;
GRANT USAGE ON SCHEMA omopcdm TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA omopcdm TO readonly_user;
```

#### **Method 3: Using .env Files**
```bash
# Create .env file
cat > .env << EOF
POSTGRES_USER=myuser
POSTGRES_PASSWORD=mypassword
POSTGRES_DB=omop
EOF

# Run with .env file
docker run -d -p 5433:5432 \
  --env-file .env \
  --name omop_partition_0_restored \
  ghcr.io/username/omop-partitions-partition-0:latest
```

### **Production Security Checklist**
- [ ] Change default passwords
- [ ] Use strong, unique passwords
- [ ] Implement SSL/TLS connections
- [ ] Restrict network access (firewall rules)
- [ ] Use read-only users for analytics
- [ ] Implement connection pooling
- [ ] Set up monitoring and logging
- [ ] Regular security audits

## 🔐 Required Credentials

### **For Public Images (No Credentials Required)**
If the images are in a **public repository**, you can download them without any credentials:

```bash
# No login required
docker pull ghcr.io/username/omop-partitions-partition-0:latest
docker pull ghcr.io/username/omop-partitions-partition-1:latest
```

### **For Private Images (Credentials Required)**
If the images are in a **private repository**, you need:

1. **GitHub Personal Access Token** with `read:packages` permission
2. **GitHub username**

```bash
# Login to GitHub Container Registry
docker login ghcr.io -u your_github_username -p your_github_token

# Then pull images
docker pull ghcr.io/username/omop-partitions-partition-0:latest
```

### **Sharing Private Images with Colleagues**

#### **Option 1: Individual Access (Recommended)**
Each colleague needs their own GitHub account and access token:

1. **Add colleague to repository**:
   - Go to repository → Settings → Collaborators
   - Add colleague's GitHub username
   - Grant "Read" access

2. **Colleague creates access token**:
   - Go to [GitHub Settings → Tokens](https://github.com/settings/tokens)
   - Click "Generate new token (classic)"
   - Select scopes: ✅ `read:packages`
   - Copy token (starts with `ghp_`)

3. **Colleague downloads images**:
   ```bash
   # Login with their credentials
   docker login ghcr.io -u colleague_username -p colleague_token
   
   # Pull images
   docker pull ghcr.io/username/omop-partitions-partition-0:latest
   ```

#### **Option 2: Shared Access Token (Less Secure)**
Create a shared token for the team:

1. **Create team access token**:
   - Go to [GitHub Settings → Tokens](https://github.com/settings/tokens)
   - Click "Generate new token (classic)"
   - Select scopes: ✅ `read:packages`
   - Set expiration (recommended: 90 days)
   - Copy token

2. **Share credentials securely**:
   ```bash
   # Share these details with colleagues via secure channel
   GITHUB_USERNAME=your_username
   GITHUB_TOKEN=ghp_your_shared_token
   REGISTRY_URL=ghcr.io
   ```

3. **Colleagues use shared token**:
   ```bash
   # Login with shared credentials
   docker login ghcr.io -u $GITHUB_USERNAME -p $GITHUB_TOKEN
   
   # Pull images
   docker pull ghcr.io/username/omop-partitions-partition-0:latest
   ```

#### **Option 3: Organization Access**
If using GitHub Organizations:

1. **Add colleague to organization**:
   - Go to organization → People
   - Invite colleague
   - Grant appropriate role (Member/Admin)

2. **Grant repository access**:
   - Go to organization → Teams
   - Add colleague to team with repository access
   - Or grant direct repository access

### **Creating GitHub Personal Access Token**

1. Go to [GitHub Settings → Tokens](https://github.com/settings/tokens)
2. Click "Generate new token (classic)"
3. Select scopes:
   - ✅ `read:packages` (required for downloading images)
   - ✅ `repo` (if repository is private)
4. Set expiration (recommended: 90 days for security)
5. Copy the token (starts with `ghp_`)

### **Security Best Practices for Private Images**

#### **✅ Recommended**:
- Use individual access tokens for each colleague
- Set token expiration dates
- Grant minimal required permissions
- Use secure channels to share credentials
- Regularly rotate access tokens

#### **⚠️ Avoid**:
- Sharing personal access tokens publicly
- Using tokens with excessive permissions
- Storing tokens in plain text files
- Using long-lived tokens without expiration

### **Troubleshooting Private Access**

#### **Common Issues**:

1. **"Not Found" Error**:
   ```bash
   # Error: manifest for ghcr.io/username/omop-partitions-partition-0:latest not found
   # Solution: Check if you're logged in and have access
   docker login ghcr.io -u your_username -p your_token
   ```

2. **"Unauthorized" Error**:
   ```bash
   # Error: unauthorized: access to the requested resource is not authorized
   # Solution: Check repository permissions and token scope
   ```

3. **"Permission Denied" Error**:
   ```bash
   # Error: permission denied for repository
   # Solution: Ensure colleague has repository access
   ```

#### **Verification Commands**:
```bash
# Check if logged in
docker login ghcr.io

# Test access to specific image
docker pull ghcr.io/username/omop-partitions-partition-0:latest

# Check available images
docker images | grep omop-partitions
```

## 📦 Downloading Images

### **Method 1: Individual Images**
```bash
# Download specific partitions
docker pull ghcr.io/username/omop-partitions-partition-0:latest
docker pull ghcr.io/username/omop-partitions-partition-1:latest
docker pull ghcr.io/username/omop-partitions-partition-2:latest
```

### **Method 2: Using Manifest (All Partitions)**
```bash
# Download all partitions at once
docker pull ghcr.io/username/omop-partitions:manifest
```

### **Method 3: Using Configuration Files**
If configuration files are available:

```bash
# Download config package from GitHub releases
wget https://github.com/username/omop-partitions/releases/latest/download/omop_partitions_config.zip

# Extract configuration files
unzip omop_partitions_config.zip

# Use the provided pull commands
cat partition_0_config.yaml | grep pull_command
```

## 🗄️ Accessing Database Internals

### **Step 1: Run the Containers**

#### **Using Configuration Files (Recommended)**
```bash
# Run partition 0
docker run -d -p 5433:5432 --name omop_partition_0_restored \
  ghcr.io/username/omop-partitions-partition-0:latest

# Run partition 1
docker run -d -p 5434:5432 --name omop_partition_1_restored \
  ghcr.io/username/omop-partitions-partition-1:latest
```

#### **Using Configuration Commands**
```bash
# Extract and run the provided commands
cat partition_0_config.yaml | grep run_command
# This will show: docker run -d -p 5433:5432 --name omop_partition_0_restored ghcr.io/username/omop-partitions-partition-0:latest
```

### **Step 2: Connect to Databases**

#### **Using psql Command Line**
```bash
# Connect to partition 0
psql postgresql://postgres:postgres@localhost:5433/omop

# Connect to partition 1
psql postgresql://postgres:postgres@localhost:5434/omop
```

#### **Using Configuration Connection Strings**
```bash
# Extract connection string from config
CONNECTION_STRING=$(grep "CONNECTION_STRING" partition_0_config.yaml | cut -d' ' -f2)
psql "$CONNECTION_STRING"
```

### **Step 3: Query the Databases**

#### **Basic Queries**
```sql
-- Check database version
SELECT version();

-- List all tables
\dt omopcdm.*

-- Count records in person table
SELECT COUNT(*) FROM omopcdm.person;

-- Check partition distribution
SELECT 
    COUNT(*) as person_count,
    MIN(person_id) as min_id,
    MAX(person_id) as max_id
FROM omopcdm.person;
```

#### **Advanced Queries**
```sql
-- Find patients with specific conditions
SELECT 
    p.person_id,
    p.gender_concept_id,
    c.concept_name as condition_name
FROM omopcdm.person p
JOIN omopcdm.condition_occurrence co ON p.person_id = co.person_id
JOIN omopcdm.concept c ON co.condition_concept_id = c.concept_id
WHERE c.concept_name ILIKE '%diabetes%'
LIMIT 10;
```

## 🔧 Programmatic Access

### **Python Example**
```python
import psycopg2
import yaml

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
print(f"Person count in partition 0: {count}")

# Get sample data
cursor.execute("""
    SELECT person_id, gender_concept_id, birth_datetime 
    FROM omopcdm.person 
    LIMIT 5
""")
for row in cursor.fetchall():
    print(f"Person {row[0]}: Gender {row[1]}, Born {row[2]}")

conn.close()
```

### **R Example**
```r
library(DBI)
library(RPostgres)

# Connect to database
con <- dbConnect(Postgres(),
                 host = "localhost",
                 port = 5433,
                 dbname = "omop",
                 user = "postgres",
                 password = "postgres")

# Query the database
person_count <- dbGetQuery(con, "SELECT COUNT(*) FROM omopcdm.person")
print(paste("Person count:", person_count))

# Get sample data
sample_data <- dbGetQuery(con, "
    SELECT person_id, gender_concept_id, birth_datetime 
    FROM omopcdm.person 
    LIMIT 5
")
print(sample_data)

dbDisconnect(con)
```

## 📊 Database Schema Information

### **OMOP CDM Tables Available**
Each partition contains the complete OMOP CDM schema:

- **Core Tables**: `person`, `observation_period`, `visit_occurrence`, `condition_occurrence`, `drug_exposure`, `procedure_occurrence`, `measurement`, `observation`, `death`
- **Vocabulary Tables**: `concept`, `concept_relationship`, `concept_ancestor`, `vocabulary`, `domain`, `concept_class`, `relationship`
- **Metadata Tables**: `cdm_source`, `metadata`
- **Additional Tables**: `note`, `note_nlp`, `specimen`, `fact_relationship`, `location`, `care_site`, `provider`, `cost`, `payer_plan_period`, `drug_era`, `dose_era`, `condition_era`, `episode`, `episode_event`

### **Data Distribution**
- **Partition 0**: Contains subset of patient data (person_id % num_partitions = 0)
- **Partition 1**: Contains subset of patient data (person_id % num_partitions = 1)
- **Partition N**: Contains subset of patient data (person_id % num_partitions = N)

## 🔐 Default Credentials

### **Database Credentials**
- **Username**: `postgres`
- **Password**: `postgres`
- **Database**: `omop`
- **Port**: Varies by partition (5433, 5434, etc.)

### **Container Credentials**
- **Container User**: `postgres`
- **Container Password**: `postgres`
- **Container Database**: `omop`

## 🛠️ Troubleshooting

### **Common Issues**

#### **1. Permission Denied**
```bash
# Error: permission denied for table person
# Solution: Check if you're connected to the right database
\c omop
```

#### **2. Connection Refused**
```bash
# Error: connection to server at "localhost" (127.0.0.1), port 5433 failed
# Solution: Check if container is running
docker ps --filter "name=omop_partition"

# If not running, start it
docker start omop_partition_0_restored
```

#### **3. Port Already in Use**
```bash
# Error: bind: address already in use
# Solution: Use different port
docker run -d -p 5435:5432 --name omop_partition_0_restored \
  ghcr.io/username/omop-partitions-partition-0:latest
```

#### **4. Image Not Found**
```bash
# Error: manifest for ghcr.io/username/omop-partitions-partition-0:latest not found
# Solution: Check image name and login to registry
docker login ghcr.io -u your_username -p your_token
docker pull ghcr.io/username/omop-partitions-partition-0:latest
```

### **Debugging Commands**
```bash
# Check container status
docker ps -a --filter "name=omop_partition"

# Check container logs
docker logs omop_partition_0_restored

# Check container environment
docker exec omop_partition_0_restored env | grep POSTGRES

# Check database connectivity
docker exec omop_partition_0_restored psql -U postgres -d omop -c "SELECT 1"
```

## 📈 Performance Tips

### **Resource Allocation**
```bash
# Run with memory limits
docker run -d -p 5433:5432 --memory=4g --name omop_partition_0_restored \
  ghcr.io/username/omop-partitions-partition-0:latest

# Run with CPU limits
docker run -d -p 5433:5432 --cpus=2 --name omop_partition_0_restored \
  ghcr.io/username/omop-partitions-partition-0:latest
```

### **Database Optimization**
```sql
-- Create indexes for better performance
CREATE INDEX idx_person_id ON omopcdm.person(person_id);
CREATE INDEX idx_condition_person ON omopcdm.condition_occurrence(person_id);
CREATE INDEX idx_drug_person ON omopcdm.drug_exposure(person_id);

-- Analyze tables for query optimization
ANALYZE omopcdm.person;
ANALYZE omopcdm.condition_occurrence;
```

## 🔄 Data Updates

### **Adding New Data**
```sql
-- Insert new person records
INSERT INTO omopcdm.person (person_id, gender_concept_id, birth_datetime)
VALUES (1000001, 8507, '1990-01-01');

-- Insert related records
INSERT INTO omopcdm.condition_occurrence (condition_occurrence_id, person_id, condition_concept_id, condition_start_date)
VALUES (1000001, 1000001, 201820, '2023-01-01');
```

### **Backup and Restore**
```bash
# Backup database
docker exec omop_partition_0_restored pg_dump -U postgres omop > backup_partition_0.sql

# Restore database
docker exec -i omop_partition_0_restored psql -U postgres omop < backup_partition_0.sql
```

## 📞 Support

For issues and questions:
1. Check the troubleshooting section above
2. Review container logs: `docker logs omop_partition_0_restored`
3. Check database connectivity: `docker exec omop_partition_0_restored psql -U postgres -d omop -c "SELECT version()"`
4. Refer to the main repository documentation
5. Open an issue on the GitHub repository

## 📋 Summary

1. **Download Images**: Use `docker pull` with appropriate credentials
2. **Run Containers**: Use `docker run` with port mapping
3. **Connect to Databases**: Use `psql` or programmatic connections
4. **Query Data**: Use standard SQL queries on OMOP CDM tables
5. **Monitor Performance**: Use Docker and PostgreSQL monitoring tools

The configuration files provide all the necessary information for accessing the databases, including connection strings, port mappings, and access commands. 