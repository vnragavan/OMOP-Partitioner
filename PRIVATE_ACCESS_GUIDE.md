# 🔒 Private OMOP Partition Access - Quick Guide

## 📋 What You Need

To access private OMOP partition images, you need:

1. **GitHub username** (your own or shared)
2. **GitHub Personal Access Token** with `read:packages` permission
3. **Repository access** (added as collaborator)

## 🚀 Quick Start

### **Step 1: Get Access**
Ask the repository owner to:
- Add you as a collaborator to the repository
- Or provide you with shared credentials

### **Step 2: Create Access Token**
1. Go to [GitHub Settings → Tokens](https://github.com/settings/tokens)
2. Click "Generate new token (classic)"
3. Select scopes: ✅ `read:packages`
4. Set expiration (recommended: 90 days)
5. Copy the token (starts with `ghp_`)

### **Step 3: Login and Download**
```bash
# Login to GitHub Container Registry
docker login ghcr.io -u your_github_username -p your_github_token

# Download the images
docker pull ghcr.io/vnragavan/omop-partitions-partition-0:latest
docker pull ghcr.io/vnragavan/omop-partitions-partition-1:latest
```

## 🔧 Complete Workflow

### **Download and Run**
```bash
# 1. Login (one-time setup)
docker login ghcr.io -u your_username -p your_token

# 2. Pull images
docker pull ghcr.io/vnragavan/omop-partitions-partition-0:latest
docker pull ghcr.io/vnragavan/omop-partitions-partition-1:latest

# 3. Run containers
docker run -d -p 5433:5432 --name omop_partition_0_restored \
  ghcr.io/vnragavan/omop-partitions-partition-0:latest

docker run -d -p 5434:5432 --name omop_partition_1_restored \
  ghcr.io/vnragavan/omop-partitions-partition-1:latest

# 4. Connect to databases
psql postgresql://postgres:postgres@localhost:5433/omop
psql postgresql://postgres:postgres@localhost:5434/omop
```

## 🔐 Database Credentials

**Default credentials** (same for all partitions):
- **Username**: `postgres`
- **Password**: `postgres`
- **Database**: `omop`
- **Ports**: 5433 (partition 0), 5434 (partition 1), etc.

## 🛠️ Troubleshooting

### **Common Issues**

#### **"Not Found" Error**
```bash
# Error: manifest for ghcr.io/vnragavan/omop-partitions-partition-0:latest not found
# Solution: Check if you're logged in
docker login ghcr.io -u your_username -p your_token
```

#### **"Unauthorized" Error**
```bash
# Error: unauthorized: access to the requested resource is not authorized
# Solution: Check repository permissions and token scope
```

#### **"Permission Denied" Error**
```bash
# Error: permission denied for repository
# Solution: Ask repository owner to add you as collaborator
```

### **Verification Commands**
```bash
# Check if logged in
docker login ghcr.io

# Test access
docker pull ghcr.io/vnragavan/omop-partitions-partition-0:latest

# Check running containers
docker ps --filter "name=omop_partition"

# Check database connectivity
docker exec omop_partition_0_restored psql -U postgres -d omop -c "SELECT 1"
```

## 📊 Available Partitions

Based on the configuration, these partitions are available:
- **Partition 0**: Port 5433
- **Partition 1**: Port 5434

## 🔗 Useful URLs

- **Repository**: https://github.com/vnragavan/omop-partitions
- **Packages**: https://github.com/vnragavan/omop-partitions/packages
- **Token Settings**: https://github.com/settings/tokens

## 📞 Support

If you encounter issues:
1. Check this troubleshooting guide
2. Verify your GitHub token has `read:packages` permission
3. Ensure you have repository access
4. Contact the repository owner for assistance

## 🔒 Security Notes

- Keep your access token secure and don't share it publicly
- Set token expiration dates for security
- Use individual tokens rather than shared ones when possible
- Regularly rotate your access tokens 