# 🚀 Programmatic Deployment Guide

This guide explains how to deploy the Medallion Architecture to Microsoft Fabric programmatically using automation scripts and CI/CD pipelines.

---

## 📋 Table of Contents

- [Deployment Options](#deployment-options)
- [Prerequisites](#prerequisites)
- [Option 1: Python Script (Recommended)](#option-1-python-script-recommended)
- [Option 2: GitHub Actions CI/CD](#option-2-github-actions-cicd)
- [Option 3: PowerShell Script](#option-3-powershell-script)
- [Authentication Setup](#authentication-setup)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Deployment Options

| Method | Best For | Automation | Platform |
|--------|----------|------------|----------|
| **Python Script** | One-time deployment, local dev | Manual/Scriptable | Cross-platform |
| **GitHub Actions** | Continuous deployment, team | Fully automated | Cloud-based |
| **PowerShell** | Windows users, AD integration | Semi-automated | Windows |

---

## ✅ Prerequisites

### Required for All Methods:

1. **Microsoft Fabric Workspace**
   - Workspace created in Fabric portal
   - Note the Workspace ID (GUID)
   - Location: Workspace Settings → About

2. **Azure Service Principal** (for automation)
   - Application (client) ID
   - Directory (tenant) ID
   - Client secret
   - Permissions: Fabric Workspace Contributor

3. **Local Tools**
   - Python 3.10+ OR PowerShell 7+
   - Git
   - Azure CLI (optional but recommended)

### Get Your Workspace ID:

```bash
# In Fabric portal:
# 1. Open your workspace
# 2. Click gear icon → Workspace settings
# 3. Go to "About" section
# 4. Copy "Workspace ID"
```

---

## 🐍 Option 1: Python Script (Recommended)

### Quick Start

```bash
# 1. Clone repository
git clone <your-repo-url>
cd hckrschl-deploy

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Set environment variables
export FABRIC_WORKSPACE_ID="your-workspace-id-here"
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"

# 4. Deploy to dev environment
python scripts/deploy_to_fabric.py --environment dev

# Or deploy to specific workspace
python scripts/deploy_to_fabric.py \
    --environment dev \
    --workspace-id "12345678-1234-1234-1234-123456789abc"
```

### Using .env File

```bash
# 1. Create .env file from template
cp deploy/.env.template deploy/.env

# 2. Edit deploy/.env with your values
nano deploy/.env

# 3. Load environment variables
source deploy/.env  # Linux/Mac
# or
. deploy/.env       # PowerShell

# 4. Deploy
python scripts/deploy_to_fabric.py --environment dev
```

### Dry Run Mode

Test deployment without making changes:

```bash
python scripts/deploy_to_fabric.py \
    --environment dev \
    --dry-run
```

### What Gets Deployed:

✅ **Lakehouses**
- hs_bronze_dev / test / prod
- hs_silver_dev / test / prod
- hs_gold_dev / test / prod
- hs_monitoring_dev / test / prod

✅ **Notebooks**
- bronze_to_silver_contact.py
- silver_to_gold_scd_contact.py
- utils/common.py

✅ **Pipelines**
- medallion_orchestrator.json

⚠️ **Manual Steps Required:**
- Execute SQL scripts via SQL endpoint (schema creation)
- Configure pipeline schedules
- Set up alerts

---

## 🔄 Option 2: GitHub Actions CI/CD

### Setup (One-Time)

#### 1. Configure GitHub Secrets

Go to: Repository → Settings → Secrets and variables → Actions

Add these secrets:

```yaml
# Azure Authentication
AZURE_TENANT_ID: "your-tenant-id"
AZURE_CLIENT_ID: "your-client-id"
AZURE_CLIENT_SECRET: "your-client-secret"

# Fabric Workspace IDs
FABRIC_WORKSPACE_ID_DEV: "dev-workspace-guid"
FABRIC_WORKSPACE_ID_TEST: "test-workspace-guid"
FABRIC_WORKSPACE_ID_PROD: "prod-workspace-guid"
```

#### 2. Enable GitHub Actions

```bash
# Workflow file already created at:
.github/workflows/deploy.yml

# Push to trigger
git push origin develop  # Deploys to dev
git push origin main     # Deploys to prod
```

### Deployment Triggers

**Automatic Deployment:**
- Push to `develop` branch → Deploys to **dev**
- Push to `main` branch → Deploys to **prod**

**Manual Deployment:**
1. Go to: Actions → Deploy to Microsoft Fabric
2. Click "Run workflow"
3. Select environment (dev/test/prod)
4. Click "Run workflow"

### Workflow Steps

```yaml
1. Validate Code
   ├── Lint Python (flake8, black)
   ├── Validate JSON files
   └── Check SQL syntax

2. Deploy
   ├── Authenticate with Azure
   ├── Create/update lakehouses
   ├── Upload notebooks
   └── Deploy pipelines

3. Post-Deploy
   ├── Generate deployment summary
   └── Send notifications (if configured)
```

### Environment Protection (Recommended)

For production, enable manual approval:

1. Repository → Settings → Environments
2. Create environment: `prod`
3. Enable "Required reviewers"
4. Add your team members

Now production deploys require approval before running.

---

## 💻 Option 3: PowerShell Script

### Quick Start (Windows)

```powershell
# 1. Install Az PowerShell module (one-time)
Install-Module -Name Az -Scope CurrentUser -Force

# 2. Run deployment
.\scripts\deploy_to_fabric.ps1 `
    -Environment dev `
    -WorkspaceId "your-workspace-id-here"

# 3. Follow interactive Azure login prompts
```

### PowerShell Features

✅ **Pros:**
- Native Windows integration
- Interactive Azure AD login
- No Python required
- Works with corporate proxies

⚠️ **Cons:**
- Limited compared to Python version
- No pipeline deployment (use Fabric UI)
- Windows-only

---

## 🔐 Authentication Setup

### Service Principal Creation

#### Option A: Azure Portal

1. **Register Application:**
   - Portal → Azure Active Directory → App registrations
   - New registration → Name: "FabricDeployment"
   - Copy: Application (client) ID, Directory (tenant) ID

2. **Create Secret:**
   - App → Certificates & secrets → New client secret
   - Description: "Fabric Deployment"
   - Copy the secret VALUE (not ID)

3. **Grant Permissions:**
   - Portal → Fabric Workspace → Manage access
   - Add member → Search for "FabricDeployment"
   - Role: Contributor

#### Option B: Azure CLI

```bash
# Create service principal
az ad sp create-for-rbac --name "FabricDeployment" \
    --role contributor \
    --scopes /subscriptions/{subscription-id}

# Output will contain:
# - appId (client ID)
# - password (client secret)
# - tenant (tenant ID)

# Save these securely!
```

### User Authentication (Interactive)

For local development, use interactive login:

```bash
# Python script will use DefaultAzureCredential
# which tries these methods in order:
# 1. Environment variables
# 2. Managed Identity
# 3. Azure CLI login
# 4. Visual Studio login
# 5. Interactive browser login

# Login via Azure CLI
az login

# Then run deployment
python scripts/deploy_to_fabric.py --environment dev
```

---

## 🛠️ Advanced Usage

### Deploy Only Notebooks

```python
# Modify deploy_to_fabric.py or create custom script
from scripts.deploy_to_fabric import FabricDeployer

deployer = FabricDeployer(workspace_id, "dev")
deployer.authenticate()

# Deploy only notebooks
notebooks_dir = Path("notebooks")
for notebook in notebooks_dir.glob("*.py"):
    deployer.upload_notebook(notebook)
```

### Deploy to Multiple Workspaces

```bash
# Deploy same code to multiple workspaces
for ws_id in dev_ws test_ws prod_ws; do
    python scripts/deploy_to_fabric.py \
        --workspace-id "$ws_id" \
        --environment dev
done
```

### Scheduled Deployment (Cron)

```bash
# Add to crontab (Linux/Mac)
# Deploy every Monday at 3 AM
0 3 * * 1 cd /path/to/repo && python scripts/deploy_to_fabric.py --environment dev

# Windows Task Scheduler
# Use .bat file wrapper for Python script
```

---

## 🐛 Troubleshooting

### Common Issues

#### 1. Authentication Failed

**Error:** `Authentication failed: AADSTS70001`

**Solution:**
```bash
# Clear cached credentials
az account clear

# Re-login
az login

# Verify correct tenant
az account show
```

#### 2. Workspace Not Found

**Error:** `404 - Workspace not found`

**Solution:**
- Verify workspace ID is correct (must be GUID format)
- Check service principal has access to workspace
- Ensure workspace is in correct region

#### 3. Notebook Upload Failed

**Error:** `409 - Resource already exists`

**This is OK!** The script detects existing resources and updates them.

**If it fails to update:**
```bash
# Delete notebook in Fabric UI, then re-run
# OR manually update via UI
```

#### 4. Permission Denied

**Error:** `403 - Forbidden`

**Solution:**
```bash
# Check service principal role
# Must be "Contributor" or higher

# In Fabric workspace:
# Settings → Manage access → Add service principal → Contributor
```

#### 5. SQL Scripts Not Executed

**This is expected!** SQL scripts must be run manually:

1. Open Fabric workspace
2. Open lakehouse SQL endpoint
3. Copy/paste SQL from `sql/schema/*.sql`
4. Execute

**Why manual?**
- SQL endpoint authentication is different from API
- Requires explicit SQL connection
- Can be automated with additional tools (pyodbc, sqlalchemy)

---

## 📊 Deployment Status

### Check Deployment Success

```bash
# Via Azure CLI
az rest --method get \
    --uri "https://api.fabric.microsoft.com/v1/workspaces/{workspace-id}/items"

# Via PowerShell
Get-AzResource -ResourceType "Microsoft.Fabric/workspaces"

# Via Python
from scripts.deploy_to_fabric import FabricDeployer
deployer = FabricDeployer(workspace_id, "dev")
deployer.authenticate()
# Check lakehouse exists
lakehouse_id = deployer.get_lakehouse_id("hs_silver_dev")
print(f"Lakehouse ID: {lakehouse_id}")
```

---

## 🔄 Rollback Strategy

### Rollback Deployment

```bash
# 1. Get previous commit hash
git log --oneline

# 2. Checkout previous version
git checkout <previous-commit-hash>

# 3. Re-deploy
python scripts/deploy_to_fabric.py --environment dev

# 4. Return to latest
git checkout main
```

### Backup Before Deployment

```bash
# Create backup tag before deploying to prod
git tag -a backup-$(date +%Y%m%d) -m "Backup before prod deploy"
git push origin backup-$(date +%Y%m%d)

# Deploy
python scripts/deploy_to_fabric.py --environment prod

# Rollback if needed
git checkout backup-20250126
python scripts/deploy_to_fabric.py --environment prod
```

---

## 📖 Next Steps

After successful deployment:

1. ✅ **Execute SQL scripts** manually (see IMPLEMENTATION_SUMMARY.md)
2. ✅ **Test notebooks** with sample data
3. ✅ **Configure pipeline schedule** in Fabric UI
4. ✅ **Setup monitoring** and alerts
5. ✅ **Document any custom changes**

---

## 🆘 Support

**Issues:**
- GitHub Issues: https://github.com/your-org/hckrschl-deploy/issues
- Email: data-team@hackerschool.de

**Resources:**
- [Fabric REST API Docs](https://learn.microsoft.com/rest/api/fabric/)
- [Azure Authentication](https://learn.microsoft.com/azure/active-directory/develop/)
- [GitHub Actions](https://docs.github.com/actions)

---

**Last Updated:** 2025-10-26
**Version:** 1.0.0
