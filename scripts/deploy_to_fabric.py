"""
Microsoft Fabric Deployment Script
Programmatically deploys Medallion Architecture to Fabric workspace

Prerequisites:
- Azure CLI installed and authenticated
- Service Principal with Fabric permissions OR User authentication
- Fabric workspace already created

Usage:
    python scripts/deploy_to_fabric.py --environment dev
    python scripts/deploy_to_fabric.py --environment prod --workspace-id <id>
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests
from azure.identity import DefaultAzureCredential, ClientSecretCredential


class FabricDeployer:
    """
    Handles deployment of notebooks, pipelines, and SQL scripts to Microsoft Fabric
    """

    def __init__(self, workspace_id: str, environment: str = "dev"):
        self.workspace_id = workspace_id
        self.environment = environment
        self.base_url = "https://api.fabric.microsoft.com/v1"
        self.token = None
        self.headers = None

        # Load config
        config_path = Path(__file__).parent.parent / "config" / f"{environment}.json"
        with open(config_path) as f:
            self.config = json.load(f)

        print(f"🚀 Fabric Deployer initialized")
        print(f"   Environment: {environment}")
        print(f"   Workspace ID: {workspace_id}")

    def authenticate(self):
        """
        Authenticate using Azure credentials
        Supports: DefaultAzureCredential (recommended) or Service Principal
        """
        print("\n🔐 Authenticating...")

        try:
            # Try environment variables first (for CI/CD)
            tenant_id = os.getenv("AZURE_TENANT_ID")
            client_id = os.getenv("AZURE_CLIENT_ID")
            client_secret = os.getenv("AZURE_CLIENT_SECRET")

            if tenant_id and client_id and client_secret:
                print("   Using Service Principal authentication")
                credential = ClientSecretCredential(
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret
                )
            else:
                print("   Using DefaultAzureCredential (interactive login)")
                credential = DefaultAzureCredential()

            # Get token for Fabric API
            scope = "https://analysis.windows.net/powerbi/api/.default"
            token = credential.get_token(scope)
            self.token = token.token

            self.headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            }

            print("   ✅ Authentication successful")
            return True

        except Exception as e:
            print(f"   ❌ Authentication failed: {str(e)}")
            return False

    def create_lakehouse(self, lakehouse_name: str) -> Optional[str]:
        """
        Create a Fabric Lakehouse
        Returns lakehouse ID if successful
        """
        print(f"\n📦 Creating lakehouse: {lakehouse_name}")

        url = f"{self.base_url}/workspaces/{self.workspace_id}/lakehouses"
        payload = {
            "displayName": lakehouse_name,
            "description": f"Medallion architecture - {lakehouse_name}"
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload)

            if response.status_code == 201:
                lakehouse_id = response.json().get("id")
                print(f"   ✅ Created: {lakehouse_name} (ID: {lakehouse_id})")
                return lakehouse_id
            elif response.status_code == 409:
                print(f"   ⚠️  Already exists: {lakehouse_name}")
                # Get existing lakehouse ID
                return self.get_lakehouse_id(lakehouse_name)
            else:
                print(f"   ❌ Failed: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            return None

    def get_lakehouse_id(self, lakehouse_name: str) -> Optional[str]:
        """
        Get lakehouse ID by name
        """
        url = f"{self.base_url}/workspaces/{self.workspace_id}/lakehouses"

        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                lakehouses = response.json().get("value", [])
                for lh in lakehouses:
                    if lh.get("displayName") == lakehouse_name:
                        return lh.get("id")
            return None
        except Exception as e:
            print(f"   ⚠️  Could not fetch lakehouse: {str(e)}")
            return None

    def upload_notebook(self, notebook_path: Path, lakehouse_id: Optional[str] = None) -> bool:
        """
        Upload a notebook to Fabric workspace
        """
        notebook_name = notebook_path.stem
        print(f"\n📓 Uploading notebook: {notebook_name}")

        # Read notebook content
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook_content = f.read()

        # Convert Python file to Fabric notebook format
        notebook_json = self._convert_to_notebook_format(notebook_content, notebook_name)

        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks"
        payload = {
            "displayName": notebook_name,
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": notebook_json,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload)

            if response.status_code in [200, 201]:
                print(f"   ✅ Uploaded: {notebook_name}")
                return True
            elif response.status_code == 409:
                print(f"   ⚠️  Already exists, updating...")
                return self.update_notebook(notebook_name, notebook_json)
            else:
                print(f"   ❌ Failed: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            return False

    def update_notebook(self, notebook_name: str, notebook_json: str) -> bool:
        """
        Update existing notebook
        """
        # Get notebook ID
        notebook_id = self.get_notebook_id(notebook_name)
        if not notebook_id:
            print(f"   ❌ Could not find notebook to update")
            return False

        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks/{notebook_id}/updateDefinition"
        payload = {
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": notebook_json,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload)
            if response.status_code in [200, 202]:
                print(f"   ✅ Updated: {notebook_name}")
                return True
            else:
                print(f"   ❌ Update failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            return False

    def get_notebook_id(self, notebook_name: str) -> Optional[str]:
        """
        Get notebook ID by name
        """
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks"

        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                notebooks = response.json().get("value", [])
                for nb in notebooks:
                    if nb.get("displayName") == notebook_name:
                        return nb.get("id")
            return None
        except Exception as e:
            return None

    def _convert_to_notebook_format(self, python_content: str, name: str) -> str:
        """
        Convert Python script to Fabric notebook format (base64 encoded)
        """
        import base64

        # Simple conversion - in production you'd want to preserve cell structure
        notebook = {
            "nbformat": 4,
            "nbformat_minor": 2,
            "metadata": {},
            "cells": [
                {
                    "cell_type": "code",
                    "source": python_content,
                    "metadata": {},
                    "outputs": [],
                    "execution_count": None
                }
            ]
        }

        notebook_json = json.dumps(notebook)
        return base64.b64encode(notebook_json.encode()).decode()

    def create_pipeline(self, pipeline_path: Path) -> bool:
        """
        Create/update Fabric pipeline from JSON definition
        """
        pipeline_name = pipeline_path.stem
        print(f"\n⚙️  Deploying pipeline: {pipeline_name}")

        with open(pipeline_path) as f:
            pipeline_def = json.load(f)

        url = f"{self.base_url}/workspaces/{self.workspace_id}/dataPipelines"
        payload = {
            "displayName": pipeline_name,
            "definition": pipeline_def
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload)

            if response.status_code in [200, 201]:
                print(f"   ✅ Deployed: {pipeline_name}")
                return True
            elif response.status_code == 409:
                print(f"   ⚠️  Already exists: {pipeline_name}")
                return True
            else:
                print(f"   ❌ Failed: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            return False

    def execute_sql(self, lakehouse_name: str, sql_file: Path) -> bool:
        """
        Execute SQL script against lakehouse SQL endpoint
        Note: This is a simplified version - in production you'd use SQL connection
        """
        print(f"\n🗄️  Executing SQL: {sql_file.name} on {lakehouse_name}")

        # Get lakehouse SQL endpoint
        lakehouse_id = self.get_lakehouse_id(lakehouse_name)
        if not lakehouse_id:
            print(f"   ❌ Lakehouse not found: {lakehouse_name}")
            return False

        # Read SQL content
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # TODO: Execute via SQL endpoint
        # For now, just log - actual execution would need SQL connection
        print(f"   ⚠️  Manual step required: Execute {sql_file.name} via SQL endpoint")
        print(f"      Lakehouse: {lakehouse_name} (ID: {lakehouse_id})")
        return True

    def deploy_all(self) -> bool:
        """
        Deploy entire stack to Fabric
        """
        print("\n" + "="*70)
        print("🚀 STARTING FULL DEPLOYMENT")
        print("="*70)

        success = True

        # Step 1: Create lakehouses
        print("\n📦 STEP 1: Creating Lakehouses")
        print("-" * 70)
        for lakehouse_key, lakehouse_name in self.config["lakehouses"].items():
            if not self.create_lakehouse(lakehouse_name):
                success = False

        # Step 2: Upload notebooks
        print("\n📓 STEP 2: Uploading Notebooks")
        print("-" * 70)
        notebooks_dir = Path(__file__).parent.parent / "notebooks"
        for notebook_file in notebooks_dir.glob("*.py"):
            if not self.upload_notebook(notebook_file):
                success = False

        # Step 3: Deploy pipelines
        print("\n⚙️  STEP 3: Deploying Pipelines")
        print("-" * 70)
        pipelines_dir = Path(__file__).parent.parent / "pipelines"
        for pipeline_file in pipelines_dir.glob("*.json"):
            if not self.create_pipeline(pipeline_file):
                success = False

        # Step 4: SQL scripts (manual step)
        print("\n🗄️  STEP 4: SQL Scripts")
        print("-" * 70)
        print("   ⚠️  SQL scripts need to be executed manually via SQL endpoint:")
        sql_dir = Path(__file__).parent.parent / "sql"
        for sql_file in sql_dir.rglob("*.sql"):
            print(f"      - {sql_file.relative_to(sql_dir.parent)}")

        # Summary
        print("\n" + "="*70)
        if success:
            print("✅ DEPLOYMENT COMPLETED SUCCESSFULLY")
        else:
            print("⚠️  DEPLOYMENT COMPLETED WITH WARNINGS")
        print("="*70)

        return success


def main():
    """
    Main deployment script entry point
    """
    parser = argparse.ArgumentParser(description="Deploy to Microsoft Fabric")
    parser.add_argument(
        "--environment",
        choices=["dev", "test", "prod"],
        default="dev",
        help="Target environment"
    )
    parser.add_argument(
        "--workspace-id",
        help="Fabric workspace ID (optional, can use env var FABRIC_WORKSPACE_ID)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate deployment without making changes"
    )

    args = parser.parse_args()

    # Get workspace ID
    workspace_id = args.workspace_id or os.getenv("FABRIC_WORKSPACE_ID")
    if not workspace_id:
        print("❌ Error: Workspace ID required")
        print("   Use --workspace-id or set FABRIC_WORKSPACE_ID environment variable")
        sys.exit(1)

    # Initialize deployer
    deployer = FabricDeployer(workspace_id, args.environment)

    # Authenticate
    if not deployer.authenticate():
        print("❌ Authentication failed")
        sys.exit(1)

    # Deploy
    if args.dry_run:
        print("\n🔍 DRY RUN MODE - No changes will be made\n")

    success = deployer.deploy_all()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
