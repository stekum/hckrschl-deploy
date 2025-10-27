<#
.SYNOPSIS
    PowerShell deployment script for Microsoft Fabric

.DESCRIPTION
    Alternative to Python script for Windows users
    Deploys notebooks and pipelines to Fabric workspace

.PARAMETER Environment
    Target environment (dev/test/prod)

.PARAMETER WorkspaceId
    Fabric workspace GUID

.EXAMPLE
    .\deploy_to_fabric.ps1 -Environment dev -WorkspaceId "12345678-1234-1234-1234-123456789abc"

.NOTES
    Requires: Az PowerShell module
    Install: Install-Module -Name Az -Scope CurrentUser
#>

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('dev','test','prod')]
    [string]$Environment,

    [Parameter(Mandatory=$true)]
    [string]$WorkspaceId
)

# Configuration
$ErrorActionPreference = "Stop"
$BaseUrl = "https://api.fabric.microsoft.com/v1"
$ConfigPath = Join-Path $PSScriptRoot ".." "config" "$Environment.json"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Fabric Deployment Script (PowerShell)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Environment: $Environment" -ForegroundColor Yellow
Write-Host "Workspace ID: $WorkspaceId" -ForegroundColor Yellow
Write-Host ""

# Load configuration
if (-not (Test-Path $ConfigPath)) {
    Write-Host "ERROR: Config file not found: $ConfigPath" -ForegroundColor Red
    exit 1
}

$Config = Get-Content $ConfigPath | ConvertFrom-Json
Write-Host "✓ Configuration loaded" -ForegroundColor Green

# Authenticate
Write-Host "`nAuthenticating with Azure..." -ForegroundColor Cyan
try {
    # Connect to Azure (interactive login)
    Connect-AzAccount -ErrorAction Stop | Out-Null

    # Get access token for Fabric
    $Token = (Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api").Token

    $Headers = @{
        "Authorization" = "Bearer $Token"
        "Content-Type" = "application/json"
    }

    Write-Host "✓ Authentication successful" -ForegroundColor Green
}
catch {
    Write-Host "ERROR: Authentication failed: $_" -ForegroundColor Red
    exit 1
}

# Function: Create Lakehouse
function New-FabricLakehouse {
    param(
        [string]$LakehouseName
    )

    Write-Host "`nCreating lakehouse: $LakehouseName" -ForegroundColor Cyan

    $Url = "$BaseUrl/workspaces/$WorkspaceId/lakehouses"
    $Body = @{
        displayName = $LakehouseName
        description = "Medallion architecture - $LakehouseName"
    } | ConvertTo-Json

    try {
        $Response = Invoke-RestMethod -Uri $Url -Method Post -Headers $Headers -Body $Body
        Write-Host "  ✓ Created: $LakehouseName" -ForegroundColor Green
        return $Response.id
    }
    catch {
        if ($_.Exception.Response.StatusCode -eq 409) {
            Write-Host "  ⚠ Already exists: $LakehouseName" -ForegroundColor Yellow
            return $null
        }
        else {
            Write-Host "  ERROR: $($_.Exception.Message)" -ForegroundColor Red
            return $null
        }
    }
}

# Function: Upload Notebook
function Publish-FabricNotebook {
    param(
        [string]$NotebookPath
    )

    $NotebookName = [System.IO.Path]::GetFileNameWithoutExtension($NotebookPath)
    Write-Host "`nUploading notebook: $NotebookName" -ForegroundColor Cyan

    # Read notebook content
    $NotebookContent = Get-Content $NotebookPath -Raw

    # Convert to base64
    $NotebookBytes = [System.Text.Encoding]::UTF8.GetBytes($NotebookContent)
    $NotebookBase64 = [Convert]::ToBase64String($NotebookBytes)

    $Url = "$BaseUrl/workspaces/$WorkspaceId/notebooks"
    $Body = @{
        displayName = $NotebookName
        definition = @{
            format = "py"
            parts = @(
                @{
                    path = "notebook-content.py"
                    payload = $NotebookBase64
                    payloadType = "InlineBase64"
                }
            )
        }
    } | ConvertTo-Json -Depth 10

    try {
        $Response = Invoke-RestMethod -Uri $Url -Method Post -Headers $Headers -Body $Body
        Write-Host "  ✓ Uploaded: $NotebookName" -ForegroundColor Green
        return $true
    }
    catch {
        if ($_.Exception.Response.StatusCode -eq 409) {
            Write-Host "  ⚠ Already exists: $NotebookName" -ForegroundColor Yellow
            return $true
        }
        else {
            Write-Host "  ERROR: $($_.Exception.Message)" -ForegroundColor Red
            return $false
        }
    }
}

# Main Deployment Process
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Starting Deployment" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$Success = $true

# Step 1: Create Lakehouses
Write-Host "`nSTEP 1: Creating Lakehouses" -ForegroundColor Magenta
foreach ($LakehouseProperty in $Config.lakehouses.PSObject.Properties) {
    $LakehouseName = $LakehouseProperty.Value
    $Result = New-FabricLakehouse -LakehouseName $LakehouseName
    if ($null -eq $Result) {
        # Already exists is OK
    }
}

# Step 2: Upload Notebooks
Write-Host "`nSTEP 2: Uploading Notebooks" -ForegroundColor Magenta
$NotebooksDir = Join-Path $PSScriptRoot ".." "notebooks"
$NotebookFiles = Get-ChildItem -Path $NotebooksDir -Filter "*.py" -File

foreach ($NotebookFile in $NotebookFiles) {
    $Result = Publish-FabricNotebook -NotebookPath $NotebookFile.FullName
    if (-not $Result) {
        $Success = $false
    }
}

# Step 3: Deploy Pipelines
Write-Host "`nSTEP 3: Deploying Pipelines" -ForegroundColor Magenta
Write-Host "  ⚠ Pipeline deployment not yet implemented in PowerShell" -ForegroundColor Yellow
Write-Host "  Use Python script or Fabric UI for pipeline deployment" -ForegroundColor Yellow

# Summary
Write-Host "`n========================================" -ForegroundColor Cyan
if ($Success) {
    Write-Host "  ✓ DEPLOYMENT COMPLETED SUCCESSFULLY" -ForegroundColor Green
}
else {
    Write-Host "  ⚠ DEPLOYMENT COMPLETED WITH WARNINGS" -ForegroundColor Yellow
}
Write-Host "========================================" -ForegroundColor Cyan

Write-Host "`nNext Steps:" -ForegroundColor Cyan
Write-Host "1. Execute SQL scripts manually via Fabric SQL endpoint" -ForegroundColor White
Write-Host "2. Configure pipeline schedules in Fabric UI" -ForegroundColor White
Write-Host "3. Validate deployment with test run" -ForegroundColor White

exit $(if ($Success) { 0 } else { 1 })
