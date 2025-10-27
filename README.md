# Hackerschool Medallion Architecture - Microsoft Fabric

Medallion Architecture implementation (Bronze → Silver → Gold) for Hackerschool data lakehouse on Microsoft Fabric.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Deployment](#deployment)
- [Configuration](#configuration)
- [Monitoring](#monitoring)
- [Development](#development)
- [Contributing](#contributing)

---

## 🎯 Overview

This project implements a production-ready data lakehouse for Hackerschool using Microsoft Fabric's Medallion Architecture pattern. It processes data from Microsoft Dataverse through three quality layers:

- **Bronze**: Raw data (1:1 copy from Dataverse)
- **Silver**: Cleaned and validated data with quality checks
- **Gold**: Business-ready analytics with dimensional modeling and SCD Type 2

### Key Features

✅ **Config-Driven Transformations** - Add new tables without code changes
✅ **Data Quality Framework** - Automated validation with quarantine pattern
✅ **SCD Type 2** - Historical tracking for dimension tables
✅ **Comprehensive Monitoring** - Metrics, logging, and alerting
✅ **Multi-Environment** - Dev, Test, Prod configurations
✅ **Delta Lake** - ACID transactions and time travel

### MVP Scope (Week 1-2)

**Tables in Scope:**
- `contact` - Students, teachers, coaches (SCD Type 2)
- `account` - Partner organizations (SCD Type 2)
- `hs_course` - Course/session data

**Status:** 🚧 **In Development**
- ✅ SQL Schemas created
- ✅ Bronze → Silver (contact) notebook
- ✅ Silver → Gold SCD (contact) notebook
- ✅ Monitoring infrastructure
- ⏳ Account and Course notebooks (next)
- ⏳ Pipeline orchestration deployment
- ⏳ Power BI dashboards

---

## 🏗️ Architecture

### Medallion Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATAVERSE                               │
│                   (Microsoft Dynamics 365)                      │
└────────────────────────┬────────────────────────────────────────┘
                         │ Fabric Link
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER - Raw Data (hs_bronze_dev)                        │
│  • 1:1 copy from Dataverse                                      │
│  • No transformations                                           │
│  • Delta Lake format                                            │
└────────────────────────┬────────────────────────────────────────┘
                         │ DQ Checks + Cleansing
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER LAYER - Cleaned Data (hs_silver_dev)                    │
│  • Data quality validation                                      │
│  • Standardization (trim, lowercase)                            │
│  • Deduplication                                                │
│  • Invalid → Quarantine                                         │
└────────────────────────┬────────────────────────────────────────┘
                         │ SCD Type 2 + Aggregations
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD LAYER - Analytics Ready (hs_gold_dev)                     │
│  • Dimensional modeling (Star schema)                           │
│  • SCD Type 2 for history tracking                              │
│  • Business aggregations                                        │
│  • Power BI ready                                               │
└─────────────────────────────────────────────────────────────────┘
```

### Data Quality & Monitoring

```
┌──────────────────────────────────────────────────────┐
│  MONITORING LAYER (hs_monitoring_dev)                │
│  ┌──────────────────┐  ┌────────────────────────┐   │
│  │ Pipeline Metrics │  │ Quarantine Records     │   │
│  │ • Records in/out │  │ • Invalid data         │   │
│  │ • Duration       │  │ • Error details        │   │
│  │ • Error rates    │  │ • Resolution tracking  │   │
│  └──────────────────┘  └────────────────────────┘   │
│                                                      │
│  ┌──────────────────┐  ┌────────────────────────┐   │
│  │ DQ Rules         │  │ Pipeline Run History   │   │
│  └──────────────────┘  └────────────────────────┘   │
└──────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
hckrschl-deploy/
├── notebooks/                          # Spark notebooks for data processing
│   ├── bronze_to_silver_contact.py    # ✅ Contact DQ pipeline
│   ├── silver_to_gold_scd_contact.py  # ✅ Contact SCD Type 2
│   └── utils/
│       └── common.py                   # ✅ Shared utility functions
│
├── sql/                                # SQL scripts
│   ├── schema/
│   │   ├── 01_monitoring_tables.sql   # ✅ Monitoring infrastructure
│   │   └── 02_transformation_config.sql # ✅ Config-driven framework
│   └── seeds/
│       └── transformation_config_seed.sql # ✅ Initial config data
│
├── pipelines/                          # Fabric pipeline definitions
│   └── medallion_orchestrator.json    # ✅ Main orchestration pipeline
│
├── config/                             # Environment configurations
│   ├── dev.json                        # ✅ Development settings
│   ├── test.json                       # ✅ Test settings
│   └── prod.json                       # ✅ Production settings
│
├── context/                            # Planning & architecture docs
│   ├── architecture-flow.md           # Detailed architecture guide
│   ├── medallion-plan.md              # Technical specs
│   └── deployment-plan.md             # 2-week implementation plan
│
├── tests/                              # Unit and integration tests (TBD)
├── .github/workflows/                  # CI/CD pipelines (TBD)
└── README.md                           # This file
```

---

## 🚀 Getting Started

### Prerequisites

1. **Microsoft Fabric Workspace** with:
   - Lakehouse capability enabled
   - Notebook capability enabled
   - Pipeline capability enabled

2. **Microsoft Dataverse** environment:
   - Fabric Link configured
   - Tables: contact, account, hs_course

3. **Access & Permissions**:
   - Fabric Workspace Contributor or Admin
   - Dataverse System Administrator (for link setup)

### Quick Start (Dev Environment)

#### Step 1: Create Lakehouses

In Microsoft Fabric workspace `hs-fabric-dev`, create:

```
hs_bronze_dev      (use existing Dataverse link: hs_shareddev)
hs_silver_dev      (new)
hs_gold_dev        (new)
hs_monitoring_dev  (new)
```

#### Step 2: Setup Monitoring Tables

1. Open SQL endpoint for `hs_monitoring_dev`
2. Run: `sql/schema/01_monitoring_tables.sql`
3. Verify tables created:
   - `pipeline_metrics`
   - `quarantine_records`
   - `dq_rules`
   - `pipeline_run_history`

#### Step 3: Setup Transformation Config

1. Open SQL endpoint for `hs_silver_dev`
2. Run: `sql/schema/02_transformation_config.sql`
3. Run: `sql/seeds/transformation_config_seed.sql`
4. Verify config loaded:
   ```sql
   SELECT config_id, source_table, target_table, active
   FROM transformation_config
   WHERE active = TRUE;
   ```

#### Step 4: Upload Notebooks

1. Import notebooks from `notebooks/` to Fabric workspace:
   - `bronze_to_silver_contact.py`
   - `silver_to_gold_scd_contact.py`
   - `utils/common.py`

2. Attach to lakehouse: `hs_silver_dev` (default)

#### Step 5: Run First Pipeline

**Manual execution:**

```python
# In Fabric notebook or SQL endpoint
# 1. Run Bronze → Silver
notebook: bronze_to_silver_contact
parameters: {"environment": "dev"}

# 2. Run Silver → Gold
notebook: silver_to_gold_scd_contact
parameters: {"environment": "dev"}
```

**Check results:**

```sql
-- Silver data
SELECT COUNT(*) as record_count FROM hs_silver_dev.contact;

-- Gold data (current)
SELECT COUNT(*) as current_count
FROM hs_gold_dev.dim_contact_scd
WHERE is_current = TRUE;

-- Metrics
SELECT * FROM hs_monitoring_dev.pipeline_metrics
ORDER BY start_time DESC
LIMIT 5;

-- Quarantine
SELECT error_type, COUNT(*) as error_count
FROM hs_monitoring_dev.quarantine_records
GROUP BY error_type;
```

---

## 🔧 Configuration

### Environment-Specific Settings

Configuration files in `config/` control behavior per environment:

| Setting | Dev | Test | Prod |
|---------|-----|------|------|
| Error Threshold | 10% | 5% | 2% |
| Batch Size | 1,000 | 5,000 | 10,000 |
| Schedule | 2:00 AM | 3:00 AM | 1:00 AM |
| Monitoring | INFO | INFO | WARN |

### Adding New Tables

**Option 1: Config-Driven (Recommended)**

Add entry to `transformation_config`:

```sql
INSERT INTO transformation_config VALUES (
    'mytable_bronze_silver_dq',
    'bronze.mytable',
    'silver.mytable',
    'silver',
    'dq_check',
    '{"validations": [...], "transformations": [...]}',
    10,
    true,
    1000,
    NULL,
    CURRENT_TIMESTAMP(),
    NULL,
    'data-team',
    'Description of mytable transformation'
);
```

**Option 2: Copy Existing Notebook**

1. Copy `bronze_to_silver_contact.py` → `bronze_to_silver_mytable.py`
2. Update table names and validation rules
3. Add to pipeline orchestrator

---

## 📊 Monitoring

### Key Metrics

**Pipeline Metrics Table:**
```sql
SELECT
    stage,
    source_table,
    AVG(error_rate) as avg_error_rate,
    AVG(duration_sec) as avg_duration,
    SUM(records_written) as total_records
FROM hs_monitoring_dev.pipeline_metrics
WHERE start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY stage, source_table;
```

**Quarantine Dashboard:**
```sql
SELECT
    source_table,
    error_type,
    COUNT(*) as error_count,
    MAX(created_at) as last_occurrence
FROM hs_monitoring_dev.quarantine_records
WHERE status = 'new'
GROUP BY source_table, error_type
ORDER BY error_count DESC;
```

### Alerts (To Be Configured)

Azure Monitor alerts trigger on:
- Error rate > threshold
- Pipeline failures
- Long-running pipelines (> 2 hours)
- Quarantine spike

---

## 👩‍💻 Development

### Local Testing

**Prerequisites:**
- Python 3.10+
- PySpark 3.4+
- pytest

```bash
# Install dependencies
pip install -r requirements.txt  # TBD

# Run unit tests
pytest tests/  # TBD

# Run notebook locally (with Databricks Connect)
python notebooks/bronze_to_silver_contact.py  # TBD
```

### Branching Strategy

- `main` - Production code
- `develop` - Integration branch
- `feature/*` - Feature branches
- `hotfix/*` - Emergency fixes

### Code Quality

- **Linting**: `flake8` (Python), `sqlfluff` (SQL)
- **Formatting**: `black` (Python)
- **Type Hints**: Use for utility functions
- **Documentation**: Docstrings for all functions

---

## 📖 Additional Documentation

See `context/` folder for detailed planning documents:

- **[architecture-flow.md](context/architecture-flow.md)** - Deep-dive into architecture decisions (899 lines)
- **[medallion-plan.md](context/medallion-plan.md)** - Executive summary and technical specs (301 lines)
- **[deployment-plan.md](deployment-plan.md)** - 14-day implementation roadmap (517 lines)

---

## 🤝 Contributing

### Current Contributors

- Stefan Kochems (EY) - Architecture & Planning
- Saber - Implementation

### How to Contribute

1. Create feature branch: `git checkout -b feature/my-feature`
2. Make changes and test locally
3. Commit with clear messages
4. Push and create Pull Request
5. Tag reviewers: @stefan-kochems

---

## 📝 License

Internal Hackerschool project - not for public distribution.

---

## 🆘 Support

**Issues:** Create GitHub issue with label:
- `bug` - Something isn't working
- `enhancement` - New feature request
- `question` - Need help

**Contact:**
- Data Team: data-team@hackerschool.de
- Stefan Kochems: stefan.kochems@ey.com

---

## 🗺️ Roadmap

### ✅ Phase 1: MVP (Weeks 1-2)
- [x] Architecture design
- [x] SQL schemas
- [x] Contact table pipeline
- [ ] Account table pipeline
- [ ] Course table pipeline
- [ ] Pipeline orchestration
- [ ] Power BI dashboards

### 🚧 Phase 2: Scale (Weeks 3-4)
- [ ] Config-driven generic notebooks
- [ ] All Dataverse tables onboarded
- [ ] CI/CD pipeline
- [ ] Test environment deployment
- [ ] Performance optimization

### 📅 Phase 3: Production (Week 5+)
- [ ] Production deployment
- [ ] Monitoring & alerting
- [ ] Data lineage tracking
- [ ] User documentation
- [ ] Training sessions

---

**Last Updated:** 2025-10-26
**Version:** 0.1.0 (MVP in progress)
**Repository:** [github.com/hackerschool/fabric-medallion](https://github.com/hackerschool/fabric-medallion)
