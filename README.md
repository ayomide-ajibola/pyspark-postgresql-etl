# Distributed PySpark & PostgreSQL Big Data ETL Pipeline

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://python.org)
[![PySpark](https://img.shields.io/badge/Apache%20Spark-PySpark%203.5-orange.svg)](https://spark.apache.org)
[![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-336791.svg)](https://postgresql.org)
[![CI](https://img.shields.io/badge/CI-GitHub%20Actions-brightgreen.svg)](#)

A modular, distributed Extract-Transform-Load (ETL) pipeline leveraging **Apache PySpark** and **PostgreSQL** over JDBC for processing, partitioning, and storing multi-attribute real estate market datasets.

---

## Architectural Highlights
- **Distributed Ingestion**: Ingests raw tabular data using PySpark DataFrame APIs with schema inference and validation.
- **Complex Transformations**:
  - Partitioning and grouping by categorical attributes (e.g., neighborhood segments).
  - Normalization of boolean indicators using custom PySpark conditional logic (`when/otherwise`).
  - Standardized date formatting (`M/d/yy` to ISO `yyyy-MM-dd`) for relational database compatibility.
  - Parallel single-file partition writes using `.coalesce(1)` with safe directory cleanups.
- **ACID Relational Storage**: Streams cleaned partitions directly into PostgreSQL tables over JDBC (`org.postgresql.Driver`) in batch mode.
- **Continuous Integration**: Configured GitHub Actions CI pipeline executing automated Black code formatting enforcement.

---

## Architecture Diagram

```
[ Raw CSV Dataset ]
        │
        ▼
[ PySpark Ingestion ] ──► Schema Inference & Column Mapping
        │
        ▼
[ Transformation Engine ]
   ├── Type-safe boolean casting
   ├── Date parsing & ISO standardization
   └── Categorical partitioning (by neighborhood)
        │
        ▼
[ Storage & Loading Layer ]
   ├── Partition-based CSV Export (.coalesce(1))
   └── High-Throughput JDBC Batch Writes ──► [ PostgreSQL Relational DB ]
```

---

## Getting Started

### 1. Prerequisites
- Python 3.11+
- Java JDK 17 (required for Apache Spark runtime)
- PostgreSQL 14+

### 2. Installation
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Environment Configuration
```bash
cp .env.example .env
# Edit .env with your local PostgreSQL credentials
```

### 4. Running the Pipeline
```bash
python src/etl_pipeline.py
```

---

## Author
**Ayomide Ajibola**  
M.S. in Computational Science & Engineering, Kean University  
- GitHub: [@uravgsk](https://github.com/uravgsk)
- LinkedIn: [Ayomide Ajibola](https://linkedin.com/in/ayomide-ajibola)
