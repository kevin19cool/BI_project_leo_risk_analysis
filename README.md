# Real-time & Historical LEO Space Environment Risk Monitoring Platform

> **MADSC301 — Business Intelligence — Final Assignment (Term 3 AY 2025/26)**
> **Student:** ChengYi Lin  
> **Lecturer:** Dr. Zainab Usman  
> **Institution:** EU Business School Munich

---

## 🪐 1. Business Case & Project Objective
With the exponential growth of Low Earth Orbit (LEO) satellite mega-constellations (e.g., SpaceX Starlink, OneWeb), orbital crowding and space debris have become critical threats to global aerospace infrastructure. 

This project delivers an enterprise-grade **End-to-End Business Intelligence & Data Engineering Pipeline** designed to monitor LEO environmental risks. By combining historical orbital data from the **U.S. Space Command (Space-Track API)** with solar activity metrics from the **German Research Centre for Geosciences (GFZ Potsdam API)**, the platform quantifies, aggregates, and visualizes dynamic risks inside specific orbital shells (200km to 2000km altitude).

---

## 🛠️ 2. Technical Infrastructure & Architecture
The platform is fully containerized using a **Docker Microservices Grid** to ensure reproducibility and scalability.

```text
  [ U.S. Space-Track API ] (Historical & Daily Snapshots) ──┐
                                                           ├──> [ Python ETL Workers ]
  [ German GFZ Potsdam API ] (30-Day Solar Kp/F10.7)      ──┘            │
                                                                         ▼ (Port 5432 / 5433)
  [ Power BI Desktop ] <─── [ PostgreSQL Data Warehouse ] <─── [ Apache Airflow Orchestrator ]
  (Dynamic DAX Slicers)        (Star Schema Design)            (LocalExecutor & Scheduler)
```
  
## 🔑 Key Technical Highlights

### Unified Metadata & Data Warehouse Architecture

Apache Airflow's metadata tables and the analytical Star Schema warehouse share a single containerized PostgreSQL instance. This architecture minimizes resource consumption while maintaining operational simplicity and ensuring efficient RAM and CPU utilization.

### Idempotent Pipeline Design

To ensure data consistency and governance, the ETL process performs surgical deletions (`DELETE WHERE date_key = today`) before data insertion. This strategy prevents duplicate primary keys and avoids unnecessary data bloating during repeated executions or manual DAG triggers.

### High-Speed Binary Copy Engine

Bulk insertion is implemented using PostgreSQL's `COPY` protocol with Python's `io.StringIO` buffer, enabling efficient loading of more than 28,000 active satellite records within milliseconds.

---

## 📐 3. Data Warehouse Star Schema Design

The analytical layer is implemented as a strictly decoupled relational Star Schema inside PostgreSQL (`leo_risk_db`) to maximize OLAP performance and support efficient Power BI analytics.

### Dimension Tables

#### `dim_time`

Date dictionary table enriched with smart calendar attributes:

* `date_key` (YYYYMMDD)
* `year`
* `month`
* `day`
* `is_weekend`

#### `dim_satellite`

Master asset catalog containing more than 28,000 active objects tracked by NORAD:

* `norad_cat_id`
* `object_name`
* `object_type`
* `country_code`
* `rcs_size`

### Fact Tables

#### `fact_space_weather`

Stores daily space weather metrics to evaluate atmospheric drag risks:

* Peak Kp-Index
* Solar radio flux (F10.7)
* Risk levels (`LOW`, `MEDIUM`, `HIGH`)

#### `fact_orbital_density`

Tracks macro-level orbital congestion indicators:

* `satellite_count`
* `debris_count`
* `total_count`
* Weighted `density_score`

Orbital shells are aggregated into altitude bins:

* 200 km
* 400 km
* 600 km
* 800 km
* 2000 km

#### `fact_satellite_7days_history`

High-granularity rolling snapshot table used for historical time-series analysis.

Stored orbital elements include:

* `inclination`
* `eccentricity`
* `history_altitude_km`

---

## 🚀 4. Deployment & Quick Start

### Prerequisites

Before deployment, ensure the following requirements are met:

* Docker and Docker Compose installed.
* A valid Space-Track.org account.

### Step 1: Environment Setup

Create a `.env` file in the project root directory and configure the API credentials:

```env
SPACETRACK_USER=your_email@example.com
SPACETRACK_PASS=your_secure_password
```

### Step 2: Launch the Platform Grid

Start all containers in detached mode:

```bash
docker compose up -d --build
```

### Step 3: Monitor Data Ingestion

The bootstrap service (`leo-data-bootstrap`) automatically backfills the previous 30 days of historical data after database health checks are completed.

Monitor the process with:

```bash
docker logs -f leo-data-bootstrap
```

### Step 4: Access the Apache Airflow Orchestrator

Open the Airflow web interface:

```text
http://localhost:8080
```

**Default credentials**

```text
Username: airflow
Password: airflow
```

Users can manually trigger DAGs, inspect execution logs, or visualize pipeline dependencies through Airflow's graphical interface.

---

## 📊 5. Power BI Analytics Dashboard

Power BI Desktop connects directly to the local PostgreSQL data warehouse for real-time analytics.

### Connection Configuration

#### Server Endpoint

```text
localhost:5433
```

Port `5433` is used to safely redirect traffic from PostgreSQL's native port (`5432`) and avoid host-machine conflicts.

#### Database

```text
leo_risk_db
```

### Automatic Dashboard Refresh

A dynamic DAX calculated column (`Is Latest Day`) is implemented to automatically identify the latest incremental batch ingested by Airflow.

This mechanism enables executive dashboards to always display the newest available data without requiring manual date selections or visual adjustments.
