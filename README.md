# Intelligent ETL Pipeline with LightAutoML + Pydantic

This project implements an ETL (Extract, Transform, Load) pipeline that intelligently infers data types using LightAutoML, validates data using Pydantic, and orchestrates the process using Airflow. The entire environment is containerized with Docker and uses PostgreSQL as the data store, with Superset for potential monitoring and data exploration.

A key feature is the dynamic handling of table schemas in the loading phase:
- If data with a new schema arrives, a new table is created.
- If the schema matches an existing table, data is appended.

## 🧠 Core Stack

| Component           | Tech                                      |
|---------------------|-------------------------------------------|
| Extraction          | Pandas (CSV, Excel)                       |
| Type Inference      | LightAutoML                               |
| Auto-casting        | Pandas + NumPy                            |
| Validation          | Pydantic                                  |
| Transformations     | Custom Python logic                       |
| Load                | SQLAlchemy → PostgreSQL (dynamic tables)  |
| Orchestration       | Apache Airflow                            |
| Monitoring          | Airflow UI + Superset (manual setup)      |
| Containerization    | Docker, Docker Compose                    |
| Database            | PostgreSQL                                |
| Data Visualization  | Apache Superset (manual setup)            |

## 🔧 Project Structure

```
etl_project/
├── dags/                     # Airflow DAGs
│   └── etl_pipeline.py
├── data/                     # Sample data
│   └── sample.csv
├── etl/                      # ETL scripts
│   ├── extract.py
│   ├── infer_types.py
│   ├── validate.py
│   ├── transform.py
│   ├── load.py
│   └── monitor.py
├── superset/                 # Superset related notes
│   └── README.md
├── Dockerfile                # Dockerfile for the Airflow service
├── docker-compose.yml        # Docker Compose setup for all services
└── README.md                 # This file
```

## 🚀 Getting Started

### Prerequisites

- Docker ( [https://www.docker.com/get-started](https://www.docker.com/get-started) )
- Docker Compose (usually included with Docker Desktop)

### Setup & Running

1.  **Clone the repository** (or ensure all files from this project are in a local directory named `etl_project`).

2.  **Navigate to the project root directory**:
    ```bash
    cd etl_project
    ```

3.  **Build and start all services using Docker Compose**:
    ```bash
    docker-compose up --build -d
    ```
    The `-d` flag runs the containers in detached mode.
    This command will:
    *   Build the custom Airflow image defined in `Dockerfile`.
    *   Start PostgreSQL, Airflow (init, webserver, scheduler), and Superset services.
    *   The `airflow_init` service will initialize the Airflow database and create a default admin user (admin/admin).

4.  **Access Airflow Web UI**:
    *   Open your browser and go to: `http://localhost:8080`
    *   Log in with credentials: `admin` / `admin` (or as created by `airflow_init` in `docker-compose.yml`).
    *   You should see the `intelligent_etl_pipeline` DAG. By default, it will be paused. Unpause it to enable scheduled runs, or trigger it manually.

5.  **Trigger the ETL DAG**:
    *   In the Airflow UI, find `intelligent_etl_pipeline`.
    *   Unpause it by toggling the switch on the left.
    *   To run it immediately, click on the DAG name and then click the "Play" button (Trigger DAG) on the top right.

6.  **Check Processed Data in PostgreSQL**:
    *   You can connect to the PostgreSQL database using any SQL client (e.g., DBeaver, pgAdmin, or `psql` CLI).
    *   Connection details (from `docker-compose.yml`):
        *   Host: `localhost` (since port 5432 is mapped)
        *   Port: `5432`
        *   Database: `mydb`
        *   User: `user`
        *   Password: `password`
    *   The pipeline will load data into tables named `clean_data` or `clean_data_YYYYMMDDHHMMSSFFFFFF` if the schema changes.

7.  **Explore with Superset (Manual Setup)**:
    *   Access Superset at `http://localhost:8088`.
    *   Follow the instructions in `superset/README.md` to connect Superset to the `postgres_db` service and start creating dashboards.

### Stopping the Environment

*   To stop all running containers:
    ```bash
    docker-compose down
    ```
*   To stop and remove volumes (deletes PostgreSQL data and Airflow logs):
    ```bash
    docker-compose down -v
    ```

## ⚙️ Pipeline Details

The ETL pipeline consists of the following steps, orchestrated by Airflow:

1.  **Extract**: Reads data from `/app/data/sample.csv` (path within the container). Supports CSV and Excel.
2.  **Infer Types**: Uses LightAutoML to guess the data types of each column.
3.  **Validate & Cast**: Applies the inferred types, casts data, and validates rows using Pydantic models generated on the fly. Errors are logged.
4.  **Transform**: Applies any custom business logic (e.g., filling missing 'amount' values).
5.  **Load**: Loads the processed DataFrame into a PostgreSQL table.
    *   If a table named `clean_data` exists and its schema matches the DataFrame, data is appended.
    *   If the schema differs, or the table doesn't exist, a new table (e.g., `clean_data_20231027103000123456`) is created with the new schema.

## Future Considerations (from Issue & Discussion)

*   **Enhanced Error Handling**: Store casting and validation errors in dedicated tables for dashboarding in Superset.
*   **Metadata Logging**: Log inferred `type_map` and other run metadata for better traceability.
*   **Advanced NLP Processing**: The `DetailedDescription` field was added to `sample.csv` with the intent of potentially using BERT transformers or other NLP techniques on it in future project phases.
*   **Production Airflow Setup**: For production, consider using the official Airflow Docker images, Helm chart, or a managed Airflow service, which offer more robust configurations (e.g., CeleryExecutor for scaling).
*   **Superset Configuration**: Persist Superset metadata (dashboards, charts) using a volume and potentially configure it to use PostgreSQL as its own backend database.
