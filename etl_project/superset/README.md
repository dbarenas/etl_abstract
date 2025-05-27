# Superset Dashboard Integration

This directory is a placeholder for Superset configurations and notes.

## Connecting Superset to PostgreSQL

1.  Once the Docker environment is up and running (`docker-compose up`), Superset will be accessible at `http://localhost:8088`.
2.  Log in to Superset (default credentials might be `admin`/`admin` unless changed, or you might need to create an admin user if it's a first-time setup without pre-configuration).
3.  Configure a new database connection in Superset:
    *   **Database Type**: PostgreSQL
    *   **SQLAlchemy URI**: `postgresql://user:password@postgres_db:5432/mydb`
    *   Test the connection and save it.

## Intended Dashboards

After connecting Superset to the PostgreSQL database where the ETL pipeline loads data, the following dashboards are planned:

1.  **Validation Failures Overview**:
    *   Chart showing the count of validation failures over time.
    *   Table listing rows/records that failed Pydantic validation (if such error data is stored).
    *   *Note: The current ETL scripts print validation errors but do not store them in a structured way in the database. To build this dashboard, the `validate.py` script and `load.py` script would need modification to log validation errors to a dedicated table.*

2.  **Top Failed Columns**:
    *   Bar chart showing columns with the highest number of casting or validation errors.
    *   *Note: Similar to the above, this requires storing detailed error information.*

3.  **Inferred Types Heatmap/Summary**:
    *   A table or chart summarizing the types inferred by LightAutoML for columns across different datasets/runs (if multiple tables are generated).
    *   Could show the frequency of each inferred type per column.
    *   *Note: This would require the pipeline to log the `type_map` to a metadata table.*

4.  **Pipeline Run Logs & Status**:
    *   Dashboard displaying Airflow task statuses (success, failure).
    *   Metrics on data processing times.
    *   Count of records processed per run.
    *   *Note: Airflow's UI already provides much of this. For custom logging to a DB table that Superset can query, the PythonOperator in the DAG would need to be enhanced to log key metrics.*

5.  **Data Table Explorer**:
    *   Allow exploration of the dynamically created `clean_data` (and its versioned variants like `clean_data_YYYYMMDDHHMMSSFFFFFF`) tables.
    *   Superset's "SQL Lab" can be used for ad-hoc queries.
    *   Charts can be built on specific columns of interest from these tables (e.g., distribution of 'amount').

## Setup Notes

*   The `SUPERSET_SECRET_KEY` in `docker-compose.yml` should be changed for a production environment.
*   Further Superset configuration can be achieved by mounting a `superset_config.py` file into the Superset container.
