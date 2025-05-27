# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONUNBUFFERED 1
ENV AIRFLOW_HOME /app/airflow_home 
# Note: The original issue had CMD ["airflow", "scheduler"], 
# but for a typical Airflow setup with docker-compose,
# the CMD is often overridden or handled by the compose file's entrypoint/command.
# For now, we'll keep the original CMD, but it might need adjustment
# depending on how Airflow is initialized (e.g. webserver, scheduler, worker).
# The `docker-compose.yml` will define the actual command for the airflow service.

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
# This includes etl/, dags/, and any other files/folders at etl_project/
COPY . /app

# Install system dependencies that might be needed by LightAutoML or other libraries
# For example, LightGBM (a common backend for LightAutoML) might need libgomp1
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    build-essential \
    # Add other system dependencies if LightAutoML or other packages require them
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# It's good practice to copy requirements.txt and install from it,
# but for this setup, we'll install directly.
# Ensure all libraries from the issue are included:
# pandas, pydantic, sqlalchemy, lightautoml, apache-airflow, psycopg2-binary (for PostgreSQL)
RUN pip install --no-cache-dir \
    pandas \
    pydantic \
    sqlalchemy \
    # LightAutoML can be a large library, consider specific extras if needed
    lightautoml \
    apache-airflow \
    psycopg2-binary \
    openpyxl # For pd.read_excel

# The original issue had: CMD ["airflow", "scheduler"]
# This is usually not sufficient for a full Airflow setup.
# Typically, you'd run `airflow db init` first, then `airflow webserver` and `airflow scheduler`.
# The docker-compose.yml will handle the command for the airflow service.
# If running standalone, you might need an entrypoint script.
# For now, let's set a default command that can be overridden.
# This command will likely be replaced by the docker-compose service definition.
CMD ["airflow", "info"]
