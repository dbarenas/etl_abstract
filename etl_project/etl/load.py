import pandas as pd
from sqlalchemy import create_engine, inspect as sql_inspect, text, types as sql_types
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import datetime
from typing import Dict, Any, Type

# Mapping from Python types (as defined in type_map) to SQLAlchemy type *classes*
PYTHON_TO_SQLALCHEMY_TYPE_MAP: Dict[Type, Type[sql_types.TypeEngine]] = {
    float: sql_types.Float,
    str: sql_types.String,
    pd.Timestamp: sql_types.DateTime,
    int: sql_types.Integer,
    bool: sql_types.Boolean,
}

def get_sqlalchemy_schema_map(type_map: Dict[str, Type], df_columns: list) -> Dict[str, Type[sql_types.TypeEngine]]:
    """Converts a Python type map to a SQLAlchemy type map, ordered by df_columns."""
    sqlalchemy_schema = {}
    for col in df_columns:
        python_type = type_map.get(col)
        sqlalchemy_type_class = PYTHON_TO_SQLALCHEMY_TYPE_MAP.get(python_type)
        if sqlalchemy_type_class:
            sqlalchemy_schema[col] = sqlalchemy_type_class
        else:
            # Try to infer from pandas dtype if not in type_map (should ideally be covered by infer_types)
            # This is a fallback.
            # Example: if df[col].dtype is a numpy type
            # For now, default to String if absolutely no type info.
            print(f"Warning: Type for column '{col}' ({python_type}) not mapped to SQLAlchemy type or missing in type_map. Defaulting to String.")
            sqlalchemy_schema[col] = sql_types.String # Fallback for unmapped or missing types
    return sqlalchemy_schema

def get_db_table_schema_map(engine: Engine, table_name: str, schema: str = None) -> Dict[str, Type[sql_types.TypeEngine]]:
    """Inspects the database and returns the schema of an existing table using SQLAlchemy type classes."""
    inspector = sql_inspect(engine)
    db_schema_map = {}
    if inspector.has_table(table_name, schema=schema):
        columns = inspector.get_columns(table_name, schema=schema)
        for col_info in columns:
            db_schema_map[col_info['name']] = type(col_info['type'])
    return db_schema_map

def are_schemas_compatible(db_schema_map: Dict[str, Type[sql_types.TypeEngine]], current_schema_map: Dict[str, Type[sql_types.TypeEngine]]) -> bool:
    """
    Compares two schemas represented as dictionaries of {column_name: SQLAlchemyTypeClass}.
    Checks for same column names, order, and type compatibility.
    """
    db_cols = list(db_schema_map.keys())
    current_cols = list(current_schema_map.keys())

    if db_cols != current_cols:
        print(f"Schema comparison failed: Column names or order differ. DB: {db_cols}, Current: {current_cols}")
        return False

    for col_name, current_type_class in current_schema_map.items():
        db_type_class = db_schema_map.get(col_name)
        
        if db_type_class is None: # Should not happen if key lists matched
            print(f"Schema comparison failed: Column '{col_name}' missing in DB schema map after key check.")
            return False

        # Direct type class equality check
        if db_type_class == current_type_class:
            continue

        # Check for subclass relationships or if they belong to the same broad category
        is_db_str = issubclass(db_type_class, sql_types.String)
        is_current_str = issubclass(current_type_class, sql_types.String)
        is_db_numeric = issubclass(db_type_class, sql_types.Numeric) # Catches Integer, Float, Numeric etc.
        is_current_numeric = issubclass(current_type_class, sql_types.Numeric)
        is_db_datetime = issubclass(db_type_class, sql_types.DateTime) # Catches DateTime, Date, Time
        is_current_datetime = issubclass(current_type_class, sql_types.DateTime)
        is_db_boolean = issubclass(db_type_class, sql_types.Boolean)
        is_current_boolean = issubclass(current_type_class, sql_types.Boolean)


        # If both are string types, numeric types, datetime types, or boolean types, consider compatible
        if (is_db_str and is_current_str) or \
           (is_db_numeric and is_current_numeric) or \
           (is_db_datetime and is_current_datetime) or \
           (is_db_boolean and is_current_boolean):
            print(f"Info: Column '{col_name}' types ({db_type_class} vs {current_type_class}) are different classes but considered compatible by category.")
            continue
        
        # If not directly equal and not compatible by broad category, then schemas are incompatible
        print(f"Schema comparison failed: Type class for column '{col_name}' differs and not compatible. DB: {db_type_class}, Current: {current_type_class}")
        return False
        
    print("Schemas are considered compatible.")
    return True

def load_to_postgres(df: pd.DataFrame, type_map: Dict[str, Type], base_table_name: str,
                     db_uri: str = "postgresql://user:password@postgres_db:5432/mydb"):
    engine = create_engine(db_uri)
    # df.empty checks for no rows. list(df.columns) checks for no columns.
    if df.empty or not list(df.columns): 
        print(f"DataFrame is empty or has no columns. Skipping load for base table '{base_table_name}'.")
        if engine: engine.dispose()
        return
    
    # Ensure all columns in df are present in type_map before generating sqlalchemy_schema
    # This is important because get_sqlalchemy_schema_map iterates over df.columns
    for col in df.columns:
        if col not in type_map:
            print(f"Warning: Column '{col}' from DataFrame is not in type_map. It will be defaulted to String for schema generation.")
            # type_map[col] = str # Optionally modify type_map, but get_sqlalchemy_schema_map handles default

    current_sqlalchemy_schema_map = get_sqlalchemy_schema_map(type_map, list(df.columns))
    final_table_name = base_table_name
    
    try:
        inspector = sql_inspect(engine)
        table_exists = inspector.has_table(base_table_name)

        if table_exists:
            print(f"Table '{base_table_name}' exists. Comparing schemas.")
            db_schema_map = get_db_table_schema_map(engine, base_table_name)
            
            if not db_schema_map: 
                print(f"Warning: Table '{base_table_name}' exists but schema could not be retrieved (empty db_schema_map). Proceeding to create new versioned table.")
                # Fallthrough to create new versioned table logic
                timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
                final_table_name = f"{base_table_name}_{timestamp}"
                print(f"Creating new table '{final_table_name}' due to schema retrieval issue for existing table.")
                df.to_sql(final_table_name, engine, if_exists='fail', index=False, dtype=current_sqlalchemy_schema_map)

            elif are_schemas_compatible(db_schema_map, current_sqlalchemy_schema_map):
                print(f"Schemas match. Appending data to '{base_table_name}'.")
                df.to_sql(base_table_name, engine, if_exists='append', index=False, dtype=current_sqlalchemy_schema_map)
            else: 
                print(f"Schemas do not match for '{base_table_name}'. Creating a new versioned table.")
                timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
                final_table_name = f"{base_table_name}_{timestamp}"
                print(f"Creating new table '{final_table_name}'.")
                df.to_sql(final_table_name, engine, if_exists='fail', index=False, dtype=current_sqlalchemy_schema_map)
        else: 
            print(f"Table '{base_table_name}' does not exist. Creating it.")
            df.to_sql(base_table_name, engine, if_exists='fail', index=False, dtype=current_sqlalchemy_schema_map)
            # final_table_name is already base_table_name

        print(f"Successfully processed data for table '{final_table_name}'.")

    except SQLAlchemyError as e:
        print(f"Database error occurred: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during data loading: {e}")
        raise
    finally:
        if engine: 
            engine.dispose()

if __name__ == '__main__':
    # Test setup
    # IMPORTANT: For local testing, ensure PostgreSQL is running and accessible.
    # Replace 'localhost' with your Docker host IP if running tests from outside a container that can see 'localhost' as the DB host.
    # If running this script INSIDE the airflow container (e.g. for dev tests), 'postgres_db' might be resolvable.
    test_db_uri = "postgresql://user:password@localhost:5432/mydb" 
    
    # Create a temporary engine for cleanup and basic connectivity check
    try:
        temp_engine_init = create_engine(test_db_uri, connect_args={'connect_timeout': 5})
        with temp_engine_init.connect() as conn:
            conn.execute(text("SELECT 1")) # Test connectivity
        print("Successfully connected to the database for setup.")
        temp_engine_init.dispose()
    except Exception as e:
        print(f"CRITICAL: Could not connect to PostgreSQL at {test_db_uri}. Ensure DB is running and accessible.")
        print(f"Error details: {e}")
        print("Skipping tests.")
        exit() # Exit if DB is not available for tests.

    # Clean up previous test tables
    try:
        temp_engine_cleanup = create_engine(test_db_uri)
        inspector = sql_inspect(temp_engine_cleanup) 
        with temp_engine_cleanup.connect() as conn:
            table_names = inspector.get_table_names()
            dropped_tables = False
            for t_name in table_names:
                if t_name.startswith("test_data") or t_name.startswith("test_empty") or t_name.startswith("test_data_ordered"):
                    conn.execute(text(f"DROP TABLE IF EXISTS {t_name} CASCADE;"))
                    print(f"Dropped table {t_name}")
                    dropped_tables = True
            if dropped_tables:
                 conn.commit() 
            print("Cleaned up old test tables.")
        temp_engine_cleanup.dispose()
    except Exception as e:
        print(f"Error cleaning up tables: {e}. This might be okay if it's the first run.")


    # Test Case 1: New table creation
    data1 = {'id': [1, 2], 'name': ['Alice', 'Bob'], 'value': [10.5, 20.3], 'active': [True, False]}
    df1 = pd.DataFrame(data1)
    type_map1 = {'id': int, 'name': str, 'value': float, 'active': bool}
    print("\n--- Test Case 1: New table creation ---")
    load_to_postgres(df1, type_map1, "test_data", db_uri=test_db_uri)

    # Test Case 2: Appending to existing table (same schema)
    data2 = {'id': [3, 4], 'name': ['Charlie', 'David'], 'value': [30.1, 40.2], 'active': [False, True]}
    df2 = pd.DataFrame(data2)
    print("\n--- Test Case 2: Appending to existing table (same schema) ---")
    load_to_postgres(df2, type_map1, "test_data", db_uri=test_db_uri)

    # Test Case 3: Different schema (new column 'category'), new versioned table
    data3 = {'id': [5, 6], 'name': ['Edward', 'Fiona'], 'value': [50.0, 60.0], 'category': ['A', 'B'], 'active': [True, True]}
    df3 = pd.DataFrame(data3)
    type_map3 = {'id': int, 'name': str, 'value': float, 'category': str, 'active': bool}
    print("\n--- Test Case 3: Different schema (new column), new versioned table ---")
    load_to_postgres(df3, type_map3, "test_data", db_uri=test_db_uri)

    # Test Case 4: Different schema (changed type for 'value' from float to str), new versioned table
    # Recreate 'test_data' with original schema (float for 'value') to ensure type conflict for this test
    print("\n--- Test Case 4 Setup: Recreating 'test_data' with original float schema for 'value' ---")
    try:
        temp_engine_recreate = create_engine(test_db_uri)
        with temp_engine_recreate.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_data CASCADE;"))
            conn.commit()
        temp_engine_recreate.dispose()
        load_to_postgres(df1.copy(), type_map1.copy(), "test_data", db_uri=test_db_uri) 
        print("Recreated 'test_data' successfully for Test Case 4.")
    except Exception as e:
        print(f"Error recreating 'test_data' for Test Case 4: {e}")

    data4 = {'id': [7, 8], 'name': ['George', 'Hannah'], 'value': ["70.0", "80.0"], 'active': [False, False]} # value is string
    df4 = pd.DataFrame(data4)
    type_map4 = {'id': int, 'name': str, 'value': str, 'active': bool} # 'value' is str
    print("\n--- Test Case 4: Different schema (type change 'value' to str), new versioned table ---")
    load_to_postgres(df4, type_map4, "test_data", db_uri=test_db_uri)


    # Test Case 5: Empty DataFrame
    print("\n--- Test Case 5: Empty DataFrame ---")
    empty_df_with_cols = pd.DataFrame(columns=['id', 'name', 'value', 'active'])
    empty_df_no_cols = pd.DataFrame()
    type_map_for_empty = {'id': int, 'name': str, 'value': float, 'active': bool}
    
    load_to_postgres(empty_df_with_cols, type_map_for_empty, "test_empty_with_cols", db_uri=test_db_uri)
    load_to_postgres(empty_df_no_cols, type_map_for_empty, "test_empty_no_cols", db_uri=test_db_uri)
    # Try loading empty df to an existing table
    load_to_postgres(empty_df_with_cols, type_map1, "test_data", db_uri=test_db_uri)


    # Test Case 6: Schema with different column order
    print("\n--- Test Case 6 Setup: Creating 'test_data_ordered' with id, name, value, active ---")
    data5_ordered = {'id': [9, 10], 'name': ['Ivy', 'Jack'], 'value': [90.1, 100.2], 'active': [True, False]}
    df5_ordered = pd.DataFrame(data5_ordered)[['id', 'name', 'value', 'active']] # Explicit order
    type_map5_ordered = {'id': int, 'name': str, 'value': float, 'active': bool}
    load_to_postgres(df5_ordered, type_map5_ordered, "test_data_ordered", db_uri=test_db_uri)

    # DataFrame with columns in a different order: name, id, value, active
    # get_sqlalchemy_schema_map uses df.columns, so the generated schema will be in this new order.
    data5_reordered_cols_df = {'name': ['Kevin', 'Laura'], 'id': [11, 12],'value': [110.0, 120.0], 'active': [False, True]}
    df5_reordered_cols_df = pd.DataFrame(data5_reordered_cols_df)[['name', 'id', 'value', 'active']] # New order
    # type_map must reflect the columns and types, but its order doesn't dictate schema order to to_sql.
    # The order for schema generation comes from df.columns.
    type_map5_for_reordered_df = {'name': str, 'id': int, 'value': float, 'active': bool} 
    print("\n--- Test Case 6: DataFrame with different column order, new versioned table ---")
    load_to_postgres(df5_reordered_cols_df, type_map5_for_reordered_df, "test_data_ordered", db_uri=test_db_uri)

    # Test Case 7: Compatible types but different classes (e.g. VARCHAR vs String)
    print("\n--- Test Case 7: Compatible types (e.g. VARCHAR vs String) ---")
    try:
        temp_engine_case7 = create_engine(test_db_uri)
        with temp_engine_case7.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_data_specific_varchar CASCADE;"))
            # Create table with specific VARCHAR length for 'name' and TEXT for 'description'
            conn.execute(text("CREATE TABLE test_data_specific_varchar (id INTEGER, name VARCHAR(50), description TEXT);"))
            conn.commit()
            print("Created table 'test_data_specific_varchar' with VARCHAR(50) for 'name' and TEXT for 'description'.")
        temp_engine_case7.dispose()

        data7 = {'id': [1, 2], 'name': ['TestName1', 'TestName2'], 'description': ['Desc1', 'Desc2']}
        df7 = pd.DataFrame(data7)
        # type_map7 uses generic 'str', which maps to sql_types.String
        # This should be compatible with VARCHAR(50) and TEXT in the DB due to enhanced are_schemas_compatible
        type_map7 = {'id': int, 'name': str, 'description': str} 
        load_to_postgres(df7, type_map7, "test_data_specific_varchar", db_uri=test_db_uri)
    except Exception as e:
        print(f"Error in Test Case 7 setup or execution: {e}")

    print("\nAll test cases executed.")
