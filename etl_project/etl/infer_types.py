import pandas as pd
from lightautoml.tasks import Task
from lightautoml.automl.presets.tabular_presets import TabularAutoML

def infer_types_lama(df: pd.DataFrame) -> dict:
    task = Task('reg') # Using 'reg' (regression) as a general task for type inference
    # A small timeout, as we are not training a model but inferring types
    automl = TabularAutoML(task=task, timeout=60) 
    
    # LightAutoML requires a target column for its operations.
    # We'll create a dummy target column and tell LAMA to use it.
    # This column will be removed after type inference.
    dummy_target_col = '__dummy_target__'
    df[dummy_target_col] = 0 # Initialize with a numeric value, e.g., 0 or 1

    # Define roles for LAMA. The key is the column name, value is its role.
    # All columns except our dummy target are features.
    roles = {col: 'feature' for col in df.columns if col != dummy_target_col}
    roles[dummy_target_col] = 'target'

    # Run a minimal fit_predict cycle to trigger type inference
    # We don't actually need the predictions.
    try:
        _ = automl.fit_predict(df.copy(), roles=roles) # Use a copy to avoid SettingWithCopyWarning
        guessed_roles = automl.reader.roles
        type_map = {}

        for col, role in guessed_roles.items():
            if col == dummy_target_col: # Skip our dummy target
                continue
            if role.name == 'numeric':
                type_map[col] = float
            elif role.name == 'category': # LAMA might infer 'category' for strings
                type_map[col] = str
            elif role.name == 'datetime':
                type_map[col] = str  # Or pd.Timestamp, depending on desired output
            else: # Default to string for other or unknown types
                type_map[col] = str
    finally:
        # Ensure the dummy target column is dropped even if an error occurs
        if dummy_target_col in df.columns:
            df.drop(columns=dummy_target_col, inplace=True)
    
    return type_map
