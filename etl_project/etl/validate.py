import pandas as pd
from pydantic import create_model, ValidationError, BaseModel
from typing import Dict, Any, Type

def auto_cast_and_validate(df: pd.DataFrame, type_map: Dict[str, Type]):
    errors = []
    casted_df = df.copy()

    # Auto-casting based on type_map
    for col, expected_dtype in type_map.items():
        if col not in df.columns:
            errors.append({"column": col, "error_type": "MissingColumn", "message": f"Column '{col}' not found in DataFrame."})
            continue
        try:
            if expected_dtype == float:
                # Attempt to convert to numeric, coercing errors to NaN
                casted_df[col] = pd.to_numeric(df[col], errors='coerce')
            elif expected_dtype == str:
                # Convert to string type
                casted_df[col] = df[col].astype(str)
            elif expected_dtype == pd.Timestamp: # Example if using pd.Timestamp
                casted_df[col] = pd.to_datetime(df[col], errors='coerce')
            # Add other type conversions as needed
        except Exception as e:
            errors.append({"column": col, "error_type": "CastingError", "message": str(e)})
    
    # Create Pydantic model dynamically
    # Pydantic field definitions are (type, default_value)
    # Using `Any` as default for Pydantic model to handle NaNs gracefully after casting
    pydantic_fields = {
        k: (v, None) if v != float else (v, float('nan')) # Allow NaN for floats
        for k, v in type_map.items() if k in casted_df.columns
    }
    
    # Ensure all columns in casted_df are in pydantic_fields, default to str if not in type_map
    for col in casted_df.columns:
        if col not in pydantic_fields:
            pydantic_fields[col] = (str, None) # Default unmapped columns to str

    # Handle empty fields dictionary
    if not pydantic_fields:
        # This case might occur if df is empty or type_map is empty/mismatched
        # Return early or handle as appropriate for the pipeline
        return casted_df, errors, []


    DataModel = create_model(
        "AutoSchema", 
        **pydantic_fields
    )
    
    validation_errors = []
    valid_rows = []

    for idx, row in casted_df.iterrows():
        try:
            # Pydantic expects a dict. Handle NaNs by converting them to None for validation
            # as Pydantic v2 has stricter type checking for None vs. specific types.
            row_dict = row.where(pd.notnull(row), None).to_dict()
            
            # Filter row_dict to only include keys defined in DataModel to prevent unexpected field errors
            filtered_row_dict = {key: value for key, value in row_dict.items() if key in pydantic_fields}

            DataModel(**filtered_row_dict)
            valid_rows.append(row)
        except ValidationError as e:
            validation_errors.append({"index": idx, "errors": e.errors()})
        except Exception as e: # Catch any other unexpected errors during row processing
             validation_errors.append({"index": idx, "errors": [{"loc": ("unknown",), "msg": str(e), "type": "runtime_error"}]})


    # Potentially return a DataFrame of only valid rows
    # validated_df = pd.DataFrame(valid_rows, columns=casted_df.columns)
    # For now, returning the casted_df which includes rows with NaNs from coercion
    return casted_df, errors, validation_errors
