import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Example business rule
    if 'amount' in df.columns:
        # Ensure 'amount' is numeric before fillna, if not already handled by casting
        if pd.api.types.is_numeric_dtype(df['amount']):
            df['amount'] = df['amount'].fillna(0)
        else:
            # Handle cases where 'amount' might not be numeric after casting (e.g. all NaNs from coercion)
            # This might involve logging, raising an error, or converting then filling
            pass # Or df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
    return df
