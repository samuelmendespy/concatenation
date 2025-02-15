# extract/excel_loader.py
import pandas as pd

def load_excel(file_path: str) -> pd.DataFrame:
    """
    Extracts data from a single Excel file using pandas.
    """
    try:
        df = pd.read_excel(file_path)
        return df
    except Exception as e:
        raise ConnectionRefusedError(f"Error loading Excel file {file_path}: {e}")