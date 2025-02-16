# transform/sales_cleaner.py
import pandas as pd

def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and transforms the sales data DataFrame.
    """
    df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]

    return df.dropna()