# load/db_writer.py
from .data_engine.config import DATABASE_URL
from sqlalchemy import create_engine
import pandas as pd 

def save_to_database(df: pd.DataFrame, table_name: str = 'sales'):
    """
    Saves the DataFrame to the database table.
    """
    try:
        engine = create_engine(DATABASE_URL)
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Data successfully loaded into '{table_name}' table in database.")
    except Exception as e:
        raise ConnectionError(f"Error loading data to database: {e}")