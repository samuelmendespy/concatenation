import os
from .extract.excel_loader import load_excel
from .transform.sales_cleaner import clean_sales_data
from .load.db_writer import save_to_database
from .utils.logger import logger

def run_etl_pipeline(data_folder: str):
    """
    Orchestrates the ETL process for all Excel files in the specified folder.
    """
    excel_files = [f for f in os.listdir(data_folder) if f.endswith('.xlsx') and not f.startswith('~')]

    if not excel_files:
        logger.warning(f"No Excel files found in {data_folder}. Skipping ETL.")
        return

    all_cleaned_data = []

    for file_name in excel_files:
        file_path = os.path.join(data_folder, file_name)
        logger.info(f"Starting ETL for file: {file_path}")

        try:
            raw_df = load_excel(file_path)
            clean_df = clean_sales_data(raw_df)
            all_cleaned_data.append(clean_df)
            logger.info(f"Successfully processed {file_name}")
        except Exception as e:
            logger.error(f"Error processing {file_name}: {e}")

    if all_cleaned_data:
        final_df = pd.concat(all_cleaned_data, ignore_index=True)
        save_to_database(final_df)
        logger.info("All processed data loaded to database.")
    else:
        logger.info("No data to load to database after processing files.")

if __name__ == '__main__':
    DATA_RAW_FOLDER = 'data/raw'
    run_etl_pipeline(DATA_RAW_FOLDER)