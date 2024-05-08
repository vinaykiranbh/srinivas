import os
import glob
from utilities import Utilities
from data_processor import DataProcessor
import logging
import tqdm
from datetime import datetime
import pandas as pd

class FileProcessor:
    @staticmethod
    def process_data(directory, archive_dir, exception_dir, output_dir):
        """
        Main method to orchestrate the reading, processing, and outputting of data.
        Handles the full lifecycle of data processing from reading CSV files, cleaning data, checking for duplicates with previous outputs, and writing the final outputs.
        """
        logging.info("Starting data processing.")
       
        files = glob.glob(os.path.join(directory, '*.csv'))
        for file in tqdm(files, desc="Processing files"):
            logging.info(f"Starting data processing for {file}")
            data = DataProcessor.read_data(file)
            if data is not None:
                comparison_req, date, comparison = DataProcessor.extract_date_range(file)
                if comparison_req:
                    logging.info(f"Comparing data with files from {comparison}")
                    previous_paths = [os.path.join(output_dir,datetime.now().strftime('%Y'),f'output_{item.upper()}.txt') for item in comparison]
                    previous_data = read_previous_outputs(previous_paths)
                print(previous_data)
                data, exceptions = DataProcessor.clean_and_handle_exceptions(data)
                data = Utilities.remove_special_characters(data)
                data = Utilities.remove_duplicates(data)
                data, found_duplicates = DataProcessor.check_for_duplicates(data, previous_data)
                exceptions = pd.concat([exceptions, found_duplicates], ignore_index=True)
                
                formatted_data = DataProcessor.format_output(data)
                Utilities.write_output(formatted_data, output_dir, exceptions, exception_dir, date)
                Utilities.archive_files(file, archive_dir)
                
                source_row_count = data.shape[0] + exceptions.shape[0]
                output_row_count = Utilities.count_rows(os.path.join(output_dir, datetime.now().strftime('%Y'), f"output_{date}.txt"))
                exception_row_count = exceptions.shape[0]

                logging.info(f"Validation for {file}: Source Rows = {source_row_count}, Output Rows = {output_row_count}, Exception Rows = {exception_row_count}")
                assert source_row_count == output_row_count + exception_row_count, "Row count mismatch: Source does not equal Output + Exceptions"
            
            logging.info(f"Data processing completed for {file}")


# Functions to handle reading previous outputs and checking for duplicates would be here.
