#---------------------------------------------------------
#                    ____    _        _   
#     ___ _____   __ |___ \  | |___  _| |_ 
#    / __/ __\ \ / /   __) | | __\ \/ / __|
#     (__\__ \\ V /   / __/  | |_ >  <| |_ 
#    \___|___/ \_/   |_____|  \__/_/\_\\__|
# 
#----------------------------------------------------------


import pandas as pd
import shutil
import logging
import os
from datetime import datetime
from glob import glob
from tqdm import tqdm
import argparse
# from Common import ConnectionHelper
# from Common import GlobalConfig

def setup():
    logs_directory = "logs"
    os.makedirs(logs_directory, exist_ok=True)
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"{logs_directory}/dataprocessor_{current_time}.log"
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(level=log_level, filename=log_filename, filemode='a',
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Logging is configured.")
    pd.set_option('display.max_columns', None)
    
setup()


def db_connection():
    try:        


        # odbc_connection, odbc_cursor = ConnectionHelper.GetSparkOdbcConnection("SparkProd2_ODBC",GlobalConfig.spark_username1, GlobalConfig.spark_password1, None)
        environment = "dev"  # "prod"
        spark_dsn = GlobalConfig.spark_dsns[environment]
        spark_username = GlobalConfig.spark_users[environment]
        spark_password = GlobalConfig.spark_password(spark_username)
        odbc_connection, odbc_cursor = ConnectionHelper.GetSparkOdbcConnection(spark_dsn, spark_username, spark_password, GlobalConfig.email_info)
        return odbc_cursor
    except Exception as e:
        logging.error(f"Failed to connect to database spark environment: {e}")
        exit(1)
# odbc_cursor=db_connection()

class DataProcessor:
    


    @staticmethod
    def extract_date_range(source_file_path):
        """
        Extracts the date range for comparison from the source file's header.
        Determines whether data from this file requires comparison with any previously processed files based on the date.
        Used to identify relevant previous output files for duplicate checks.
        """
        try:
            with open(source_file_path, 'r') as file:
                lines = file.readlines()
                file_name = lines[0].split(',')[0].strip().upper().replace(' ', '_')
                
                date_str = lines[2].split(',')[0].strip().upper().replace('RUN DATE: ', '').strip().split(' ')[0].replace('-', '_').upper()
                run_date = datetime.strptime(date_str.replace('_', '-'), '%b-%d-%y')
                output_name = f"{file_name}_{date_str}"
                required_comparisons = []

                if run_date.month == 1 and run_date.day == 1:
                    logging.info("Processing January 1st file. No previous file comparison needed.")
                    return False, output_name, required_comparisons
                else:
                    current_year = run_date.year
                    for month in range(1, run_date.month + (1 if run_date.day > 1 else 0)):
                        for day in (1, 15):
                            if (month == run_date.month and day >= run_date.day):
                                break
                            comparison_date = datetime(current_year, month, day)
                            required_comparisons.append(comparison_date.strftime('%b_%d_%y'))
                    logging.info(f"File from {output_name} requires comparison with files from: {', '.join(required_comparisons)}")
                    return True, output_name, required_comparisons
        except Exception as e:
            logging.error(f"Method Failed: extract_date_range, Error: {e}")
            raise

    @staticmethod
    def read_data(source_file_path):
        """
        Reads data from a CSV file, skipping the initial rows that do not contain relevant data.
        This method ensures that only meaningful data rows are loaded into the DataFrame for processing.
        """
        try:
            logging.debug(f"Attempting to read data from {source_file_path}.")
            data = pd.read_csv(source_file_path, skiprows=5,keep_default_na=False,na_filter=False)

            if data.empty:
                raise ValueError(f"No data found in {source_file_path}")
            
            for col in data.columns:
                if 'Unnamed' in col:
                    new_col_name = f"extra_col_{col.split('_')[-1]}"
                    data.rename(columns={col:new_col_name},inplace=True)
            logging.info(f"Data read successfully from {source_file_path} with {data.shape[0]} rows.")
            return data
        except Exception as e:
            logging.error(f"Method Failed: read_data, Error: {e}")
            raise

    @staticmethod
    def lookup_records(tax_ids):
        """
        Method to look up records in a database using Spark SQL.
        Intended to enrich or validate data against an external database source.
        """        
        
        try:
            tax_id_str = ', '.join(f"'{str(tax_id)}'" for tax_id in tax_ids)
            query = f"""SELECT pers.ssn, pers.first_name, pers.last_name, pers.mid_name
                        FROM edr.org org
                        JOIN edr.pers pers ON org.tax_num_identif = pers.ssn
                        WHERE org.tax_num_identif IN ({tax_id_str});"""
            cursor = odbc_cursor.execute(query)
            results = cursor.fetchall()
            columns=[col[0] for col in cursor.description]
            return [ dict(zip(columns,result)) for result in results]
        except Exception as e:
            logging.error(f"Method Failed : lookup_records, Error: {e}")
            raise


    @staticmethod
    def correct_misalignment(data):
        """
        Corrects data misalignment in rows where the 'Organization Street Line1 Address' field is empty or does not contain alphabetical characters.
        This method ensures data integrity by shifting misaligned data into their correct column positions.
        """

        
        try:
            actual_columns = data.columns.tolist()
            start_shift_index = actual_columns.index('Organization Street Line2 Address')
            for index, row in data.iterrows():
                if pd.isna(row['Organization Street Line1 Address']) or not any(char.isalpha() for char in str(row['Organization Street Line1 Address'])):
                    for i in range(start_shift_index, len(actual_columns)):
                        if i > 0:
                            prev_col = actual_columns[i - 1]
                            current_col = actual_columns[i]
                            data.at[index, prev_col] = row[current_col]
                            data.at[index, current_col] = pd.NA
                    
                    last_col = actual_columns[-1]
                    if pd.isna(row[last_col]):
                        data.at[index, last_col] = pd.NA
            return data
        except Exception as e:
            logging.error(f"Method Failed: correct_misalignment, Error: {e}")
            raise

    @staticmethod
    def clean_and_handle_exceptions(data):
        """
        Processes data to identify and handle exceptions based on predefined conditions such as keywords in organization names,
        invalid addresses, or mismatches with database records for first and last names.
        """
        try:
            data = DataProcessor.correct_misalignment(data)
            data['exception'] = False
            data['comments'] = ''

            keywords = set(["CENTER", "INC", "LLC", "CARE", "COMMONS", "OFFICE", "KIDS", 
                            "LEARNING", "RANCH", "APARTMENTS", "KIDZ", "PROPERTIES", "BRIDGE",
                            "CLUB", "LP", "FRIENDS", "PRESCHOOL", "PARK"])
            
            tax_ids = data['Tax ID'].unique().tolist()
            # db_results = DataProcessor.lookup_records(tax_ids)
            # db_dict ={result['SSN']:result for result in db_results}

            for index, row in data.iterrows():
                keyword_exception = any(keyword in str(row['Organization First Name']).upper() for keyword in keywords)
                address_exception = pd.isna(row['Organization Street Line1 Address']) or str(row['Organization Street Line1 Address']).strip() == ''
                taxid_exception = '-' in str(row['Tax ID'])

                # db_record = db_dict.get(str(row['Tax ID']))

                
                # if db_record:
                #     data.at[index, 'Organization First Name'] =db_record['FIRST_NAME']
                #     data.at[index, 'Organization Middle Name'] =db_record['MID_NAME']
                #     data.at[index, 'Organization Last Name'] =db_record['LAST_NAME']
                #     data.at[index, 'comments'] += ' Names aligned with database.'


                if any([keyword_exception, address_exception, taxid_exception]):
                    data.at[index, 'exception'] = True
                    if keyword_exception:
                        data.at[index, 'comments'] += ' This is an Organisation TaxID.'
                    if address_exception:
                        data.at[index, 'comments'] += ' Missing Line1 Address.'
                    if taxid_exception:
                        data.at[index, 'comments'] += ' Invalid Tax ID.'

            exceptions = data[data['exception']].copy()
            clean_data = data[~data['exception']].copy()
            return clean_data, exceptions
        except Exception as e:
            logging.error(f"Method Failed: clean_and_handle_exceptions, Error: {e}")
            raise

    @staticmethod
    def remove_special_characters(data):
        """
        Removes special characters from the data to standardize and clean the text fields.
        This is particularly useful for preparing data for analysis, reporting, or further processing stages.
        """
        try:
            data.replace({'[,.#]': ''}, regex=True, inplace=True)
            return data
        except Exception as e:
            logging.error(f"Method Failed: remove_special_characters, Error: {e}")
            raise


    @staticmethod
    def remove_duplicates(data):
        """
        Identifies and removes duplicate records within the same dataset based on all columns.
        Helps maintain the uniqueness of records in the dataset to prevent redundant processing.
        """
        try:
            data.drop_duplicates(inplace=True)
            return data
        except Exception as e:
            logging.error(f"Method Failed: remove_duplicates, Error: {e}")
            raise

    @staticmethod
    def archive_files(source_file_path, archive_dir):
        """
        Archives processed files into a structured directory system organized by year.
        Ensures that source files are not reprocessed and are stored safely post-processing.
        """
        try:
            current_year_str = datetime.now().strftime('%Y')
            year_based_archive_dir = os.path.join(archive_dir, current_year_str)
            DataProcessor.ensure_directory_exists(year_based_archive_dir)
            shutil.move(source_file_path, os.path.join(year_based_archive_dir, os.path.basename(source_file_path)))
            logging.info(f"File {source_file_path} archived successfully.")
        except Exception as e:
            logging.error(f"Method Failed: archive_files, Error: {e}", exc_info=True)
            raise

    @staticmethod
    def write_output(data, output_dir, exceptions, exception_dir, date,source_file):
        """
        Writes processed data and exceptions into separate files in their respective directories.
        This method handles the output of clean data for downstream use and exception records for review or debugging.
        """
        try:
            current_year_str = datetime.now().strftime('%Y')
            year_based_output_dir = os.path.join(output_dir, current_year_str)
            year_based_exception_dir = os.path.join(exception_dir, current_year_str)
            DataProcessor.ensure_directory_exists(year_based_output_dir)
            DataProcessor.ensure_directory_exists(year_based_exception_dir)
            output_file_path = os.path.join(year_based_output_dir, f"output_{date}.txt")
            output_file_path2 = os.path.join(source_file, f"output_{date}.txt")
            exception_file_path = os.path.join(year_based_exception_dir, f"exceptions_{date}.csv")

            with open(output_file_path, 'w') as file:
                file.write(data)
            with open(output_file_path2, 'w') as file:
                file.write(data)

            if isinstance(exceptions, pd.DataFrame):
                exceptions.to_csv(exception_file_path, index=False)

            logging.info(f"Output and exceptions written successfully to {output_file_path} and {exception_file_path}.")
        except Exception as e:
            logging.error(f"Method Failed: write_output, Error: {e}", exc_info=True)
            raise

    @staticmethod
    def ensure_directory_exists(directory):
        """
        Checks if a directory exists and if not, creates it.
        Ensures that the file writing operations do not fail due to missing directories.
        """
        
        try:
            if not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
        except Exception as e:
            logging.error(f"Method Failed : ensure_directory_exists, Error: {e}")
            raise

    @staticmethod
    def format_output(data):
        """
        Formats the data into a fixed-width formatted string for each record.
        This format is used for consistent file outputs that can be easily read and processed by systems that require fixed-width inputs.
        """
        try:
            if not isinstance(data, pd.DataFrame):
                raise TypeError("Expected a DataFrame but got a different datatype.")
            data = data.fillna('')
            formatted_data = ''
            for index, row in data.iterrows():
                record_identifier = "PIC".ljust(3)
                ssn = str(row['Tax ID']).replace('-', '').ljust(9)[:9]
                first_name = str(row['Organization First Name']).ljust(16)[:16]
                middle_initial = (str(row['Organization Middle Name'])[:1] if pd.notna(row['Organization Middle Name']) else '').ljust(1)
                last_name = str(row['Organization Last Name']).ljust(30)[:30]
                address_line1 = str(row['Organization Street Line1 Address'])
                address_line2 = str(row['Organization Street Line2 Address']) if pd.notna(row['Organization Street Line2 Address']) else ''
                address = (address_line1 + " " + address_line2).strip().ljust(40)[:40]
                city = str(row['Organization City']).ljust(25)[:25]
                state = str(row['Organization State']).ljust(2)[:2]
                zip_code = str(row['Organization Zip code']).split('-')[0].ljust(5)[:5]
                zip_extension = (row['Organization Zip code'].split('-')[1] if '-' in str(row['Organization Zip code']) else '').ljust(4)[:4]
                try:
                    start_date = pd.to_datetime(row['Start Date of Contract'], format='%m/%d/%Y').strftime('%Y%m%d')
                except ValueError as e:
                    logging.error(f"Method Failed: format_output, Error: Error parsing date {row['Start Date of Contract']}: {e}")
                    start_date = 'InvalidDate'
                try:
                    
                    amount = f"{int(str(row['Amount of Contract']).replace('$', '').replace(',', '')):011d}".rjust(11)
                except ValueError:
                    logging.error(f"Method Failed: format_output, Error: Failed to convert amount for record: {row['Amount of Contract']}")
                    amount = 'InvalidAmount'
                contract_exp = ' ' * 8
                ongoing_contract = 'Y'
                blank = ' ' * 8
                formatted_row = (record_identifier + ssn + first_name + middle_initial + last_name +
                                address + city + state + zip_code + zip_extension +
                                start_date + amount + contract_exp + ongoing_contract + blank)
                formatted_data += formatted_row + '\n'
            return formatted_data
        except Exception as e:
            logging.error(f"Method Failed: format_output, Error: {e}")
            raise

    @staticmethod
    def process_data(directory, archive_dir, exception_dir, output_dir):
        """
        Main method to orchestrate the reading, processing, and outputting of data.
        Handles the full lifecycle of data processing from reading CSV files, cleaning data, checking for duplicates with previous outputs, and writing the final outputs.
        """
        try:
            logging.info("Starting data processing.")
            files = glob(os.path.join(directory, '*.csv'))
            for file in tqdm(files, desc="Processing files"):
                logging.info(f"Starting data processing for {file}")
                data = DataProcessor.read_data(file)
                if data is not None:
                    previous_data = pd.DataFrame()
                    comparison_req, date, comparison = DataProcessor.extract_date_range(file)
                    if comparison_req:
                        logging.info(f"Comparing data with files from {comparison}")
                        previous_paths = [os.path.join(output_dir,datetime.now().strftime('%Y'),f'output_{item.upper()}.txt') for item in comparison]
                        previous_data = DataProcessor.read_previous_outputs(previous_paths)
                        
                    data, exceptions = DataProcessor.clean_and_handle_exceptions(data)
                    data = DataProcessor.remove_special_characters(data)
                    data = DataProcessor.remove_duplicates(data)
                    data, found_duplicates = DataProcessor.check_for_duplicates(data, previous_data)
                    exceptions = pd.concat([exceptions, found_duplicates], ignore_index=True)
                    exceptions = exceptions.drop(columns=['exception','extra_col_Unnamed: 11'])
                    formatted_data = DataProcessor.format_output(data)            
                    DataProcessor.write_output(formatted_data, output_dir, exceptions, exception_dir, date,directory)
                    DataProcessor.archive_files(file, archive_dir)
                    source_row_count = data.shape[0] + exceptions.shape[0]
                    output_row_count = DataProcessor.count_rows(os.path.join(output_dir, datetime.now().strftime('%Y'), f"output_{date}.txt"))
                    exception_row_count = exceptions.shape[0]
                    logging.info(f"Validation for {file}: Source Rows = {source_row_count}, Output Rows = {output_row_count}, Exception Rows = {exception_row_count}")
                    assert source_row_count == output_row_count + exception_row_count, "Row count mismatch: Source does not equal Output + Exceptions"
                else:
                    raise ValueError(f"Data is Empty for {file}")
                logging.info(f"Data processing completed for {file}")
        except Exception as e:
            logging.critical(f"Critical error during processing: {e}")
            exit(1)

    @staticmethod
    def count_rows(file_path):
        """
        Counts the number of rows in a given text file.
        Useful for validation and reporting of how many records are written to output files.
        """
        
        try:
            with open(file_path, 'r') as file:
                return sum(1 for line in file)
        except Exception as e:
            logging.error(f"Method Failed : count_rows, Error: {e}")
            raise

    @staticmethod
    def read_previous_outputs(previous_files):
        """
        Reads previously generated output files into a DataFrame.
        These files are read using fixed-width formatting, which matches the structure of the output files.
        """
        previous_data = pd.DataFrame()
        
        try:
            previous_data = pd.DataFrame()
            colspecs = [
                (0, 12)
            ]
            colnames = ['PIC']
            for file_path in previous_files:
                if os.path.exists(file_path):
                    temp_data = pd.read_fwf(file_path, colspecs=colspecs, header=None, names=colnames)
                    previous_data = pd.concat([previous_data, temp_data], ignore_index=True)
                else:
                    logging.warning(f"File not found: {file_path}")
                    # raise FileExistsError(f"Method Failed : read_previous_outputs, Error: File not found: {file_path}")
            return previous_data
        except Exception as e:
            logging.error(f"Method Failed : read_previous_outputs, Error: {e}")
            return previous_data

    @staticmethod
    def check_for_duplicates(current_data, previous_data):
        """
        Compares current processing data against previously generated outputs to identify duplicate records based on the 'TaxID'.
        Duplicates are identified by temporarily prefixing 'PIC' to the 'TaxID' for compatibility with previous output formats.
        Each duplicate record is marked with a comment indicating it previously existed.
        """
        # Add 'PIC' prefix to the 'TaxID' of current data for comparison       
        
        try:
            if previous_data.empty or 'PIC' not in previous_data.columns:
                logging.info("No previous data to compare against for duplicates.")
                current_data['comments'] = None
                return current_data, pd.DataFrame()
            current_data['ModifiedTaxID'] = 'PIC' + current_data['Tax ID'].astype(str)
            previous_pics = previous_data['PIC'].tolist()
            current_data['is_duplicate'] = current_data['ModifiedTaxID'].isin(previous_pics)
            duplicates = current_data[current_data['is_duplicate']].copy()
            non_duplicates = current_data[~current_data['is_duplicate']]
            duplicates['comments'] = 'Exists in Previous file'
            non_duplicates.drop(columns=['ModifiedTaxID'], inplace=True)
            duplicates.drop(columns=['is_duplicate', 'ModifiedTaxID'], inplace=True)
            logging.info(f"Found {len(duplicates)} duplicates in the current dataset.")
            return non_duplicates, duplicates
        except Exception as e:
            logging.error(f"Method Failed : check_for_duplicates, Error: {e}")
            raise

def parse_args():
    parser = argparse.ArgumentParser(description="Process the arguments.")
    parser.add_argument('--directory', type=str, default='sourcefiles', help='Directory containing source files')
    parser.add_argument('--archive_dir', type=str, default='archive', help='Directory to archive processed files')
    parser.add_argument('--exception_dir', type=str, default='exceptions', help='Directory to store exceptions')
    parser.add_argument('--output_dir', type=str, default='output', help='Directory to store output files')
    return parser.parse_args()

# if __name__ == "__main__":
# # # """Example: python Csv2Txt_2.py --directory "\\dha\userdata\Projects\FTP-Download\DOF\DE542 Report\sourcefiles" --archive_dir "\\dha\userdata\Projects\FTP-Download\DOF\DE542 Report\archive" --exception_dir "\\dha\userdata\Projects\FTP-Download\DOF\DE542 Report\exceptions" --output_dir "\\dha\userdata\Projects\FTP-Download\DOF\DE542 Report\output"
# # # """    
#     try:
#         args = parse_args()
#         processor_args = {
#             "directory": args.directory,
#             "archive_dir": args.archive_dir,
#             "exception_dir": args.exception_dir,
#             "output_dir": args.output_dir
#         }
#         DataProcessor.process_data(**processor_args)
#     except Exception as e:
#         logging.critical(f"An error occurred during data processing: {str(e)}")
#         exit(1)
        
        
if __name__ == "__main__":
    processor_args = {
        "directory": "./sourcefiles",
        "archive_dir": "./archive",
        "exception_dir": "./exceptions",
        "output_dir": "./output"
    }
    DataProcessor.process_data(**processor_args)
