import os
import logging
from datetime import datetime
import shutil


class Utilities:
    @staticmethod
    def ensure_directory_exists(directory):
        """Ensures that a directory exists; if not, it creates it."""
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

    @staticmethod
    def count_rows(file_path):
        """Counts the number of rows in a text file."""
        with open(file_path, 'r') as file:
            return sum(1 for line in file)

    @staticmethod
    def archive_files(source_file_path, archive_dir):
        """Archives processed files into a structured directory system organized by year."""
        current_year_str = datetime.now().strftime('%Y')
        year_based_archive_dir = os.path.join(archive_dir, current_year_str)
        ensure_directory_exists(year_based_archive_dir)
        shutil.move(source_file_path, os.path.join(year_based_archive_dir, os.path.basename(source_file_path)))
        logging.info(f"File {source_file_path} archived successfully.")

    @staticmethod
    def remove_special_characters(data):
        """
        Removes special characters from the data to standardize and clean the text fields.
        This is particularly useful for preparing data for analysis, reporting, or further processing stages.
        """
        try:
            
            data.replace({'[-,.#]': ''}, regex=True, inplace=True)
            return data
        except Exception as e:
            logging.error(f"Error removing special characters: {e}")
            return data

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
            logging.error(f"Error removing duplicates: {e}")
            return data
