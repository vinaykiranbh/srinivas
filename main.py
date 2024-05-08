import os
import logging
from file_processor import FileProcessor

def setup_logging():
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(level=log_level, filename='dataprocessor.log', filemode='a',
                        format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    setup_logging()
    logging.info("Starting processing.")
    processor_args = {
        "directory": "sourcefiles",
        "archive_dir": "archive",
        "exception_dir": "exceptions",
        "output_dir": "output"
    }
    FileProcessor.process_files(**processor_args)
    logging.info("Processing completed.")

if __name__ == "__main__":
    main()
