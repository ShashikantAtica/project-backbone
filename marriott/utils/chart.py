import os
import psycopg2
import tabula
import json
import logging
from dotenv.main import load_dotenv

load_dotenv()

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='logfile.log')
from utils.db import db_config


def read_table_from_pdf(path, page):
    try:
        table = tabula.read_pdf(path, pages=page)

        if len(table) == 0:
            raise Exception(f'Cannot read table from PDF: {path}')

        chart = table[0]
        chart_dict = chart.to_dict()

        return chart_dict

    except tabula.errors.JavaNotFoundError as e:
        logging.error(f"Java not found. Make sure Java is installed and in the system path: {e}")
        return None

    except Exception as e:
        logging.error(f"Failed to read table from PDF '{path}' due to: {e}")
        return None


def fetch_data_from_table(pms_name):
    try:
        conn = db_config.get_db_connection()
        res = conn.execute(f"""SELECT * FROM tbl_properties WHERE "pmsName" = '{pms_name}';""")
        result = res.fetchall()
        conn.close()

        return result

    except psycopg2.Error as e:
        logging.error(f"Error connecting to the database: {e}")
        return None

    except Exception as e:
        logging.error(f"Failed to fetch data from the database due to: {e}")
        return None


def insert_data_into_table(property_code, marriott_json):
    try:
        json_str = json.dumps(marriott_json)
        conn = db_config.get_db_connection()
        conn.execute(f"""UPDATE "tbl_properties" SET "marriott_json" = '{json_str}' WHERE "propertyCode" = '{property_code}';""")
        conn.close()

        logging.info(f"Data inserted successfully for property code: {property_code}")

    except psycopg2.Error as e:
        logging.error(f"Error updating data in the table: {e}")

    except Exception as e:
        logging.error(f"Failed to update data in the table due to: {e}")


def get_pdf_files_in_folder(folder_path):
    pdf_files = [file for file in os.listdir(folder_path) if file.endswith('.pdf')]
    return pdf_files


def match_filename_with_propertycode(filename, rows):
    for row in rows:
        property_code = row[2]

        if property_code == filename:
            return True

    return False


def multiple_pdf(folder_path, page=1, PMS_NAME=""):
    pdf_files = get_pdf_files_in_folder(folder_path)

    if not pdf_files:
        logging.warning(f"No PDF files found in the folder: {folder_path}")
    else:
        rows = fetch_data_from_table(PMS_NAME)

        if rows is not None:
            for pdf_file in pdf_files:
                file_name, file_extension = pdf_file.rsplit('.', 1)

                if match_filename_with_propertycode(file_name, rows):
                    pdf_file_path = os.path.join(folder_path, pdf_file)
                    result = read_table_from_pdf(pdf_file_path, page)

                    if result is not None:
                        marrirote_json = result
                        print(marrirote_json)
                        insert_data_into_table(file_name, marrirote_json)


if __name__ == '__main__':
    # folder_path = r'C:\test-project-backbone\marriott\utils'
    PMS_NAME = "Marriott"
    PROJECT_PATH = os.environ['PROJECT_PATH']
    folder_path = f"{PROJECT_PATH}\\marriott\\utils"
    multiple_pdf(folder_path, page=1, PMS_NAME=PMS_NAME)
