import csv
import os
import sys

sys.path.append("..")
import time
import arrow
import pandas as pd
from utils.secrets.SecretManager import get_secret_from_api as get_secret_dict

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select

from utils.db import db_config
from utils.db import db_models


def bulk_insert_choice_cancellation_list(propertyCode, cancellation_list, res_before, res_after):
    start_date = "'" + res_before.format("YYYY-MM-DD") + "'"
    end_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    print("start_date :: ", start_date)
    print("end_date :: ", end_date)

    # delete all data
    # current_date = arrow.now()
    # print("current_date :: ", current_date)
    # start_date = current_date.shift(days=-90)
    # print("start_date :: ", start_date)
    reservation = '"Arrival_Group"'
    db_propertyCode = "'" + propertyCode + "'"
    # current_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    # start_date = "'" + res_before.format("YYYY-MM-DD") + "'"

    # Delete existing data of cancellation list (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(
        f'DELETE from choice_cancellation_list where {reservation} between {start_date} and {end_date} and "propertyCode" = {db_propertyCode};')
    conn.close()

    # Add new data of cancellation list (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(db_models.choice_cancellation_list_model.insert(), cancellation_list)
    conn.close()


def choice_cancellation(row):
    atica_property_code = row['atica_property_code']
    pullDateId = row['pullDateId']
    propertyCode = row['propertyCode']
    platform = "PMS"

    username = None
    password = None

    folder_name = "./reports/"
    save_dir = os.path.abspath('reports/')
    driver = None
    try:
        print(f"Getting Secret for {atica_property_code}")
        json_dict = get_secret_dict(propertyCode, platform)
        print("res ::")
        username = json_dict['u']
        password = json_dict['p']

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--hide-scrollbars')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--window-size=1280,1000")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": save_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        service = Service('../chromedriver.exe')
        driver = webdriver.Chrome(options=chrome_options, service=service)
        driver.maximize_window()

        print(f"{atica_property_code} Script start for Choice Cancellation List")
        login_url = "https://www.choiceadvantage.com/choicehotels/sign_in.jsp"
        driver.get(login_url)

        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.NAME, "j_username")))
        driver.find_element(By.NAME, "j_username").send_keys(username)
        driver.find_element(By.NAME, "j_password").send_keys(password)
        driver.find_element(By.ID, 'greenButton').click()

        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.LINK_TEXT, "Run")))
        driver.find_element(By.LINK_TEXT, "Run").click()
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.LINK_TEXT, "Reports")))
        driver.find_element(By.LINK_TEXT, "Reports").click()

        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.ID, "CancellationListReport")))
        driver.find_element(By.ID, "CancellationListReport").click()

        start_date = row['res_before'].format('M/D/YYYY')
        end_date = row['res_after'].shift(days=-1).format('M/D/YYYY')
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.NAME, "activityDateType")))
        select = Select(driver.find_element(By.NAME, 'activityDateType'))
        select.select_by_value('cancel')
        start_date_field = driver.find_element(By.NAME, "startDatePastCurrent")
        start_date_field.clear()
        start_date_field.send_keys(start_date)
        end_date_field = driver.find_element(By.NAME, "endDatePastCurrent")
        end_date_field.clear()
        end_date_field.send_keys(end_date)

        csv_input = driver.find_element(By.NAME, "CSV")
        driver.execute_script("arguments[0].setAttribute('value', 'true');", csv_input)
        print(f"{atica_property_code} Choice Cancellation List report generated")

        filepath = os.path.join(f'{folder_name}report.csv')
        if os.path.exists(filepath):
            os.remove(filepath)
        submit_button = driver.find_element(By.ID, "doSubmit")
        submit_button.click()
        while not os.path.exists(filepath):
            time.sleep(1)
        time.sleep(5)
        driver.quit()

        df = pd.read_csv(f'{folder_name}report.csv', skiprows=1)
        new_column_names = ["Account", "Guest_Name", "Arrival_Group", "Nights", "Rate_Plan", "GTD", "Source", "Rm_Type", "Cxl_Code", "Cxl_Date", "Cxl_Clk"]
        df.columns = new_column_names
        df.dropna(inplace=True, how="all")
        df.insert(0, column="propertyCode", value=propertyCode)
        df.insert(1, column="pullDateId", value=pullDateId)
        df['Account'] = df['Account'].fillna(0).astype(int)
        df['Nights'] = df['Nights'].fillna(0).astype(int)
        df.to_csv(f'{folder_name}{propertyCode}_Cancellation_List.csv', index=False)
        if os.path.exists(f'{folder_name}report.csv'):
            os.remove(f'{folder_name}report.csv')
        print(f"{atica_property_code} Choice Cancellation List report pulled successfully")

        cancellation_list_file_path = f'{folder_name}{propertyCode}_Cancellation_List.csv'

        check_cancellation_list_file = os.path.isfile(cancellation_list_file_path)

        error_msg = ""

        if not check_cancellation_list_file:
            error_msg = error_msg + " Cancellation List file - N/A"

        if check_cancellation_list_file:
            cancellation_list_result = csv.DictReader(open(cancellation_list_file_path, encoding="utf-8"))
            cancellation_list_result = list(cancellation_list_result)
            print(len(cancellation_list_result))
            bulk_insert_choice_cancellation_list(propertyCode, cancellation_list_result, row['res_before'], row['res_after'])
            print("CANCELLATION LIST DONE")

            update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
        else:
            update_into_pulldate(pullDateId, ERROR_NOTE=error_msg, IS_ERROR=True)

    except Exception as e:
        if driver:
            driver.quit()
        msg = f"[{atica_property_code}] failed due to {e}"
        print(msg)
        update_into_pulldate(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)
        return 0


def insert_into_pulldate(PROPERTY_CODE, PULLED_DATE):
    LAST_PULL_DATE_ID = None
    DB_PROPERTY_CODE = "'" + PROPERTY_CODE + "'"
    DB_PULLED_DATE = "'" + str(PULLED_DATE) + "'"
    DB_STATUS = "'INPROGRESS'"
    query_string = f'INSERT INTO "tbl_pullDate" ("propertyCode", "pulledDate", "status") VALUES ({DB_PROPERTY_CODE}, {DB_PULLED_DATE}, {DB_STATUS}) RETURNING id; '
    conn = db_config.get_db_connection()
    try:
        result = conn.execute(query_string)
        LAST_PULL_DATE_ID = result.fetchone()['id']
        print(LAST_PULL_DATE_ID)
        conn.close()
        print("Added successfully!!!")
    except Exception as e:
        conn.close()
        error_message = str(e)
        print(error_message)
    return LAST_PULL_DATE_ID


def update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE, IS_ERROR):
    # Update entry into pull date table
    print("ERROR_NOTE :: ", ERROR_NOTE)
    DB_STATUS = ""
    if IS_ERROR:
        DB_STATUS = "'FAILED'"
    else:
        DB_STATUS = "'FINISHED'"
    DB_ERROR_NOTE = "'" + str(ERROR_NOTE) + "'"
    DB_UPDATED_AT = "'" + str(arrow.now()) + "'"
    DB_LAST_PULL_DATE_ID = "'" + str(LAST_PULL_DATE_ID) + "'"
    query_string = f'UPDATE "tbl_pullDate" SET status={DB_STATUS}, "updatedAt"={DB_UPDATED_AT}, "errorNote"={DB_ERROR_NOTE} WHERE "id"={DB_LAST_PULL_DATE_ID};'
    conn = db_config.get_db_connection()
    try:
        conn.execute(query_string)
        conn.close()
        print("Updated successfully!!!")
    except Exception as e:
        conn.close()
        error_message = str(e)
        print(error_message)


if __name__ == '__main__':
    # Get all property using brand
    PMS_NAME = "'Choice'"
    print(f"SCRIPT STARTED FOR {PMS_NAME}")
    conn = db_config.get_db_connection()
    result = conn.execute(f'SELECT * FROM tbl_properties WHERE "pmsName" = {PMS_NAME};')
    conn.close()
    print(result)
    print("Fetched successfully")
    for item in result:

        PROPERTY_ID = item['id']
        PROPERTY_CODE = item['propertyCode']
        EXTERNAL_PROPERTY_CODE = item['externalPropertyCode']
        PROPERTY_SECRET = item['propertySecret']
        PMS_NAME = item['pmsName']
        RES_AFTER = item['resAfter']
        RES_BEFORE = item['resBefore']
        OCC_AFTER = item['occAfter']
        OCC_BEFORE = item['occBefore']
        CURRENT_DATE = arrow.now()
        PULLED_DATE = CURRENT_DATE.date()

        # Add entry into pull date table
        LAST_PULL_DATE_ID = insert_into_pulldate(PROPERTY_CODE, PULLED_DATE)

        if LAST_PULL_DATE_ID is not None:
            if EXTERNAL_PROPERTY_CODE is None:
                EXTERNAL_PROPERTY_CODE = ""
            row = {
                'atica_property_code': '' + PMS_NAME + '_' + EXTERNAL_PROPERTY_CODE,
                'external_property_code': EXTERNAL_PROPERTY_CODE,
                'gcp_secret': PROPERTY_SECRET,
                'property_type': PMS_NAME,
                'current_date': CURRENT_DATE,
                'res_before': CURRENT_DATE.shift(days=-RES_BEFORE),
                'res_after': CURRENT_DATE.shift(days=+RES_AFTER),
                'occ_before': CURRENT_DATE.shift(days=-OCC_BEFORE),
                'occ_after': CURRENT_DATE.shift(days=+OCC_AFTER),
                "propertyCode": PROPERTY_CODE,
                "pullDateId": LAST_PULL_DATE_ID
            }
            print("row :: ", row)
            choice_cancellation(row)
            print("SCRIPT DONE FOR Choice Cancellation List")
        else:
            print("LAST_PULL_DATE_ID is NULL")
