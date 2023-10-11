import csv
import os
import sys

sys.path.append("..")
import time
import arrow
import pandas as pd
from utils.secrets.SecretManager import get_secret_dict

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils.db import db_config
from utils.db import db_models


def bulk_insert_bestrev_total_forecast(propertyCode, total_forecast_list):
    current_date = arrow.now()
    print("current_date :: ", current_date)

    pulledDateValue = "'" + current_date.format("YYYY-MM-DD") + "'"
    pulledDate = '"pulledDate"'

    propertyCodeValue = "'" + propertyCode + "'"
    propertyCode = '"propertyCode"'

    DB_STATUS = "'FINISHED'"

    conn = db_config.get_db_connection()
    result = conn.execute(
        f'SELECT * from "tbl_pullDate" where {pulledDate} = {pulledDateValue} and {propertyCode} = {propertyCodeValue} and "status"={DB_STATUS} ORDER BY id DESC LIMIT 1;')
    conn.close()

    pullDateIdValue = None
    try:
        pullDateIdValue = result.first()['id']
    except:
        print("result none")

    if pullDateIdValue is not None:
        pullDateId = '"pullDateId"'
        pullDateIdValue = "'" + str(pullDateIdValue) + "'"

        # Delete existing data of total forecast
        conn = db_config.get_db_connection()
        conn.execute(
            f'DELETE from bestrev_total_forecast where {pullDateId} = {pullDateIdValue};')
        conn.close()
        print("DELETE OLD DATA!!!", pullDateIdValue)
    else:
        print("Not previous data!!!")

    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.bestrev_total_forecast_model.insert(), total_forecast_list)
    conn.close()
    print("Data imported")


def BestRev_Pms(row):
    try:
        atica_property_code = row['atica_property_code']
        secret_name = row['gcp_secret']
        pullDateId = row['pullDateId']
        propertyCode = row['propertyCode']

        save_dir = os.path.abspath(f'reports/')
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        folder_name = "./reports/"

        print(f"{atica_property_code} secret_name :: ", secret_name)
        json_dict = get_secret_dict(secret_name)
        print(f"{atica_property_code} Getting Secret Details")
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

        driver = webdriver.Chrome(options=chrome_options, executable_path="../chromedriver.exe")
        driver.maximize_window()

        print(f"{atica_property_code} Getting the Login Page")
        login_url = "https://bestrev.bwhhotelgroup.com/"
        driver.get(login_url)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//h5[contains(@class, 'user-details__name')]")))
        driver.find_element(By.XPATH, "//h5[contains(@class, 'user-details__name')]").click()

        print(f"{atica_property_code} Entering the Username")
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.NAME, "identifier")))
        driver.find_element(By.NAME, "identifier").send_keys(username)
        driver.find_element(By.XPATH, '//*[@id="form20"]/div[2]/input').click()

        print(f"{atica_property_code} Entering the Password")
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.NAME, "credentials.passcode")))
        driver.find_element(By.NAME, "credentials.passcode").send_keys(password)
        driver.find_element(By.XPATH, '//*[@id="form53"]/div[2]/input').click()

        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//h2[contains(@class, 'name')]")))
        if "Hello" in driver.page_source:
            print(f"{atica_property_code} Login Successful")
        else:
            print(f"{atica_property_code} Login Failed")
            return str(f"{atica_property_code} Login Failed")

        print(f"{atica_property_code} Getting the Total Forecast Report")

        driver.get('https://bestrev.bwhhotelgroup.com/my-view/metrics')
        time.sleep(20)
        driver.find_element(By.ID, "outlined-select").click()
        time.sleep(2)
        driver.find_element(By.XPATH, "//span[text()='All days - Next 365 days']").click()

        print(f"{atica_property_code} Exporting Total Forecast Report")
        time.sleep(20)
        driver.find_element(By.CSS_SELECTOR, 'a[href^="blob:"] button[aria-label="export"]').click()
        time.sleep(5)

        previous_forecast_path = os.path.join(save_dir, f'{propertyCode}_TotalForecast.csv')
        if os.path.exists(previous_forecast_path):
            os.remove(previous_forecast_path)

        files = os.listdir(save_dir)

        for file in files:
            if file.endswith('.csv'):
                downloaded_file = os.path.join(save_dir, file)
                new_file = os.path.join(save_dir, f'{propertyCode}_TotalForecast.csv')
                os.rename(downloaded_file, new_file)
        print(f"{atica_property_code} Total Forecast Exported Successfully")
        driver.quit()

        print(f"{atica_property_code} Modification of Total Forecast Report")
        new_file = os.path.join(save_dir, f'{propertyCode}_TotalForecast.csv')
        df = pd.read_csv(new_file, engine='python')
        df['Stay Date'] = pd.to_datetime(df['Stay Date'])
        df.insert(0, column="propertyCode", value=propertyCode)
        df.insert(1, column="pullDateId", value=pullDateId)
        headers = ["propertyCode", "pullDateId", "Alert", "Priority", "StayDate", "DayofWeek", "Favorite", "Event", "BestWesternRate", "RecommendedRate", "RatetoUpload",
                   "RecommendationStatus", "MarketRate", "AvailableRooms", "TransientCapacity", "TotalForecast_IncludesGroup", "OntheBooks_IncludesGroup",
                   "AverageDailyRate", "RevPAR", "Occupancy_IncludesGroup", "ForecastOccupancy_IncludesGroup"]
        df.to_csv(new_file, header=headers, index=False)
        print(f"{atica_property_code} Total Forecast Report Modified Successfully")

        total_forecast_file_path = f'{folder_name}{propertyCode}_TotalForecast.csv'

        check_total_forecast_file = os.path.isfile(total_forecast_file_path)

        error_msg = ""

        if not check_total_forecast_file:
            error_msg = error_msg + " Total Forecast file - N/A"

        if check_total_forecast_file:
            # Insert into Database
            total_forecast_result = csv.DictReader(open(f"{folder_name}{propertyCode}_TotalForecast.csv", encoding="utf-8"))
            total_forecast_result = list(total_forecast_result)
            print(len(total_forecast_result))
            bulk_insert_bestrev_total_forecast(propertyCode, total_forecast_result)
            print("TOTAL FORECAST DONE")

            update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
        else:
            update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=error_msg, IS_ERROR=True)
    except Exception as e:
        print(e)
        update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=f"Failed to pull report due to {e}", IS_ERROR=True)


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
    PMS_NAME = "'BestRev'"
    print("SCRIPT STARTED FOR BestRev")
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
            BestRev_Pms(row)
            print("SCRIPT DONE FOR BestRev")
        else:
            print("LAST_PULL_DATE_ID is NULL")
