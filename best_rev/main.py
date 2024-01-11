import argparse
import csv
import os
import sys

from sqlalchemy import text

sys.path.append("..")
import time
import arrow
import pandas as pd
from utils.secrets.SecretManager import get_secret_from_api

import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

from utils.db import db_config
from utils.db import db_models
from sqlalchemy.dialects.postgresql import insert


def bulk_insert_bestrev_total_forecast(propertyCode, total_forecast_list, fore_start_date, fore_end_date):
    print("Data importing...")
    error_temp = ""
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.bestrev_total_forecast_model).values(total_forecast_list)
        conn.commit()
        stmt = stmt.on_conflict_do_update(
            index_elements=['uniqueKey'],
            set_={
                'pullDateId': stmt.excluded.pullDateId,
                'updatedAt': stmt.excluded.updatedAt,
                'updatedAtEpoch': stmt.excluded.updatedAtEpoch,
                'Alert': stmt.excluded.Alert,
                'Priority': stmt.excluded.Priority,
                'StayDate': stmt.excluded.StayDate,
                'DayofWeek': stmt.excluded.DayofWeek,
                'Favorite': stmt.excluded.Favorite,
                'Event': stmt.excluded.Event,
                'BestWesternRate': stmt.excluded.BestWesternRate,
                'RecommendedRate': stmt.excluded.RecommendedRate,
                'RatetoUpload': stmt.excluded.RatetoUpload,
                'RecommendationStatus': stmt.excluded.RecommendationStatus,
                'MarketRate': stmt.excluded.MarketRate,
                'AvailableRooms': stmt.excluded.AvailableRooms,
                'TransientCapacity': stmt.excluded.TransientCapacity,
                'TotalForecast_IncludesGroup': stmt.excluded.TotalForecast_IncludesGroup,
                'OntheBooks_IncludesGroup': stmt.excluded.OntheBooks_IncludesGroup,
                'AverageDailyRate': stmt.excluded.AverageDailyRate,
                'RevPAR': stmt.excluded.RevPAR,
                'Occupancy_IncludesGroup': stmt.excluded.Occupancy_IncludesGroup,
                'ForecastOccupancy_IncludesGroup': stmt.excluded.ForecastOccupancy_IncludesGroup,
            }
        )
        # Execute the insert statement
        conn.execute(stmt)
        conn.commit()
        conn.close()
        print("Data imported")
    except Exception as e:
        error_message = str(e)
        print(error_message)
        error_temp=error_message[:250]
    return error_temp


def BestRev_Pms(row):
    try:
        atica_property_code = row['atica_property_code']
        pullDateId = row['pullDateId']
        propertyCode = row['propertyCode']
        external_property_code = row['external_property_code']
        platform = "RMS"

        save_dir = os.path.abspath('reports/')
        folder_name = "./reports/"
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        total_forecast_file_path = f'{folder_name}{propertyCode}_TotalForecast.csv'
        if os.path.exists(total_forecast_file_path):
            os.remove(total_forecast_file_path)

        print(f"Getting Secret for {atica_property_code}")
        json_dict = get_secret_from_api(propertyCode, platform)
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
            "safebrowsing.enabled": True,
            "download.default_filename": "default_filename.csv"
        })

        service = Service('../chromedriver.exe')
        driver = webdriver.Chrome(options=chrome_options, service=service)
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

        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//h2[contains(@class, 'name')]")))
            if "Hello" in driver.page_source:
                print(f"{atica_property_code} Login Successful")
            else:
                print(f"{atica_property_code} Login Failed")
                update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=f"{atica_property_code} Login Failed", IS_ERROR=True)
                return str(f"{atica_property_code} Login Failed")
        except:
            print(f"{atica_property_code} Login Failed")
            update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=f"{atica_property_code} Login Failed", IS_ERROR=True)
            return str(f"{atica_property_code} Login Failed")

        print(f"{atica_property_code} Getting the Total Forecast Report")

        driver.get('https://bestrev.bwhhotelgroup.com/my-view/metrics')
        time.sleep(25)
        driver.find_element(By.ID, "outlined-select").click()
        time.sleep(2)
        driver.find_element(By.XPATH, "//span[text()='All days - Next 365 days']").click()

        print(f"{atica_property_code} Exporting Total Forecast Report")

        time.sleep(30)
        driver.find_element(By.CSS_SELECTOR, 'a[href^="blob:"] button[aria-label="export"]').click()

        file_present = False
        presence = False
        while not presence:
            files = os.listdir(save_dir)
            if len(files) > 0:
                presence = True
                break
        files = os.listdir(save_dir)
        while not file_present:
            for file in files:
                if os.path.exists(file.startswith(external_property_code + '_metrics')):
                    file_present = True
                    downloaded_file = os.path.join(save_dir, file)
                    os.replace(downloaded_file, os.path.join(save_dir, f'{propertyCode}_TotalForecast.csv'))
                    print("File renamed")
                    break

        print(f"{atica_property_code} Total Forecast Exported Successfully")
        driver.quit()

        createdAt = "'" + str(arrow.now()) + "'"
        updatedAt = "'" + str(arrow.now()) + "'"
        createdAtEpoch = int(arrow.utcnow().timestamp())
        updatedAtEpoch = int(arrow.utcnow().timestamp())

        print(f"{atica_property_code} Modification of Total Forecast Report")
        new_file = os.path.join(save_dir, f'{propertyCode}_TotalForecast.csv')
        df = pd.read_csv(new_file, engine='python')
        if df.empty:
            errorMessage = "Total Forecast file is empty"
            return errorMessage
        df['Stay Date'] = pd.to_datetime(df['Stay Date'])
        df.insert(0, column="propertyCode", value=propertyCode)
        df.insert(1, column="pullDateId", value=pullDateId)
        df.insert(2, column="createdAt", value=createdAt)
        df.insert(3, column="updatedAt", value=updatedAt)
        df.insert(4, column="createdAtEpoch", value=createdAtEpoch)
        df.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
        df.insert(6, column="uniqueKey", value=df['propertyCode'].astype(str) + "_" + df['Stay Date'].astype(str))            
        df['Priority'] = df['Priority'].fillna(0).astype(int)

        headers = ["propertyCode", "pullDateId", "createdAt", "updatedAt", "createdAtEpoch", "updatedAtEpoch", "uniqueKey", "Alert", "Priority", "StayDate", "DayofWeek", "Favorite", "BestWesternRate", "RecommendedRate", "RatetoUpload",
                   "RecommendationStatus", "MarketRate", "AvailableRooms", "TransientCapacity", "TotalForecast_IncludesGroup", "OntheBooks_IncludesGroup","AverageDailyRate", "RevPAR", "Occupancy_IncludesGroup", "ForecastOccupancy_IncludesGroup"]

        df.to_csv(new_file, header=headers, index=False)
        print(f"{atica_property_code} Total Forecast Report Modified Successfully")

        total_forecast_file_path = f'{folder_name}{propertyCode}_TotalForecast.csv'

        check_total_forecast_file = os.path.isfile(total_forecast_file_path)

        errorMessage = ""

        if not check_total_forecast_file:
            errorMessage = errorMessage + " Total Forecast file - N/A"

        if check_total_forecast_file:
            # Insert into Database
            total_forecast_result = csv.DictReader(
                open(f"{folder_name}{propertyCode}_TotalForecast.csv", encoding="utf-8"))
            total_forecast_result = list(total_forecast_result)
            print(len(total_forecast_result))
            fore_start_date = arrow.now().format("YYYY-MM-DD")
            fore_end_date = arrow.now().shift(days=+365).format("YYYY-MM-DD")
            
            error_temp = bulk_insert_bestrev_total_forecast(propertyCode, total_forecast_result, fore_start_date, fore_end_date)
            if(error_temp == ""):
                print("TOTAL FORECAST DONE")
                update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
            else:
                print("TOTAL FORECAST FAILED")
                errorMessage = errorMessage + " FORECAST Failed: " + error_temp

        else:
            update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=errorMessage, IS_ERROR=True)
    except Exception as e:
        print(e)
        update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=f"Failed to pull report due to {e}", IS_ERROR=True)


def insert_into_pulldate(PROPERTY_CODE, PULLED_DATE, PMS_NAME):
    LAST_PULL_DATE_ID = None
    DB_PMS_NAME = "'" + PMS_NAME + "'"
    DB_PROPERTY_CODE = "'" + PROPERTY_CODE + "'"
    DB_PULLED_DATE = "'" + str(PULLED_DATE) + "'"
    DB_STATUS = "'INPROGRESS'"
    query_string = f'INSERT INTO "tbl_pullDate" ("propertyCode", "pulledDate", "status","pmsName") VALUES ({DB_PROPERTY_CODE}, {DB_PULLED_DATE}, {DB_STATUS},{DB_PMS_NAME}) RETURNING id; '
    conn = db_config.get_db_connection()
    try:
        result = conn.execute(text(query_string))
        conn.commit()
        LAST_PULL_DATE_ID = result.fetchone()
        print(LAST_PULL_DATE_ID)
        conn.close()
        print("Added successfully!!!")
    except Exception as e:
        conn.close()
        error_message = str(e)
        print(error_message)
    return str(list(LAST_PULL_DATE_ID)[0])


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
        conn.execute(text(query_string))
        conn.commit()
        conn.close()
        print("Updated successfully!!!")
    except Exception as e:
        conn.close()
        error_message = str(e)
        print(error_message)


if __name__ == '__main__':
    PMS_NAME = "BestRev"
    print(f"[{PMS_NAME}] SCRIPT IS STARTING...")

    propertycode = None
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--propertycode", type=str, required=False, help="Type in the propertycode")
        args = parser.parse_args()
        propertycode = args.propertycode
        print(f"propertycode :: {propertycode}")
    except:
        pass

    result = None
    if propertycode is None:
        print("All properties run")
        conn = db_config.get_db_connection()
        res = conn.execute(text(f"""SELECT * FROM tbl_properties WHERE "pmsName" = '{PMS_NAME}';"""))
        result = res.fetchall()
        columns = res.keys()
        results_as_dict = [dict(zip(columns, row)) for row in result]
        conn.close()
        print("Fetched successfully")
    else:
        print(f"{propertycode} property run")
        conn = db_config.get_db_connection()
        res = conn.execute(text(f"""SELECT * FROM tbl_properties WHERE "pmsName" = '{PMS_NAME}' and "propertyCode" = '{propertycode}';"""))
        result = res.fetchall()
        columns = res.keys()
        results_as_dict = [dict(zip(columns, row)) for row in result]
        conn.close()
        print("Fetched successfully")

    if results_as_dict is not None and len(results_as_dict) > 0:
        print(f"Total Properties :: {len(results_as_dict)}")
        for item in results_as_dict:

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
            LAST_PULL_DATE_ID = insert_into_pulldate(PROPERTY_CODE, PULLED_DATE, PMS_NAME)

            if LAST_PULL_DATE_ID is not None:
                row = {
                    'atica_property_code': '' + PMS_NAME + '_' + PROPERTY_CODE,
                    'external_property_code': EXTERNAL_PROPERTY_CODE,
                    'gcp_secret': PROPERTY_SECRET,
                    'property_type': PMS_NAME,
                    'current_date': CURRENT_DATE,
                    'res_before': CURRENT_DATE.shift(days=-RES_BEFORE),
                    'res_after': CURRENT_DATE.shift(days=RES_AFTER),
                    'occ_before': CURRENT_DATE.shift(days=-OCC_BEFORE),
                    'occ_after': CURRENT_DATE.shift(days=OCC_AFTER),
                    "propertyCode": PROPERTY_CODE,
                    "pullDateId": LAST_PULL_DATE_ID
                }
                print("row :: ", row)
                BestRev_Pms(row)
            else:
                print("LAST_PULL_DATE_ID is NULL")
    else:
        print(f"Property not available in database!!!")
    print(f"[{PMS_NAME}] SCRIPT STOP!!!")
