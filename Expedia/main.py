#add X-token in env for otp api
import sys

from sqlalchemy import text

sys.path.append("..")
import argparse
import csv
import os
import time
import arrow
import pandas as pd
from utils.secrets.SecretManager import get_secret_from_api
from utils.secrets.SecretManager import get_otp_from_api

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from urllib.parse import urlparse, parse_qs


import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

from utils.db import db_config
from utils.db import db_models

def insert_into_pulldate_expedia(PROPERTY_CODE, PULLED_DATE):
    LAST_PULL_DATE_ID = None
    DB_PROPERTY_CODE = "'" + PROPERTY_CODE + "'"
    DB_PULLED_DATE = "'" + str(PULLED_DATE) + "'"
    DB_STATUS = "'INPROGRESS'"
    query_string = f'INSERT INTO "tbl_pullDate_expedia" ("propertyCode", "pulledDate", "status") VALUES ({DB_PROPERTY_CODE}, {DB_PULLED_DATE}, {DB_STATUS}) RETURNING id; '
    conn = db_config.get_db_connection()
    try:
        result = conn.execute(text(query_string))
        conn.commit()
        LAST_PULL_DATE_ID = result.fetchone()
        print("LAST_PULL_DATE_ID:", LAST_PULL_DATE_ID)
        conn.close()
        print("Added successfully!!!")
    except Exception as e:
        conn.close()
        error_message = str(e)
        print(error_message)
    return str(list(LAST_PULL_DATE_ID)[0])


def update_into_pulldate_expedia(LAST_PULL_DATE_ID, ERROR_NOTE, IS_ERROR):
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
    query_string = f'UPDATE "tbl_pullDate_expedia" SET status={DB_STATUS}, "updatedAt"={DB_UPDATED_AT}, "errorNote"={DB_ERROR_NOTE} WHERE "id"={DB_LAST_PULL_DATE_ID};'
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

def bulk_insert_expedia_revplus(propertyCode, revplus_list, lowest_date, highest_date):
    start_date = "'" + str(lowest_date) + "'"
    print("Lowest Date :: ", start_date)

    end_date = "'" + str(highest_date) + "'"
    print("Highest Date :: ", end_date)
    db_propertyCode = "'" + propertyCode + "'"

    # Delete existing data from date range of report
    conn = db_config.get_db_connection()
    conn.execute(text(f'DELETE FROM expedia_revplus where "date" between {start_date} and {end_date} and "property_code" = {db_propertyCode};'))
    conn.commit()
    conn.close()

    # Add new data of revplus
    conn = db_config.get_db_connection()
    conn.execute(db_models.choice_cancellation_model.insert(), revplus_list)
    conn.close()

def delete_files_in_directory(directory):
    try:
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
        print(f"All files in {directory} have been deleted.")
    except Exception as e:
        print(f"Error deleting files: {e}")

def Expedia(row):
    pullDateId = row['pullDateId'] #"1234" 
    propertyCode = row['propertyCode'] #"USASP001" 
    htid_value = row['hotelId']
    platform = "expedia"
    driver = None
    
    try:
        username = "AticaGlobal2975" #None
        password = "Atica@123" #None
        print("test1")
        try:
            print(f"Getting Secret for {propertyCode}")
            json_dict = get_secret_from_api(propertyCode, platform)
            print("res ::")
            username = json_dict['u']
            password = json_dict['p']
        except Exception:
            msg = f"[{propertyCode}] secret fetch failed due to bad json"
            print(msg)
            update_into_pulldate_expedia(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)
            return 0

        if username is None or password is None:
            msg = f"[{propertyCode}] username and password is wrong!!!"
            print(msg)
            update_into_pulldate_expedia(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)

        folder_name = f"./reports/{propertyCode}/"
        filename = f"{propertyCode}_Revplus.xlsx"
        # save_dir = os.path.abspath(f'./reports/{propertyCode}/')
        save_dir =  os.path.abspath(folder_name)
        # Create the directory if it doesn't exist
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        delete_files_in_directory(save_dir)

        print("test2")
        chrome_options = Options()
        # chrome_options.add_argument("--headless")
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
            "download.default_filename": filename
            # "plugins.always_open_pdf_externally": True
        })
        print("test3")
        service = Service('../chromedriver.exe')
        driver = webdriver.Chrome(options=chrome_options, service=service)
        driver.maximize_window()
        print("test4")

        print(f"{propertyCode} Script start for Choice Cancellation List")
        login_url = "https://www.expediapartnercentral.com/Account/Logon"
        driver.get(login_url)
        print("test5")
        otpUpdateEpoch = 1
        current_time_epoch = int(time.time())

        try:
            username_field = driver.find_element(By.NAME, "username")
        except NoSuchElementException:
            username_field = driver.find_element(By.ID, "emailControl")
        username_field.send_keys(username)

        try:
            password_field = driver.find_element(By.NAME, "password")
        except NoSuchElementException:
            password_field = driver.find_element(By.ID, "passwordControl")
        password_field.send_keys(password)
        print("test6")

        driver.find_element(By.ID, 'signInButton').click()
        time.sleep(10)

        #otp on email only
        element_otp = None
        print("test6.1")
        element_otp = driver.find_element(By.ID, "passcode")
        print("test6.2")
        if element_otp is not None:

            print("test6.3")
            data_test_id_value = element_otp.get_attribute("data-testid")
            if data_test_id_value == "SmsPasscode-verification":
                print("test6.4")
                email_me_link = driver.find_element(By.LINK_TEXT, "Email me")
                print("test6.5")
                email_me_link.click()
                time.sleep(5)

            
            # otp = input("Enter the OTP: ")
            otp = None

            #while loop to try 10times to get latest otp bases on otpUpdateEpoch
            max_iterations = 10
            current_iteration = 0

            while current_iteration < max_iterations and current_time_epoch > otpUpdateEpoch:
                print("test6.6")
                try:
                    print(f"Getting OTP for {propertyCode}")
                    json_dict = get_otp_from_api(propertyCode, platform)
                    print("test6.7")
                    print("res ::")
                    otp = json_dict['otp']
                    otpUpdateEpoch = json_dict['otp_epoch']
                except Exception:
                    msg = f"[{propertyCode}] OTP fetch failed due to bad json"
                    print(msg)
                    update_into_pulldate_expedia(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)
                    return 0

                time.sleep(20)
                current_iteration += 1
                

            if otp is None or otpUpdateEpoch is None:
                msg = f"[{propertyCode}] No OTP or Time Stamp of OTP!!!"
                print(msg)
                update_into_pulldate_expedia(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)
                return 0
            print("test7")
            
            
            try:
                otp_field = driver.find_element(By.NAME, "passcode-input")  
            except NoSuchElementException:
                otp_field = driver.find_element(By.CLASS_NAME, "fds-field-input.replay-conceal")
            
            otp_field.send_keys(otp)
            print("test8")


            button_locator = (By.CLASS_NAME, 'fds-button.fds-button-action')
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable(button_locator))

            #Find the button and click it
            action_button = driver.find_element(*button_locator)
            action_button.click()
            print("test9")

            time.sleep(10)

        report_url = f"https://apps.expediapartnercentral.com/lodging/revplus/api/priceGridExport?htid={htid_value}&los=1&adults=2&tpid=1&countryCode=USA&roomTypeId=0&fetchMembersOnlyRates=false&fullyRefundableOnly=false&breakfastIncludedOnly=false&fetchMobileRates=false&fetchModTiers=&isSubMarket=false&useMockData=false&numOfDays=90"
        driver.get(report_url)
        print("test11")
        time.sleep(10)

        for file_name_i in os.listdir(save_dir):
            file_path = os.path.join(save_dir, file_name_i)
            new_file_name = os.path.join(save_dir, filename)
            if os.path.exists(new_file_name):
                os.remove(new_file_name)
            os.rename(file_path, new_file_name)
            break
        
        driver.quit()

        excel_file_path = os.path.join(save_dir, filename)
        null_row_posi= 100


        for idx, row in enumerate(pd.read_excel(excel_file_path, header=None, skiprows=9).values):
            if pd.isna(row[0]):
                null_row_posi = idx + 1
                break
        print(null_row_posi)

        df = pd.read_excel(excel_file_path, header=None, skiprows=8)
        df.iloc[0, 0]="date"
        df.iloc[1, 0]="property_price"
        df.iloc[2, 0]="compset_avg"
        for x in range(3, null_row_posi):
            tempstr="compset_" + str(x-2)
            df.iloc[x,0]=tempstr
        df.iloc[null_row_posi, 0]="NULLWALA"
        df.iloc[null_row_posi+1, 0]="search_demand"
        df.iloc[null_row_posi+2, 0]="previous_year"
        df.iloc[null_row_posi+3, 0]="current_occupancy_perc"
        df.iloc[null_row_posi+4, 0]="occupancy_forecast_perc"

        transposed_df = df.transpose()
        csv_file_path = f"./reports/{propertyCode}/temp.csv"
        transposed_df.to_csv(csv_file_path, index=False, header=False)

        createdAt = "'" + str(arrow.now()) + "'"
        updatedAt = "'" + str(arrow.now()) + "'"
        createdAtEpoch = int(arrow.utcnow().timestamp())
        updatedAtEpoch = int(arrow.utcnow().timestamp())
        read = pd.read_csv(csv_file_path)
        read = read.drop('NULLWALA', axis=1)
        read.dropna(inplace=True, how="all")
        read.dropna(subset=['date'], inplace=True)
        # base_date = pd.Timestamp('1899-12-30')  # Excel's base date
        # read['date'] = pd.to_datetime(base_date + pd.to_timedelta(read['date'], unit='D'))
        read['date'] = pd.to_datetime(read['date'],  format='mixed', errors='coerce')
        read['occupancy_forecast_perc'] = read['occupancy_forecast_perc'] * 100
        read['occupancy_forecast_perc'] = read['occupancy_forecast_perc'].round(2)
        read['current_occupancy_perc'] = read['current_occupancy_perc'] * 100
        read['current_occupancy_perc'] = read['current_occupancy_perc'].round(2)
        read.insert(0, column="property_code", value=propertyCode)
        read.insert(1, column="pull_date_id", value=pullDateId)
        read.insert(2, column="created_at", value=createdAt)
        read.insert(3, column="updated_at", value=updatedAt)
        read.insert(4, column="created_at_epoch", value=createdAtEpoch)
        read.insert(5, column="updated_at_epoch", value=updatedAtEpoch)
        read.insert(6, column="unique_key", value=read["property_code"].astype(str) + "_" + read['date'].astype(str))
        date_set = set(read['date'])
        date_set.discard(pd.NaT)
        revplus_file_path = f"./reports/{propertyCode}/Revplus_final.csv"
        read.to_csv(revplus_file_path, index=False)

        check_Revplus_file = os.path.isfile(revplus_file_path)
        errorMessage=""

        if check_Revplus_file:

            Revplus_result = csv.DictReader(open(revplus_file_path, encoding="utf-8"))
            Revplus_result = list(Revplus_result)
            print(len(Revplus_result))
            if len(Revplus_result) > 0:
                error_temp = bulk_insert_expedia_revplus(propertyCode, Revplus_result, min(date_set), max(date_set))
                if(error_temp == ""):
                    print("Revplus DONE")
                else:
                    print("Revplus FAILED")
                    errorMessage = errorMessage + " Revplus Failed: " + error_temp
            else:
                errorMessage = errorMessage + "Revplus File Was Blank, "
        else:
            errorMessage = errorMessage + "Revplus File Not Found, "

        if errorMessage == "":
            update_into_pulldate_expedia(pullDateId, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
        else:
            update_into_pulldate_expedia(pullDateId, ERROR_NOTE=errorMessage, IS_ERROR=True)

    except Exception as e:
            print(e)
            if driver:
                driver.quit()
            update_into_pulldate_expedia(pullDateId, ERROR_NOTE=f"Failed to pull report due to {e}", IS_ERROR=True)



    



    


if __name__ == '__main__':
    
    print(f"Expedia Revplus SCRIPT IS STARTING...")

    propertycode = None

    result = None
    # if propertycode is None:
    #     print("All properties run")
    #     conn = db_config.get_db_connection()
    #     res = conn.execute(text(f"""SELECT * FROM tbl_properties_expedia WHERE "pmsName" = '{PMS_NAME}';"""))
    #     result = res.fetchall()
    #     columns = res.keys()
    #     results_as_dict = [dict(zip(columns, row)) for row in result]
    #     conn.close()
    #     print("Fetched successfully")
    # else:
    #     print(f"{propertycode} property run")
    #     conn = db_config.get_db_connection()
    #     res = conn.execute(text(f"""SELECT * FROM tbl_properties_expedia WHERE "pmsName" = '{PMS_NAME}' and "propertyCode" = '{propertycode}';"""))
    #     result = res.fetchall()
    #     columns = res.keys()
    #     results_as_dict = [dict(zip(columns, row)) for row in result]
    #     conn.close()
    #     print("Fetched successfully")

    results_as_dict = [
        {"propertyCode": "US000001", "hotelId": 18748}
    # {"propertyCode": "US000001", "hotelId": 18748},
    # {"propertyCode": "US000009", "hotelId": 882709},
    # {"propertyCode": "US000018", "hotelId": 117294},
    # {"propertyCode": "US000019", "hotelId": 2292810},
    # {"propertyCode": "US000020", "hotelId": 212873},
    # {"propertyCode": "USNJ230104", "hotelId": 24346}
    ]

    if results_as_dict is not None and len(results_as_dict) > 0:
        print(f"Total Properties :: {len(results_as_dict)}")
        for item in results_as_dict:

            PROPERTY_CODE = item['propertyCode']
            HOTEL_ID = item['hotelId']
            CURRENT_DATE = arrow.now()
            PULLED_DATE = CURRENT_DATE.date()

            # Add entry into pull date table
            LAST_PULL_DATE_ID = insert_into_pulldate_expedia(PROPERTY_CODE, PULLED_DATE)

            if LAST_PULL_DATE_ID is not None:
                row = {
                    "propertyCode": PROPERTY_CODE,
                    "hotelId": HOTEL_ID,
                    "pullDateId": LAST_PULL_DATE_ID
                }
                print("row :: ", row)
                Expedia(row)
            else:
                print("LAST_PULL_DATE_ID is NULL")
    else:
        print(f"Property not available in database!!!")
    print(f"Expedia Revplus SCRIPT STOP!!!")