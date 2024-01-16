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

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select

from utils.db import db_config
from utils.db import db_models
from sqlalchemy.dialects.postgresql import insert


def bulk_insert_choice_cancellation_list(propertyCode, cancellation_list, res_before, res_after):
    # log(n)*M indexing check.
    conn = db_config.get_db_connection()
    stmt = insert(db_models.choice_cancellation_list_model).values(cancellation_list)
    stmt = stmt.on_conflict_do_nothing(index_elements=['uniqueKey'])
    # Execute the insert statement
    conn.execute(stmt)
    conn.commit()
    conn.close()
    # start_date = "'" + res_before.format("YYYY-MM-DD") + "'"
    # end_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    # print("start_date :: ", start_date)
    # print("end_date :: ", end_date)

    # reservation = '"Cxl_Date"'
    # db_propertyCode = "'" + propertyCode + "'"

    # # Delete existing data of cancellation list (up to 90 Days)
    # conn = db_config.get_db_connection()
    # conn.execute(text(f'DELETE from choice_cancellation_list where {reservation} between {start_date} and {end_date} and "propertyCode" = {db_propertyCode};'))
    # conn.commit()
    # conn.close()

    # # Add new data of cancellation list (up to 90 Days)
    # conn = db_config.get_db_connection()
    # conn.execute(db_models.choice_cancellation_list_model.insert(), cancellation_list)
    # conn.commit()
    # conn.close()


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
        json_dict = get_secret_from_api(propertyCode, platform)
        print("res ::")
        username = json_dict['u']
        password = json_dict['p']

        cancellation_list_file = f'{folder_name}{propertyCode}_Cancellation_List.csv'
        if os.path.exists(cancellation_list_file):
            os.remove(cancellation_list_file)

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

        WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.NAME, "j_username")))
        driver.find_element(By.NAME, "j_username").send_keys(username)
        driver.find_element(By.NAME, "j_password").send_keys(password)
        driver.find_element(By.ID, 'greenButton').click()
        time.sleep(5)

        driver.get("https://www.choiceadvantage.com/choicehotels/ReportViewStart.init")

        try:
            WebDriverWait(driver, 5).until(EC.alert_is_present())
            error_msg = "PopUp issue found"
            update_into_pulldate(pullDateId, ERROR_NOTE=error_msg, IS_ERROR=True)
            driver.quit()
            print("alert Exists in page")
        except:
            print("alert does not Exist in page")

        WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.ID, "CancellationListReport")))
        driver.find_element(By.ID, "CancellationListReport").click()

        start_date = row['res_before'].format('M/D/YYYY')
        end_date = row['res_after'].shift(days=-1).format('M/D/YYYY')
        WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.NAME, "activityDateType")))
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

        df = pd.read_csv(f'{folder_name}report.csv')
        df = df.drop('Company', axis=1)
        df = df.drop('Group', axis=1)
        new_column_names = ["Account", "Guest_Name", "Arrival_Group", "Nights", "Rate_Plan", "GTD", "Source", "Rm_Type", "Cxl_Code", "Cxl_Date", "Cxl_Clk"]
        df.columns = new_column_names
        df.dropna(inplace=True, how="all")
        createdAt = "'" + str(arrow.now()) + "'"
        updatedAt = "'" + str(arrow.now()) + "'"
        createdAtEpoch = int(arrow.utcnow().timestamp())
        updatedAtEpoch = int(arrow.utcnow().timestamp())
        df.dropna(subset=["Account"], inplace=True)
        df.insert(0, column="propertyCode", value=propertyCode)
        df.insert(1, column="pullDateId", value=pullDateId)
        df.insert(2, column="createdAt", value=createdAt)
        df.insert(3, column="updatedAt", value=updatedAt)
        df.insert(4, column="createdAtEpoch", value=createdAtEpoch)
        df.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
        df['Account'] = df['Account'].fillna(0).astype(int)
        df['Nights'] = df['Nights'].fillna(0).astype(int)
        df.insert(6, column="uniqueKey", value=df["Account"].astype(str))

        output_df = pd.DataFrame(columns=df.columns)

        previous_row = None

        for index, data in df.iterrows():
            if previous_row is not None and int(data["Account"]) == 0:
                previous_row["Guest_Name"] += f", {data['Guest_Name']}"
            else:
                if previous_row is not None:
                    output_df = pd.concat([output_df, previous_row.to_frame().T], ignore_index=True)
                previous_row = data

        if previous_row is not None:
            output_df = pd.concat([output_df, previous_row.to_frame().T], ignore_index=True)
        output_df['Arrival_Group'] = pd.to_datetime(output_df['Arrival_Group'], format="%m/%d/%y").dt.date
        output_df['Cxl_Date'] = pd.to_datetime(output_df['Cxl_Date'], format="%m/%d/%y").dt.date
        output_df.to_csv(f'{folder_name}{propertyCode}_Cancellation_List.csv', index=False)
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
    # Get all property using brand
    PMS_NAME = "Choice"
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
            LAST_PULL_DATE_ID = insert_into_pulldate(PROPERTY_CODE, PULLED_DATE, PMS_NAME + "_selenium")

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
                choice_cancellation(row)
            else:
                print("LAST_PULL_DATE_ID is NULL")
    else:
        print(f"Property not available in database!!!")
    print(f"[{PMS_NAME}] SCRIPT STOP!!!")
