import csv
import os
import sys

sys.path.append("..")
import time
import arrow
import pandas as pd
from PyPDF2 import PdfReader
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


def bulk_insert_choice_noshow(propertyCode, Noshow_result):
# log(n)*M indexing check.
    conn = db_config.get_db_connection()
    stmt = insert(db_models.choice_noshow_model).values(Noshow_result)
    stmt = stmt.on_conflict_do_nothing(index_elements=['ACCOUNT'])
    # Execute the insert statement
    conn.execute(stmt)
    conn.close()
    


def choice_noshow(row):
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

        print(f"{atica_property_code} Script start for Choice Noshow")
        login_url = "https://www.choiceadvantage.com/choicehotels/sign_in.jsp"
        driver.get(login_url)

        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.NAME, "j_username")))
        driver.find_element(By.NAME, "j_username").send_keys(username)
        driver.find_element(By.NAME, "j_password").send_keys(password)
        driver.find_element(By.ID, 'greenButton').click()

        WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.LINK_TEXT, "Run")))
        driver.find_element(By.LINK_TEXT, "Run").click()
        WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.LINK_TEXT, "Reports")))
        driver.find_element(By.LINK_TEXT, "Reports").click()

        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.ID, "NoShowReport")))
        driver.find_element(By.ID, "NoShowReport").click()

        start_date = row['res_before'].format('M/D/YYYY')
        end_date = row['res_after'].shift(days=-1).format('M/D/YYYY')
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.NAME, "startDatePastCurrent")))
        
        start_date_field = driver.find_element(By.NAME, "startDatePastCurrent")
        start_date_field.clear()
        start_date_field.send_keys(start_date)
        end_date_field = driver.find_element(By.NAME, "endDatePastCurrent")
        end_date_field.clear()
        end_date_field.send_keys(end_date)

        filepath = os.path.join(f'{folder_name}report.pdf')
        if os.path.exists(filepath):
            os.remove(filepath)
        submit_button = driver.find_element(By.ID, "doSubmit")
        submit_button.click()
        while not os.path.exists(filepath):
            time.sleep(1)
        
        print(f"{atica_property_code} Choice noshow report generated")
        time.sleep(5)
        driver.quit()

        #Main code to parse pdf
        reader = PdfReader(filepath)
        account=list()
        auth_status=list()
        print('total number of pages in report :', len(reader.pages))
        itr_pop=0
        for itr_page in range(0, len(reader.pages)-1):
            page=reader.pages[itr_page]
            textt=page.extract_text()
            textt = textt.replace("\n", " ")
            #Extracting account numbers & Auth Status
            for idx in range(0, len(textt)):
                next_nine_char_str=textt[idx : idx+9]
                if(next_nine_char_str.isnumeric() and textt[idx-6 : idx-2]!="User"):
                    account.append(next_nine_char_str)
                if((next_nine_char_str.isnumeric() and textt[idx-6 : idx-2]!="User") or (textt[idx : idx+9]=="Date/Time")):
                    if(textt[idx-8:idx-1]=="No Auth"):
                        auth_status.append("No Auth")
                    elif(textt[idx-8:idx-1]=="Settled"):
                        auth_status.append("Settled")
                    elif(textt[idx-11:idx-1]=="Authorized"):
                        auth_status.append("Authorized")
                    elif(textt[idx-6:idx-1]=="Error"):
                        auth_status.append("Error")
                    else:
                        auth_status.append("")
            auth_status.pop(itr_pop)
            itr_pop=len(auth_status) 

        for itr_page in range(len(reader.pages)-1, len(reader.pages)):
            page=reader.pages[itr_page]
            textt=page.extract_text()
            textt = textt.replace("\n", " ")
            #Extracting account numbers & Auth_Status
            for idx in range(0, len(textt)):
                next_nine_char_str=textt[idx : idx+9]
                if(next_nine_char_str.isnumeric() and textt[idx-6 : idx-2]!="User"):
                    account.append(next_nine_char_str)
                if((next_nine_char_str.isnumeric() and textt[idx-6 : idx-2]!="User") or (textt[idx : idx+14]=="Total No Shows")):
                    if(textt[idx-8:idx-1]=="No Auth"):
                        auth_status.append("No Auth")
                    elif(textt[idx-8:idx-1]=="Settled"):
                        auth_status.append("Settled")
                    elif(textt[idx-11:idx-1]=="Authorized"):
                        auth_status.append("Authorized")
                    elif(textt[idx-6:idx-1]=="Error"):
                        auth_status.append("Error")
                    else:
                        auth_status.append("")
            auth_status.pop(itr_pop) 

        if(len(auth_status)!=len(account)):
            update_into_pulldate(pullDateId, ERROR_NOTE=f"{atica_property_code} Data is mismatched for Account-Auth_status", IS_ERROR=True)
        else:
            # account, created_at, updated_at, property_code, auth_status
            cols = ["propertyCode","pullDateId","createdAt","updatedAt","createdAtEpoch","updatedAtEpoch","ACCOUNT","AUTH_STATUS"]
            rows = []
                    
            print("Number of auth_status : ",len(auth_status), "Number of account : ", len(account))
            for x in range(0, len(account)):
                rows.append({
                        "propertyCode": propertyCode,
                        "pullDateId": pullDateId,
                        "createdAt": "'" + str(arrow.now()) + "'",
                        "updatedAt": "'" + str(arrow.now()) + "'",
                        "createdAtEpoch" : int(arrow.utcnow().timestamp()),
                        "updatedAtEpoch" : int(arrow.utcnow().timestamp()),
                        "ACCOUNT": account[x],
                        "AUTH_STATUS": auth_status[x]})
                
            df = pd.DataFrame(rows, columns=cols)
            df.to_csv(f"{folder_name}{propertyCode}_Noshow.csv", index=False)
            Noshow_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Noshow.csv", encoding="utf-8"))
            Noshow_result = list(Noshow_result)
            print(Noshow_result)
            
            print(f"{atica_property_code} Choice noshow report pulled successfully")

            Noshow_file_path = f'{folder_name}{propertyCode}_Noshow.csv'

            check_Noshow_file = os.path.isfile(Noshow_file_path)

            error_msg = ""

            if not check_Noshow_file:
                error_msg = error_msg + " Noshow file - N/A"

            if check_Noshow_file:
                Noshow_result = csv.DictReader(open(Noshow_file_path, encoding="utf-8"))
                Noshow_result = list(Noshow_result)
                print(len(Noshow_result))
                bulk_insert_choice_noshow(propertyCode, Noshow_result)
                print("Noshow DONE")
                #insert type of report also
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

            #insert type of report also
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
            choice_noshow(row)
            print("SCRIPT DONE FOR Choice Noshow")
        else:
            print("LAST_PULL_DATE_ID is NULL")
