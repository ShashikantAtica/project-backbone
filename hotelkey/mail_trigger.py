import sys

from sqlalchemy import text

sys.path.append("..")
import argparse
import arrow
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from utils.secrets.SecretManager import get_secret_from_api as get_secret_dict
from utils.db import db_config


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


def Hotelkey_Pms(row):
    atica_property_code = row['atica_property_code']
    external_property_code = row['external_property_code']
    propertyCode = row['propertyCode']
    pullDateId = row['pullDateId']
    platform = "RMS"
    try:
        print(f"Getting Secret for {atica_property_code}")
        json_dict = get_secret_dict(propertyCode, platform)
        print("res ::")
        username = json_dict['u']
        password = json_dict['p']
        login_url = "https://motel6.aboveproperty.com/#/login"
        send_email = "data@aticaglobal.com"

        with requests.Session() as s:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument('--hide-scrollbars')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument("--log-level=3")
            chrome_options.add_argument("--window-size=1280,1000")
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

            chrome_options.add_experimental_option("prefs", {
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            })

            service = Service('../chromedriver.exe')
            driver = webdriver.Chrome(options=chrome_options, service=service)
            driver.maximize_window()

            driver.get(login_url)

            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "okta-signin-username")))
            driver.find_element(By.ID, "okta-signin-username").send_keys(username)
            driver.find_element(By.ID, "okta-signin-password").send_keys(password)
            driver.find_element(By.ID, "okta-signin-submit").click()

            while True:
                url = driver.current_url
                if "sessionToken" in url:
                    break
                else:
                    continue

            session_token = url.split("sessionToken=")[-1]
            print("session_token ::", session_token)

            headers = {
                "authorization": f"Bearer {session_token}"
            }
            reports_url = s.get("https://motel6.aboveproperty.com/rest/v1/reports", headers=headers)

            # Occupancy Report Start
            report_type = 'Occupancy'
            occ_start_date = row['occ_before'].format("YYYY-MM-DD")
            occ_end_date = row['occ_after'].format("YYYY-MM-DD")

            occ_payload = {"channelIDList": None, "processID": None, "alertType": "REPORT", "actionCode": "EMAIL", "alertMethod": "EMAIL", "status": "PENDING", "alertProcessDate": None, "alertRequestedDate": None, "creationDate": None, "payloadType": "MIXEDMEDIA", "responseRequiredYN": "N", "alertSeqno": None, "alertDate": None, "controlCount": None, "controlTotalAmount": None, "controlAmount": None, "orderBy": None, "retryCount": 1, "maxRetryCount": None, "responseStatus": "PENDING", "responseType": "NONE", "responseLocation": None, "responseValue": None, "payloadLocation": send_email, "payloadStatus": None,
                           "payload": "REPORT_ID=246a0000-0000-0000-0000-000000000000|FILENAME=g6_corporate_pricing_report.jasper|userCustomerID=a63636db-d1c0-4926-8d1e-902ccf6625a3|userID=bc168288-84ad-4270-89dc-f5a39a0614a5|rptTitle=G6 Corporate Pricing|inFranchiseCorporate=FRANCHISE|inPropertyId=1c5d3575-d73f-423f-a1f1-83184e7a816a|inPropertyName=" + external_property_code + "|inBeginDate=" + str(occ_start_date) + "|inEndDate=" + str(occ_end_date) + "|DATASOURCE_CLASS=com.abvprp.data.queries.g6.CorporateReportQuery|inPropertyTags=|inMin_otb=0|attachmentType=CSV", "batchSeqno": None, "retryMinutes": 30, "payloadLanguage": None, "deliveryMethod": None, "serverAddress": None, "serverSubaddress": None, "serverUsername": None, "serverPassword": None, "serverProtocol": None, "serverPort": None, "landingPageUrl": None, "schedule": None, "scheduleAlertType": None, "scheduleBeginDate": None, "scheduleEndDate": None, "referenceValue": None, "fkReference": "REPORT",
                           "fkID": "246a0000-0000-0000-0000-000000000000",
                           "inactivated": "false",
                           "lastModified": None, "customerID": "1c5d3575-d73f-423f-a1f1-83184e7a816a", "parentID": None, "channelID": "15a00000-0000-0000-0000-000000000000", "channelParameterID": None, "ruleID": None, "marketingMediaCalendarID": None, "marketingMediaID": None, "marketingCampaignID": None, "contactID": None, "contactInformationID": None, "mmediaCalendarImpressionID": None, "lastModifiedByID": None, "rowLanguage": "en-us"}
            headers = {
                "authorization": f"Bearer {session_token}"
            }
            occ_report = s.post("https://motel6.aboveproperty.com/rest/v1/alerts", json=occ_payload, headers=headers)
            if occ_report.status_code == 200:
                print(f"{report_type} report sent successfully to {send_email}")
            else:
                print(f"{report_type} report email sent failed")
            # Occupancy Report End

            # Reservation Report Start
            report_type = 'Reservation'
            res_start_date = row['res_before'].format("YYYY-MM-DD")
            res_end_date = row['res_after'].format("YYYY-MM-DD")

            res_payload = {"channelIDList": None, "processID": None, "alertType": "REPORT", "actionCode": "EMAIL", "alertMethod": "EMAIL", "status": "PENDING", "alertProcessDate": None, "alertRequestedDate": None, "creationDate": None, "payloadType": "MIXEDMEDIA", "responseRequiredYN": "N", "alertSeqno": None, "alertDate": None, "controlCount": None, "controlTotalAmount": None, "controlAmount": None, "orderBy": None, "retryCount": 1, "maxRetryCount": None, "responseStatus": "PENDING", "responseType": "NONE", "responseLocation": None, "responseValue": None, "payloadLocation": send_email, "payloadStatus": None,
                           "payload": "REPORT_ID=316a0000-0000-0000-0000-000000000000|FILENAME=daily_bookings.jasper|userCustomerID=a63636db-d1c0-4926-8d1e-902ccf6625a3|userID=bc168288-84ad-4270-89dc-f5a39a0614a5|rptTitle=Daily Bookings/Cancels Report|inPropertyId=1c5d3575-d73f-423f-a1f1-83184e7a816a|inPropertyName=" + external_property_code + "|inProductTemplate=10a00000-0000-0000-0000-000000000000|inBeginDate=" + str(res_start_date) + "|inEndDate=" + str(res_end_date) + "|inRangeType=BOOKED|inChannelCode=ALL|inPriceCode=f99999a0-0000-0000-0000-000000000000|inZero=ALL|inOrder=UNITNUMBER|inEmail=ALL|attachmentType=CSV", "batchSeqno": None, "retryMinutes": 30, "payloadLanguage": None, "deliveryMethod": None, "serverAddress": None, "serverSubaddress": None, "serverUsername": None, "serverPassword": None, "serverProtocol": None, "serverPort": None, "landingPageUrl": None, "schedule": None, "scheduleAlertType": None, "scheduleBeginDate": None, "scheduleEndDate": None,
                           "referenceValue": None,
                           "fkReference": "REPORT",
                           "fkID": "316a0000-0000-0000-0000-000000000000", "inactivated": "false", "lastModified": None, "customerID": "1c5d3575-d73f-423f-a1f1-83184e7a816a", "parentID": None, "channelID": "15a00000-0000-0000-0000-000000000000", "channelParameterID": None, "ruleID": None, "marketingMediaCalendarID": None, "marketingMediaID": None, "marketingCampaignID": None, "contactID": None, "contactInformationID": None, "mmediaCalendarImpressionID": None, "lastModifiedByID": None, "rowLanguage": "en-us"}
            headers = {
                "authorization": f"Bearer {session_token}"
            }
            res_report = s.post("https://motel6.aboveproperty.com/rest/v1/alerts", json=res_payload, headers=headers)
            if res_report.status_code == 200:
                print(f"{report_type} report sent successfully to {send_email}")
                update_into_pulldate(pullDateId, ERROR_NOTE="Mail sent successfully", IS_ERROR=False)
            else:
                msg = f"{report_type} report email sent failed"
                print(msg)
                update_into_pulldate(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)
            # Reservation Report End
    except Exception as e:
        print(e)
        update_into_pulldate(pullDateId, ERROR_NOTE="Something went wrong", IS_ERROR=True)


if __name__ == '__main__':
    PMS_NAME = "Hotelkey"
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
                Hotelkey_Pms(row)
            else:
                print("LAST_PULL_DATE_ID is NULL")
    else:
        print(f"Property not available in database!!!")
    print(f"[{PMS_NAME}] SCRIPT STOP!!!")
