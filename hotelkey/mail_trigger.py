import arrow
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils.secrets.SecretManager import get_secret_dict
from utils.db import db_config


def Hotelkey_Pms(row):
    secret_name = row['gcp_secret']
    external_property_code = row['external_property_code']
    try:
        print("secret_name :: ", secret_name)
        json_dict = get_secret_dict(secret_name)
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
            driver = webdriver.Chrome(options=chrome_options, executable_path='../chromedriver.exe')
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
            else:
                print(f"{report_type} report email sent failed")
            # Reservation Report End
    except Exception as e:
        print(e)
    finally:
        driver.quit()


if __name__ == '__main__':
    # Get all property using brand
    PMS_NAME = "'Hotelkey'"
    print("SCRIPT STARTED FOR Hotelkey")
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
        }
        print("row :: ", row)
        Hotelkey_Pms(row)
        print("SCRIPT DONE FOR Hotelkey")
