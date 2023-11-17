import csv
import os
import sys

sys.path.append("..")
import time
import arrow
import pandas as pd
from utils.secrets.SecretManager import get_secret_from_api

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

from utils.db import db_config
from utils.db import db_models


def bulk_insert_auto_clerk_res(propertyCode, res_list, res_before, res_after):
    start_date = "'" + res_before.format("YYYY-MM-DD") + "'"
    end_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    print("start_date :: ", start_date)
    print("end_date :: ", end_date)

    # delete all data
    # current_date = arrow.now()
    # print("current_date :: ", current_date)
    # start_date = current_date.shift(days=-90)
    # print("start_date :: ", start_date)
    reservation = '"DateTime"'
    db_propertyCode = "'" + propertyCode + "'"
    # current_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    # start_date = "'" + res_before.format("YYYY-MM-DD") + "'"

    # Delete existing data of reservation (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(
        f'DELETE from auto_clerk_res where {reservation} between {start_date} and {end_date} and "propertyCode" = {db_propertyCode};')
    conn.close()

    # Add new data of reservation (up to 90 Days)
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.auto_clerk_res_model.insert(), res_list)
    conn.close()
    print("Data imported")


def bulk_insert_auto_clerk_occ(propertyCode, occ_list, occ_before, occ_after):
    start_date = "'" + occ_before.format("YYYY-MM-DD") + "'"
    end_date = "'" + occ_after.format("YYYY-MM-DD") + "'"
    print("start_date :: ", start_date)
    print("end_date :: ", end_date)

    # delete all data
    # current_date = arrow.now()
    # print("current_date :: ", current_date)
    # start_date = current_date.shift(days=-90)
    # print("start_date :: ", start_date)
    reservation = '"Date"'
    db_propertyCode = "'" + propertyCode + "'"
    # current_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    # start_date = "'" + res_before.format("YYYY-MM-DD") + "'"

    # Delete existing data of reservation (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(
        f'DELETE from auto_clerk_occ where {reservation} between {start_date} and {end_date} and "propertyCode" = {db_propertyCode};')
    conn.close()

    # Add new data of occ (up to 90 Days)
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.auto_clerk_occ_model.insert(), occ_list)
    conn.close()
    print("Data imported")


def bulk_insert_auto_clerk_group_block_summary(propertyCode, group_block_summary_list, res_before, res_after):
    start_date = "'" + res_before.format("YYYY-MM-DD") + "'"
    end_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    print("start_date :: ", start_date)
    print("end_date :: ", end_date)

    # delete all data
    # current_date = arrow.now()
    # print("current_date :: ", current_date)
    # start_date = current_date.shift(days=-90)
    # print("start_date :: ", start_date)
    reservation = '"Arrive"'
    db_propertyCode = "'" + propertyCode + "'"
    # current_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    # start_date = "'" + res_before.format("YYYY-MM-DD") + "'"

    # Delete existing data of reservation (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(
        f'DELETE from auto_clerk_group_block_summary where {reservation} between {start_date} and {end_date} and "propertyCode" = {db_propertyCode};')
    conn.close()

    # Add new data of occ (up to 90 Days)
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.auto_clerk_group_block_summary_model.insert(), group_block_summary_list)
    conn.close()
    print("Data imported")


def AutoClerk_Pms(row):
    atica_property_code = row['atica_property_code']
    pullDateId = row['pullDateId']
    propertyCode = row['propertyCode']
    platform = "PMS"

    username = None
    password = None

    folder_name = f"./reports/{propertyCode}/"
    # save_dir = os.path.abspath('reports/')
    save_dir = os.path.abspath(f'reports/{propertyCode}/')
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

        driver.get('https://www.bwh.autoclerkcloud.com/logon.do2')
        time.sleep(10)

        try:
            username_field = driver.find_element(By.NAME, "username")
        except NoSuchElementException:
            username_field = driver.find_element(By.NAME, "j_username")
        username_field.send_keys(username)

        try:
            password_field = driver.find_element(By.NAME, "password")
        except NoSuchElementException:
            password_field = driver.find_element(By.NAME, "j_password")
        password_field.send_keys(password)

        driver.find_element(By.NAME, "loginSubmit").click()

        try:
            webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(2)
        except NoSuchElementException:
            print("No Message popup detected")

        # Forecast Report Start
        report_type = 'Forecast'
        driver.find_element(By.LINK_TEXT, 'Reports').click()

        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.LINK_TEXT, "Forecast Report")))
        driver.find_element(By.LINK_TEXT, "Forecast Report").click()
        time.sleep(2)
        driver.find_element(By.ID, "startdate").clear()
        driver.find_element(By.ID, "startdate").send_keys(row['occ_before'].format("M/D/YY"))
        driver.find_element(By.ID, "enddate").clear()
        driver.find_element(By.ID, "enddate").send_keys(row['occ_after'].format("M/D/YY"))

        try:
            if driver.execute_script("return $('#includeGroupRevenue').prop('checked')") is True:
                driver.execute_script("$('#includeGroupRevenue').prop('checked', false)")
                print("Unclicked box!")
                time.sleep(1)
        except NoSuchElementException:
            print("project group revenue checkbox not found, skipping...")

        try:
            driver.find_element(By.XPATH, "//select[@name='output_type']/option[text()='Data (CSV)']").click()
        except NoSuchElementException:
            driver.find_element(By.XPATH, "//select[@name='output_type']/option[@value='csv']").click()

        driver.find_element(By.ID, "reportbutton").click()
        print(f"{report_type} report request sent")

        filename = "occForecast.csv"
        filepath = os.path.join(save_dir, filename)
        if os.path.exists(filepath):
            os.remove(filepath)
        while not os.path.exists(filepath):
            time.sleep(1)
        time.sleep(5)

        new_file_name = os.path.join(save_dir, "Occupancy.csv")
        if os.path.exists(new_file_name):
            os.remove(new_file_name)
        os.rename(filepath, new_file_name)

        print(f"{report_type} report saved successfully")

        # Reservation Report Start
        report_type = 'Reservation'
        driver.find_element(By.LINK_TEXT, "Main Menu").click()
        time.sleep(2)
        driver.find_element(By.LINK_TEXT, "Front Desk").click()
        time.sleep(2)

        try:
            webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(4)
        except NoSuchElementException:
            print("No Message popup detected")

        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CLASS_NAME, "sidebar")))
        driver.find_element(By.LINK_TEXT, 'Reservation Activity').click()

        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, '//input[@type="submit"]')))
        driver.find_element(By.XPATH, '//input[@type="submit"]').click()

        list_of_dataframes_reservation = []
        res_start_date = row['res_before']
        res_end_date = row['res_after']
        curr = res_start_date
        while curr <= res_end_date:
            Res_Start_date = curr
            Res_End_date = curr.shift(days=+14)
            if Res_End_date > res_end_date:
                Res_End_date = res_end_date
            curr = curr.shift(days=+15)

            WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.NAME, "createdAtBegin")))
            driver.find_element(By.NAME, "createdAtBegin").send_keys(Res_Start_date.format('M/D/YY'))
            driver.find_element(By.NAME, "createdAtEnd").send_keys(Res_End_date.format('M/D/YY'))
            driver.find_element(By.ID, "buttonCSV").click()

            filename = "result.csv"
            filepath = os.path.join(save_dir, filename)
            driver.switch_to.alert.accept()

            while not os.path.exists(filepath):
                time.sleep(1)
            time.sleep(7)

            new_file_name = os.path.join(save_dir,
                                         f"result_{Res_Start_date.format('M-D-YY')} to {Res_Start_date.format('M-D-YY')}.csv")
            os.rename(filepath, new_file_name)
            driver.refresh()
            webdriver.ActionChains(driver).send_keys(Keys.ENTER).perform()

            df = pd.read_csv(new_file_name, header=0, engine='python')
            df.fillna("null", inplace=True)
            list_of_dataframes_reservation.append(df)

            if os.path.exists(new_file_name):
                os.remove(new_file_name)

        head = ["Date/Time", "Status", "Myhms Conf", "CRS Conf", "room number", "room type", "rate plan", "rate",
                "Paytype",
                "arrival date", "departure date", "# nights", "guest last name", "guest first name", "address1",
                "address2",
                "city", "state", "zip", "country", "phone", "email", "Total Revenue for stay", "Room Revenue for stay",
                "source", "market", "channel", "companyName", "drivingLicence", ""]
        merged_df = pd.concat(list_of_dataframes_reservation, ignore_index=True)
        filename = os.path.join(save_dir, "Reservation.csv")
        merged_df.to_csv(filename, header=head, index=False)
        print(f"{report_type} report saved successfully")

        # Start Group Block Summary Report
        report_type = 'Group_Block_Summary'
        driver.find_element(By.LINK_TEXT, "Reports").click()
        time.sleep(2)
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.LINK_TEXT, "Group Block")))
        driver.find_element(By.LINK_TEXT, "Group Block").click()
        time.sleep(2)
        driver.find_element(By.ID, "startdate").clear()
        driver.find_element(By.ID, "startdate").send_keys(row['res_before'].format("M/D/YY"))
        driver.find_element(By.ID, "enddate").clear()
        driver.find_element(By.ID, "enddate").send_keys(row['res_after'].format("M/D/YY"))

        try:
            driver.find_element(By.XPATH, "//select[@name='output_type']/option[text()='Data (CSV)']").click()
        except NoSuchElementException:
            driver.find_element(By.XPATH, "//select[@name='output_type']/option[@value='csv']").click()

        filepath = f'{folder_name}groupBlockSummary.csv'
        if os.path.exists(filepath):
            os.remove(filepath)

        driver.find_element(By.ID, "reportbutton").click()
        print(f"{report_type} report request sent")
        time.sleep(5)
        while not os.path.exists(filepath):
            time.sleep(1)

        print(f"{report_type} report saved successfully")
        driver.quit()

        reservation_file_path = f'{folder_name}Reservation.csv'
        occupancy_file_path = f'{folder_name}Occupancy.csv'
        group_block_summary_file_path = f'{folder_name}groupBlockSummary.csv'

        check_reservation_file = os.path.isfile(reservation_file_path)
        check_occupancy_file = os.path.isfile(occupancy_file_path)
        check_group_block_summary_file = os.path.isfile(group_block_summary_file_path)

        if check_reservation_file and check_occupancy_file and check_group_block_summary_file:
            # Start Data Modification Occupancy
            df = pd.read_csv(occupancy_file_path)
            df = df.drop([0, 1, 2, 3, 4, 5])
            df = df.reset_index(drop=True)

            updated_data = pd.DataFrame(df)

            value_to_delete = 'Date'
            index_to_delete = []
            index_to_delete = updated_data[updated_data['B/W Joliet Inn & Suites'] == value_to_delete].index

            # Check if the index is not empty
            delete_rows = []
            for i in index_to_delete:
                # Remove the previous and succeeding rows
                rows_to_delete = [i - 1, i, i + 1]
                delete_rows.extend(rows_to_delete)

            updated_data = updated_data.drop(delete_rows)
            updated_data = updated_data[:-1]

            shifted_df = updated_data.T.apply(lambda x: sorted(x, key=pd.isnull)).T
            shifted_df = shifted_df.iloc[:, 0:17]

            shifted_df.columns = ['Date', 'Rooms', 'Arrivals', 'StayOver', 'DepartToday', 'OccupRooms',
                                  'UnusedGroupBlocks',
                                  'Occup', 'A', 'C', 'I', 'RoomRevenue', 'ADR', 'REVPAR', 'OOORooms', 'UnusedAllotment',
                                  'AvailRooms']
            shifted_df.insert(0, column="propertyCode", value=propertyCode)
            shifted_df.insert(1, column="pullDateId", value=pullDateId)
            shifted_df['Date'] = pd.to_datetime(shifted_df['Date'])
            shifted_df['UnusedAllotment'] = shifted_df['UnusedAllotment'].fillna(0).astype(int)
            shifted_df['AvailRooms'] = shifted_df['AvailRooms'].fillna(0).astype(int)
            shifted_df.to_csv(occupancy_file_path, index=False)
            # End Data Modification Occupancy

            # Start Data Modification Reservation
            df = pd.read_csv(reservation_file_path)
            df = df[df.columns[:-1]]
            columns = [
                'propertyCode',
                'pullDateId',
                'DateTime',
                'Status',
                'MyhmsConf',
                'CRSConf',
                'roomnumber',
                'roomtype',
                'rateplan',
                'rate',
                'Paytype',
                'arrivaldate',
                'departuredate',
                'nights',
                'guestlastname',
                'guestfirstname',
                'address1',
                'address2',
                'city',
                'state',
                'zip',
                'country',
                'phone',
                'email',
                'TotalRevenueforstay',
                'RoomRevenueforstay',
                'source',
                'market',
                'channel',
                'companyName',
                'drivingLicence'
            ]
            df['Date/Time'] = pd.to_datetime(df["Date/Time"]).dt.date
            df['arrival date'] = pd.to_datetime(df['arrival date'])
            df['departure date'] = pd.to_datetime(df['departure date'])
            df.insert(0, column="propertyCode", value=propertyCode)
            df.insert(1, column="pullDateId", value=pullDateId)
            df['room number'] = df['room number'].fillna(0).astype(int)
            df.to_csv(reservation_file_path, index=False, header=columns)
            # End Data Modification Reservation

            # Start Group Block Summary Modification
            df = pd.read_csv(group_block_summary_file_path, header=None, skiprows=6, skipfooter=1, engine='python')
            df = df.dropna(axis=0, how='all').dropna(axis=1, how='all')
            headers = ['GroupBookingName', 'Confirmation', 'Arrive', 'Depart', 'Block', 'P_U', 'Diff', "BlockedRevenue", "PickedUpRevenue", "CutoffDate"]
            df.columns = headers
            df['Arrive'] = pd.to_datetime(df['Arrive'], errors='coerce', format='%m/%d/%y')
            df['Depart'] = pd.to_datetime(df['Depart'], errors='coerce', format='%m/%d/%y')
            df['CutoffDate'] = pd.to_datetime(df['CutoffDate'], errors='coerce', format='%m/%d/%y')
            df.insert(0, column="propertyCode", value=propertyCode)
            df.insert(1, column="pullDateId", value=pullDateId)
            df['Block'] = df['Block'].fillna(0).astype(int)
            df['P_U'] = df['P_U'].fillna(0).astype(int)
            df['Diff'] = df['Diff'].fillna(0).astype(int)
            df.to_csv(group_block_summary_file_path, index=False)
            # End Group Block Summary Modification

            # Insert Into Database
            res_result = csv.DictReader(open(reservation_file_path, encoding="utf-8"))
            res_result = list(res_result)
            print(len(res_result))
            bulk_insert_auto_clerk_res(propertyCode, res_result, row['res_before'], row['res_after'])
            print("RES DONE")

            occ_result = csv.DictReader(open(occupancy_file_path, encoding="utf-8"))
            occ_result = list(occ_result)
            print(len(occ_result))
            bulk_insert_auto_clerk_occ(propertyCode, occ_result, row['occ_before'], row['occ_after'])
            print("OCC DONE")

            group_block_summary_result = csv.DictReader(open(group_block_summary_file_path, encoding="utf-8"))
            group_block_summary_result = list(group_block_summary_result)
            print(len(group_block_summary_result))
            bulk_insert_auto_clerk_group_block_summary(propertyCode, group_block_summary_result, row['res_before'], row['res_after'])
            print("GROUP BLOCK SUMMARY DONE")

            update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
        else:
            msg = "File Not found!!!"
            update_into_pulldate(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)
    except Exception as e:
        if driver:
            driver.quit()
        msg = f"[{atica_property_code}] Somethings went wrong."
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
    PMS_NAME = "'AutoClerk'"
    print("SCRIPT STARTED FOR AutoClerk")
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
            AutoClerk_Pms(row)
            print("SCRIPT DONE FOR AutoClerk")
        else:
            print("LAST_PULL_DATE_ID is NULL")
