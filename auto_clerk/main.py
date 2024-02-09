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

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

from utils.db import db_config
from utils.db import db_models
from sqlalchemy.dialects.postgresql import insert



def bulk_insert_auto_clerk_res(propertyCode, res_list, res_before, res_after):
    print("Data importing...")
    error_temp = ""
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.auto_clerk_res_model).values(res_list)
        conn.commit()
        stmt = stmt.on_conflict_do_update(
            index_elements=['uniqueKey'],
            set_={
                'pullDateId': stmt.excluded.pullDateId,
                'updatedAt': stmt.excluded.updatedAt,
                'updatedAtEpoch': stmt.excluded.updatedAtEpoch,
                'DateTime': stmt.excluded.DateTime,
                'Status': stmt.excluded.Status,
                'MyhmsConf': stmt.excluded.MyhmsConf,
                'CRSConf': stmt.excluded.CRSConf,
                'roomnumber': stmt.excluded.roomnumber,
                'roomtype': stmt.excluded.roomtype,
                'rateplan': stmt.excluded.rateplan,
                'rate': stmt.excluded.rate,
                'Paytype': stmt.excluded.Paytype,
                'arrivaldate': stmt.excluded.arrivaldate,
                'departuredate': stmt.excluded.departuredate,
                'nights': stmt.excluded.nights,
                'guestlastname': stmt.excluded.guestlastname,
                'guestfirstname': stmt.excluded.guestfirstname,
                'address1': stmt.excluded.address1,
                'address2': stmt.excluded.address2,
                'city': stmt.excluded.city,
                'state': stmt.excluded.state,
                'zip': stmt.excluded.zip,
                'country': stmt.excluded.country,
                'phone': stmt.excluded.phone,
                'email': stmt.excluded.email,
                'TotalRevenueforstay': stmt.excluded.TotalRevenueforstay,
                'RoomRevenueforstay': stmt.excluded.RoomRevenueforstay,
                'source': stmt.excluded.source,
                'market': stmt.excluded.market,
                'channel': stmt.excluded.channel,
                'companyName': stmt.excluded.companyName,
                'drivingLicence': stmt.excluded.drivingLicence,
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


def bulk_insert_auto_clerk_occ(propertyCode, occ_list, occ_before, occ_after):
    print("Data importing...")
    error_temp = ""
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.auto_clerk_occ_model).values(occ_list)
        conn.commit()
        stmt = stmt.on_conflict_do_update(
            index_elements=['uniqueKey'],
            set_={
                'pullDateId': stmt.excluded.pullDateId,
                'updatedAt': stmt.excluded.updatedAt,
                'updatedAtEpoch': stmt.excluded.updatedAtEpoch,
                'Date': stmt.excluded.Date,
                'Rooms': stmt.excluded.Rooms,
                'Arrivals': stmt.excluded.Arrivals,
                'StayOver': stmt.excluded.StayOver,
                'DepartToday': stmt.excluded.DepartToday,
                'OccupRooms': stmt.excluded.OccupRooms,
                'UnusedGroupBlocks': stmt.excluded.UnusedGroupBlocks,
                'Occup': stmt.excluded.Occup,
                'A': stmt.excluded.A,
                'C': stmt.excluded.C,
                'I': stmt.excluded.I,
                'RoomRevenue': stmt.excluded.RoomRevenue,
                'ADR': stmt.excluded.ADR,
                'REVPAR': stmt.excluded.REVPAR,
                'OOORooms': stmt.excluded.OOORooms,
                'UnusedAllotment': stmt.excluded.UnusedAllotment,
                'AvailRooms': stmt.excluded.AvailRooms,
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


def bulk_insert_auto_clerk_group_block_summary(propertyCode, group_block_summary_list, res_before, res_after):
    print("Data importing...")
    error_temp = ""
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.auto_clerk_group_block_summary_model).values(group_block_summary_list)
        conn.commit()
        stmt = stmt.on_conflict_do_update(
            index_elements=['uniqueKey'],
            set_={
                'pullDateId': stmt.excluded.pullDateId,
                'updatedAt': stmt.excluded.updatedAt,
                'updatedAtEpoch': stmt.excluded.updatedAtEpoch,
                'GroupBookingName': stmt.excluded.GroupBookingName,
                'Confirmation': stmt.excluded.Confirmation,
                'Arrive': stmt.excluded.Arrive,
                'Depart': stmt.excluded.Depart,
                'Block': stmt.excluded.Block,
                'P_U': stmt.excluded.P_U,
                'Diff': stmt.excluded.Diff,
                'BlockedRevenue': stmt.excluded.BlockedRevenue,
                'PickedUpRevenue': stmt.excluded.PickedUpRevenue,
                'CutoffDate': stmt.excluded.CutoffDate,
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


def AutoClerk_Pms(row):
    atica_property_code = row['atica_property_code']
    pullDateId = row['pullDateId']
    propertyCode = row['propertyCode']
    platform = "PMS"

    username = None
    password = None
    try:
        print(f"Getting Secret for {atica_property_code}")
        json_dict = get_secret_from_api(propertyCode, platform)
        print("res ::")
        username = json_dict['u']
        password = json_dict['p']
    except Exception:
        msg = f"[{atica_property_code}] secret fetch failed due to bad json"
        print(msg)
        update_into_pulldate(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)
        return 0

    if username is None or password is None:
        msg = f"[{atica_property_code}] username and password is wrong!!!"
        print(msg)
        update_into_pulldate(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)

    folder_name = f"./reports/{propertyCode}/"
    # save_dir = os.path.abspath('reports/')
    save_dir = os.path.abspath(f'./reports/{propertyCode}/')
    driver = None
    try:
        file_paths = [
            f'{folder_name}Occupancy.csv',
            f'{folder_name}Reservation.csv',
            f'{folder_name}groupBlockSummary.csv',
        ]
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)

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
            time.sleep(10)

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

        createdAt = "'" + str(arrow.now()) + "'"
        updatedAt = "'" + str(arrow.now()) + "'"
        createdAtEpoch = int(arrow.utcnow().timestamp())
        updatedAtEpoch = int(arrow.utcnow().timestamp())

        errorMessage = ""
        fileCount = 0

        try:
            if check_occupancy_file:

                fileCount = fileCount + 1
                # Start Data Modification Occupancy
                df = pd.read_csv(occupancy_file_path)
                df = df.drop([0, 1, 2, 3, 4, 5])
                df = df.reset_index(drop=True)

                updated_data = pd.DataFrame(df)

                value_to_delete = 'Date'
                index_to_delete = []
                index_to_delete = updated_data[updated_data.iloc[:, 0] == value_to_delete].index

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
                shifted_df.dropna(subset=["Date"], inplace=True)
                shifted_df.insert(0, column="propertyCode", value=propertyCode)
                shifted_df.insert(1, column="pullDateId", value=pullDateId)
                shifted_df.insert(2, column="createdAt", value=createdAt)
                shifted_df.insert(3, column="updatedAt", value=updatedAt)
                shifted_df.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                shifted_df.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                shifted_df['Date'] = pd.to_datetime(shifted_df['Date'], format='mixed', errors='coerce')
                shifted_df.insert(6, column="uniqueKey", value=shifted_df["propertyCode"].astype(str) + "_" + shifted_df["Date"].astype(str))
                shifted_df.to_csv(occupancy_file_path, index=False)
                # End Data Modification Occupancy

                # Insert Into Database
                occ_result = csv.DictReader(open(occupancy_file_path, encoding="utf-8"))
                occ_result = list(occ_result)
                print(len(occ_result))

                if len(occ_result) > 0:
                    error_temp = bulk_insert_auto_clerk_occ(propertyCode, occ_result, row['occ_before'], row['occ_after'])
                    if(error_temp == ""):
                        print("OCC DONE")
                    else:
                        print("OCC FAILED")
                        errorMessage = errorMessage + " OCC Failed: " + error_temp
                    
                else:
                    errorMessage = errorMessage + "Occupancy File Was Blank, "
            else:
                errorMessage = errorMessage + "Occupancy File Not Found, "
        except Exception as e:
            error_message = str(e)
            print(error_message)
            error_temp=error_message[:250]
            errorMessage = errorMessage + " OCC Parsing Failed: " + error_temp

        try:
            if check_reservation_file:

                fileCount = fileCount + 1
                # Start Data Modification Reservation
                df = pd.read_csv(reservation_file_path)
                df = df[df.columns[:-1]]
                columns = [
                    'propertyCode',
                    'pullDateId',
                    'createdAt',
                    'updatedAt',
                    'createdAtEpoch',
                    'updatedAtEpoch',
                    'uniqueKey',
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
                df['arrival date'] = pd.to_datetime(df['arrival date'], format='mixed', errors='coerce')
                df['departure date'] = pd.to_datetime(df['departure date'], format='mixed', errors='coerce')
                df.dropna(subset=["Myhms Conf"], inplace=True)
                df.insert(0, column="propertyCode", value=propertyCode)
                df.insert(1, column="pullDateId", value=pullDateId)
                df.insert(2, column="createdAt", value=createdAt)
                df.insert(3, column="updatedAt", value=updatedAt)
                df.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                df.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                df.insert(6, column="uniqueKey", value=df["Myhms Conf"].astype(str))
                df.to_csv(reservation_file_path, index=False, header=columns)
                # End Data Modification Reservation

                # Insert Into Database
                res_result = csv.DictReader(open(reservation_file_path, encoding="utf-8"))
                res_result = list(res_result)
                print(len(res_result))
                if len(res_result) > 0:
                    error_temp = bulk_insert_auto_clerk_res(propertyCode, res_result, row['res_before'], row['res_after'])
                    if(error_temp == ""):
                        print("RES DONE")
                    else:
                        print("RES FAILED")
                        errorMessage = errorMessage + " RES Failed: " + error_temp
                else:
                    errorMessage = errorMessage + "Reservation File Was Blank, "
            else:
                errorMessage = errorMessage + "Reservation File Not Found, "
        except Exception as e:
            error_message = str(e)
            print(error_message)
            error_temp=error_message[:250]
            errorMessage = errorMessage + " RES Parsing Failed: " + error_temp

        try:
            if check_group_block_summary_file:

                fileCount = fileCount + 1
                # Start Group Block Summary Modification
                df = pd.read_csv(group_block_summary_file_path, header=None, skiprows=6, skipfooter=1, engine='python', on_bad_lines='skip')
                df = df.dropna(axis=0, how='all').dropna(axis=1, how='all')
                headers = ['GroupBookingName', 'Confirmation', 'Arrive', 'Depart', 'Block', 'P_U', 'Diff', "BlockedRevenue", "PickedUpRevenue", "CutoffDate"]
                df.columns = headers
                df['Arrive'] = pd.to_datetime(df['Arrive'], errors='coerce', format='%m/%d/%y')
                df['Depart'] = pd.to_datetime(df['Depart'], errors='coerce', format='%m/%d/%y')
                df['CutoffDate'] = pd.to_datetime(df['CutoffDate'], errors='coerce', format='%m/%d/%y')
                df.dropna(subset=["Confirmation"], inplace=True)
                df.insert(0, column="propertyCode", value=propertyCode)
                df.insert(1, column="pullDateId", value=pullDateId)
                df.insert(2, column="createdAt", value=createdAt)
                df.insert(3, column="updatedAt", value=updatedAt)
                df.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                df.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                df.insert(6, column="uniqueKey", value=df["Confirmation"].astype(str))
                df.to_csv(group_block_summary_file_path, index=False)
                # End Group Block Summary Modification

                # Insert Into Database
                group_block_summary_result = csv.DictReader(open(group_block_summary_file_path, encoding="utf-8"))
                group_block_summary_result = list(group_block_summary_result)
                print(len(group_block_summary_result))
                if len(group_block_summary_result) > 0:
                    error_temp = bulk_insert_auto_clerk_group_block_summary(propertyCode, group_block_summary_result, row['res_before'], row['res_after'])
                    if(error_temp == ""):
                        print("GROUP BLOCK SUMMARY DONE")
                    else:
                        print("GROUP BLOCK SUMMARY FAILED")
                        errorMessage = errorMessage + " GROUP BLOCK SUMMARY Failed: " + error_temp
                    
                else:
                    errorMessage = errorMessage + "Group Block Summary File Was Blank, "
            else:
                errorMessage = errorMessage + "Group Block Summary File Not Found, "
        except Exception as e:
            error_message = str(e)
            print(error_message)
            error_temp=error_message[:250]
            errorMessage = errorMessage + " GROUP BLOCK SUMMARY Parsing Failed: " + error_temp

        if (fileCount == 3):
            if (errorMessage == ""):
                update_into_pulldate(pullDateId, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
            else:
                errorMessage = "Partially Successfull:- " + errorMessage
                update_into_pulldate(pullDateId, ERROR_NOTE=errorMessage, IS_ERROR=True)
        else:
            if (fileCount == 0):
                errorMessage = "All File Not Found"
            else:
                errorMessage = "Partially Successfull:- " + errorMessage
            update_into_pulldate(pullDateId, ERROR_NOTE=errorMessage, IS_ERROR=True)

    except Exception as e:
        print(e)
        if driver:
            driver.quit()
        msg = f"[{atica_property_code}] Somethings went wrong. Due to {e}"
        print(msg)
        update_into_pulldate(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)
        return 0
    


def insert_into_pulldate(PROPERTY_CODE, PULLED_DATE, PMS_NAME):
    LAST_PULL_DATE_ID = None

    query_string = f"""INSERT INTO "tbl_pullDate" ("propertyCode", "pulledDate", "status","pmsName") VALUES ('{PROPERTY_CODE}', '{str(PULLED_DATE)}', 'INPROGRESS','{PMS_NAME}') RETURNING id; """
    print("query_string : ", query_string)
    conn = db_config.get_db_connection()
    try:
        result = conn.execute(text(query_string))
        conn.commit()
        LAST_PULL_DATE_ID = result.fetchone()
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
    query_string = f"""UPDATE "tbl_pullDate" SET status={DB_STATUS}, "updatedAt"='{str(arrow.now())}', "errorNote"='{str(ERROR_NOTE)}' WHERE "id"='{str(LAST_PULL_DATE_ID)}';"""
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

    PMS_NAME = "AutoClerk"
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
            print("item : ", item)
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
                AutoClerk_Pms(row)
            else:
                print("LAST_PULL_DATE_ID is NULL")
    else:
        print(f"Property not available in database!!!")
    print(f"[{PMS_NAME}] SCRIPT STOP!!!")
