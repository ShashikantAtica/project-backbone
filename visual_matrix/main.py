import json
import os
import sys

sys.path.append("..")
import arrow
import pandas as pd
import numpy as np
import requests
from utils.secrets.SecretManager import get_secret_from_api as get_secret_dict
import csv

from utils.db import db_config
from utils.db import db_models


def bulk_insert_visual_matrix_front_office_arrival(propertyCode, front_office_arrival_list):
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

        # Delete existing data of reservation
        conn = db_config.get_db_connection()
        conn.execute(
            f'DELETE from visual_matrix_front_office_arrival where {pullDateId} = {pullDateIdValue};')
        conn.close()
        print("DELETE OLD DATA!!!", pullDateIdValue)
    else:
        print("Not previous data!!!")

    # Add new data of reservation (up to 90 Days)
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.visual_matrix_front_office_arrival_model.insert(), front_office_arrival_list)
    conn.close()
    print("Data imported")


def bulk_insert_visual_matrix_occupancy(propertyCode, occ_list):
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

        # Delete existing data of reservation
        conn = db_config.get_db_connection()
        conn.execute(
            f'DELETE from visual_matrix_occupancy where {pullDateId} = {pullDateIdValue};')
        conn.close()
        print("DELETE OLD DATA!!!", pullDateIdValue)
    else:
        print("Not previous data!!!")

    # Add new data of occ (up to 90 Days)
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.visual_matrix_occupancy_model.insert(), occ_list)
    conn.close()
    print("Data imported")


def VisualMatrix_Pms(row):
    atica_property_code = row['atica_property_code']
    external_property_code = row['external_property_code']
    propertyCode = row['propertyCode']
    pullDateId = row['pullDateId']
    platform = "PMS"
    folder_name = "./reports/"

    username = None
    password = None
    try:
        print(f"Getting Secret for {atica_property_code}")
        json_dict = get_secret_dict(propertyCode, platform)
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
    else:
        try:
            with requests.Session() as s:
                s.headers.update({'Upgrade-Insecure-Requests': '1',
                                  'User-Agent': 'Mozilla/5.0 '
                                                '(Windows NT 10.0; Win64; x64) '
                                                'AppleWebKit/537.36 (KHTML, like Gecko) '
                                                'Chrome/70.0.3538.77 Safari/537.36'})

                login_url = "https://login.vmcloudpms.com/account/token"
                payload = {
                    'client_id': external_property_code,
                    'username': username,
                    'password': password,
                    'grant_type': 'password',
                }
                headers = {'user-agent': 'XY'}

                login_url_response = s.post(login_url, headers=headers, data=payload)

                if login_url_response.status_code == 200:
                    json_data = json.loads(login_url_response.text)
                    access_token = json_data.get("access_token")
                    print(f"[{atica_property_code}] Login Successful")
                else:
                    print(f"[{atica_property_code}] Login failed")
                    return 0

                if access_token is not None:
                    # Start Front Office Arrival Report
                    report_type = "[Front Office Arrival]"
                    front_office_arrival_params = {
                        'handlerId': 'FroRepArrivals',
                        'date1': row['res_before'].format('YYYY-MM-DD'),
                        'date2': row['res_after'].format('YYYY-MM-DD'),
                        'includeCheckIns': 'true',
                        'excludeRates': 'false',
                        'excludeCrsComments': 'false',
                        'centralResOnly': 'false',
                        'groupOptionId': '0',
                        'groupId': '-1',
                        'excludePreRegNotes': 'true',
                        'preRegOnly': 'false',
                        'exportFormat': 'Excel',
                        'docName': 'Arrivals.xls',
                        'access_token': access_token
                    }
                    front_office_arrival_report_url = f"https://002831.prod3.vmcloudpms.com/v1/property/reporting/export"

                    front_office_arrival_response = s.get(front_office_arrival_report_url, params=front_office_arrival_params)
                    if front_office_arrival_response.status_code == 200:
                        filename = f'{folder_name}{propertyCode}_Front_Office_Arrival.xls'
                        open(filename, "wb").write(front_office_arrival_response.content)

                        read = pd.read_excel(filename, skiprows=13)
                        read.insert(0, column="propertyCode", value=propertyCode)
                        read.insert(1, column="pullDateId", value=pullDateId)
                        new_df = read[['propertyCode', 'pullDateId', 'Guest Name', 'Status', 'Arrive', 'Departs', 'A', 'C', 'Type', 'Rate', 'PKG', 'MOP', 'ADV DEP', 'Share', 'Room']].copy()
                        new_df.fillna(value=np.nan, inplace=True)
                        new_df.loc[:, 'Arrive'] = pd.to_datetime(new_df['Arrive'], format='%m/%d/%y', errors='coerce').dt.date
                        new_df.loc[:, 'Departs'] = pd.to_datetime(new_df['Departs'], format='%m/%d/%y', errors='coerce').dt.date
                        new_df = new_df.dropna(subset=['Arrive', 'Departs'], how='all')
                        new_df.loc[new_df['Guest Name'].isnull(), 'Guest Name'] = ''
                        headers = ['propertyCode', 'pullDateId', 'GuestName', 'Status', 'Arrive', 'Departs', 'A', 'C', 'Type', 'Rate', 'PKG', 'MOP', 'ADV_DEP', 'Share', 'Room']
                        new_df.to_csv(f'{folder_name}{propertyCode}_Front_Office_Arrival.csv', header=headers, index=False)
                        print(f"[{atica_property_code}]{report_type} report pulled successfully")
                    else:
                        print(f"[{atica_property_code}]{report_type} report failed due to status code: {front_office_arrival_response.status_code}")
                    # End Front Office Arrival Report

                    # Start Occupancy Report
                    report_type = "[Occupancy]"
                    occupancy_params = {
                        'handlerId': 'SmReportForecastOccupancy',
                        'dateFrom': row['occ_before'].format('YYYY-MM-DD'),
                        'dateTo': row['occ_after'].format('YYYY-MM-DD'),
                        'taxInclusive': '0',
                        'inclHld': 'true',
                        'pickUp': '0',
                        'dayOfWeek': 'ALL',
                        'reportLocation': 'Sales and Marketing',
                        'ignoreOutOfOrderRooms': '0',
                        'title': 'Occupancy Forecast',
                        'exportFormat': 'Excel',
                        'docName': 'Occupancy Forecast.xls',
                        'access_token': access_token
                    }
                    occupancy_report_url = f"https://002831.prod3.vmcloudpms.com/v1/property/reporting/export"

                    occupancy_response = s.get(occupancy_report_url, params=occupancy_params)
                    if occupancy_response.status_code == 200:
                        filename = f'{folder_name}{propertyCode}_Occupancy.xls'
                        open(filename, "wb").write(occupancy_response.content)

                        read = pd.read_excel(filename, skiprows=11)
                        read.insert(0, column="propertyCode", value=propertyCode)
                        read.insert(1, column="pullDateId", value=pullDateId)
                        new_df = read[['propertyCode', 'pullDateId', 'Date', 'Day', 'Maint', 'Non-GTD', 'GTD', 'Non-GTD', 'GTD', 'Alloc', 'ARV',
                                       'Dep', 'Overs', 'Rms', 'Rms', 'Occ %', 'Rm Rev', 'ADR', 'Par', 'Rooms LY', 'Variance']].copy()
                        new_df['Date'] = pd.to_datetime(new_df['Date'], errors='coerce', format='%m/%d/%y').dt.date
                        new_df = new_df.dropna(subset=['Date'])
                        headers = ['propertyCode', 'pullDateId', 'Date', 'Day', 'Maint', 'GuestArrivals_Non_GTD', 'GuestArrivals_GTD', 'GroupArrivals_Non_GTD', 'GroupArrivals_GTD', 'GroupAlloc', 'ARV',
                                   'Dep', 'StayOvers', 'AvailRms', 'OccRms', 'OccPer', 'RmRev', 'ADR', 'RevPar', 'ActualRoomsLY', 'OccPerVariance']
                        new_df.to_csv(f'{folder_name}{propertyCode}_Occupancy.csv', header=headers, index=False)
                        print(f"[{atica_property_code}]{report_type} report pulled successfully")
                    else:
                        print(f"[{atica_property_code}]{report_type} report failed due to status code: {occupancy_response.status_code}")
                    # End Occupancy Report

            front_office_arrival_file_path = f'{folder_name}{propertyCode}_Front_Office_Arrival.csv'
            occupancy_file_path = f'{folder_name}{propertyCode}_Occupancy.csv'

            check_front_office_arrival_file = os.path.isfile(front_office_arrival_file_path)
            check_occupancy_file = os.path.isfile(occupancy_file_path)

            error_msg = ""

            if not check_front_office_arrival_file:
                error_msg = error_msg + " Front Office Arrival file - N/A"

            if not check_occupancy_file:
                error_msg = error_msg + " Occupancy file - N/A"

            if check_front_office_arrival_file and check_occupancy_file:
                # Insert into Database
                front_office_arrival_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Front_Office_Arrival.csv", encoding="utf-8"))
                front_office_arrival_result = list(front_office_arrival_result)
                print(len(front_office_arrival_result))
                bulk_insert_visual_matrix_front_office_arrival(propertyCode, front_office_arrival_result)
                print("FRONT OFFICE ARRIVAL DONE")

                occ_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Occupancy.csv", encoding="utf-8"))
                occ_result = list(occ_result)
                print(len(occ_result))
                bulk_insert_visual_matrix_occupancy(propertyCode, occ_result)
                print("OCC DONE")

                update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
            else:
                update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=error_msg, IS_ERROR=True)
        except Exception as e:
            update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=e, IS_ERROR=True)


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
    PMS_NAME = "'VisualMatrix'"
    print("SCRIPT STARTED FOR VISUAL MATRIX")
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
            row = {
                'atica_property_code': '' + PMS_NAME + '_' + EXTERNAL_PROPERTY_CODE,
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
            VisualMatrix_Pms(row)
            print("SCRIPT DONE FOR VISUAL MATRIX")
        else:
            print("LAST_PULL_DATE_ID is NULL")
