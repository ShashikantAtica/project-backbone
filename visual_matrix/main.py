import argparse
import json
import os
import sys

from sqlalchemy import text

sys.path.append("..")
import arrow
import pandas as pd
import numpy as np
import requests
from utils.secrets.SecretManager import get_secret_from_api
import csv

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from utils.db import db_config
from utils.db import db_models


def bulk_insert_visual_matrix_front_office_arrival(propertyCode, front_office_arrival_list, res_before, res_after):
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
    conn.execute(text(f'DELETE from visual_matrix_front_office_arrival where {reservation} between {start_date} and {end_date} and "propertyCode" = {db_propertyCode};'))
    conn.commit()
    conn.close()

    # Add new data of reservation (up to 90 Days)
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.visual_matrix_front_office_arrival_model.insert(), front_office_arrival_list)
    conn.commit()
    conn.close()
    print("Data imported")


def bulk_insert_visual_matrix_occupancy(propertyCode, occ_list, occ_before, occ_after):
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
    conn.execute(text(f'DELETE from visual_matrix_occupancy where {reservation} between {start_date} and {end_date} and "propertyCode" = {db_propertyCode};'))
    conn.commit()
    conn.close()

    # Add new data of occ (up to 90 Days)
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.visual_matrix_occupancy_model.insert(), occ_list)
    conn.commit()
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
    else:
        try:
            file_paths = [
                f'{folder_name}{propertyCode}_Front_Office_Arrival.csv',
                f'{folder_name}{propertyCode}_Front_Office_Arrival.xls',
                f'{folder_name}{propertyCode}_Occupancy.csv',
                f'{folder_name}{propertyCode}_Occupancy.xls',
            ]
            for file_path in file_paths:
                if os.path.exists(file_path):
                    os.remove(file_path)
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
                    createdAt = "'" + str(arrow.now()) + "'"
                    updatedAt = "'" + str(arrow.now()) + "'"
                    createdAtEpoch =  int(arrow.utcnow().timestamp())
                    updatedAtEpoch =  int(arrow.utcnow().timestamp())
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
                        read.insert(2, column="createdAt", value=createdAt)
                        read.insert(3, column="updatedAt", value=updatedAt)
                        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                        new_df = read[['propertyCode', 'pullDateId', 'createdAt', 'updatedAt', 'createdAtEpoch', 'updatedAtEpoch', 'Guest Name', 'Status', 'Arrive', 'Departs', 'A', 'C', 'Type', 'Rate', 'PKG', 'MOP', 'ADV DEP', 'Share', 'Room']].copy()
                        new_df.fillna(value=np.nan, inplace=True)
                        new_df.loc[:, 'Arrive'] = pd.to_datetime(new_df['Arrive'], format='%m/%d/%y', errors='coerce').dt.date
                        new_df.loc[:, 'Departs'] = pd.to_datetime(new_df['Departs'], format='%m/%d/%y', errors='coerce').dt.date
                        new_df = new_df.dropna(subset=['Arrive', 'Departs'], how='all')
                        new_df.loc[new_df['Guest Name'].isnull(), 'Guest Name'] = ''
                        headers = ['propertyCode', 'pullDateId', 'createdAt', 'updatedAt', 'createdAtEpoch', 'updatedAtEpoch', 'GuestName', 'Status', 'Arrive', 'Departs', 'A', 'C', 'Type', 'Rate', 'PKG', 'MOP', 'ADV_DEP', 'Share', 'Room']
                        new_df.columns = headers
                        new_df['A'] = new_df['A'].fillna(0).astype(int)
                        new_df['C'] = new_df['C'].fillna(0).astype(int)
                        new_df.to_csv(f'{folder_name}{propertyCode}_Front_Office_Arrival.csv', index=False)
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
                        read.dropna(subset=['Date'], inplace=True)
                        read.insert(0, column="propertyCode", value=propertyCode)
                        read.insert(1, column="pullDateId", value=pullDateId)
                        read.insert(2, column="createdAt", value=createdAt)
                        read.insert(3, column="updatedAt", value=updatedAt)
                        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                        new_df = read[['propertyCode', 'pullDateId', 'createdAt', 'updatedAt', 'createdAtEpoch', 'updatedAtEpoch', 'Date', 'Day', 'Maint', 'Non-GTD', 'GTD', 'Non-GTD', 'GTD', 'Alloc', 'ARV',
                                       'Dep', 'Overs', 'Rms', 'Rms', 'Occ %', 'Rm Rev', 'ADR', 'Par', 'Rooms LY', 'Variance']].copy()
                        new_df['Date'] = pd.to_datetime(new_df['Date'], errors='coerce', format='%m/%d/%y').dt.date
                        new_df = new_df.dropna(subset=['Date'])
                        new_df.insert(6, column="uniqueKey", value=new_df["propertyCode"].astype(str) + "_" + new_df['Date'].astype(str))
                        headers = ['propertyCode', 'pullDateId', 'createdAt', 'updatedAt', 'createdAtEpoch', 'updatedAtEpoch', 'uniqueKey', 'Date', 'Day', 'Maint', 'GuestArrivals_Non_GTD', 'GuestArrivals_GTD', 'GroupArrivals_Non_GTD', 'GroupArrivals_GTD', 'GroupAlloc', 'ARV',
                                   'Dep', 'StayOvers', 'AvailRms', 'OccRms', 'OccPer', 'RmRev', 'ADR', 'RevPar', 'ActualRoomsLY', 'OccPerVariance']
                        new_df.columns = headers
                        new_df['ActualRoomsLY'] = new_df['ActualRoomsLY'].fillna(0).astype(int)
                        new_df.to_csv(f'{folder_name}{propertyCode}_Occupancy.csv', index=False)
                        print(f"[{atica_property_code}]{report_type} report pulled successfully")
                    else:
                        print(f"[{atica_property_code}]{report_type} report failed due to status code: {occupancy_response.status_code}")
                    # End Occupancy Report

            front_office_arrival_file_path = f'{folder_name}{propertyCode}_Front_Office_Arrival.csv'
            occupancy_file_path = f'{folder_name}{propertyCode}_Occupancy.csv'

            check_front_office_arrival_file = os.path.isfile(front_office_arrival_file_path)
            check_occupancy_file = os.path.isfile(occupancy_file_path)

            error_msg = ""
            fileCount=0

            if not check_front_office_arrival_file:
                error_msg = error_msg + " Front Office Arrival file - N/A"

            if not check_occupancy_file:
                error_msg = error_msg + " Occupancy file - N/A"

            if check_front_office_arrival_file:

                fileCount=fileCount+1
                front_office_arrival_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Front_Office_Arrival.csv", encoding="utf-8"))
                front_office_arrival_result = list(front_office_arrival_result)
                print(len(front_office_arrival_result))
                if len(front_office_arrival_result) > 0:
                    bulk_insert_visual_matrix_front_office_arrival(propertyCode, front_office_arrival_result, row['res_before'], row['res_after'])
                    print("FRONT OFFICE ARRIVAL DONE")
                else:
                    error_msg = error_msg + "Front Office Arrival File Was Blank, "

            if check_occupancy_file:

                fileCount=fileCount+1
                occ_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Occupancy.csv", encoding="utf-8"))
                occ_result = list(occ_result)
                print(len(occ_result))
                if len(occ_result) > 0:
                    bulk_insert_visual_matrix_occupancy(propertyCode, occ_result, row['occ_before'], row['occ_after'])
                    print("OCC DONE")
                else:
                    error_msg = error_msg + "Occupancy File Was Blank, "

            if (fileCount==2):
                if(error_msg==""):
                    update_into_pulldate(pullDateId, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
                else:
                    error_msg="Partially Successfull:- "+error_msg
                    update_into_pulldate(pullDateId, ERROR_NOTE=error_msg, IS_ERROR=True)
            else:
                if (fileCount==0):
                    error_msg = "All File Not Found"
                else:
                    error_msg="Partially Successfull:- "+error_msg
                update_into_pulldate(pullDateId, ERROR_NOTE=error_msg, IS_ERROR=True)    

        except Exception as e:
            update_into_pulldate(pullDateId, ERROR_NOTE=e, IS_ERROR=True)


def insert_into_pulldate(PROPERTY_CODE, PULLED_DATE,PMS_NAME):
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

    PMS_NAME = "VisualMatrix"
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
            LAST_PULL_DATE_ID = insert_into_pulldate(PROPERTY_CODE, PULLED_DATE,PMS_NAME)

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
                VisualMatrix_Pms(row)
            else:
                print("LAST_PULL_DATE_ID is NULL")
    else:
        print(f"Property not available in database!!!")
    print(f"[{PMS_NAME}] SCRIPT STOP!!!")