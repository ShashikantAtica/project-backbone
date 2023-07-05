import json
import os
import sys

sys.path.append("..")
import re
import time
import arrow
import pandas as pd
import requests
from utils.secrets.SecretManager import get_secret_dict
from bs4 import BeautifulSoup
import csv

# from utils.db.db_config import get_db_connection
# from utils.db.db_models import choice_res_model, choice_occ_model
from utils.db import db_config
from utils.db import db_models


def bulk_insert_choice_res(res_list, res_before, res_after):
    start_date = "'" + res_before.format("YYYY-MM-DD") + "'"
    end_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    print("start_date :: ", start_date)
    print("end_date :: ", end_date)

    # delete all data
    # current_date = arrow.now()
    # print("current_date :: ", current_date)
    # start_date = current_date.shift(days=-90)
    # print("start_date :: ", start_date)
    reservation = '"ReserveDate"'
    # current_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    # start_date = "'" + res_before.format("YYYY-MM-DD") + "'"

    # Delete existing data of reservation (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(
        f'DELETE from choice_res where {reservation} between {start_date} and {end_date};')
    conn.close()

    # Add new data of reservation (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(db_models.choice_res_model.insert(), res_list)
    conn.close()


def bulk_insert_choice_occ(occ_list, occ_before, occ_after):
    start_date = "'" + occ_before.format("YYYY-MM-DD") + "'"
    print("start_date :: ", start_date)

    end_date = "'" + occ_after.format("YYYY-MM-DD") + "'"
    print("end_date :: ", end_date)

    # Delete existing data of occ (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(f'DELETE FROM choice_occ where "IDS_DATE" between {start_date} and {end_date};')
    conn.close()

    # Add new data of occ (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(db_models.choice_occ_model.insert(), occ_list)
    conn.close()


def Choice_Pms(row):
    atica_property_code = row['atica_property_code']
    external_property_code = row['external_property_code']
    secret_name = row['gcp_secret']
    property_type = row['property_type']
    ignore_size_check = False
    current_date = row['current_date']
    propertyCode = row['propertyCode']
    pullDateId = row['pullDateId']

    username = None
    password = None
    try:
        print("secret_name :: ", secret_name)
        json_dict = get_secret_dict(secret_name)
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
        BASE_URL = ""
        if property_type == 'Skytouch':
            BASE_URL = "https://www.skytouchhos.com/pms"
        elif property_type == 'Choice':
            BASE_URL = "https://www.choiceadvantage.com/choicehotels"

        with requests.Session() as s:
            s.headers.update({'Upgrade-Insecure-Requests': '1',
                              'User-Agent': 'Mozilla/5.0 '
                                            '(Windows NT 10.0; Win64; x64) '
                                            'AppleWebKit/537.36 (KHTML, like Gecko) '
                                            'Chrome/70.0.3538.77 Safari/537.36'})

            url2 = f'{BASE_URL}/j_security_check'
            data = {'j_username': username, 'j_password': password}
            login_url = s.post(url2, data=data)
            print(login_url.status_code)
            page = s.post(f'{BASE_URL}/Login.do')

            final_login_params = {
                "propertyCode": external_property_code
            }
            final_login = s.post(f"{BASE_URL}/Login.do?cleanSession=true", params=final_login_params)
            soup = BeautifulSoup(final_login.content, 'html.parser')
            scr = soup.findAll("script", {"src": re.compile('propInfoCached.js')})
            if not scr:
                msg = f"{atica_property_code} login failed"
                print(msg)
                update_into_pulldate(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)
                return 0
            else:
                try:
                    js_path = scr[0]["src"]
                except IndexError as e:
                    print(f"{atica_property_code} due to {e}")
                    raise e

            testurl = f'{BASE_URL}/' + js_path
            propidpage = s.get(testurl)
            myvars = str(propidpage.content).split(';')
            serverkey = None
            if 'propInfoCached.reportServerKey' in str(myvars):
                serverkey = re.findall(r'propInfoCached.reportServerKey = "(.*?)"', str(myvars))[0]
            else:
                msg = f"{atica_property_code} failed to get the Server Key"
                print(msg)
                update_into_pulldate(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)
                return 0

            print(f"The server key for {atica_property_code} is: {serverkey}")

            if serverkey is not None:
                # Start Reservation Report
                reservation_data_post = {
                    'JOD': 'apache_reservation_activity',
                    'locale': 'en_US',
                    'ARG0': external_property_code,
                    'ARG1': username,
                    'ARG2': current_date.format("M/DD/YYYY"),
                    # startdatepastcurrent
                    'ARG3': row['res_before'].format("M/DD/YYYY"),
                    # enddatepastcurrent
                    'ARG4': row['res_after'].format("M/DD/YYYY"),
                    'ARG5': '*',
                    'ARG6': '*',
                    'ARG7': '*',
                    'ARG8': '*',
                    'ARG9': 'arrival',
                    'ARG10': 'false',
                    'ARG11': '',
                    'ARG12': '',
                    'ARG13': '',
                    'ARG14': '',
                    'ARG15': '',
                    'CSV': 'true',
                    'CSV_SUPPRESS_HEADERS': 'false',
                    'reportId': '50',
                    'updateCounter': 'Y',
                    'includeCheckedOut': '',
                    'includeInHouse': '',
                    'altLocale': '',
                    'reportName': '',
                    'isGuestServiceEnabled': '',
                    'sortorder': '',
                    'showCurrencySymbol': '',
                    'commaSeparatedAccounts': '',
                    'activityDateType': '',
                    'reportServerKey': serverkey,
                    'reportServerUsername': username,
                    'fullPageRequestTime': int(time.time())
                }

                k = s.post(f"{BASE_URL}/ReservationActivityReport.go")

                y = s.post(f'{BASE_URL}/ReportProxyServlet.proxy?ie=pdf', data=reservation_data_post)
                report_type = '[Reservation]'

                print(f"[{atica_property_code}]{report_type} Sent request!")

                if not y.ok:
                    print(
                        f"[{atica_property_code}]{report_type} Something went wrong for the report: {y.status_code} {y.reason}")
                elif not ignore_size_check and len(y.content) < 5000:
                    print(f"[{atica_property_code}]{report_type} Report size < 5 kb not sending to the spider")
                else:
                    print(f"[{atica_property_code}]{report_type} Uploading reservations")
                    filename = f'Reservation.csv'
                    with open(filename, 'wb') as f:
                        f.write(y.content)
                        f.close()
                    read = pd.read_csv(filename)
                    read.insert(0, column="propertyCode", value=propertyCode)
                    read.insert(1, column="pullDateId", value=pullDateId)
                    # read.head()
                    read.columns = read.columns.str.replace(' ', '', regex=True).str.replace('.', '', regex=True)
                    read['Arrive'] = pd.to_datetime(read['Arrive'])
                    read['Depart'] = pd.to_datetime(read['Depart'])
                    read['ReserveDate'] = pd.to_datetime(read['ReserveDate'])
                    read['CancellationDate'] = pd.to_datetime(read['CancellationDate'])
                    read.to_csv(filename, index=False)
                # End Reservation Report

                # Start Occupancy Report
                occupancy_data_post = {
                    'JOD': 'apache_occ_forecast',
                    'locale': 'en_US',
                    'ARG0': external_property_code,
                    'ARG1': username,
                    'ARG2': current_date.format("M/DD/YYYY"),
                    # startdatepastcurrent
                    'ARG3': row['occ_before'].format("M/DD/YYYY"),
                    # enddatepastcurrent
                    'ARG4': row['occ_after'].format("M/DD/YYYY"),
                    'ARG5': '',
                    'ARG6': '',
                    'ARG7': '',
                    'ARG8': '',
                    'ARG9': '',
                    'ARG10': '',
                    'ARG11': '',
                    'ARG12': '',
                    'ARG13': '',
                    'ARG14': '',
                    'ARG15': '',
                    'CSV': 'true',
                    'CSV_SUPPRESS_HEADERS': 'false',
                    'reportId': '49',
                    'updateCounter': 'Y',
                    'includeCheckedOut': '',
                    'includeInHouse': '',
                    'altLocale': '',
                    'reportName': '',
                    'isGuestServiceEnabled': '',
                    'sortorder': '',
                    'showCurrencySymbol': '',
                    'reportServerKey': serverkey,
                    'reportServerUsername': username,
                    'fullPageRequestTime': int(time.time())
                }

                if property_type == 'Skytouch':
                    occupancy_data_post['commaSeparatedAccounts'] = ""
                    occupancy_data_post['activityDateType'] = ""

                z = s.post(f'{BASE_URL}/ReportProxyServlet.proxy?ie=pdf', data=occupancy_data_post)
                report_type = '[Occupancy]'
                print(f"[{atica_property_code}]{report_type} Sent request!")

                if not z.ok:
                    print(
                        f"[{atica_property_code}]{report_type} Something went wrong for the report: {z.status_code} {z.reason}")
                elif not ignore_size_check and len(z.content) < 5000:
                    print(f"[{atica_property_code}]{report_type} Report size < 5 kb not sending to the spider")
                else:
                    print(f"[{atica_property_code}]{report_type} Uploading occupancies")
                    filename = f'Occupancy.csv'
                    with open(filename, 'wb') as f:
                        f.write(z.content)
                        f.close()
                    read = pd.read_csv(filename)
                    read.insert(0, column="propertyCode", value=propertyCode)
                    read.insert(1, column="pullDateId", value=pullDateId)
                    # read.head()
                    read.rename(columns={read.columns[2]: "IDS_DATE"}, inplace=True)
                    read.columns = read.columns.str.replace(' ', '', regex=True).str.replace('.', '', regex=True)
                    try:
                        read['﻿IDS_DATE'] = pd.to_datetime(read['﻿IDS_DATE'])
                    except Exception:
                        read['IDS_DATE'] = pd.to_datetime(read['IDS_DATE'])
                    read.to_csv(filename, index=False)

            if serverkey is not None:
                # Insert into Database
                res_result = csv.DictReader(open("Reservation.csv"))
                res_result = list(res_result)
                print(len(res_result))
                bulk_insert_choice_res(res_result, row['res_before'], row['res_after'])

                # if os.path.exists("Reservation.csv"): #Not need to delete file
                #     os.remove("Reservation.csv")
                print("RES DONE")

                occ_result = csv.DictReader(open("Occupancy.csv"))
                occ_result = list(occ_result)
                print(len(occ_result))
                bulk_insert_choice_occ(occ_result, row['occ_before'], row['occ_after'])
                # if os.path.exists("Occupancy.csv"): #Not need to delete file
                #     os.remove("Occupancy.csv")
                print("OCC DONE")

            if serverkey is not None:
                update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE="Successfully Finished", IS_ERROR=False)


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
    print("SCRIPT STARTED FOR CHOICE")
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
            Choice_Pms(row)
            print("SCRIPT DONE FOR CHOICE")
        else:
            print("LAST_PULL_DATE_ID is NULL")
