import argparse
import os
import sys
import tempfile
from io import BytesIO

from sqlalchemy import text

sys.path.append("..")
import re
import time
import arrow
import pandas as pd
import requests
from utils.secrets.SecretManager import get_secret_from_api
from bs4 import BeautifulSoup
import csv
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

from utils.db import db_config
from utils.db import db_models
from sqlalchemy.dialects.postgresql import insert


def bulk_insert_choice_noshow(propertyCode, Noshow_result):
# log(n)*M indexing check.
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.choice_noshow_model).values(Noshow_result)
        stmt = stmt.on_conflict_do_nothing(index_elements=['uniqueKey'])
        # Execute the insert statement
        conn.execute(stmt)
        conn.commit()
        conn.close()
    except Exception as e:
        error_message = str(e)
        print(error_message)

def bulk_insert_choice_cancellation_list(propertyCode, cancellation_list_result, res_before, res_after):
    # log(n)*M indexing check.
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.choice_cancellation_list_model).values(cancellation_list_result)
        stmt = stmt.on_conflict_do_nothing(index_elements=['uniqueKey'])
        # Execute the insert statement
        conn.execute(stmt)
        conn.commit()
        conn.close()
    except Exception as e:
        error_message = str(e)
        print(error_message)

def bulk_insert_choice_res(propertyCode, res_list, res_before, res_after):
    print("Data importing...")
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.choice_res_model).values(res_list)
        conn.commit()
        stmt = stmt.on_conflict_do_update(
            index_elements=['uniqueKey'],
            set_={
                'pullDateId': stmt.excluded.pullDateId,
                'updatedAt': stmt.excluded.updatedAt,
                'updatedAtEpoch': stmt.excluded.updatedAtEpoch,
                'Account': stmt.excluded.Account,
                'GuestName': stmt.excluded.GuestName,
                'Arrive': stmt.excluded.Arrive,
                'Depart': stmt.excluded.Depart,
                'Nights': stmt.excluded.Nights,
                'Status': stmt.excluded.Status,
                'Rate': stmt.excluded.Rate,
                'RateCode': stmt.excluded.RateCode,
                'Type': stmt.excluded.Type,
                'Room': stmt.excluded.Room,
                'Source': stmt.excluded.Source,
                'CRSConfNo': stmt.excluded.CRSConfNo,
                'GTD': stmt.excluded.GTD,
                'ReserveDate': stmt.excluded.ReserveDate,
                'User': stmt.excluded.User,
                'SharedAccount': stmt.excluded.SharedAccount,
                'TrackCode': stmt.excluded.TrackCode,
                'Package': stmt.excluded.Package,
                'CancellationDate': stmt.excluded.CancellationDate,
                'CXLUserID': stmt.excluded.CXLUserID,
                
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


def bulk_insert_choice_occ(propertyCode, occ_list, occ_before, occ_after):
    print("Data importing...")
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.choice_occ_model).values(occ_list)
        conn.commit()
        stmt = stmt.on_conflict_do_update(
            index_elements=['uniqueKey'],
            set_={
                'pullDateId': stmt.excluded.pullDateId,
                'updatedAt': stmt.excluded.updatedAt,
                'updatedAtEpoch': stmt.excluded.updatedAtEpoch,
                'IDS_DATE': stmt.excluded.IDS_DATE,
                'Day': stmt.excluded.Day,
                'Rooms': stmt.excluded.Rooms,
                'OOO': stmt.excluded.OOO,
                'StayOver': stmt.excluded.StayOver,
                'Arrivals': stmt.excluded.Arrivals,
                'DueOut': stmt.excluded.DueOut,
                'Available': stmt.excluded.Available,
                'GroupBlock': stmt.excluded.GroupBlock,
                'GroupPickedUp': stmt.excluded.GroupPickedUp,
                'TransNGTD': stmt.excluded.TransNGTD,
                'TransGTD': stmt.excluded.TransGTD,
                'Occupied': stmt.excluded.Occupied,
                'OccPercent': stmt.excluded.OccPercent,
                'RoomRev': stmt.excluded.RoomRev,
                'RevPAR': stmt.excluded.RevPAR,
                'ADR': stmt.excluded.ADR,
                'Ppl': stmt.excluded.Ppl,
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


def bulk_insert_choice_cancel(propertyCode, cancel_list, cancel_before, cancel_after):
    start_date = "'" + cancel_before.format("YYYY-MM-DD") + "'"
    print("start_date :: ", start_date)

    end_date = "'" + cancel_after.format("YYYY-MM-DD") + "'"
    print("end_date :: ", end_date)
    db_propertyCode = "'" + propertyCode + "'"

    # Delete existing data of occ (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(text(f'DELETE FROM choice_cancellation where "CxlDate" between {start_date} and {end_date} and "propertyCode" = {db_propertyCode};'))
    conn.commit()
    conn.close()

    # Add new data of occ (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(db_models.choice_cancellation_model.insert(), cancel_list)
    conn.close()


def bulk_insert_choice_revenue(propertyCode, revenue_list):
    property_code = "'" + propertyCode + "'"
    print("property_code :: ", property_code)

    # Delete existing data of occ (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(text(f'DELETE FROM choice_revenue where "propertyCode" = {property_code}'))
    conn.commit()
    conn.close()

    # Add new data of occ (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(db_models.choice_revenue_model.insert(), revenue_list)
    conn.commit()
    conn.close()


def bulk_insert_choice_revenue_detail(propertyCode, revenue_detail_list, revenue_before, revenue_after):
    print("Data importing...")
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.choice_revenue_detail_model).values(revenue_detail_list)
        conn.commit()
        stmt = stmt.on_conflict_do_update(
            index_elements=['uniqueKey'],
            set_={
                'pullDateId': stmt.excluded.pullDateId,
                'updatedAt': stmt.excluded.updatedAt,
                'updatedAtEpoch': stmt.excluded.updatedAtEpoch,
                'IDS_DATE_DAY': stmt.excluded.IDS_DATE_DAY,
                'RateCode': stmt.excluded.RateCode,
                'RoomNights': stmt.excluded.RoomNights,
                'RoomNightsPer': stmt.excluded.RoomNightsPer,
                'RoomRevenue': stmt.excluded.RoomRevenue,
                'RoomRevenuePer': stmt.excluded.RoomRevenuePer,
                'DailyAVG': stmt.excluded.DailyAVG,
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


def bulk_insert_choice_group_pickup_detail(propertyCode, group_pickup_detail_list, group_pickup_before, group_pickup_after):
    print("Data importing...")
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.choice_group_pickup_detail_model).values(group_pickup_detail_list)
        conn.commit()
        stmt = stmt.on_conflict_do_update(
            index_elements=['uniqueKey'],
            set_={
                'pullDateId': stmt.excluded.pullDateId,
                'updatedAt': stmt.excluded.updatedAt,
                'updatedAtEpoch': stmt.excluded.updatedAtEpoch,
                'GroupName': stmt.excluded.GroupName,
                'GroupStatus': stmt.excluded.GroupStatus,
                'RollingCutOffDays': stmt.excluded.RollingCutOffDays,
                'FixedCutOffDate': stmt.excluded.FixedCutOffDate,
                'SalesManager': stmt.excluded.SalesManager,
                'RoomType': stmt.excluded.RoomType,
                'BlockDate': stmt.excluded.BlockDate,
                'OriginalBlock': stmt.excluded.OriginalBlock,
                'CurrentBlock': stmt.excluded.CurrentBlock,
                'GuaranteedArrivalsPickedUp': stmt.excluded.GuaranteedArrivalsPickedUp,
                'NonGuaranteedArrivalsPickedUp': stmt.excluded.NonGuaranteedArrivalsPickedUp,
                'TotalPickedUp': stmt.excluded.TotalPickedUp,
                'RoomsNotPickedUp': stmt.excluded.RoomsNotPickedUp,
                'Revenue': stmt.excluded.Revenue,
                'ADR': stmt.excluded.ADR,
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


def Choice_Pms(row):
    atica_property_code = row['atica_property_code']
    external_property_code = row['external_property_code']
    property_type = row['property_type']
    ignore_size_check = False
    current_date = row['current_date']
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
        domain_url = "https://www.choiceadvantage.com/choicehotels"

        try:
            with requests.Session() as s:
                s.headers.update({'Upgrade-Insecure-Requests': '1',
                                  'User-Agent': 'Mozilla/5.0 '
                                                '(Windows NT 10.0; Win64; x64) '
                                                'AppleWebKit/537.36 (KHTML, like Gecko) '
                                                'Chrome/70.0.3538.77 Safari/537.36'})

                url2 = f'{domain_url}/j_security_check'
                data = {'j_username': username, 'j_password': password}
                login_url = s.post(url2, data=data)
                print(login_url.status_code)
                page = s.post(f'{domain_url}/Login.do')

                final_login_params = {
                    "propertyCode": external_property_code
                }
                final_login = s.post(f"{domain_url}/Login.do?cleanSession=true", params=final_login_params)
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

                testurl = f'{domain_url}/' + js_path
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
                    createdAt = "'" + str(arrow.now()) + "'"
                    updatedAt = "'" + str(arrow.now()) + "'"
                    createdAtEpoch = int(arrow.utcnow().timestamp())
                    updatedAtEpoch = int(arrow.utcnow().timestamp())
                    reservation_dataframe = []
                    start_date = row['res_before']
                    end_date = row['res_after']
                    curr = start_date
                    while curr <= end_date:
                        start = curr
                        end = curr.shift(days=+45)
                        if end > end_date:
                            end = end_date
                        curr = curr.shift(days=+46)

                        reservation_data_post = {
                            'JOD': 'apache_reservation_activity',
                            'locale': 'en_US',
                            'ARG0': external_property_code,
                            'ARG1': username,
                            'ARG2': current_date.format("M/D/YYYY"),
                            # startdatepastcurrent
                            'ARG3': start.format("M/D/YYYY"),
                            # enddatepastcurrent
                            'ARG4': end.format("M/D/YYYY"),
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

                        k = s.post(f"{domain_url}/ReservationActivityReport.go")

                        y = s.post(f'{domain_url}/ReportProxyServlet.proxy?ie=pdf', data=reservation_data_post)
                        report_type = '[Reservation]'

                        print(f"[{atica_property_code}]{report_type} Sent request!")
                        if y.status_code == 200:
                            temp = tempfile.TemporaryFile()
                            temp.write(y.content)
                            temp.seek(0)
                            read_xl = pd.read_csv(BytesIO(temp.read()), index_col=False)
                            reservation_dataframe.append(read_xl)
                            temp.close()
                            print(
                                f"[{atica_property_code}]{report_type} successfully pulled for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")
                        else:
                            print(
                                f"[{atica_property_code}]{report_type} failed to pull for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")

                    reservation_report = pd.concat(reservation_dataframe, ignore_index=True)

                    filename = f'{folder_name}{propertyCode}_Reservation.csv'
                    reservation_report.to_csv(filename, index=False)
                    reservation_binary_stream = open(filename, 'rb')
                    reservation_binary_data = reservation_binary_stream.read()

                    if not ignore_size_check and len(reservation_binary_data) < 5000:
                        print(f"[{atica_property_code}]{report_type} Report size < 5 kb not sending, having only header")
                        # os.remove(filename)
                    else:

                        print(f"[{atica_property_code}]{report_type} Uploading reservations")
                        read = pd.read_csv(filename)
                        if 'IDS_ACCOUNT' in read.columns:
                            read.rename(columns={"IDS_ACCOUNT": "Account"}, inplace=True)
                        read.insert(0, column="propertyCode", value=propertyCode)
                        read.insert(1, column="pullDateId", value=pullDateId)
                        read.insert(2, column="createdAt", value=createdAt)
                        read.insert(3, column="updatedAt", value=updatedAt)
                        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                        read.insert(6, column="uniqueKey", value=read["Account"].astype(str))
                        read['Arrive'] = pd.to_datetime(read['Arrive'], format="%m/%d/%y", errors='coerce')
                        read['Depart'] = pd.to_datetime(read['Depart'], format="%m/%d/%y", errors='coerce')
                        read['Reserve Date'] = pd.to_datetime(read['Reserve Date'], format="%m/%d/%y", errors='coerce')
                        read['Cancellation Date'] = pd.to_datetime(read['Cancellation Date'], format="%m/%d/%y", errors='coerce')
                        read['CRS Conf. No'] = read['CRS Conf. No'].fillna(0).astype(int)
                        read['Room'] = read['Room'].fillna(0).astype(int)

                        headers = ['propertyCode', 'pullDateId', 'createdAt', 'updatedAt', 'createdAtEpoch', 'updatedAtEpoch', 'uniqueKey', 'Account', 'GuestName', 'Arrive', 'Depart', 'Nights', 'Status',
                                   'Rate', 'RateCode', 'Type', 'Room', 'Source', 'CRSConfNo', 'GTD', 'ReserveDate', 'User',
                                   'SharedAccount', 'TrackCode', 'Package', 'CancellationDate', 'CXLUserID']
                        read.to_csv(filename, header=headers, index=False)
                    # End Reservation Report

                    # Start Occupancy Report
                    occupancy_dataframe = []
                    start_date = row['occ_before']
                    end_date = row['occ_after']
                    curr = start_date
                    while curr <= end_date:
                        start = curr
                        end = curr.shift(days=+45)
                        if end > end_date:
                            end = end_date
                        curr = curr.shift(days=+46)

                        occupancy_data_post = {
                            'JOD': 'apache_occ_forecast',
                            'locale': 'en_US',
                            'ARG0': external_property_code,
                            'ARG1': username,
                            'ARG2': current_date.format("M/D/YYYY"),
                            # startdatepastcurrent
                            'ARG3': start.format("M/D/YYYY"),
                            # enddatepastcurrent
                            'ARG4': end.format("M/D/YYYY"),
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

                        z = s.post(f'{domain_url}/ReportProxyServlet.proxy?ie=pdf', data=occupancy_data_post)
                        report_type = '[Occupancy]'
                        print(f"[{atica_property_code}]{report_type} Sent request!")

                        if z.status_code == 200:
                            temp = tempfile.TemporaryFile()
                            temp.write(z.content)
                            temp.seek(0)
                            read_xl = pd.read_csv(BytesIO(temp.read()), index_col=False)
                            occupancy_dataframe.append(read_xl)
                            temp.close()
                            print(
                                f"[{atica_property_code}]{report_type} successfully pulled for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")
                        else:
                            print(
                                f"[{atica_property_code}]{report_type} failed to pull for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")

                    occupancy_report = pd.concat(occupancy_dataframe, ignore_index=True)

                    filename = f'{folder_name}{propertyCode}_Occupancy.csv'
                    occupancy_report.to_csv(filename, index=False)
                    occupancy_binary_stream = open(filename, 'rb')
                    occupancy_binary_data = occupancy_binary_stream.read()

                    if not ignore_size_check and len(occupancy_binary_data) < 5000:
                        print(f"[{atica_property_code}]{report_type} Report size < 5 kb not sending, having only header")
                        # os.remove(filename)
                    else:
                        print(f"[{atica_property_code}]{report_type} Uploading occupancies")
                        read = pd.read_csv(filename)
                        read.insert(0, column="propertyCode", value=propertyCode)
                        read.insert(1, column="pullDateId", value=pullDateId)
                        read.insert(2, column="createdAt", value=createdAt)
                        read.insert(3, column="updatedAt", value=updatedAt)
                        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                        try:
                            read['﻿IDS_DATE'] = pd.to_datetime(read['﻿IDS_DATE'], format="%m/%d/%y")
                            read.insert(6, column="uniqueKey", value=read["propertyCode"].astype(str) + "_" + read['﻿IDS_DATE'].astype(str))
                        except Exception:
                            read['IDS_DATE'] = pd.to_datetime(read['IDS_DATE'], format="%m/%d/%y")
                            read.insert(6, column="uniqueKey", value=read["propertyCode"].astype(str) + "_" + read['IDS_DATE'].astype(str))
                        headers = ['propertyCode', 'pullDateId', 'createdAt', 'updatedAt', 'createdAtEpoch', 'updatedAtEpoch', 'uniqueKey', 'IDS_DATE', 'Day', 'Rooms', 'OOO', 'StayOver', 'Arrivals',
                                   'DueOut', 'Available', 'GroupBlock', 'GroupPickedUp', 'TransNGTD', 'TransGTD', 'Occupied',
                                   'OccPercent', 'RoomRev', 'RevPAR', 'ADR', 'Ppl']
                        read.to_csv(filename, header=headers, index=False)
                    # End Occupancy Report

                    # Start Cancellation Report
                    cancellation_dataframe = []
                    start_date = row['res_before']
                    end_date = row['res_after']
                    curr = start_date
                    while curr <= end_date:
                        start = curr
                        end = curr.shift(days=+45)
                        if end > end_date:
                            end = end_date
                        curr = curr.shift(days=+46)

                        cancellation_data_post = {
                            "ie": "csv",
                            "locale": "en_US",
                            "JOD": "cancellation_summary_csv",
                            "userId": username,
                            "businessDate": current_date.format("M/D/YYYY"),
                            "property": external_property_code,
                            "CSV": "true",
                            "fileName": f"Cancellation Summary Report {external_property_code}",
                            "reportId": "166",
                            "startDate": start.format("M/D/YYYY"),
                            "endDate": end.format("M/D/YYYY"),
                            "cancelCodes": "*",
                            "reportServerKey": serverkey,
                            "reportServerUsername": username,
                            "fullPageRequestTime": int(time.time())
                        }

                        if property_type == 'Skytouch':
                            cancellation_data_post['commaSeparatedAccounts'] = ""
                            cancellation_data_post['activityDateType'] = ""

                        cancel_page_get = s.post(f'{domain_url}/CancellationSummaryReport.go')

                        cancel_report_get = s.post(f'{domain_url}/ReportProxyServlet.proxy?ie=pdf',
                                                   data=cancellation_data_post)
                        report_type = '[Cancellation]'
                        print(f"[{atica_property_code}]{report_type} Sent request!")

                        if cancel_report_get.status_code == 200:
                            temp = tempfile.TemporaryFile()
                            temp.write(cancel_report_get.content)
                            temp.seek(0)
                            read_xl = pd.read_csv(BytesIO(temp.read()), index_col=False)
                            cancellation_dataframe.append(read_xl)
                            temp.close()
                            print(
                                f"[{atica_property_code}]{report_type} successfully pulled for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")
                        else:
                            print(
                                f"[{atica_property_code}]{report_type} failed to pull for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")

                    cancellation_report = pd.concat(cancellation_dataframe, ignore_index=True)

                    filename = f'{folder_name}{propertyCode}_Cancellation.csv'
                    cancellation_report.to_csv(filename, index=False)
                    cancellation_binary_stream = open(filename, 'rb')
                    cancellation_binary_data = cancellation_binary_stream.read()

                    if not ignore_size_check and len(cancellation_binary_data) < 5000:
                        print(f"[{atica_property_code}]{report_type} Report size < 5 kb not sending, having only header")
                        # os.remove(filename)
                    else:
                        print(f"[{atica_property_code}]{report_type} Uploading Cancellation report")
                        read = pd.read_csv(filename)
                        read.drop(columns=['Unnamed: 0'], inplace=True)
                        read.dropna(axis=0, inplace=True)
                        read.insert(0, column="propertyCode", value=propertyCode)
                        read.insert(1, column="pullDateId", value=pullDateId)
                        read.insert(2, column="createdAt", value=createdAt)
                        read.insert(3, column="updatedAt", value=updatedAt)
                        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                        read['Cxl Date'] = pd.to_datetime(read['Cxl Date'], format='mixed', errors='coerce')
                        read['# Resv'] = read['# Resv'].fillna(0).astype(int)
                        read['Room nights'] = read['Room nights'].fillna(0).astype(int)
                        headers_list = ["propertyCode", "pullDateId", "createdAt", "updatedAt", "createdAtEpoch", "updatedAtEpoch", "CancellationReason", "CxlDate", "Resv", "RoomNights",
                                        "RoomRev"]
                        read.to_csv(filename, index=False, header=headers_list)
                    # End Cancellation Report

                    # Start Revenue By Rate Code Report
                    revenue_data_post = {
                        "JOD": "apache_revenue_by_rate_code",
                        "locale": "en_US",
                        "ARG0": external_property_code,
                        "ARG1": username,
                        "ARG2": arrow.now().shift(days=-2).format("M/D/YYYY"),
                        "ARG3": "",
                        "ARG4": "",
                        "ARG5": "",
                        "ARG6": "",
                        "ARG7": "",
                        "ARG8": "",
                        "ARG9": "",
                        "ARG10": "",
                        "ARG11": "",
                        "ARG12": "",
                        "ARG13": "",
                        "ARG14": "",
                        "ARG15": "",
                        "CSV": "true",
                        "CSV_SUPPRESS_HEADERS": "false",
                        "reportId": "76",
                        "updateCounter": "Y",
                        "includeCheckedOut": "",
                        "includeInHouse": "",
                        "altLocale": "",
                        "reportName": "",
                        "isGuestServiceEnabled": "",
                        "sortorder": "",
                        "showCurrencySymbol": "",
                        "commaSeparatedAccounts": "",
                        "activityDateType": "",
                        "reportServerKey": serverkey,
                        "reportServerUsername": username,
                        "fullPageRequestTime": int(time.time())
                    }

                    if property_type == 'Skytouch':
                        revenue_data_post['commaSeparatedAccounts'] = ""
                        revenue_data_post['activityDateType'] = ""

                    revenue_page_get = s.post(f'{domain_url}/RevenueByRateCodeReport.go')

                    revenue_report_get = s.post(f'{domain_url}/ReportProxyServlet.proxy?ie=pdf', data=revenue_data_post)
                    report_type = '[Revenue]'
                    print(f"[{atica_property_code}]{report_type} Sent request!")

                    if not revenue_report_get.ok:
                        print(
                            f"[{atica_property_code}]{report_type} Something went wrong for the report: {revenue_report_get.status_code} {revenue_report_get.reason}")
                    else:
                        print(f"[{atica_property_code}]{report_type} Uploading Revenue report")
                        filename = f'{folder_name}{propertyCode}_Revenue.csv'
                        with open(filename, 'wb') as f:
                            f.write(revenue_report_get.content)
                            f.close()
                        read = pd.read_csv(filename)
                        read.insert(0, column="propertyCode", value=propertyCode)
                        read.insert(1, column="pullDateId", value=pullDateId)
                        read.insert(2, column="createdAt", value=createdAt)
                        read.insert(3, column="updatedAt", value=updatedAt)
                        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                        read['%Room Nights'] = read['%Room Nights'].fillna(0).astype(int)
                        headers_list = ["propertyCode", "pullDateId", "createdAt", "updatedAt", "createdAtEpoch", "updatedAtEpoch", "IDS_RATE_CODE", "RoomNights", "RoomNightsPer",
                                        "RoomRevenue", "RoomRevenuePer", "DailyAVG", "PTDRoomNights", "PTDRoomNightsPer",
                                        "PTDRoomRevenue", "PTDRoomRevenuePer", "PTD_AVG", "YTDRoomNights",
                                        "YTDRoomNightsPer",
                                        "YTDRoomRevenue", "YTDRoomRevenuePer", "YTD_AVG"]
                        read.to_csv(filename, index=False, header=headers_list)
                    # End Revenue By Rate Code Report

                    # Start Revenue By Rate Code Detail Report
                    revenue_detail_dataframe = []
                    start_date = row['res_before']
                    end_date = row['res_after']
                    curr = start_date
                    while curr <= end_date:
                        start = curr
                        end = curr.shift(days=+45)
                        if end > end_date:
                            end = end_date
                        curr = curr.shift(days=+46)

                        revenue_detail_data_post = {
                            "JOD": "apache_revenue_by_rate_code_detail",
                            "locale": "en_US",
                            "ARG0": external_property_code,
                            "ARG1": username,
                            "ARG2": current_date.format("M/D/YYYY"),
                            "ARG3": start.format("M/D/YYYY"),
                            "ARG4": end.format("M/D/YYYY"),
                            "ARG5": "",
                            "ARG6": "",
                            "ARG7": "",
                            "ARG8": "",
                            "ARG9": "",
                            "ARG10": "",
                            "ARG11": "",
                            "ARG12": "",
                            "ARG13": "",
                            "ARG14": "",
                            "ARG15": "",
                            "CSV": "true",
                            "CSV_SUPPRESS_HEADERS": "false",
                            "reportId": "77",
                            "updateCounter": "Y",
                            "includeCheckedOut": "",
                            "includeInHouse": "",
                            "altLocale": "",
                            "reportName": "",
                            "isGuestServiceEnabled": "",
                            "sortorder": "",
                            "showCurrencySymbol": "",
                            "commaSeparatedAccounts": "",
                            "activityDateType": "",
                            "reportServerKey": serverkey,
                            "reportServerUsername": username,
                            "fullPageRequestTime": int(time.time())
                        }
                        revenue_detail_page_get = s.post(f'{domain_url}/RevenueByRateCodeDetailReport.go')

                        revenue_detail_report_get = s.post(f'{domain_url}/ReportProxyServlet.proxy?ie=pdf', data=revenue_detail_data_post)
                        report_type = '[Revenue Detail]'
                        print(f"[{atica_property_code}]{report_type} Sent request!")

                        if revenue_detail_report_get.status_code == 200:
                            temp = tempfile.TemporaryFile()
                            temp.write(revenue_detail_report_get.content)
                            temp.seek(0)
                            read_xl = pd.read_csv(BytesIO(temp.read()), index_col=False)
                            revenue_detail_dataframe.append(read_xl)
                            temp.close()
                            print(
                                f"[{atica_property_code}]{report_type} successfully pulled for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")
                        else:
                            print(
                                f"[{atica_property_code}]{report_type} failed to pull for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")

                    revenue_detail_report = pd.concat(revenue_detail_dataframe, ignore_index=True)

                    filename = f'{folder_name}{propertyCode}_Revenue_Detail.csv'
                    revenue_detail_report.to_csv(filename, index=False)
                    revenue_detail_binary_stream = open(filename, 'rb')
                    revenue_detail_binary_data = revenue_detail_binary_stream.read()

                    if not ignore_size_check and len(revenue_detail_binary_data) < 5000:
                        print(f"[{atica_property_code}]{report_type} Report size < 5 kb not sending, having only header")
                        # os.remove(filename)
                    else:
                        print(f"[{atica_property_code}]{report_type} Uploading Revenue Detail report")
                        read = pd.read_csv(filename)
                        read.insert(0, column="propertyCode", value=propertyCode)
                        read.insert(1, column="pullDateId", value=pullDateId)
                        read.insert(2, column="createdAt", value=createdAt)
                        read.insert(3, column="updatedAt", value=updatedAt)
                        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                        try:
                            read['﻿IDS_DATE_DAY'] = pd.to_datetime(read['﻿IDS_DATE_DAY'], format="%m/%d/%y - %a")
                            read.insert(6, column="uniqueKey", value=read["propertyCode"].astype(str) + "_" + read['﻿IDS_DATE_DAY'].astype(str) + "_" + read['Rate Code'].astype(str))
                        except Exception:
                            read['IDS_DATE_DAY'] = pd.to_datetime(read['IDS_DATE_DAY'], format="%m/%d/%y - %a")
                            read.insert(6, column="uniqueKey", value=read["propertyCode"].astype(str) + "_" + read['IDS_DATE_DAY'].astype(str) + "_" + read['Rate Code'].astype(str))
                        read['%Room Nights'] = read['%Room Nights'].fillna(0).astype(int)
                        headers_list = ["propertyCode", "pullDateId", "createdAt", "updatedAt", "createdAtEpoch", "updatedAtEpoch", "uniqueKey", "IDS_DATE_DAY", "RateCode", "RoomNights", "RoomNightsPer", "RoomRevenue", "RoomRevenuePer", "DailyAVG"]
                        read.to_csv(filename, index=False, header=headers_list)
                    # End Revenue By Rate Code Detail Report

                    # Start Group Pickup Detail Report
                    group_pickup_detail_dataframe = []
                    start_date = row['res_before']
                    end_date = row['res_after']
                    curr = start_date
                    while curr <= end_date:
                        start = curr
                        end = curr.shift(days=+45)
                        if end > end_date:
                            end = end_date
                        curr = curr.shift(days=+46)

                        group_pickup_detail_data_post = {
                            "ie": "pdf",
                            "locale": "en_US",
                            "JOD": "apache_group_pickup_detail",
                            "userId": username,
                            "businessDate": current_date.format("M/D/YYYY"),
                            "property": external_property_code,
                            "CSV": "true",
                            "CSV_SUPPRESS_HEADERS": "false",
                            "PRINT": "N",
                            "COPIES": "1",
                            "SHARED": "false",
                            "sortorder": "sortby_roomType, sortby_blockDate",
                            "reportId": "58",
                            "updateCounter": "Y",
                            "queryType": "dateRange",
                            "arrivalDateFrom": start.format("M/D/YYYY"),
                            "arrivalDateTo": end.format("M/D/YYYY"),
                            "groupId": "",
                            "selectAll": "",
                            "groupStatus": "T",
                            "groupStatus": "D",
                            "groupStatus": "I",
                            "groupStatus": "O",
                            "revenueType": "current_revenue",
                            "roomNights": "total_picked_up",
                            "salesManagerUserId": "*",
                            "CSVcheckbox": "on",
                            "reportServerKey": serverkey,
                            "reportServerUsername": username,
                            "fullPageRequestTime": int(time.time())
                        }
                        group_pickup_detail_page_get = s.post(f'{domain_url}/GroupPickupDetailsReport.go')

                        group_pickup_detail_report_get = s.post(f'{domain_url}/ReportProxyServlet.proxy?ie=pdf', data=group_pickup_detail_data_post)
                        report_type = '[Group Pickup Detail]'
                        print(f"[{atica_property_code}]{report_type} Sent request!")

                        if group_pickup_detail_report_get.status_code == 200:
                            temp = tempfile.TemporaryFile()
                            temp.write(group_pickup_detail_report_get.content)
                            temp.seek(0)
                            read_xl = pd.read_csv(BytesIO(temp.read()), index_col=False)
                            group_pickup_detail_dataframe.append(read_xl)
                            temp.close()
                            print(
                                f"[{atica_property_code}]{report_type} successfully pulled for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")
                        else:
                            print(
                                f"[{atica_property_code}]{report_type} failed to pull for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")

                    group_pickup_detail_report = pd.concat(group_pickup_detail_dataframe, ignore_index=True)

                    filename = f'{folder_name}{propertyCode}_Group_Pickup_Detail.csv'
                    group_pickup_detail_report.to_csv(filename, index=False)
                    print(f"[{atica_property_code}]{report_type} Uploading Group Pickup Detail report")
                    read = pd.read_csv(filename)
                    read.dropna(subset=['Block Date'], inplace=True)
                    read.insert(0, column="propertyCode", value=propertyCode)
                    read.insert(1, column="pullDateId", value=pullDateId)
                    read.insert(2, column="createdAt", value=createdAt)
                    read.insert(3, column="updatedAt", value=updatedAt)
                    read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                    read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                    read['Fixed Cut Off Date'] = pd.to_datetime(read['Fixed Cut Off Date'], format="%m/%d/%y", errors='coerce')
                    read['Block Date'] = pd.to_datetime(read['Block Date'], format="%m/%d/%y", errors='coerce')
                    read.insert(6, column="uniqueKey", value=read["propertyCode"].astype(str) + "_" + read["Group Name"].astype(str) + "_" + read["Fixed Cut Off Date"].astype(str) + "_" + read["Room Type"].astype(str) + "_" + read["Block Date"].astype(str))

                    headers_list = ["propertyCode", "pullDateId", "createdAt", "updatedAt", "createdAtEpoch", "updatedAtEpoch", "uniqueKey", "GroupName", "GroupStatus", "RollingCutOffDays", "FixedCutOffDate", "SalesManager",
                                    "RoomType", "BlockDate", "OriginalBlock", "CurrentBlock", "GuaranteedArrivalsPickedUp", "NonGuaranteedArrivalsPickedUp",
                                    "TotalPickedUp", "RoomsNotPickedUp", "Revenue", "ADR"]
                    read.to_csv(filename, index=False, header=headers_list)
                    # End Group Pickup Detail Report

                    # Start cancelltionlist Report
                    cancelltionlist_dataframe = []
                    start_date = row['res_before']
                    end_date = row['res_after']
                    curr = start_date
                    while curr <= end_date:
                        start = curr
                        end = curr.shift(days=+45)
                        if end > end_date:
                            end = end_date
                        curr = curr.shift(days=+46)

                        cancelltionlist_data_post = {
                            'JOD': 'apache_cancel_list', #
                            'locale': 'en_US', #
                            'ARG0': external_property_code, #
                            'ARG1': username, #
                            'ARG2': current_date.format("M/D/YYYY"),
                            'activityDateType':'cancel',
                            'ARG3': start.format("M/D/YYYY"),
                            'ARG4': end.format("M/D/YYYY"),
                            'ARG5':'*',
                            'ARG6':'*',
                            'ARG7':'cancelDate',
                            'reportId': '26', #
                            'updateCounter': 'Y', #
                            'CSV': 'true', #
                            'CSV_SUPPRESS_HEADERS': 'false', #
                            'reportServerKey': serverkey,
                            'reportServerUsername': username,
                            'fullPageRequestTime': int(time.time())
                        }

                        if property_type == 'Skytouch':
                            cancelltionlist_data_post['commaSeparatedAccounts'] = ""
                            cancelltionlist_data_post['activityDateType'] = ""

                        z = s.post(f'{domain_url}/ReportProxyServlet.proxy?ie=pdf', data=cancelltionlist_data_post)
                        report_type = '[cancelltionlist]'
                        print(f"[{atica_property_code}]{report_type} Sent request!")
                        # print(z.content)

                        if z.status_code == 200:
                            temp = tempfile.TemporaryFile()
                            temp.write(z.content)
                            temp.seek(0)
                            read_xl = pd.read_csv(BytesIO(temp.read()), index_col=False)
                            cancelltionlist_dataframe.append(read_xl)
                            temp.close()
                            print(
                                f"[{atica_property_code}]{report_type} successfully pulled for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")
                        else:
                            print(
                                f"[{atica_property_code}]{report_type} failed to pull for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")

                    occupancy_report = pd.concat(cancelltionlist_dataframe, ignore_index=True)

                    filename = f'{folder_name}{propertyCode}_Cancelltion_List.csv'
                    occupancy_report.to_csv(filename, index=False)
                    occupancy_binary_stream = open(filename, 'rb')
                    occupancy_binary_data = occupancy_binary_stream.read()

                    if not ignore_size_check and len(occupancy_binary_data) < 1000:
                        print(f"[{atica_property_code}]{report_type} Report size < 5 kb not sending, having only header")
                        # os.remove(filename)
                    else:
                        df = pd.read_csv(filename)
                        df = df.drop('Company', axis=1)
                        df = df.drop('Group', axis=1)
                        new_column_names = ["Account", "Guest_Name", "Arrival_Group", "Nights", "Rate_Plan", "GTD", "Source", "Rm_Type", "Cxl_Code", "Cxl_Date", "Cxl_Clk"]
                        df.columns = new_column_names
                        df.dropna(inplace=True, how="all")
                        createdAt = "'" + str(arrow.now()) + "'"
                        updatedAt = "'" + str(arrow.now()) + "'"
                        createdAtEpoch = int(arrow.utcnow().timestamp())
                        updatedAtEpoch = int(arrow.utcnow().timestamp())
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
                        output_df.to_csv(f'{folder_name}{propertyCode}_Cancellation_List_Final.csv', index=False)
                        print(f"{atica_property_code} Choice Cancellation List report pulled successfully")

                        # read.to_csv(filename, header=headers, index=False)

                    # End cancelltionlist Report

                    # Start noshow Report
                    noshow_dataframe = []
                    start_date = row['res_before']
                    end_date = row['res_after']
                    curr = start_date
                    while curr <= end_date:
                        start = curr
                        end = curr.shift(days=+45)
                        if end > end_date:
                            end = end_date
                        curr = curr.shift(days=+46)

                        noshow_data_post = {
                            'JOD': 'apache_no_show', 
                            'locale': 'en_US', 
                            'property': external_property_code, 
                            'userId': username, 
                            'reportId': '48', 
                            'updateCounter': 'Y', 
                            'creditCardStatusTypeXML': '', 
                            'isNoShowBatchEnabled': 'Y',
                            'CSV': 'true', 
                            'CSV_SUPPRESS_HEADERS': 'false', 
                            'startDatePastCurrent': start.format("M/D/YYYY"),
                            'endDatePastCurrent': end.format("M/D/YYYY"),
                            'ARG2': current_date.format("M/D/YYYY"),
                            'guaranteeCode': '*',
                            'reportServerKey': serverkey,
                            'reportServerUsername': username,
                            'fullPageRequestTime': int(time.time())
                        }

                        if property_type == 'Skytouch':
                            noshow_data_post['commaSeparatedAccounts'] = ""
                            noshow_data_post['activityDateType'] = ""

                        z = s.post(f'{domain_url}/ReportProxyServlet.proxy?ie=pdf', data=noshow_data_post)
                        report_type = '[Noshow]'
                        print(f"[{atica_property_code}]{report_type} Sent request!")

                        if z.status_code == 200:
                            temp = tempfile.TemporaryFile()
                            temp.write(z.content)
                            temp.seek(0)
                            read_xl = pd.read_csv(BytesIO(temp.read()), index_col=False)
                            noshow_dataframe.append(read_xl)
                            temp.close()
                            print(
                                f"[{atica_property_code}]{report_type} successfully pulled for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")
                        else:
                            print(
                                f"[{atica_property_code}]{report_type} failed to pull for {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')}")

                    noshow_report = pd.concat(noshow_dataframe, ignore_index=True)

                    filename = f'{folder_name}{propertyCode}_Noshow.csv'
                    noshow_report.to_csv(filename, index=False)
                    noshow_binary_stream = open(filename, 'rb')
                    noshow_binary_data = noshow_binary_stream.read()

                    if not ignore_size_check and len(noshow_binary_data) < 1:
                        print(f"[{atica_property_code}]{report_type} Report size < 1 kb not sending, having only header")
                        # os.remove(filename)
                    else:
                        print(f"[{atica_property_code}]{report_type} Uploading Noshow")
                        read = pd.read_csv(filename)
                        read.dropna(inplace=True, how="all")
                        read.insert(0, column="propertyCode", value=propertyCode)
                        read.insert(1, column="pullDateId", value=pullDateId)
                        read.insert(2, column="createdAt", value=createdAt)
                        read.insert(3, column="updatedAt", value=updatedAt)
                        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                        read['Account'] = read['Account'].fillna(0).astype(int)
                        read.insert(6, column="uniqueKey", value=read["Account"].astype(str))
                        
                        headers = ['propertyCode','pullDateId','createdAt','updatedAt','createdAtEpoch',
                                   'updatedAtEpoch','uniqueKey','account','guestName','arrival','departure',
                                   'source','GTD','ratePlan','rate','balance','payment','authStatus']
                        read.to_csv(filename, header=headers, index=False)

                    # End noshow Report
                        
            reservation_file_path = f'{folder_name}{propertyCode}_Reservation.csv'
            occupancy_file_path = f'{folder_name}{propertyCode}_Occupancy.csv'
            cancellation_file_path = f'{folder_name}{propertyCode}_Cancellation.csv'
            revenue_file_path = f'{folder_name}{propertyCode}_Revenue.csv'
            revenue_detail_file_path = f'{folder_name}{propertyCode}_Revenue_Detail.csv'
            group_pickup_detail_file_path = f'{folder_name}{propertyCode}_Group_Pickup_Detail.csv'
            Noshow_file_path = f'{folder_name}{propertyCode}_Noshow.csv'
            cancellation_list_file_path = f'{folder_name}{propertyCode}_Cancellation_List_Final.csv'
            
            
            check_cancellation_list_file = os.path.isfile(cancellation_list_file_path)
            check_Noshow_file = os.path.isfile(Noshow_file_path)
            check_reservation_file = os.path.isfile(reservation_file_path)
            check_occupancy_file = os.path.isfile(occupancy_file_path)
            check_cancellation_file = os.path.isfile(cancellation_file_path)
            check_revenue_file = os.path.isfile(revenue_file_path)
            check_revenue_detail_file = os.path.isfile(revenue_detail_file_path)
            check_group_pickup_detail_file = os.path.isfile(group_pickup_detail_file_path)

            error_msg = ""
            fileCount = 0

            if not check_Noshow_file:
                error_msg = error_msg + " Noshow file - N/A"

            if not check_cancellation_list_file:
                error_msg = error_msg + " Cancellation List file - N/A"
            
            if not check_reservation_file:
                error_msg = error_msg + " Reservation file - N/A"

            if not check_occupancy_file:
                error_msg = error_msg + " Occupancy file - N/A"

            if not check_cancellation_file:
                error_msg = error_msg + " Cancellation file - N/A"

            if not check_revenue_file:
                error_msg = error_msg + " Revenue file - N/A"

            if not check_revenue_detail_file:
                error_msg = error_msg + " Revenue Detail file - N/A"

            if not check_group_pickup_detail_file:
                error_msg = error_msg + " Group Pickup Detail file - N/A"

            if check_reservation_file:

                fileCount = fileCount + 1
                res_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Reservation.csv", encoding="utf-8"))
                res_result = list(res_result)
                print(len(res_result))
                if len(res_result) > 0:
                    bulk_insert_choice_res(propertyCode, res_result, row['res_before'], row['res_after'])
                    print("RES DONE")
                else:
                    error_msg = error_msg + "RES File Was Blank, "

            if check_occupancy_file:

                fileCount = fileCount + 1
                occ_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Occupancy.csv", encoding="utf-8"))
                occ_result = list(occ_result)
                print(len(occ_result))
                if len(occ_result) > 0:
                    bulk_insert_choice_occ(propertyCode, occ_result, row['occ_before'], row['occ_after'])
                    print("OCC DONE")
                else:
                    error_msg = error_msg + "OCC File Was Blank, "

            if check_cancellation_file:

                fileCount = fileCount + 1
                cancel_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Cancellation.csv", encoding="utf-8"))
                cancel_result = list(cancel_result)
                print(len(cancel_result))
                if len(cancel_result) > 0:
                    bulk_insert_choice_cancel(propertyCode, cancel_result, row['res_before'], row['res_after'])
                    print("CANCELLATION DONE")
                else:
                    error_msg = error_msg + "CANCELLATION File Was Blank, "

            if check_revenue_file:

                fileCount = fileCount + 1
                revenue_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Revenue.csv", encoding="utf-8"))
                revenue_result = list(revenue_result)
                print(len(revenue_result))
                if len(revenue_result) > 0:
                    bulk_insert_choice_revenue(propertyCode, revenue_result)
                    print("REVENUE DONE")
                else:
                    error_msg = error_msg + "REVENUE File Was Blank, "

            if check_revenue_detail_file:

                fileCount = fileCount + 1
                revenue_detail_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Revenue_Detail.csv", encoding="utf-8"))
                revenue_detail_result = list(revenue_detail_result)
                print(len(revenue_detail_result))
                if len(revenue_detail_result) > 0:
                    bulk_insert_choice_revenue_detail(propertyCode, revenue_detail_result, row['res_before'], row['res_after'])
                    print("REVENUE DETAIL DONE")
                else:
                    error_msg = error_msg + "REVENUE DETAIL File Was Blank, "

            if check_group_pickup_detail_file:

                fileCount = fileCount + 1
                group_pickup_detail_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Group_Pickup_Detail.csv", encoding="utf-8"))
                group_pickup_detail_result = list(group_pickup_detail_result)
                print(len(group_pickup_detail_result))
                if len(group_pickup_detail_result) > 0:
                    bulk_insert_choice_group_pickup_detail(propertyCode, group_pickup_detail_result, row['res_before'], row['res_after'])
                    print("GROUP PICKUP DETAIL DONE")
                else:
                    error_msg = error_msg + "GROUP PICKUP DETAIL File Was Blank, "

            if check_Noshow_file:

                fileCount = fileCount + 1
                Noshow_result = csv.DictReader(open(Noshow_file_path, encoding="utf-8"))
                Noshow_result = list(Noshow_result)
                print(len(Noshow_result))
                if len(Noshow_result) > 0:
                    bulk_insert_choice_noshow(propertyCode, Noshow_result)
                    print("Noshow DONE")
                else:
                    error_msg = error_msg + "Noshow File Was Blank, "
            
            if check_cancellation_list_file:

                fileCount = fileCount + 1
                cancellation_list_result = csv.DictReader(open(cancellation_list_file_path, encoding="utf-8"))
                cancellation_list_result = list(cancellation_list_result)
                print(len(cancellation_list_result))
                if len(cancellation_list_result) > 0:
                    bulk_insert_choice_cancellation_list(propertyCode, cancellation_list_result, row['res_before'], row['res_after'])
                    print("CANCELLATION LIST DONE")
                else:
                    error_msg = error_msg + "Cancellation List File Was Blank, "
            
            if (fileCount == 8):
                if (error_msg == ""):
                    update_into_pulldate(pullDateId, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
                else:
                    error_msg = "Partially Successfull:- " + error_msg
                    update_into_pulldate(pullDateId, ERROR_NOTE=error_msg, IS_ERROR=True)
            else:
                if (fileCount == 0):
                    error_msg = "All File Not Found"
                else:
                    error_msg = "Partially Successfull:- " + error_msg
                update_into_pulldate(pullDateId, ERROR_NOTE=error_msg, IS_ERROR=True)


        except Exception as e:
            print(e)
            update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=f"Failed to pull report due to {e}", IS_ERROR=True)


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
                Choice_Pms(row)
            else:
                print("LAST_PULL_DATE_ID is NULL")
    else:
        print(f"Property not available in database!!!")
    print(f"[{PMS_NAME}] SCRIPT STOP!!!")
