import os
import sys

from bs4 import BeautifulSoup

sys.path.append("..")
import arrow

import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import csv
import base64
import sys
import pathlib
import xml.etree.ElementTree as Xet
import pandas as pd

from utils.db import db_config
from utils.db import db_models

SCOPES = ['https://www.googleapis.com/auth/gmail.modify',
          'https://www.googleapis.com/auth/gmail.send']


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


def bulk_insert_opera_cloud_res(res_list, propertyCode, res_before, res_after):
    start_date = "'" + res_before.format("YYYY-MM-DD") + "'"
    end_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    print("start_date :: ", start_date)
    print("end_date :: ", end_date)

    # delete all data
    # current_date = arrow.now()
    # print("current_date :: ", current_date)
    # start_date = current_date.shift(days=-90)
    # print("start_date :: ", start_date)
    reservation = '"INSERT_DATE"'
    db_propertyCode = "'" + propertyCode + "'"
    # current_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    # start_date = "'" + res_before.format("YYYY-MM-DD") + "'"

    # Delete existing data of reservation (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(
        f'DELETE from opera_res where {reservation} between {start_date} and {end_date} and "propertyCode" = {db_propertyCode};')
    conn.close()

    # Add new data of reservation
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.opera_res_model.insert(), res_list)
    conn.close()
    print("Data imported")


def bulk_insert_opera_cloud_occ(occ_list, propertyCode, occ_before, occ_after):
    start_date = "'" + occ_before.format("YYYY-MM-DD") + "'"
    end_date = "'" + occ_after.format("YYYY-MM-DD") + "'"
    print("start_date :: ", start_date)
    print("end_date :: ", end_date)

    # delete all data
    # current_date = arrow.now()
    # print("current_date :: ", current_date)
    # start_date = current_date.shift(days=-90)
    # print("start_date :: ", start_date)
    reservation = '"CONSIDERED_DATE"'
    db_propertyCode = "'" + propertyCode + "'"
    # current_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    # start_date = "'" + res_before.format("YYYY-MM-DD") + "'"

    # Delete existing data of reservation (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(
        f'DELETE from opera_occ where {reservation} between {start_date} and {end_date} and "propertyCode" = {db_propertyCode};')
    conn.close()

    # Add new data of reservation
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.opera_occ_model.insert(), occ_list)
    conn.close()
    print("Data imported")

def bulk_insert_opera_cloud_arrival(arrival_list, propertyCode, res_before, res_after):
    start_date = "'" + res_before.format("YYYY-MM-DD") + "'"
    end_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    print("start_date :: ", start_date)
    print("end_date :: ", end_date)

    # delete all data
    # current_date = arrow.now()
    # print("current_date :: ", current_date)
    # start_date = current_date.shift(days=-90)
    # print("start_date :: ", start_date)
    reservation = '"ARRIVAL"'
    db_propertyCode = "'" + propertyCode + "'"
    # current_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    # start_date = "'" + res_before.format("YYYY-MM-DD") + "'"

    # Delete existing data of reservation (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(
        f'DELETE from opera_arrival where {reservation} between {start_date} and {end_date} and "propertyCode" = {db_propertyCode};')
    conn.close()

    # Add new data of reservation
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.opera_arrival_model.insert(), arrival_list)
    conn.close()
    print("Data imported")


def prep_service():
    creds = None
    AticaCred = '../utils/email/AticaCred.json'
    TokenPickle = '../utils/email/token.pickle'
    if os.path.exists(TokenPickle):
        with open(TokenPickle, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                AticaCred, SCOPES)
            creds = flow.run_local_server()

        with open(TokenPickle, 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)
    return service


def get_archive_label(archiveLabelName):
    service = prep_service()
    response = service.users().labels().list(userId="me").execute()
    labels = response["labels"]
    for pulled_label in labels:
        if pulled_label["name"] == archiveLabelName:
            return pulled_label

    # Creates new archive label if not found.
    body = {
        "type": "user",
        "name": archiveLabelName,
        "messageListVisibility": "show",
        "labelListVisibility": "labelShow"
    }

    response = service.users().labels().create(userId="me",
                                               body=body).execute()
    return response


def create_filter(label, archiveLabel):
    filter = "-label:" + archiveLabel
    filter += " label:" + label
    return "has:attachment " + filter


def OperaCloud_Pms(row):
    atica_property_code = row['atica_property_code']
    secret_name = row['gcp_secret']
    pullDateId = row['pullDateId']
    propertyCode = row['propertyCode']

    label_array = [f"{propertyCode} Reservation", f"{propertyCode} Occupancy", f"{propertyCode} Arrival"]
    folder_name = "./reports/"
    messages_array = []
    for label_name in label_array:
        print("label_name :: ", label_name)
        response = service.users().messages().list(userId="me",
                                                   q=create_filter(label_name, "Saved")
                                                   ).execute()
        if 'messages' in response:
            messages = response['messages']
            item = {
                "label_name": label_name,
                "messages": messages
            }
            messages_array.append(item)

        else:
            msg = f"No new messages for {label_name} label"
            print(msg)
            update_into_pulldate(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)
            return 0
    if len(label_array) == len(messages_array):
        for item in messages_array:
            messages = item["messages"]
            label_name = item["label_name"]
            # get first message only
            message = messages[0]
            a_message = service.users().messages().get(userId="me",
                                                       id=message["id"]
                                                       ).execute()

            for part in a_message['payload']['parts']:
                save_flag = True
                if part['filename']:

                    print("save flag : ", save_flag)
                    if save_flag:
                        if 'data' in part['body']:
                            file_data = base64.urlsafe_b64decode(part['body']['data'].encode('UTF-8'))
                        else:
                            attachment_id = part['body']['attachmentId']
                            attachment = service.users().messages().attachments().get(userId="me",
                                                                                      messageId=message["id"],
                                                                                      id=attachment_id).execute()
                            data = attachment['data']
                            file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))

                        print("saving file of size " + str(sys.getsizeof(file_data)) + " bytes")
                        # file_data

                        # Open file in binary write mode
                        file_name = label_name.split(" ")[1]
                        binary_file = open(f"{folder_name}{propertyCode}_{file_name}.xml", "wb")
                        binary_file.write(file_data)
                        binary_file.close()

                        # save message asap
                        archive_label = get_archive_label("Saved")
                        label_apply_body = {
                            "addLabelIds": archive_label["id"]
                        }
                        response = service.users().messages().modify(userId="me",
                                                                     id=message["id"],
                                                                     body=label_apply_body).execute()

                    else:
                        print("Attachment format match fail for message ")

            saved_messages_ids = []
            for message in messages:
                saved_messages_ids.append(message["id"])
            archive_label = get_archive_label("Saved")
            print(f"{archive_label['name']} : {archive_label['id']}")

            # Apply archive label to saved messages
            label_apply_body = {
                "addLabelIds": archive_label["id"],
                "ids": saved_messages_ids
            }

            if saved_messages_ids:
                response = service.users().messages().batchModify(userId="me",
                                                                  body=label_apply_body
                                                                  ).execute()
                saved_messages_count = len(saved_messages_ids)
                print(f"Saved label applied to {saved_messages_count} messages.")

            else:
                print("No messages to save")

    # Modification of res report
    reservation_file_path = f'{folder_name}{propertyCode}_Reservation.xml'
    occupancy_file_path = f'{folder_name}{propertyCode}_Occupancy.xml'
    arrival_file_path = f'{folder_name}{propertyCode}_Arrival.xml'

    check_reservation_file = os.path.isfile(reservation_file_path)
    check_occupancy_file = os.path.isfile(occupancy_file_path)
    check_arrival_file = os.path.isfile(arrival_file_path)

    if check_reservation_file and check_occupancy_file and check_arrival_file:
        createdAt = "'" + str(arrow.now()) + "'"
        updatedAt = "'" + str(arrow.now()) + "'"
        # Start Reservation Report
        cols = ["RESV_NAME_ID", "GUARANTEE_CODE", "RESV_STATUS", "ROOM", "FULL_NAME", "DEPARTURE", "PERSONS",
                "GROUP_NAME",
                "NO_OF_ROOMS", "ROOM_CATEGORY_LABEL", "RATE_CODE", "INSERT_USER", "INSERT_DATE", "GUARANTEE_CODE_DESC",
                "COMPANY_NAME", "TRAVEL_AGENT_NAME", "ARRIVAL", "NIGHTS", "COMP_HOUSE_YN", "SHARE_AMOUNT", "C_T_S_NAME",
                "SHORT_RESV_STATUS", "SHARE_AMOUNT_PER_STAY"]
        rows = []

        # Parsing the XML file
        xmlparse = Xet.parse(reservation_file_path)
        root = xmlparse.getroot()
        try:
            for i in root[0][0][2][0][2]:
                RESV_NAME_ID = i.find("RESV_NAME_ID").text
                GUARANTEE_CODE = i.find("GUARANTEE_CODE").text
                RESV_STATUS = i.find("RESV_STATUS").text
                ROOM = i.find("ROOM").text
                FULL_NAME = i.find("FULL_NAME").text
                DEPARTURE = i.find("DEPARTURE").text
                PERSONS = i.find("PERSONS").text
                GROUP_NAME = i.find("GROUP_NAME").text
                NO_OF_ROOMS = i.find("NO_OF_ROOMS").text
                ROOM_CATEGORY_LABEL = i.find("ROOM_CATEGORY_LABEL").text
                RATE_CODE = i.find("RATE_CODE").text
                INSERT_USER = i.find("INSERT_USER").text
                INSERT_DATE = i.find("INSERT_DATE").text
                GUARANTEE_CODE_DESC = i.find("GUARANTEE_CODE_DESC").text
                COMPANY_NAME = i.find("COMPANY_NAME").text
                TRAVEL_AGENT_NAME = i.find("TRAVEL_AGENT_NAME").text
                ARRIVAL = i.find("ARRIVAL").text
                NIGHTS = i.find("NIGHTS").text
                COMP_HOUSE_YN = i.find("COMP_HOUSE_YN").text
                SHARE_AMOUNT = i.find("SHARE_AMOUNT").text
                C_T_S_NAME = i.find("C_T_S_NAME").text
                SHORT_RESV_STATUS = i.find("SHORT_RESV_STATUS").text
                SHARE_AMOUNT_PER_STAY = i.find("SHARE_AMOUNT_PER_STAY").text
                #
                rows.append({"RESV_NAME_ID": RESV_NAME_ID,
                             "GUARANTEE_CODE": GUARANTEE_CODE,
                             "RESV_STATUS": RESV_STATUS,
                             "ROOM": ROOM,
                             "FULL_NAME": FULL_NAME,
                             "DEPARTURE": DEPARTURE,
                             "PERSONS": PERSONS,
                             "GROUP_NAME": GROUP_NAME,
                             "NO_OF_ROOMS": NO_OF_ROOMS,
                             "ROOM_CATEGORY_LABEL": ROOM_CATEGORY_LABEL,
                             "RATE_CODE": RATE_CODE,
                             "INSERT_USER": INSERT_USER,
                             "INSERT_DATE": INSERT_DATE,
                             "GUARANTEE_CODE_DESC": GUARANTEE_CODE_DESC,
                             "COMPANY_NAME": COMPANY_NAME,
                             "TRAVEL_AGENT_NAME": TRAVEL_AGENT_NAME,
                             "ARRIVAL": ARRIVAL,
                             "NIGHTS": NIGHTS,
                             "COMP_HOUSE_YN": COMP_HOUSE_YN,
                             "SHARE_AMOUNT": SHARE_AMOUNT,
                             "C_T_S_NAME": C_T_S_NAME,
                             "SHORT_RESV_STATUS": SHORT_RESV_STATUS,
                             "SHARE_AMOUNT_PER_STAY": SHARE_AMOUNT_PER_STAY})

            df = pd.DataFrame(rows, columns=cols)
            df.insert(0, column="propertyCode", value=propertyCode)
            df.insert(1, column="pullDateId", value=pullDateId)
            df.insert(2, column="createdAt", value=createdAt)
            df.insert(3, column="updatedAt", value=updatedAt)
            df['DEPARTURE'] = pd.to_datetime(df['DEPARTURE'])
            df['INSERT_DATE'] = pd.to_datetime(df['INSERT_DATE'])
            df['ARRIVAL'] = pd.to_datetime(df['ARRIVAL'])
            df.to_csv(f"{folder_name}{propertyCode}_Reservations.csv", index=False)

            res_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Reservations.csv", encoding="utf-8"))
            res_result = list(res_result)
        except Exception:
            res_result = []
            print("Reservation Data not available")
        # End Reservation Report

        # Start Occupancy Report
        cols = ['REVENUE', 'NO_ROOMS', 'IND_DEDUCT_ROOMS', 'IND_NON_DEDUCT_ROOMS', 'GRP_DEDUCT_ROOMS',
                'GRP_NON_DEDUCT_ROOMS',
                'NO_PERSONS', 'ARRIVAL_ROOMS', 'DEPARTURE_ROOMS', 'COMPLIMENTARY_ROOMS', 'HOUSE_USE_ROOMS',
                'DAY_USE_ROOMS',
                'NO_SHOW_ROOMS', 'INVENTORY_ROOMS', 'CONSIDERED_DATE', 'CHAR_CONSIDERED_DATE', 'IND_DEDUCT_REVENUE',
                'IND_NON_DEDUCT_REVENUE', 'GRP_NON_DEDUCT_REVENUE', 'GRP_DEDUCT_REVENUE', 'OWNER_ROOMS', 'FF_ROOMS',
                'CF_OOO_ROOMS', 'CF_CALC_OCC_ROOMS', 'CF_CALC_INV_ROOMS', 'CF_AVERAGE_ROOM_RATE', 'CF_OCCUPANCY',
                'CF_IND_DED_REV', 'CF_IND_NON_DED_REV', 'CF_BLK_DED_REV', 'CF_BLK_NON_DED_REV']
        rows = []

        # Parsing the XML file
        xmlparse = Xet.parse(occupancy_file_path)
        root = xmlparse.getroot()
        if root[0][0][1][0][1].text == 'Forecast':
            for i in root[0][0][1][0][2]:
                REVENUE = i.find('REVENUE').text
                NO_ROOMS = i.find('NO_ROOMS').text
                IND_DEDUCT_ROOMS = i.find('IND_DEDUCT_ROOMS').text
                IND_NON_DEDUCT_ROOMS = i.find('IND_NON_DEDUCT_ROOMS').text
                GRP_DEDUCT_ROOMS = i.find('GRP_DEDUCT_ROOMS').text
                GRP_NON_DEDUCT_ROOMS = i.find('GRP_NON_DEDUCT_ROOMS').text
                NO_PERSONS = i.find('NO_PERSONS').text
                ARRIVAL_ROOMS = i.find('ARRIVAL_ROOMS').text
                DEPARTURE_ROOMS = i.find('DEPARTURE_ROOMS').text
                COMPLIMENTARY_ROOMS = i.find('COMPLIMENTARY_ROOMS').text
                HOUSE_USE_ROOMS = i.find('HOUSE_USE_ROOMS').text
                DAY_USE_ROOMS = i.find('DAY_USE_ROOMS').text
                NO_SHOW_ROOMS = i.find('NO_SHOW_ROOMS').text
                INVENTORY_ROOMS = i.find('INVENTORY_ROOMS').text
                CONSIDERED_DATE = i.find('CONSIDERED_DATE').text
                CHAR_CONSIDERED_DATE = i.find('CHAR_CONSIDERED_DATE').text
                IND_DEDUCT_REVENUE = i.find('IND_DEDUCT_REVENUE').text
                IND_NON_DEDUCT_REVENUE = i.find('IND_NON_DEDUCT_REVENUE').text
                GRP_NON_DEDUCT_REVENUE = i.find('GRP_NON_DEDUCT_REVENUE').text
                GRP_DEDUCT_REVENUE = i.find('GRP_DEDUCT_REVENUE').text
                OWNER_ROOMS = i.find('OWNER_ROOMS').text
                FF_ROOMS = i.find('FF_ROOMS').text
                CF_OOO_ROOMS = i.find('CF_OOO_ROOMS').text
                CF_CALC_OCC_ROOMS = i.find('CF_CALC_OCC_ROOMS').text
                CF_CALC_INV_ROOMS = i.find('CF_CALC_INV_ROOMS').text
                CF_AVERAGE_ROOM_RATE = i.find('CF_AVERAGE_ROOM_RATE').text
                CF_OCCUPANCY = i.find('CF_OCCUPANCY').text
                CF_IND_DED_REV = i.find('CF_IND_DED_REV').text
                CF_IND_NON_DED_REV = i.find('CF_IND_NON_DED_REV').text
                CF_BLK_DED_REV = i.find('CF_BLK_DED_REV').text
                CF_BLK_NON_DED_REV = i.find('CF_BLK_NON_DED_REV').text

                rows.append({
                    'REVENUE': REVENUE,
                    'NO_ROOMS': NO_ROOMS,
                    'IND_DEDUCT_ROOMS': IND_DEDUCT_ROOMS,
                    'IND_NON_DEDUCT_ROOMS': IND_NON_DEDUCT_ROOMS,
                    'GRP_DEDUCT_ROOMS': GRP_DEDUCT_ROOMS,
                    'GRP_NON_DEDUCT_ROOMS': GRP_NON_DEDUCT_ROOMS,
                    'NO_PERSONS': NO_PERSONS,
                    'ARRIVAL_ROOMS': ARRIVAL_ROOMS,
                    'DEPARTURE_ROOMS': DEPARTURE_ROOMS,
                    'COMPLIMENTARY_ROOMS': COMPLIMENTARY_ROOMS,
                    'HOUSE_USE_ROOMS': HOUSE_USE_ROOMS,
                    'DAY_USE_ROOMS': DAY_USE_ROOMS,
                    'NO_SHOW_ROOMS': NO_SHOW_ROOMS,
                    'INVENTORY_ROOMS': INVENTORY_ROOMS,
                    'CONSIDERED_DATE': CONSIDERED_DATE,
                    'CHAR_CONSIDERED_DATE': CHAR_CONSIDERED_DATE,
                    'IND_DEDUCT_REVENUE': IND_DEDUCT_REVENUE,
                    'IND_NON_DEDUCT_REVENUE': IND_NON_DEDUCT_REVENUE,
                    'GRP_NON_DEDUCT_REVENUE': GRP_NON_DEDUCT_REVENUE,
                    'GRP_DEDUCT_REVENUE': GRP_DEDUCT_REVENUE,
                    'OWNER_ROOMS': OWNER_ROOMS,
                    'FF_ROOMS': FF_ROOMS,
                    'CF_OOO_ROOMS': CF_OOO_ROOMS,
                    'CF_CALC_OCC_ROOMS': CF_CALC_OCC_ROOMS,
                    'CF_CALC_INV_ROOMS': CF_CALC_INV_ROOMS,
                    'CF_AVERAGE_ROOM_RATE': CF_AVERAGE_ROOM_RATE,
                    'CF_OCCUPANCY': CF_OCCUPANCY,
                    'CF_IND_DED_REV': CF_IND_DED_REV,
                    'CF_IND_NON_DED_REV': CF_IND_NON_DED_REV,
                    'CF_BLK_DED_REV': CF_BLK_DED_REV,
                    'CF_BLK_NON_DED_REV': CF_BLK_NON_DED_REV
                })
            df = pd.DataFrame(rows, columns=cols)
            df.insert(0, column="propertyCode", value=propertyCode)
            df.insert(1, column="pullDateId", value=pullDateId)
            df.insert(2, column="createdAt", value=createdAt)
            df.insert(3, column="updatedAt", value=updatedAt)
            df['CONSIDERED_DATE'] = pd.to_datetime(df['CONSIDERED_DATE'])
            df['CHAR_CONSIDERED_DATE'] = pd.to_datetime(df['CHAR_CONSIDERED_DATE'])
            df.to_csv(f"{folder_name}{propertyCode}_Occupancy.csv", index=False)

            occ_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Occupancy.csv", encoding="utf-8"))
            occ_result = list(occ_result)
        elif root[0][0][1][1][1].text == 'Forecast':
            for i in root[0][0][1][1][2]:
                REVENUE = i.find('REVENUE').text
                NO_ROOMS = i.find('NO_ROOMS').text
                IND_DEDUCT_ROOMS = i.find('IND_DEDUCT_ROOMS').text
                IND_NON_DEDUCT_ROOMS = i.find('IND_NON_DEDUCT_ROOMS').text
                GRP_DEDUCT_ROOMS = i.find('GRP_DEDUCT_ROOMS').text
                GRP_NON_DEDUCT_ROOMS = i.find('GRP_NON_DEDUCT_ROOMS').text
                NO_PERSONS = i.find('NO_PERSONS').text
                ARRIVAL_ROOMS = i.find('ARRIVAL_ROOMS').text
                DEPARTURE_ROOMS = i.find('DEPARTURE_ROOMS').text
                COMPLIMENTARY_ROOMS = i.find('COMPLIMENTARY_ROOMS').text
                HOUSE_USE_ROOMS = i.find('HOUSE_USE_ROOMS').text
                DAY_USE_ROOMS = i.find('DAY_USE_ROOMS').text
                NO_SHOW_ROOMS = i.find('NO_SHOW_ROOMS').text
                INVENTORY_ROOMS = i.find('INVENTORY_ROOMS').text
                CONSIDERED_DATE = i.find('CONSIDERED_DATE').text
                CHAR_CONSIDERED_DATE = i.find('CHAR_CONSIDERED_DATE').text
                IND_DEDUCT_REVENUE = i.find('IND_DEDUCT_REVENUE').text
                IND_NON_DEDUCT_REVENUE = i.find('IND_NON_DEDUCT_REVENUE').text
                GRP_NON_DEDUCT_REVENUE = i.find('GRP_NON_DEDUCT_REVENUE').text
                GRP_DEDUCT_REVENUE = i.find('GRP_DEDUCT_REVENUE').text
                OWNER_ROOMS = i.find('OWNER_ROOMS').text
                FF_ROOMS = i.find('FF_ROOMS').text
                CF_OOO_ROOMS = i.find('CF_OOO_ROOMS').text
                CF_CALC_OCC_ROOMS = i.find('CF_CALC_OCC_ROOMS').text
                CF_CALC_INV_ROOMS = i.find('CF_CALC_INV_ROOMS').text
                CF_AVERAGE_ROOM_RATE = i.find('CF_AVERAGE_ROOM_RATE').text
                CF_OCCUPANCY = i.find('CF_OCCUPANCY').text
                CF_IND_DED_REV = i.find('CF_IND_DED_REV').text
                CF_IND_NON_DED_REV = i.find('CF_IND_NON_DED_REV').text
                CF_BLK_DED_REV = i.find('CF_BLK_DED_REV').text
                CF_BLK_NON_DED_REV = i.find('CF_BLK_NON_DED_REV').text

                rows.append({
                    'REVENUE': REVENUE,
                    'NO_ROOMS': NO_ROOMS,
                    'IND_DEDUCT_ROOMS': IND_DEDUCT_ROOMS,
                    'IND_NON_DEDUCT_ROOMS': IND_NON_DEDUCT_ROOMS,
                    'GRP_DEDUCT_ROOMS': GRP_DEDUCT_ROOMS,
                    'GRP_NON_DEDUCT_ROOMS': GRP_NON_DEDUCT_ROOMS,
                    'NO_PERSONS': NO_PERSONS,
                    'ARRIVAL_ROOMS': ARRIVAL_ROOMS,
                    'DEPARTURE_ROOMS': DEPARTURE_ROOMS,
                    'COMPLIMENTARY_ROOMS': COMPLIMENTARY_ROOMS,
                    'HOUSE_USE_ROOMS': HOUSE_USE_ROOMS,
                    'DAY_USE_ROOMS': DAY_USE_ROOMS,
                    'NO_SHOW_ROOMS': NO_SHOW_ROOMS,
                    'INVENTORY_ROOMS': INVENTORY_ROOMS,
                    'CONSIDERED_DATE': CONSIDERED_DATE,
                    'CHAR_CONSIDERED_DATE': CHAR_CONSIDERED_DATE,
                    'IND_DEDUCT_REVENUE': IND_DEDUCT_REVENUE,
                    'IND_NON_DEDUCT_REVENUE': IND_NON_DEDUCT_REVENUE,
                    'GRP_NON_DEDUCT_REVENUE': GRP_NON_DEDUCT_REVENUE,
                    'GRP_DEDUCT_REVENUE': GRP_DEDUCT_REVENUE,
                    'OWNER_ROOMS': OWNER_ROOMS,
                    'FF_ROOMS': FF_ROOMS,
                    'CF_OOO_ROOMS': CF_OOO_ROOMS,
                    'CF_CALC_OCC_ROOMS': CF_CALC_OCC_ROOMS,
                    'CF_CALC_INV_ROOMS': CF_CALC_INV_ROOMS,
                    'CF_AVERAGE_ROOM_RATE': CF_AVERAGE_ROOM_RATE,
                    'CF_OCCUPANCY': CF_OCCUPANCY,
                    'CF_IND_DED_REV': CF_IND_DED_REV,
                    'CF_IND_NON_DED_REV': CF_IND_NON_DED_REV,
                    'CF_BLK_DED_REV': CF_BLK_DED_REV,
                    'CF_BLK_NON_DED_REV': CF_BLK_NON_DED_REV
                })
            df = pd.DataFrame(rows, columns=cols)
            df.insert(0, column="propertyCode", value=propertyCode)
            df.insert(1, column="pullDateId", value=pullDateId)
            df.insert(2, column="createdAt", value=createdAt)
            df.insert(3, column="updatedAt", value=updatedAt)
            df['CONSIDERED_DATE'] = pd.to_datetime(df['CONSIDERED_DATE'])
            df['CHAR_CONSIDERED_DATE'] = pd.to_datetime(df['CHAR_CONSIDERED_DATE'])
            df.to_csv(f"{folder_name}{propertyCode}_Occupancy.csv", index=False)

            occ_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Occupancy.csv", encoding="utf-8"))
            occ_result = list(occ_result)
        else:
            occ_result = []
            print("Occupancy Data not available")
        # End Occupancy Report

        # Start Arrival Report
        arrival_dataframe = []

        with open(arrival_file_path, 'r') as f:
            read = f.read()
            soup_data = BeautifulSoup(read, "xml")
            column_names = soup_data.find_all('G_RESERVATION')

            for column_name in column_names:
                data_dict = {}
                for element in column_name:
                    tag = element.name
                    text = element.get_text(strip=True)
                    data_dict[tag] = text
                arrival_dataframe.append(data_dict)

        arrival_data_concat = pd.DataFrame(arrival_dataframe)
        headers = arrival_data_concat.columns[1:]
        final_df = arrival_data_concat[headers]
        final_df.insert(0, column="propertyCode", value=propertyCode)
        final_df.insert(1, column="pullDateId", value=pullDateId)
        final_df.insert(2, column="createdAt", value=createdAt)
        final_df.insert(3, column="updatedAt", value=updatedAt)
        final_df['UPDATE_DATE'] = pd.to_datetime(final_df['UPDATE_DATE'])
        final_df['TRUNC_BEGIN'] = pd.to_datetime(final_df['TRUNC_BEGIN'])
        final_df['TRUNC_END'] = pd.to_datetime(final_df['TRUNC_END'])
        final_df['ARRIVAL'] = pd.to_datetime(final_df['ARRIVAL'])
        final_df['DEPARTURE'] = pd.to_datetime(final_df['DEPARTURE'])
        final_df['BEGIN_DATE'] = pd.to_datetime(final_df['BEGIN_DATE'])
        final_df.to_csv(f"{folder_name}{propertyCode}_Arrival.csv", index=False)

        arrival_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Arrival.csv", encoding="utf-8"))
        arrival_result = list(arrival_result)
        # End Arrival Report

        print("RES RESULT")
        print(res_result)
        print("OCC RESULT")
        print(occ_result)
        print("ARRIVAL RESULT")
        print(arrival_result)

        if len(res_result) > 0 and len(occ_result) > 0 and len(arrival_result) > 0:
            bulk_insert_opera_cloud_res(res_result, propertyCode, row['res_before'], row['res_after'])
            print("RES DONE")

            bulk_insert_opera_cloud_occ(occ_result, propertyCode, row['occ_before'], row['occ_after'])
            print("OCC DONE")

            bulk_insert_opera_cloud_arrival(arrival_result, propertyCode, row['res_before'], row['res_after'])
            print("ARRIVAL DONE")

            update_into_pulldate(pullDateId, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
        else:
            print("File was blank!!!")
            update_into_pulldate(pullDateId, ERROR_NOTE="File was blank!!!", IS_ERROR=True)
    else:
        msg = "File Not found!!!"
        update_into_pulldate(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)


if __name__ == '__main__':

    service = prep_service()

    # Get all property using brand
    PMS_NAME = "'OperaCloud'"
    print("SCRIPT STARTED FOR OperaCloud")
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
                'res_after': CURRENT_DATE.shift(days=RES_AFTER),
                'occ_before': CURRENT_DATE.shift(days=OCC_BEFORE),
                'occ_after': CURRENT_DATE.shift(days=+OCC_AFTER),
                "propertyCode": PROPERTY_CODE,
                "pullDateId": LAST_PULL_DATE_ID
            }
            print("row :: ", row)
            OperaCloud_Pms(row)
            print("SCRIPT DONE FOR OperaCloud")
        else:
            print("LAST_PULL_DATE_ID is NULL")