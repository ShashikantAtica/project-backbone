import os
import sys

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


def bulk_insert_hotelkey_res(res_list, propertyCode):
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
            f'DELETE from hotelkey_res where {pullDateId} = {pullDateIdValue};')
        conn.close()
        print("DELETE OLD DATA!!!", pullDateIdValue)
    else:
        print("Not previous data!!!")

    # Add new data of reservation
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.hotelkey_res_model.insert(), res_list)
    conn.close()
    print("Data imported")


def bulk_insert_hotelkey_occ(occ_list, propertyCode):
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

        # Delete existing data of occupancy
        conn = db_config.get_db_connection()
        conn.execute(
            f'DELETE from hotelkey_occ where {pullDateId} = {pullDateIdValue};')
        conn.close()
        print("DELETE OLD DATA!!!", pullDateIdValue)
    else:
        print("Not previous data!!!")

    # Add new data of reservation
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.hotelkey_occ_model.insert(), occ_list)
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


def Hotelkey_Pms(row):
    atica_property_code = row['atica_property_code']
    secret_name = row['gcp_secret']
    pullDateId = row['pullDateId']
    propertyCode = row['propertyCode']

    label_array = [f"{propertyCode} Reservation", f"{propertyCode} Occupancy"]
    attachment_format = "./reports"
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
                try:
                    print("Saving with New Way")
                    for sub_part in part['parts']:
                        save_flag = True
                        if sub_part['filename']:
                            if save_flag:
                                if 'data' in sub_part['body']:
                                    file_data = base64.urlsafe_b64decode(sub_part['body']['data'].encode('UTF-8'))
                                else:
                                    attachment_id = sub_part['body']['attachmentId']
                                    attachment = service.users().messages().attachments().get(userId="me",
                                                                                              messageId=message["id"],
                                                                                              id=attachment_id).execute()
                                    data = attachment['data']
                                    file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))

                                print("saving file of size " + str(sys.getsizeof(file_data)) + " bytes")
                                # file_data

                                # Open file in binary write mode
                                file_name = label_name.split(" ")[1]
                                print(file_name)
                                binary_file = open(f"{attachment_format}/{propertyCode}_{file_name}.csv", "wb")
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
                except Exception:
                    save_flag = True
                    if part['filename']:
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
                            print(file_name)
                            binary_file = open(f"{attachment_format}/{propertyCode}_{file_name}.csv", "wb")
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
    reservation_file_path = f'{attachment_format}/{propertyCode}_Reservation.csv'
    occupancy_file_path = f'{attachment_format}/{propertyCode}_Occupancy.csv'

    check_reservation_file = os.path.isfile(reservation_file_path)
    check_occupancy_file = os.path.isfile(occupancy_file_path)

    if check_reservation_file and check_occupancy_file:
        createdAt = "'" + str(arrow.now()) + "'"
        updatedAt = "'" + str(arrow.now()) + "'"
        createdAtEpoch =  str(int(arrow.utcnow().timestamp()))
        updatedAtEpoch =  str(int(arrow.utcnow().timestamp()))
        # Reservation Data Clean and Insert
        read = pd.read_csv(reservation_file_path, skipfooter=1, engine='python')
        values = ['', 'Book', 'Total']
        read = read[read['Arrival\nDate'].isin(values) == False]
        read = read[read['Depart\nDate'].isin(values) == False]
        read = read[read['Creation Date'].isin(values) == False]
        read['Arrival\nDate'] = pd.to_datetime(read['Arrival\nDate'])
        read['Depart\nDate'] = pd.to_datetime(read['Depart\nDate'])
        read['Creation Date'] = pd.to_datetime(read['Creation Date'])
        read.insert(0, column="propertyCode", value=propertyCode)
        read.insert(1, column="pullDateId", value=pullDateId)
        read.insert(2, column="createdAt", value=createdAt)
        read.insert(3, column="updatedAt", value=updatedAt)
        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
        header = ['propertyCode', 'pullDateId', 'createdAt', 'updatedAt', 'createdAtEpoch', 'updatedAtEpoch', 'Property', 'Status', 'Channel', 'Res', 'GuestName', 'ArrivalDate', 'DepartDate', 'Nts', 'Adl', 'MarketSegment',
                  'Group', 'RateCode', 'Product', 'AssignedProduct', 'Rate', 'TaxInc', 'ProjectedRevenue', 'TotalWithoutTax', 'PayMth', 'PaymentsTaken',
                  'DepositsScheduled', 'BalanceDue', 'CreationDate', 'CreationUser', 'CP', 'CPName', 'Blank']
        read.columns = header
        read['Nts'] = read['Nts'].fillna(0).astype(int)
        read['Adl'] = read['Adl'].fillna(0).astype(int)
        read['PaymentsTaken'] = read['PaymentsTaken'].fillna(0).astype(int)
        read['DepositsScheduled'] = read['DepositsScheduled'].fillna(0).astype(int)
        read.to_csv(f"{attachment_format}/{propertyCode}_Reservation.csv", index=False)

        res_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Reservation.csv", encoding="utf-8"))
        res_result = list(res_result)

        # Forecast Data Clean and Insert
        read = pd.read_csv(occupancy_file_path)
        read['Date'] = pd.to_datetime(read['Date'])
        read.insert(0, column="propertyCode", value=propertyCode)
        read.insert(1, column="pullDateId", value=pullDateId)
        read.insert(2, column="createdAt", value=createdAt)
        read.insert(3, column="updatedAt", value=updatedAt)
        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
        header = ['propertyCode', 'pullDateId', 'createdAt', 'updatedAt', 'createdAtEpoch', 'updatedAtEpoch', 'Date', 'Property', 'PFRZ', 'FRZ', 'TOD', 'DOW', 'GTD', 'LOS', 'CXL', 'OO', 'Hold', 'Yieldable', 'Sold', 'BLK',
                  'SD', 'OccOTB', 'OccPYClose', 'OccPYVar', 'ADROTB', 'ADRPYClose', 'ADRPYVar', 'DiscPer', 'Price1', 'LT1', 'Per1', 'RM1', 'Price2', 'LT2', 'Per2',
                  'RM2', 'Price3', 'LT3', 'Per3', 'RM3', 'Price4', 'LT4', 'Per4', 'RM4']
        read.columns = header
        read['DiscPer'] = read['DiscPer'].fillna(0).astype(int)
        read['LT1'] = read['LT1'].fillna(0).astype(int)
        read['Per1'] = read['Per1'].fillna(0).astype(int)
        read['RM1'] = read['RM1'].fillna(0).astype(int)
        read['LT2'] = read['LT2'].fillna(0).astype(int)
        read['Per2'] = read['Per2'].fillna(0).astype(int)
        read['RM2'] = read['RM2'].fillna(0).astype(int)
        read['LT3'] = read['LT3'].fillna(0).astype(int)
        read['Per3'] = read['Per3'].fillna(0).astype(int)
        read['RM3'] = read['RM3'].fillna(0).astype(int)
        read['LT4'] = read['LT4'].fillna(0).astype(int)
        read['Per4'] = read['Per4'].fillna(0).astype(int)
        read['RM4'] = read['RM4'].fillna(0).astype(int)
        read.to_csv(f"{attachment_format}/{propertyCode}_Occupancy.csv", index=False)

        occ_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Occupancy.csv", encoding="utf-8"))
        occ_result = list(occ_result)

        if len(res_result) > 0 and len(occ_result) > 0:
            bulk_insert_hotelkey_res(res_result, propertyCode=propertyCode)
            print("RES DONE")

            bulk_insert_hotelkey_occ(occ_result, propertyCode=propertyCode)
            print("OCC DONE")

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
            Hotelkey_Pms(row)
            print("SCRIPT DONE FOR Hotelkey")
        else:
            print("LAST_PULL_DATE_ID is NULL")