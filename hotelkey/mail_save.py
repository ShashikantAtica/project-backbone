import argparse
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


def insert_into_pulldate(PROPERTY_CODE, PULLED_DATE, PMS_NAME):
    LAST_PULL_DATE_ID = None
    DB_PMS_NAME = "'" + PMS_NAME + "'"
    DB_PROPERTY_CODE = "'" + PROPERTY_CODE + "'"
    DB_PULLED_DATE = "'" + str(PULLED_DATE) + "'"
    DB_STATUS = "'INPROGRESS'"
    query_string = f'INSERT INTO "tbl_pullDate" ("propertyCode", "pulledDate", "status","pmsName") VALUES ({DB_PROPERTY_CODE}, {DB_PULLED_DATE}, {DB_STATUS},{DB_PMS_NAME}) RETURNING id; '
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


def bulk_insert_hotelkey_res(res_list, propertyCode, res_before, res_after):
    start_date = res_before.format("YYYY-MM-DD")
    end_date = res_after.format("YYYY-MM-DD")
    print("res_start_date :: ", start_date)
    print("res_end_date :: ", end_date)

    conn = db_config.get_db_connection()
    conn.execute(
        f"""DELETE from hotelkey_res where "CreationDate" between '{start_date}' and '{end_date}' and "propertyCode" = '{propertyCode}';""")
    conn.close()

    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.hotelkey_res_model.insert(), res_list)
    conn.close()
    print("Data imported")


def bulk_insert_hotelkey_occ(occ_list, propertyCode, occ_before, occ_after):
    start_date = occ_before.format("YYYY-MM-DD")
    end_date = occ_after.format("YYYY-MM-DD")
    print("occ_start_date :: ", start_date)
    print("occ_end_date :: ", end_date)

    conn = db_config.get_db_connection()
    conn.execute(
        f"""DELETE from hotelkey_occ where "Date" between '{start_date}' and '{end_date}' and "propertyCode" = '{propertyCode}';""")
    conn.close()

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

def label_messege_saver(label_name, messages_array, track_label_nomsg_set):
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
        track_label_nomsg_set.add(label_name)
        return 0

def Hotelkey_Pms(row):
    global archive_label
    atica_property_code = row['atica_property_code']
    secret_name = row['gcp_secret']
    pullDateId = row['pullDateId']
    propertyCode = row['propertyCode']

    label_array = [f"{propertyCode} Reservation", f"{propertyCode} Occupancy"]
    attachment_format = "./reports"
    messages_array = []
    saved_messages_ids = []
    track_label_nomsg_set = set()

    file_paths = [
        f'{attachment_format}/{propertyCode}_Reservation.csv',
        f'{attachment_format}/{propertyCode}_Occupancy.csv'
    ]

    for file_path in file_paths:
        if os.path.exists(file_path):
            os.remove(file_path)


    for label_name in label_array:
        label_messege_saver(label_name, messages_array, track_label_nomsg_set)
        
    
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

        for message in messages:
            saved_messages_ids.append(message["id"])
        archive_label = get_archive_label("Saved")
        print(f"{archive_label['name']} : {archive_label['id']}")

        # # Apply archive label to saved messages
        # label_apply_body = {
        #     "addLabelIds": archive_label["id"],
        #     "ids": saved_messages_ids
        # }
        #
        # if saved_messages_ids:
        #     response = service.users().messages().batchModify(userId="me",
        #                                                       body=label_apply_body
        #                                                       ).execute()
        #     saved_messages_count = len(saved_messages_ids)
        #     print(f"Saved label applied to {saved_messages_count} messages.")
        #
        # else:
        #     print("No messages to save")

    # Modification of res report
    reservation_file_path = f'{attachment_format}/{propertyCode}_Reservation.csv'
    occupancy_file_path = f'{attachment_format}/{propertyCode}_Occupancy.csv'

    check_reservation_file = os.path.isfile(reservation_file_path)
    check_occupancy_file = os.path.isfile(occupancy_file_path)

    createdAt = "'" + str(arrow.now()) + "'"
    updatedAt = "'" + str(arrow.now()) + "'"
    createdAtEpoch = int(arrow.utcnow().timestamp())
    updatedAtEpoch = int(arrow.utcnow().timestamp())

    errorMessage = ""
    fileCount=0

    if not check_reservation_file:
        res_labelname=f"{propertyCode} Reservation"
        if res_labelname in track_label_nomsg_set:
            errorMessage = errorMessage + f"No new messages for {res_labelname} label, "
        else:
            errorMessage = errorMessage + " Reservation file - N/A"

    if not check_occupancy_file:
        occ_labelname=f"{propertyCode} Occupancy"
        if occ_labelname in track_label_nomsg_set:
            errorMessage = errorMessage + f"No new messages for {occ_labelname} label, "
        else:
            errorMessage = errorMessage + " Occupancy file - N/A"

    if check_reservation_file:
        
        fileCount = fileCount + 1
        # Reservation Data Clean and Insert
        read = pd.read_csv(reservation_file_path, skipfooter=1, engine='python')
        read = read[(read['Status'] == 'Book')]
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
        read.insert(6, column="uniqueKey", value=read["Res #"].astype(str))
        header = ['propertyCode', 'pullDateId', 'createdAt', 'updatedAt', 'createdAtEpoch', 'updatedAtEpoch', 'uniqueKey', 'Property', 'Status', 'Channel', 'Res', 'GuestName', 'ArrivalDate', 'DepartDate', 'Nts', 'Adl', 'MarketSegment',
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
        if len(res_result) > 0:
            bulk_insert_hotelkey_res(res_result, propertyCode=propertyCode, res_before=row['res_before'], res_after=row['res_after'])
            print("RES DONE")
        else:
            errorMessage = errorMessage + "Reservation File Was Blank, "

    if check_occupancy_file:

        fileCount = fileCount + 1
        # Forecast Data Clean and Insert
        read = pd.read_csv(occupancy_file_path)
        read['Date'] = pd.to_datetime(read['Date'])
        read.insert(0, column="propertyCode", value=propertyCode)
        read.insert(1, column="pullDateId", value=pullDateId)
        read.insert(2, column="createdAt", value=createdAt)
        read.insert(3, column="updatedAt", value=updatedAt)
        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
        read.insert(6, column="uniqueKey", value=read["propertyCode"].astype(str) + "_" + read['Date'].astype(str)) 
        header = ['propertyCode', 'pullDateId', 'createdAt', 'updatedAt', 'createdAtEpoch', 'updatedAtEpoch', 'uniqueKey', 'Date', 'Property', 'PFRZ', 'FRZ', 'TOD', 'DOW', 'GTD', 'LOS', 'CXL', 'OO', 'Hold', 'Yieldable', 'Sold', 'BLK',
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

        if len(res_result) > 0:
            bulk_insert_hotelkey_occ(occ_result, propertyCode=propertyCode, occ_before=row['occ_before'], occ_after=row['occ_after'])
            print("OCC DONE")
        else:
            errorMessage = errorMessage + "Occupancy File Was Blank, "

    if len(track_label_nomsg_set) != 2:
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
        
    if fileCount == 2:
        if errorMessage == "":
            update_into_pulldate(pullDateId, ERROR_NOTE="Successfully Finished", IS_ERROR=False)

        else:
            errorMessage="Partially Successfull:- "+errorMessage
            update_into_pulldate(pullDateId, ERROR_NOTE=errorMessage, IS_ERROR=True)
    else:
        if fileCount == 0 :
            if len(track_label_nomsg_set) == 2:
                errorMessage = "No new messages for all label"
            else:
                errorMessage = "All File Not Found"
        else:
            errorMessage="Partially Successfull:- "+errorMessage
        update_into_pulldate(pullDateId, ERROR_NOTE=errorMessage, IS_ERROR=True)


if __name__ == '__main__':

    service = prep_service()

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
        res = conn.execute(f"""SELECT * FROM tbl_properties WHERE "pmsName" = '{PMS_NAME}';""")
        result = res.fetchall()
        conn.close()
        print("Fetched successfully")
    else:
        print(f"{propertycode} property run")
        conn = db_config.get_db_connection()
        res = conn.execute(f"""SELECT * FROM tbl_properties WHERE "pmsName" = '{PMS_NAME}' and "propertyCode" = '{propertycode}';""")
        result = res.fetchall()
        conn.close()
        print("Fetched successfully")

    if result is not None and len(result) > 0:
        print(f"Total Properties :: {len(result)}")
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
