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



def bulk_insert_opera_cloud_rbrc(rbrc_list, propertyCode):
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
            f'DELETE from opera_rbrc where {pullDateId} = {pullDateIdValue};')
        conn.close()
        print("DELETE OLD DATA!!!", pullDateIdValue)
    else:
        print("Not previous data!!!")

    # Add new data of reservation
    print("Data importing...")
    # print(rbrc_list)
    conn = db_config.get_db_connection()
    # rbrc_list1=[{'id': 1234 ,'propertyCode': 'USTX230602-test', 'pullDateId': '804', 'RESORT': '12345', 'BUSINESS_DATE': '2023-10-01', 'CHAR_BUSINESS_DATE': '2023-01-06', 'MASTER_VALUE': 'HOUSE', 'CF_MASTER_SEQ': 'HOUSE', 'GROUP_NAME': 'HOUSE', 'ARR_TODAY': '0', 'NO_DEFINITE_ROOMS': '0', 'IN_GUEST': '0', 'OCC_SINGLE': '0', 'DOUBLE_OCC': '0', 'REVENUE': '0', 'FB_REV': '0', 'OTHER_REV': '0', 'TOTAL_REVENUE': '0', 'RESORT_ROOM': '152', 'PER_OCC': '0', 'GET_ARR': '0', 'MULTI_OCC_PER': '0'}]
    conn.execute(db_models.opera_rbrc_model.insert(), rbrc_list)
    # conn.execute(db_models.opera_rbrc_model.insert(
    #     'propertyCode': 'USTX230602',
    #     'pullDateId': '804',
    #     'RESORT': '12345',
    #     'BUSINESS_DATE': '2023-10-01',
    #     'CHAR_BUSINESS_DATE': '2023-01-06',
    #     'MASTER_VALUE': 'IOUSE',
    #     'CF_MASTER_SEQ': 'IOUSE',
    #     'GROUP_NAME': 'IOUSE',
    #     'ARR_TODAY': '01',
    #     'NO_DEFINITE_ROOMS': '01',
    #     'IN_GUEST': '01',
    #     'OCC_SINGLE': '01',
    #     'DOUBLE_OCC': '01',
    #     'REVENUE': '01',
    #     'FB_REV': '01',
    #     'OTHER_REV': '01',
    #     'TOTAL_REVENUE': '568',
    #     'RESORT_ROOM': '152',
    #     'PER_OCC': '5.26315789',
    #     'GET_ARR': '71',
    #     'MULTI_OCC_PER': '0'
    # )
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

    label_array = [f"{propertyCode} RBRC"]
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
            print("####",f"{archive_label['name']} : {archive_label['id']}")

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
    rbrc_file_path = f'{folder_name}{propertyCode}_RBRC.xml'

    check_rbrc_file = os.path.isfile(rbrc_file_path)

    if check_rbrc_file:
        
        # Start RBRC Report
        cols = ["RESORT","BUSINESS_DATE","CHAR_BUSINESS_DATE","MASTER_VALUE","CF_MASTER_SEQ","GROUP_NAME","ARR_TODAY","NO_DEFINITE_ROOMS",
        "IN_GUEST","OCC_SINGLE","DOUBLE_OCC","REVENUE",
        "FB_REV","OTHER_REV","TOTAL_REVENUE","RESORT_ROOM",
        "PER_OCC","GET_ARR","MULTI_OCC_PER"]
        rows = []

        # Parsing the XML file
        xmlparse = Xet.parse(rbrc_file_path)
        root = xmlparse.getroot()

        # total_entry=len(root[0][0][0][0][3])
        
        try:
            itr_day=0
            for i in root[0][0][0]:
                RESORT = i.find("RESORT").text
                BUSINESS_DATE = i.find("BUSINESS_DATE").text
                CHAR_BUSINESS_DATE = i.find("CHAR_BUSINESS_DATE").text
                # print(RESORT, BUSINESS_DATE, CHAR_BUSINESS_DATE)
                itr_market=0
                for i in root[0][0][0][itr_day][3]:
                    MASTER_VALUE = i.find("MASTER_VALUE").text
                    CF_MASTER_SEQ = i.find("CF_MASTER_SEQ").text
                    GROUP_NAME  = i.find("GROUP_NAME").text
                    for j in root[0][0][0][itr_day][3][itr_market][3]:
                        ARR_TODAY = j.find("ARR_TODAY").text
                        NO_DEFINITE_ROOMS = j.find("NO_DEFINITE_ROOMS").text
                        IN_GUEST  = j.find("IN_GUEST").text
                        OCC_SINGLE = j.find("OCC_SINGLE").text
                        DOUBLE_OCC = j.find("DOUBLE_OCC").text
                        REVENUE = j.find("REVENUE").text
                        FB_REV = j.find("FB_REV").text
                        OTHER_REV = j.find("OTHER_REV").text
                        TOTAL_REVENUE  = j.find("TOTAL_REVENUE").text
                        RESORT_ROOM = j.find("RESORT_ROOM").text
                        PER_OCC = j.find("PER_OCC").text
                        GET_ARR  = j.find("GET_ARR").text
                        MULTI_OCC_PER  = j.find("MULTI_OCC_PER").text
                    itr_market+=1
                    rows.append({
                            "RESORT": RESORT,
                            "BUSINESS_DATE": BUSINESS_DATE,
                            "CHAR_BUSINESS_DATE": CHAR_BUSINESS_DATE,
                            "MASTER_VALUE": MASTER_VALUE,
                            "CF_MASTER_SEQ": CF_MASTER_SEQ,
                            "GROUP_NAME": GROUP_NAME,
                            "ARR_TODAY": ARR_TODAY,
                            "NO_DEFINITE_ROOMS": NO_DEFINITE_ROOMS,
                            "IN_GUEST": IN_GUEST,
                            "OCC_SINGLE": OCC_SINGLE,
                            "DOUBLE_OCC": DOUBLE_OCC,
                            "REVENUE": REVENUE,
                            "FB_REV": FB_REV,
                            "OTHER_REV": OTHER_REV,
                            "TOTAL_REVENUE": TOTAL_REVENUE,
                            "RESORT_ROOM": RESORT_ROOM,
                            "PER_OCC": PER_OCC,
                            "GET_ARR": GET_ARR,
                            "MULTI_OCC_PER": MULTI_OCC_PER})
                    # print(RESORT, BUSINESS_DATE, CHAR_BUSINESS_DATE, MASTER_VALUE, CF_MASTER_SEQ, GROUP_NAME, ARR_TODAY, NO_DEFINITE_ROOMS)
                itr_day+=1

            df = pd.DataFrame(rows, columns=cols)
            df.insert(0, column="propertyCode", value=propertyCode)
            df.insert(1, column="pullDateId", value=pullDateId)
            df['BUSINESS_DATE'] = pd.to_datetime(df['BUSINESS_DATE'])
            df['CHAR_BUSINESS_DATE'] = pd.to_datetime(df['CHAR_BUSINESS_DATE'])
            df.to_csv(f"{folder_name}{propertyCode}_RBRC.csv", index=False)
            rbrc_result = csv.DictReader(open(f"{folder_name}{propertyCode}_RBRC.csv", encoding="utf-8"))
            rbrc_result = list(rbrc_result)
        except Exception:
            rbrc_result = []
            print("Reservation Data not available")
        
        
        # End RBRC Report

        print("RBRC RESULT")
        print(rbrc_result)

        if len(rbrc_result) > 0:
            bulk_insert_opera_cloud_rbrc(rbrc_result, propertyCode=propertyCode)
            print("RBRC DONE")

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
        # if(item['propertyCode']!="USTX230602"):
        #     continue
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
