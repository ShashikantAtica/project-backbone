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
    conn = db_config.get_db_connection()
    conn.execute(db_models.opera_rbrc_model.insert(), rbrc_list)
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

        
        try:
            for i in root[0][0][0]:
                RESORT = i.find("RESORT").text if(i.find("RESORT")) is not None else ""
                BUSINESS_DATE = i.find("BUSINESS_DATE").text if(i.find("BUSINESS_DATE")) is not None else ""
                CHAR_BUSINESS_DATE = i.find("CHAR_BUSINESS_DATE").text if(i.find("CHAR_BUSINESS_DATE")) is not None else ""
                for k in i.find("LIST_MARKET"):
                    MASTER_VALUE = k.find("MASTER_VALUE").text if(k.find("MASTER_VALUE") is not None and k.find("MASTER_VALUE").text != "{NULL}")   else ""
                    CF_MASTER_SEQ = k.find("CF_MASTER_SEQ").text if(k.find("CF_MASTER_SEQ")) is not None else ""
                    GROUP_NAME  = k.find("GROUP_NAME").text if(k.find("GROUP_NAME") is not None and k.find("GROUP_NAME").text != "Unknown") else ""
                    for j in k.find("LIST_DETAIL"):
                        ARR_TODAY = j.find("ARR_TODAY").text if(j.find("ARR_TODAY")) is not None else ""
                        NO_DEFINITE_ROOMS = j.find("NO_DEFINITE_ROOMS").text if(j.find("NO_DEFINITE_ROOMS")) is not None else ""
                        IN_GUEST  = j.find("IN_GUEST").text if(j.find("IN_GUEST")) is not None else ""
                        OCC_SINGLE = j.find("OCC_SINGLE").text if(j.find("OCC_SINGLE")) is not None else ""
                        DOUBLE_OCC = j.find("DOUBLE_OCC").text if(j.find("DOUBLE_OCC")) is not None else ""
                        REVENUE = j.find("REVENUE").text if(j.find("REVENUE")) is not None else ""
                        FB_REV = j.find("FB_REV").text if(j.find("FB_REV")) is not None else ""
                        OTHER_REV = j.find("OTHER_REV").text if(j.find("OTHER_REV")) is not None else ""
                        TOTAL_REVENUE  = j.find("TOTAL_REVENUE").text if(j.find("TOTAL_REVENUE")) is not None else ""
                        RESORT_ROOM = j.find("RESORT_ROOM").text if(j.find("RESORT_ROOM")) is not None else ""
                        PER_OCC = j.find("PER_OCC").text if(j.find("PER_OCC")) is not None else ""
                        GET_ARR  = j.find("GET_ARR").text if(j.find("GET_ARR")) is not None else ""
                        MULTI_OCC_PER  = j.find("MULTI_OCC_PER").text if(j.find("MULTI_OCC_PER")) is not None else ""
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
