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


def insert_into_pulldate(PROPERTY_CODE, PULLED_DATE,PMS_NAME):
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



def bulk_insert_IDeaSG3_occ(occ_list, low_input_date_str, propertyCode):
    # input_date_str is minimum date in report and all data with date equal more than this will get deleted and data of report will get added
    Day_of_Arrival = '"Day_of_Arrival"'

    print("lowest_date_in_report :: ", low_input_date_str)

    formatted_yesterday_date_of_report = "'" + low_input_date_str + "'"
    db_propertyCode = "'" + propertyCode + "'"

    # Delete existing data of RBRC
    conn = db_config.get_db_connection()
    conn.execute(
        f'DELETE from ideasg3_occ where {Day_of_Arrival} >= {formatted_yesterday_date_of_report} and "propertyCode" = {db_propertyCode};')
    conn.close()
    print("DELETE OLD DATA >= !!!", formatted_yesterday_date_of_report)

    # Add new data of RBRC
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.ideasg3_occ_model.insert(), occ_list)
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


def IDeaSG3_Rms(row):
    atica_property_code = row['atica_property_code']
    secret_name = row['gcp_secret']
    pullDateId = row['pullDateId']
    propertyCode = row['propertyCode']

    label_array = [f"{propertyCode} Occupancy"]
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
                        binary_file = open(f"{folder_name}{propertyCode}_{file_name}.xlsx", "wb")
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
    occ_file_path = f'{folder_name}{propertyCode}_Occupancy.xlsx'

    check_occ_file = os.path.isfile(occ_file_path)

    if check_occ_file:
        
        # Start Occupancy snapshot Report
        try:
        # Parsing the Excel file
            date_set = set()
            createdAt = "'" + str(arrow.now()) + "'"
            updatedAt = "'" + str(arrow.now()) + "'"
            createdAtEpoch =  int(arrow.utcnow().timestamp())
            updatedAtEpoch =  int(arrow.utcnow().timestamp())
            column_mapping = {
                'Day_of_Week': 1,
                'Day_of_Arrival': 2,
                'Special_Event': 2,
                'Out_of_Order': 2,
                'Occupancy_On_Books_Current': 2,
                'Occupancy_On_Books_Change': 3,
                'Occupancy_Forecast_Current': 2,
                'Occupancy_Forecast_Change': 2,
                'Occupancy_Forecast%Current': 1,
                'Occupancy_Forecast%Change': 1,
                'Revenue_On_Books(USD)_Current': 1,
                'Revenue_On_Books(USD)_Change': 1,
                'Revenue_Forecast(USD)_Current': 1,
                'Revenue_Forecast(USD)_Change': 1,
                'ADR_On_Books(USD)_Current': 1,
                'ADR_On_Books(USD)_Change': 1,
                'ADR_Forecast(USD)_Current': 1,
                'ADR_Forecast(USD)_Change': 1,
                'RevPAR_On_Books(USD)_Current': 1,
                'RevPAR_On_Books(USD)_Change': 1,
                'RevPAR_Forecast(USD)_Current': 1,
                'RevPAR_Forecast(USD)_Change': 1,
                'Last_Room_Value_For_RC_DLX(USD)_Current': 1,
                'Last_Room_Value_For_RC_DLX(USD)_Change': 1,
                'Overbooking_Current': 1,
                'Overbooking_Change': 1,
                'BAR_by_Day_for_Room_Class_DLX(USD)_Current': 1,
                'BAR_by_Day_for_Room_Class_DLX(USD)_Change': 1,
                'BAR_Restricted_by_LRV_for_Room_Class_DLX_Current': 1,
                'BAR_Restricted_by_LRV_for_Room_Class_DLX_Change': 1,
            }

            rows = []
            skip_rows = 2
            for idx, row in enumerate(pd.read_excel(occ_file_path, header=None).values):
                if 'Day of Week' in str(row[0]):
                    skip_rows = idx + 2
                    break
            
            print("## Skip Rows: ", skip_rows)

            for row in pd.read_excel(occ_file_path, header=None, skiprows=skip_rows).values:
                row_data = {}
                current_col = 0
                flag=0
                for column, num_cols in column_mapping.items():
                    cell_values = row[current_col:current_col + num_cols]
                    cell_values = [str(value).replace('nan', '').strip() if pd.notna(value) else '' for value in cell_values]
                    row_data[column] = ''.join(cell_values)
                    current_col += num_cols
                    if(column=='Day_of_Arrival' and row_data[column]!=''):
                        date_set.add(row_data[column])
                    if(row_data[column]!=''):
                        flag=1
                if(flag==1):
                    rows.append(row_data)
            df = pd.DataFrame(rows)
            df.insert(0, column="propertyCode", value=propertyCode)
            df.insert(1, column="pullDateId", value=pullDateId)
            df.insert(2, column="createdAt", value=createdAt)
            df.insert(3, column="updatedAt", value=updatedAt)
            df.insert(4, column="createdAtEpoch", value=createdAtEpoch)
            df.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
            df['Day_of_Arrival'] = pd.to_datetime(df['Day_of_Arrival']).dt.strftime('%Y-%m-%d')

            df = df.reset_index(drop=True)
            df.to_csv(f"{folder_name}{propertyCode}_Occupancy.csv", index=False)
            occ_result = csv.DictReader(open(f"{folder_name}{propertyCode}_Occupancy.csv", encoding="utf-8"))
            occ_result = list(occ_result)
        except Exception:
            occ_result = []
            print("Occupancy Data not available")
        
        
        # End RBRC Report
        low_input_date_str = min(date_set)
        low_input_date_str = low_input_date_str[:10]
        print("Occupancy RESULT", low_input_date_str)
        # print(occ_result) #This can be uncommented to test/see the result of parsed data

        if len(occ_result) > 0:
            bulk_insert_IDeaSG3_occ(occ_result, low_input_date_str, propertyCode=propertyCode)
            print("Occupancy DONE")

            update_into_pulldate(pullDateId, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
        else:
            print("File was blank!!!")
            update_into_pulldate(pullDateId, ERROR_NOTE="File was blank!!!", IS_ERROR=True)
    else:
        msg = "File Not found!!!"
        update_into_pulldate(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)


if __name__ == '__main__':

    service = prep_service()
    PMS_NAME = "IDeaSG3"
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
                IDeaSG3_Rms(row)
            else:
                print("LAST_PULL_DATE_ID is NULL")
    else:
        print(f"Property not available in database!!!")
    print(f"[{PMS_NAME}] SCRIPT STOP!!!")
