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


def bulk_insert_synxis_cloud_res(res_list, propertyCode):
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
            f'DELETE from synxis_cloud_reservation where {pullDateId} = {pullDateIdValue};')
        conn.close()
        print("DELETE OLD DATA!!!", pullDateIdValue)
    else:
        print("Not previous data!!!")

    # Add new data of reservation
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.synxis_cloud_reservation_model.insert(), res_list)
    conn.close()
    print("Data imported")


def bulk_insert_synxis_cloud_forecast(res_list, propertyCode):
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
            f'DELETE from synxis_cloud_forecast where {pullDateId} = {pullDateIdValue};')
        conn.close()
        print("DELETE OLD DATA!!!", pullDateIdValue)
    else:
        print("Not previous data!!!")

    # Add new data of reservation
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.synxis_cloud_forecast_model.insert(), res_list)
    conn.close()
    print("Data imported")


def bulk_insert_synxis_cloud_revenue_recap(res_list, propertyCode):
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
            f'DELETE from synxis_cloud_revenue_recap where {pullDateId} = {pullDateIdValue};')
        conn.close()
        print("DELETE OLD DATA!!!", pullDateIdValue)
    else:
        print("Not previous data!!!")

    # Add new data of reservation
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.synxis_cloud_revenue_recap_model.insert(), res_list)
    conn.close()
    print("Data imported")


def bulk_insert_synxis_cloud_monthly_summary(res_list, propertyCode):
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
            f'DELETE from synxis_cloud_monthly_summary where {pullDateId} = {pullDateIdValue};')
        conn.close()
        print("DELETE OLD DATA!!!", pullDateIdValue)
    else:
        print("Not previous data!!!")

    # Add new data of reservation
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.synxis_cloud_monthly_summary_model.insert(), res_list)
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


def Synxis_Cloud_Pms(row):
    atica_property_code = row['atica_property_code']
    secret_name = row['gcp_secret']
    pullDateId = row['pullDateId']
    propertyCode = row['propertyCode']

    label_array = [f"{propertyCode} Reservation", f"{propertyCode} Forecast", f"{propertyCode} Revenue", f"{propertyCode} Monthly"]
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
    forecast_file_path = f'{attachment_format}/{propertyCode}_Forecast.csv'
    revenue_file_path = f'{attachment_format}/{propertyCode}_Revenue.csv'
    monthly_file_path = f'{attachment_format}/{propertyCode}_Monthly.csv'

    check_reservation_file = os.path.isfile(reservation_file_path)
    check_forecast_file = os.path.isfile(forecast_file_path)
    check_revenue_file = os.path.isfile(revenue_file_path)
    check_monthly_file = os.path.isfile(monthly_file_path)

    if check_reservation_file and check_forecast_file and check_revenue_file and check_monthly_file:
        createdAt = "'" + str(arrow.now()) + "'"
        updatedAt = "'" + str(arrow.now()) + "'"
        # Reservation Data Clean and Insert
        read = pd.read_csv(reservation_file_path, skipfooter=3, engine='python')
        read['Status_Dt'] = pd.to_datetime(read['Status_Dt'])
        read['Arrival_Dt'] = pd.to_datetime(read['Arrival_Dt'])
        read['Depart_Dt'] = pd.to_datetime(read['Depart_Dt'])
        read['VCC_Card_Activation_Start'] = pd.to_datetime(read['VCC_Card_Activation_Start'])
        read['VCC_Card_Activation_End'] = pd.to_datetime(read['VCC_Card_Activation_End'])
        read.insert(0, column="propertyCode", value=propertyCode)
        read.insert(1, column="pullDateId", value=pullDateId)
        read.insert(2, column="createdAt", value=createdAt)
        read.insert(3, column="updatedAt", value=updatedAt)
        read['Pay_At_Property'] = read['Pay_At_Property'].fillna(0).astype(int)
        read['Total_Price_For_Adult'] = read['Total_Price_For_Adult'].fillna(0).astype(int)
        read['Total_Price_For_Child_Age_Group1'] = read['Total_Price_For_Child_Age_Group1'].fillna(0).astype(int)
        read['Total_Price_For_Child_Age_Group2'] = read['Total_Price_For_Child_Age_Group2'].fillna(0).astype(int)
        read['Total_Price_For_Child_Age_Group3'] = read['Total_Price_For_Child_Age_Group3'].fillna(0).astype(int)
        read['Total_Price_For_Child_Age_Group4'] = read['Total_Price_For_Child_Age_Group4'].fillna(0).astype(int)
        read['Total_Price_For_Child_Age_Group5'] = read['Total_Price_For_Child_Age_Group5'].fillna(0).astype(int)
        read['Total_Price_For_Child_Unknown_Age_Group'] = read['Total_Price_For_Child_Unknown_Age_Group'].fillna(0).astype(int)
        read['Coupon_Discount_Total'] = read['Coupon_Discount_Total'].fillna(0).astype(int)
        read['Promo_Discount'] = read['Promo_Discount'].fillna(0).astype(int)
        read['Paynow_Discount'] = read['Paynow_Discount'].fillna(0).astype(int)
        read.to_csv(f"{attachment_format}/{propertyCode}_Reservation.csv", index=False)

        res_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Reservation.csv", encoding="utf-8"))
        res_result = list(res_result)

        # Forecast Data Clean and Insert
        read = pd.read_csv(forecast_file_path, skipfooter=3, engine='python')
        read['cal_dt'] = pd.to_datetime(read['cal_dt'])
        read.insert(0, column="propertyCode", value=propertyCode)
        read.insert(1, column="pullDateId", value=pullDateId)
        read.insert(2, column="createdAt", value=createdAt)
        read.insert(3, column="updatedAt", value=updatedAt)
        read.to_csv(f"{attachment_format}/{propertyCode}_Forecast.csv", index=False)

        fore_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Forecast.csv", encoding="utf-8"))
        fore_result = list(fore_result)

        # Revenue Recap Data Clean and Insert
        date_df = pd.read_csv(revenue_file_path, skiprows=1, engine='python')
        date = date_df.columns.str.extract(r'(\d{2}\s\w{3}\s\d{4})').values[0][0]
        read = pd.read_csv(revenue_file_path, skiprows=3, skipfooter=3, engine='python')
        read.insert(0, column="propertyCode", value=propertyCode)
        read.insert(1, column="pullDateId", value=pullDateId)
        read.insert(2, column="createdAt", value=createdAt)
        read.insert(3, column="updatedAt", value=updatedAt)
        read.insert(4, column="Date", value=date)
        read.loc[:, 'Date'] = pd.to_datetime(read['Date'], format='%d %b %Y', errors='coerce').dt.date
        read['F_B'] = read['F_B'].fillna(0).astype(int)
        read['Other'] = read['Other'].fillna(0).astype(int)
        read['Month_To_Date_F_B'] = read['Month_To_Date_F_B'].fillna(0).astype(int)
        read['Year_To_Date_F_B'] = read['Year_To_Date_F_B'].fillna(0).astype(int)
        read['F_B_Total'] = read['F_B_Total'].fillna(0).astype(int)
        read['Other_Total'] = read['Other_Total'].fillna(0).astype(int)
        read['Month_To_Date_F_B_total'] = read['Month_To_Date_F_B_total'].fillna(0).astype(int)
        read['Year_To_Date_F_B_Total'] = read['Year_To_Date_F_B_Total'].fillna(0).astype(int)
        read.to_csv(f"{attachment_format}/{propertyCode}_Revenue.csv", index=False)

        rev_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Revenue.csv", encoding="utf-8"))
        rev_result = list(rev_result)

        # Monthly Summary Data Clean and Insert
        read = pd.read_csv(monthly_file_path, skipfooter=3, engine='python')
        read['BUSINESS_DT'] = pd.to_datetime(read['BUSINESS_DT'], format="%b %d, %Y(%a)")
        read.insert(0, column="propertyCode", value=propertyCode)
        read.insert(1, column="pullDateId", value=pullDateId)
        read.insert(2, column="createdAt", value=createdAt)
        read.insert(3, column="updatedAt", value=updatedAt)
        read['TOTAL_FOOD_AND_BEV_REV'] = read['TOTAL_FOOD_AND_BEV_REV'].fillna(0).astype(int)
        read['FoodandBeverage'] = read['FoodandBeverage'].fillna(0).astype(int)
        read.to_csv(f"{attachment_format}/{propertyCode}_Monthly.csv", index=False)

        monthly_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Monthly.csv", encoding="utf-8"))
        monthly_result = list(monthly_result)

        if len(res_result) > 0 and len(fore_result) > 0 and len(rev_result) > 0 and len(monthly_result) > 0:
            bulk_insert_synxis_cloud_res(res_result, propertyCode=propertyCode)
            print("RES DONE")

            bulk_insert_synxis_cloud_forecast(fore_result, propertyCode=propertyCode)
            print("FORE DONE")

            bulk_insert_synxis_cloud_revenue_recap(rev_result, propertyCode=propertyCode)
            print("REV DONE")

            bulk_insert_synxis_cloud_monthly_summary(monthly_result, propertyCode=propertyCode)
            print("MONTHLY DONE")

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
    PMS_NAME = "'SYNXIS CLOUD'"
    print("SCRIPT STARTED FOR SYNXIS CLOUD")
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
            Synxis_Cloud_Pms(row)
            print("SCRIPT DONE FOR SYNXIS CLOUD")
        else:
            print("LAST_PULL_DATE_ID is NULL")