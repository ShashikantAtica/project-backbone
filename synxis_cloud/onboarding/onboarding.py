import argparse
import os
import sys

from sqlalchemy import text

sys.path.append("../../")
import arrow
import csv
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from utils.db import db_config
from utils.db import db_models


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


def bulk_insert_synxis_cloud_res(res_list):
    # Add new data of reservation
    try:
        print("Data importing...")
        conn = db_config.get_db_connection()
        conn.execute(db_models.synxis_cloud_reservation_model.insert(), res_list)
        conn.commit()
        conn.close()
        print("Data imported")
    except Exception as e:
        error_message = str(e)
        print(error_message)


def bulk_insert_synxis_cloud_forecast(for_list):
    # Add new data of reservation
    try:
        print("Data importing...")
        conn = db_config.get_db_connection()
        conn.execute(db_models.synxis_cloud_forecast_model.insert(), for_list)
        conn.commit()
        conn.close()
        print("Data imported")
    except Exception as e:
        error_message = str(e)
        print(error_message)


def bulk_insert_synxis_cloud_revenue_recap(rev_list):
    # Add new data of revenue
    try:
        print("Data importing...")
        conn = db_config.get_db_connection()
        conn.execute(db_models.synxis_cloud_revenue_recap_model.insert(), rev_list)
        conn.commit()
        conn.close()
        print("Data imported")
    except Exception as e:
        error_message = str(e)
        print(error_message)


def bulk_insert_synxis_cloud_monthly_summary(mon_list):
    # Add new data of reservation
    try:
        print("Data importing...")
        conn = db_config.get_db_connection()
        conn.execute(db_models.synxis_cloud_monthly_summary_model.insert(), mon_list)
        conn.commit()
        conn.close()
        print("Data imported")
    except Exception as e:
        error_message = str(e)
        print(error_message)


def SynxisCloud_Pms(row):
    pullDateId = row['pullDateId']
    propertyCode = row['propertyCode']
    attachment_format = "../reports"
    reservation_file_path = f'{attachment_format}/{propertyCode}_Reservation_Onboarding.csv' 
    forecast_file_path = f'{attachment_format}/{propertyCode}_Forecast_Onboarding.csv'
    revenue_file_path = f'{attachment_format}/{propertyCode}_Revenue_Onboarding.csv'
    monthly_file_path = f'{attachment_format}/{propertyCode}_Monthly_Onboarding.csv'

    check_reservation_file = os.path.isfile(reservation_file_path)
    check_forecast_file = os.path.isfile(forecast_file_path)
    check_revenue_file = os.path.isfile(revenue_file_path)
    check_monthly_file = os.path.isfile(monthly_file_path)

    createdAt = "'" + str(arrow.now()) + "'"
    updatedAt = "'" + str(arrow.now()) + "'"
    createdAtEpoch = int(arrow.utcnow().timestamp())
    updatedAtEpoch = int(arrow.utcnow().timestamp())

    errorMessage = ""
    fileCount=0

    if check_reservation_file:

        fileCount=fileCount+1
         # Reservation Data Clean and Insert
        read = pd.read_csv(reservation_file_path, skipfooter=3, engine='python')
        read['Status_Dt'] = pd.to_datetime(read['Status_Dt'], format='mixed')
        read['Arrival_Dt'] = pd.to_datetime(read['Arrival_Dt'], format='mixed')
        read['Depart_Dt'] = pd.to_datetime(read['Depart_Dt'], format='mixed')
        read['VCC_Card_Activation_Start'] = pd.to_datetime(read['VCC_Card_Activation_Start'], format='mixed')
        read['VCC_Card_Activation_End'] = pd.to_datetime(read['VCC_Card_Activation_End'], format='mixed')
        read.insert(0, column="propertyCode", value=propertyCode)
        read.insert(1, column="pullDateId", value=pullDateId)
        read.insert(2, column="createdAt", value=createdAt)
        read.insert(3, column="updatedAt", value=updatedAt)
        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
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
        read['Nights_Qty'] = read['Nights_Qty'].fillna(0).astype(int)
        read['Room_Qty'] = read['Room_Qty'].fillna(0).astype(int)
        read['Total_Adult_Occupancy'] = read['Total_Adult_Occupancy'].fillna(0).astype(int)
        read['Total_Child_Occupancy_For_Age_Group1'] = read['Total_Child_Occupancy_For_Age_Group1'].fillna(0).astype(int)
        read['Total_Child_Occupancy_For_Age_Group2'] = read['Total_Child_Occupancy_For_Age_Group2'].fillna(0).astype(int)
        read['Total_Child_Occupancy_For_Age_Group3'] = read['Total_Child_Occupancy_For_Age_Group3'].fillna(0).astype(int)
        read['Total_Child_Occupancy_For_Age_Group4'] = read['Total_Child_Occupancy_For_Age_Group4'].fillna(0).astype(int)
        read['Total_Child_Occupancy_For_Age_Group5'] = read['Total_Child_Occupancy_For_Age_Group5'].fillna(0).astype(int)
        read['Total_Child_Occupancy_For_Unknown_Age_Group'] = read['Total_Child_Occupancy_For_Unknown_Age_Group'].fillna(0).astype(int)
        #This eleminated the very last row with all empty columns where MIGHT be Confirm No. was empty
        read = read[read['Confirm_No'].str.len() >= 1]
        read.insert(6, column="uniqueKey", value=read['Confirm_No'].astype(str))
        read.to_csv(f"{attachment_format}/{propertyCode}_Reservation.csv", index=False)
    
        res_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Reservation.csv", encoding="utf-8"))
        res_result = list(res_result)
        if len(res_result) > 0:
            bulk_insert_synxis_cloud_res(res_result)
            print("RES DONE")
        else:
            errorMessage = errorMessage + "Reservation File Was Blank, "
    else:
        errorMessage = errorMessage + "Reservation File Not Found, "

    if check_forecast_file:

        fileCount=fileCount+1
        # Forecast Data Clean and Insert
        read = pd.read_csv(forecast_file_path, skipfooter=3, engine='python')
        read['cal_dt'] = pd.to_datetime(read['cal_dt'])
        read.insert(0, column="propertyCode", value=propertyCode)
        read.insert(1, column="pullDateId", value=pullDateId)
        read.insert(2, column="createdAt", value=createdAt)
        read.insert(3, column="updatedAt", value=updatedAt)
        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
        read.insert(6, column="uniqueKey", value=read["propertyCode"].astype(str) + "_" + read['cal_dt'].astype(str))
        read.to_csv(f"{attachment_format}/{propertyCode}_Forecast.csv", index=False)

        fore_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Forecast.csv", encoding="utf-8"))
        fore_result = list(fore_result)
        if len(fore_result) > 0:
            bulk_insert_synxis_cloud_forecast(fore_result)
            print("FORE DONE")
        else:
            errorMessage = errorMessage + "Forecast File Was Blank, "
    else:
        errorMessage = errorMessage + "Forecast File Not Found, "

    if check_revenue_file:

        fileCount=fileCount+1
        # Revenue Recap Data Clean and Insert
        date_df = pd.read_csv(revenue_file_path, skiprows=1, engine='python')
        date = date_df.columns.str.extract(r'(\d{2}\s\w{3}\s\d{4})').values[0][0]
        read = pd.read_csv(revenue_file_path, skiprows=3, skipfooter=3, engine='python')
        read.insert(0, column="propertyCode", value=propertyCode)
        read.insert(1, column="pullDateId", value=pullDateId)
        read.insert(2, column="createdAt", value=createdAt)
        read.insert(3, column="updatedAt", value=updatedAt)
        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
        read.insert(6, column="Date", value=date)
        read.loc[:, 'Date'] = pd.to_datetime(read['Date'], format='%d %b %Y', errors='coerce').dt.date
        read.insert(7, column="uniqueKey", value=read["propertyCode"].astype(str) + "_" + read['Date'].astype(str) + "_" + read['report_type_values'].astype(str))            
        read['F_B'] = read['F_B'].fillna(0).astype(int)
        read['Other'] = read['Other'].fillna(0).astype(int)
        read['Month_To_Date_F_B'] = read['Month_To_Date_F_B'].fillna(0).astype(int)
        read['Year_To_Date_F_B'] = read['Year_To_Date_F_B'].apply(str).str.replace(',', '').astype(float)
        read['F_B_Total'] = read['F_B_Total'].fillna(0).astype(int)
        read['Other_Total'] = read['Other_Total'].fillna(0).astype(int)
        read['Month_To_Date_F_B_total'] = read['Month_To_Date_F_B_total'].fillna(0).astype(float)
        read['Year_To_Date_F_B_Total'] = read['Year_To_Date_F_B_Total'].apply(str).str.replace(',', '').astype(float)
        read.to_csv(f"{attachment_format}/{propertyCode}_Revenue.csv", index=False)
        rev_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Revenue.csv", encoding="utf-8"))
        rev_result = list(rev_result)
        if len(rev_result) > 0:
            bulk_insert_synxis_cloud_revenue_recap(rev_result)
            print("REV DONE")
        else:
            errorMessage = errorMessage + "Revenue File Was Blank, "
    else:
        errorMessage = errorMessage + "Revenue File Not Found, "

    if check_monthly_file:

        fileCount=fileCount+1
        # Monthly Summary Data Clean and Insert
        read = pd.read_csv(monthly_file_path, skipfooter=3, engine='python')
        read['BUSINESS_DT'] = pd.to_datetime(read['BUSINESS_DT'], format="%b %d, %Y(%a)")
        read.insert(0, column="propertyCode", value=propertyCode)
        read.insert(1, column="pullDateId", value=pullDateId)
        read.insert(2, column="createdAt", value=createdAt)
        read.insert(3, column="updatedAt", value=updatedAt)
        read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
        read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
        read.insert(6, column="uniqueKey", value=read["propertyCode"].astype(str) + "_" + read['BUSINESS_DT'].astype(str))
        read['TOTAL_FOOD_AND_BEV_REV'] = read['TOTAL_FOOD_AND_BEV_REV'].fillna(0).astype(int)
        read['FoodandBeverage'] = read['FoodandBeverage'].apply(str).str.replace(',', '').astype(float)
        read.to_csv(f"{attachment_format}/{propertyCode}_Monthly.csv", index=False)

        monthly_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Monthly.csv", encoding="utf-8"))
        monthly_result = list(monthly_result)
        
        if len(monthly_result) > 0:
            bulk_insert_synxis_cloud_monthly_summary(monthly_result)
            print("MONTHLY DONE")
        else:
            errorMessage = errorMessage + "Monthly File Was Blank, "
    else:
        errorMessage = errorMessage + "Monthly File Not Found, "

    if (fileCount==4):
        if(errorMessage==""):
            update_into_pulldate(pullDateId, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
        else:
            errorMessage="Partially Successfull:- "+errorMessage
            update_into_pulldate(pullDateId, ERROR_NOTE=errorMessage, IS_ERROR=True)
    else:
        if (fileCount==0):
            errorMessage = "All File Not Found"
        else:
            errorMessage="Partially Successfull:- "+errorMessage
        update_into_pulldate(pullDateId, ERROR_NOTE=errorMessage, IS_ERROR=True)


if __name__ == '__main__':

    # Get all property using brand
    PMS_NAME = 'SYNXIS CLOUD'
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
                SynxisCloud_Pms(row)
            else:
                print("LAST_PULL_DATE_ID is NULL")
    else:
        print(f"Property not available in database!!!")
    print(f"[{PMS_NAME}] SCRIPT STOP!!!")
