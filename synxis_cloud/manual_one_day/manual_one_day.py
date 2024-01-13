import argparse
import os
import sys

from sqlalchemy import text

sys.path.append("../../")
import arrow
import csv
import pandas as pd

from utils.db import db_config
from utils.db import db_models
from sqlalchemy.dialects.postgresql import insert


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
    print("Data importing...")
    error_temp = ""
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.synxis_cloud_reservation_model).values(res_list)
        conn.commit()
        stmt = stmt.on_conflict_do_update(
            index_elements=['uniqueKey'],
            set_={
                'pullDateId': stmt.excluded.pullDateId,
                'updatedAt': stmt.excluded.updatedAt,
                'updatedAtEpoch': stmt.excluded.updatedAtEpoch,
                'Chain': stmt.excluded.Chain,
                'Brand': stmt.excluded.Brand,
                'Hotel': stmt.excluded.Hotel,
                'Confirm_No': stmt.excluded.Confirm_No,
                'Rez_Status_Desc': stmt.excluded.Rez_Status_Desc,
                'Status_Dt': stmt.excluded.Status_Dt,
                'Guest_Nm': stmt.excluded.Guest_Nm,
                'Channel_Cd': stmt.excluded.Channel_Cd,
                'Sec_Channel_Desc': stmt.excluded.Sec_Channel_Desc,
                'Sub_Source': stmt.excluded.Sub_Source,
                'Sub_Src_CD': stmt.excluded.Sub_Src_CD,
                'Location': stmt.excluded.Location,
                'Arrival_Dt': stmt.excluded.Arrival_Dt,
                'Depart_Dt': stmt.excluded.Depart_Dt,
                'Nights_Qty': stmt.excluded.Nights_Qty,
                'Override_Oversell': stmt.excluded.Override_Oversell,
                'Room_Qty': stmt.excluded.Room_Qty,
                'Rate_Category_Name': stmt.excluded.Rate_Category_Name,
                'Rate_Type_Code': stmt.excluded.Rate_Type_Code,
                'Rez_Avg_Rate_Amt': stmt.excluded.Rez_Avg_Rate_Amt,
                'Onshore_Rate_Type_Code': stmt.excluded.Onshore_Rate_Type_Code,
                'Onshore_Rez_Avg_Rate_Amt': stmt.excluded.Onshore_Rez_Avg_Rate_Amt,
                'Room_Type_Code': stmt.excluded.Room_Type_Code,
                'Record_Locator': stmt.excluded.Record_Locator,
                'Booker_Loyalty_Program_Level_List': stmt.excluded.Booker_Loyalty_Program_Level_List,
                'Loyalty_Type': stmt.excluded.Loyalty_Type,
                'Loyalty_Program': stmt.excluded.Loyalty_Program,
                'Loyalty_Number': stmt.excluded.Loyalty_Number,
                'Loyalty_Level_Code': stmt.excluded.Loyalty_Level_Code,
                'Loyalty_Level_Name': stmt.excluded.Loyalty_Level_Name,
                'Loyalty_Points_Payment': stmt.excluded.Loyalty_Points_Payment,
                'Payment_Typ': stmt.excluded.Payment_Typ,
                'VCC_Authorization_Amount': stmt.excluded.VCC_Authorization_Amount,
                'VCC_Currency_Code': stmt.excluded.VCC_Currency_Code,
                'VCC_Payment_Model': stmt.excluded.VCC_Payment_Model,
                'VCC_Card_Activation_Start': stmt.excluded.VCC_Card_Activation_Start,
                'VCC_Card_Activation_End': stmt.excluded.VCC_Card_Activation_End,
                'Direct_Bill_Account_Number': stmt.excluded.Direct_Bill_Account_Number,
                'Direct_Bill_Project_Number': stmt.excluded.Direct_Bill_Project_Number,
                'Total_Points_Pymnt': stmt.excluded.Total_Points_Pymnt,
                'Points_Refund': stmt.excluded.Points_Refund,
                'Total_Cash_Pymnt': stmt.excluded.Total_Cash_Pymnt,
                'Pay_At_Property': stmt.excluded.Pay_At_Property,
                'Cash_Refund': stmt.excluded.Cash_Refund,
                'Total_Price_For_Adult': stmt.excluded.Total_Price_For_Adult,
                'Total_Adult_Occupancy': stmt.excluded.Total_Adult_Occupancy,
                'Total_Price_For_Child_Age_Group1': stmt.excluded.Total_Price_For_Child_Age_Group1,
                'Total_Child_Occupancy_For_Age_Group1': stmt.excluded.Total_Child_Occupancy_For_Age_Group1,
                'Total_Price_For_Child_Age_Group2': stmt.excluded.Total_Price_For_Child_Age_Group2,
                'Total_Child_Occupancy_For_Age_Group2': stmt.excluded.Total_Child_Occupancy_For_Age_Group2,
                'Total_Price_For_Child_Age_Group3': stmt.excluded.Total_Price_For_Child_Age_Group3,
                'Total_Child_Occupancy_For_Age_Group3': stmt.excluded.Total_Child_Occupancy_For_Age_Group3,
                'Total_Price_For_Child_Age_Group4': stmt.excluded.Total_Price_For_Child_Age_Group4,
                'Total_Child_Occupancy_For_Age_Group4': stmt.excluded.Total_Child_Occupancy_For_Age_Group4,
                'Total_Price_For_Child_Age_Group5': stmt.excluded.Total_Price_For_Child_Age_Group5,
                'Total_Child_Occupancy_For_Age_Group5': stmt.excluded.Total_Child_Occupancy_For_Age_Group5,
                'Total_Price_For_Child_Unknown_Age_Group': stmt.excluded.Total_Price_For_Child_Unknown_Age_Group,
                'Total_Child_Occupancy_For_Unknown_Age_Group': stmt.excluded.Total_Child_Occupancy_For_Unknown_Age_Group,
                'Share_With': stmt.excluded.Share_With,
                'Channel_Connect_Confirm_NO': stmt.excluded.Channel_Connect_Confirm_NO,
                'PMS_Confirm_Code': stmt.excluded.PMS_Confirm_Code,
                'Original_Room_Type_Code': stmt.excluded.Original_Room_Type_Code,
                'Original_Rm_Typ_Nm': stmt.excluded.Original_Rm_Typ_Nm,
                'Template_Code': stmt.excluded.Template_Code,
                'Template_Name': stmt.excluded.Template_Name,
                'Blacklist_Reason': stmt.excluded.Blacklist_Reason,
                'Visa_Information': stmt.excluded.Visa_Information,
                'Installment_Amount': stmt.excluded.Installment_Amount,
                'Number_of_Installments': stmt.excluded.Number_of_Installments,
                'Interest_Amount': stmt.excluded.Interest_Amount,
                'Interest_Rate_Percentage': stmt.excluded.Interest_Rate_Percentage,
                'Total_Installment_Amount': stmt.excluded.Total_Installment_Amount,
                'Rate_Credential_ID': stmt.excluded.Rate_Credential_ID,
                'Trace_Date': stmt.excluded.Trace_Date,
                'Trace_Completion_Flag': stmt.excluded.Trace_Completion_Flag,
                'Trace_Completion_Date': stmt.excluded.Trace_Completion_Date,
                'Trace_Text': stmt.excluded.Trace_Text,
                'RM_Upgrade_Reason_CD': stmt.excluded.RM_Upgrade_Reason_CD,
                'Room_Upsell': stmt.excluded.Room_Upsell,
                'Room_To_Charge': stmt.excluded.Room_To_Charge,
                'Onhold_Duration': stmt.excluded.Onhold_Duration,
                'AutoCancel_ReleaseDateTime_ScheduledOrActual': stmt.excluded.AutoCancel_ReleaseDateTime_ScheduledOrActual,
                'Commission_Cd': stmt.excluded.Commission_Cd,
                'Comm_Percent_Override_List': stmt.excluded.Comm_Percent_Override_List,
                'Coupon_Offer': stmt.excluded.Coupon_Offer,
                'Coupon_Discount_Total': stmt.excluded.Coupon_Discount_Total,
                'Promotion': stmt.excluded.Promotion,
                'Promo_Discount': stmt.excluded.Promo_Discount,
                'Maximum_Discount_Applied': stmt.excluded.Maximum_Discount_Applied,
                'Paynow_Discount': stmt.excluded.Paynow_Discount,
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
        error_temp=error_message[:250]
    return error_temp


def bulk_insert_synxis_cloud_forecast(fore_list):
    print("Data importing...")
    error_temp = ""
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.synxis_cloud_forecast_model).values(fore_list)
        conn.commit()
        stmt = stmt.on_conflict_do_update(
            index_elements=['uniqueKey'],
            set_={
                'pullDateId': stmt.excluded.pullDateId,
                'updatedAt': stmt.excluded.updatedAt,
                'updatedAtEpoch': stmt.excluded.updatedAtEpoch,
                'cal_dt': stmt.excluded.cal_dt,
                'Total_Rooms': stmt.excluded.Total_Rooms,
                'OOO_OOI': stmt.excluded.OOO_OOI,
                'rentable_rooms': stmt.excluded.rentable_rooms,
                'Rooms_Sold': stmt.excluded.Rooms_Sold,
                'total_stayovers': stmt.excluded.total_stayovers,
                'total_due_out': stmt.excluded.total_due_out,
                'Total_Reservations': stmt.excluded.Total_Reservations,
                'day_use_in_house': stmt.excluded.day_use_in_house,
                'gtd': stmt.excluded.gtd,
                'non_gtd': stmt.excluded.non_gtd,
                'Not_Picked_Up': stmt.excluded.Not_Picked_Up,
                'Left_To_Sell': stmt.excluded.Left_To_Sell,
                'occ_per': stmt.excluded.occ_per,
                'Total_Room_Revenue': stmt.excluded.Total_Room_Revenue,
                'ADR': stmt.excluded.ADR,
                'Adult_Child': stmt.excluded.Adult_Child,
                'Sum_Total_Rooms': stmt.excluded.Sum_Total_Rooms,
                'SumOOO_OOI': stmt.excluded.SumOOO_OOI,
                'Sum_Rentable_Rooms': stmt.excluded.Sum_Rentable_Rooms,
                'Sum_Rooms_Sold': stmt.excluded.Sum_Rooms_Sold,
                'Sum_Stayovers': stmt.excluded.Sum_Stayovers,
                'Sum_Checkouts': stmt.excluded.Sum_Checkouts,
                'Sum_Arrivals': stmt.excluded.Sum_Arrivals,
                'Sum_Day_Use': stmt.excluded.Sum_Day_Use,
                'SumGtd': stmt.excluded.SumGtd,
                'SumNonGtd': stmt.excluded.SumNonGtd,
                'Sum_GroupsRoomsNotPickedUp': stmt.excluded.Sum_GroupsRoomsNotPickedUp,
                'Sum_Rooms_Left_To_Sell': stmt.excluded.Sum_Rooms_Left_To_Sell,
                'Sum_RoomsOccupied_Percentage': stmt.excluded.Sum_RoomsOccupied_Percentage,
                'SumTotalRoomRevenue': stmt.excluded.SumTotalRoomRevenue,
                'Sum_ADR': stmt.excluded.Sum_ADR,
                'SUM_ADULT_CHILD': stmt.excluded.SUM_ADULT_CHILD,
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
        error_temp=error_message[:250]
    return error_temp



def bulk_insert_synxis_cloud_revenue_recap(rev_list):
    print("Data importing...")
    error_temp = ""
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.synxis_cloud_revenue_recap_model).values(rev_list)
        conn.commit()
        stmt = stmt.on_conflict_do_update(
            index_elements=['uniqueKey'],
            set_={
                'pullDateId': stmt.excluded.pullDateId,
                'updatedAt': stmt.excluded.updatedAt,
                'updatedAtEpoch': stmt.excluded.updatedAtEpoch,
                'Date': stmt.excluded.Date,
                'report_type': stmt.excluded.report_type,
                'report_type_values': stmt.excluded.report_type_values,
                'Room_Nights': stmt.excluded.Room_Nights,
                'Room': stmt.excluded.Room,
                'F_B': stmt.excluded.F_B,
                'Taxes': stmt.excluded.Taxes,
                'Other': stmt.excluded.Other,
                'Month_To_Date_Room_Nights': stmt.excluded.Month_To_Date_Room_Nights,
                'Month_To_Date_Room': stmt.excluded.Month_To_Date_Room,
                'Month_To_Date_F_B': stmt.excluded.Month_To_Date_F_B,
                'Month_To_Date_Taxes': stmt.excluded.Month_To_Date_Taxes,
                'Month_To_Date_Others': stmt.excluded.Month_To_Date_Others,
                'Year_To_Date_Room_Nights': stmt.excluded.Year_To_Date_Room_Nights,
                'Year_To_Date__Room': stmt.excluded.Year_To_Date__Room,
                'Year_To_Date_F_B': stmt.excluded.Year_To_Date_F_B,
                'Year_To_Date_Taxes': stmt.excluded.Year_To_Date_Taxes,
                'Year_To_Date_Other': stmt.excluded.Year_To_Date_Other,
                'Room_Nights_Total': stmt.excluded.Room_Nights_Total,
                'Room_Total': stmt.excluded.Room_Total,
                'F_B_Total': stmt.excluded.F_B_Total,
                'Taxes_Total': stmt.excluded.Taxes_Total,
                'Other_Total': stmt.excluded.Other_Total,
                'Month_To_Date_Room_Nights_Total': stmt.excluded.Month_To_Date_Room_Nights_Total,
                'Month_To_Date_Room_Total': stmt.excluded.Month_To_Date_Room_Total,
                'Month_To_Date_F_B_total': stmt.excluded.Month_To_Date_F_B_total,
                'Month_To_Date_Taxes_Total': stmt.excluded.Month_To_Date_Taxes_Total,
                'Month_To_Date_Total': stmt.excluded.Month_To_Date_Total,
                'Year_To_Date_Room_Nights_Total': stmt.excluded.Year_To_Date_Room_Nights_Total,
                'Year_To_Date_Room_Total': stmt.excluded.Year_To_Date_Room_Total,
                'Year_To_Date_F_B_Total': stmt.excluded.Year_To_Date_F_B_Total,
                'Year_To_Date_Taxes_Total': stmt.excluded.Year_To_Date_Taxes_Total,
                'Year_To_Date_Total': stmt.excluded.Year_To_Date_Total,
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
        error_temp=error_message[:250]
    return error_temp


def bulk_insert_synxis_cloud_monthly_summary(mon_list):
    print("Data importing...")
    error_temp = ""
    try:
        conn = db_config.get_db_connection()
        stmt = insert(db_models.synxis_cloud_monthly_summary_model).values(mon_list)
        conn.commit()
        stmt = stmt.on_conflict_do_update(
            index_elements=['uniqueKey'],
            set_={
                'pullDateId': stmt.excluded.pullDateId,
                'updatedAt': stmt.excluded.updatedAt,
                'updatedAtEpoch': stmt.excluded.updatedAtEpoch,
                'BUSINESS_DT': stmt.excluded.BUSINESS_DT,
                'OCCUPIED_ROOM_QTY': stmt.excluded.OCCUPIED_ROOM_QTY,
                'RENTABLE_ROOM_QTY': stmt.excluded.RENTABLE_ROOM_QTY,
                'OCCUPANCY_PERCENT': stmt.excluded.OCCUPANCY_PERCENT,
                'ADR_WITH_COMPS': stmt.excluded.ADR_WITH_COMPS,
                'TOTAL_FOOD_AND_BEV_REV': stmt.excluded.TOTAL_FOOD_AND_BEV_REV,
                'TOTAL_ROOM_REV1': stmt.excluded.TOTAL_ROOM_REV1,
                'TOTAL_TAX_REV': stmt.excluded.TOTAL_TAX_REV,
                'TOTAL_OTH_REV': stmt.excluded.TOTAL_OTH_REV,
                'TOTAL_REV': stmt.excluded.TOTAL_REV,
                'Date': stmt.excluded.Date,
                'OCCUPIED_ROOM_QTY_Total': stmt.excluded.OCCUPIED_ROOM_QTY_Total,
                'RENTABLE_ROOM_QTY_Total': stmt.excluded.RENTABLE_ROOM_QTY_Total,
                'Occupancy_Percent_Total': stmt.excluded.Occupancy_Percent_Total,
                'ADR_Total': stmt.excluded.ADR_Total,
                'FoodandBeverage': stmt.excluded.FoodandBeverage,
                'RoomCharges': stmt.excluded.RoomCharges,
                'Taxes': stmt.excluded.Taxes,
                'Others': stmt.excluded.Others,
                'Total': stmt.excluded.Total,
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
        error_temp=error_message[:250]
    return error_temp



def SynxisCloud_Pms(row, reporttype, localfilepath):
    pullDateId = row['pullDateId']
    propertyCode = row['propertyCode']
    attachment_format = "../reports"

    try:
        reservation_file_path = f'{attachment_format}/{propertyCode}_Reservation_Onboarding.csv' 
        forecast_file_path = f'{attachment_format}/{propertyCode}_Forecast_Onboarding.csv'
        revenue_file_path = f'{attachment_format}/{propertyCode}_Revenue_Onboarding.csv'
        monthly_file_path = f'{attachment_format}/{propertyCode}_Monthly_Onboarding.csv'

        report_file_path = ""

        if(reporttype == 'Reservation'):
            reservation_file_path = localfilepath
        elif(reporttype == 'Forecast'):
            forecast_file_path = localfilepath
        elif(reporttype == 'Revenue'):
            revenue_file_path = localfilepath
        elif(reporttype == 'Monthly'):
            monthly_file_path = localfilepath


        check_reservation_file = os.path.isfile(reservation_file_path)
        check_forecast_file = os.path.isfile(forecast_file_path)
        check_revenue_file = os.path.isfile(revenue_file_path)
        check_monthly_file = os.path.isfile(monthly_file_path)

        createdAt = "'" + str(arrow.now()) + "'"
        updatedAt = "'" + str(arrow.now()) + "'"
        createdAtEpoch = int(arrow.utcnow().timestamp())
        updatedAtEpoch = int(arrow.utcnow().timestamp())

        errorMessage = ""

        if check_reservation_file:

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
                error_temp = bulk_insert_synxis_cloud_res(res_result)
                if(error_temp == ""):
                    print("RES DONE")   
                else:
                    print("RES FAILED")
                    errorMessage = errorMessage + " RES Failed: " + error_temp
            else:
                errorMessage = errorMessage + "Reservation File Was Blank, "

        if check_forecast_file:

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
                error_temp = bulk_insert_synxis_cloud_forecast(fore_result)
                if(error_temp == ""):
                    print("FORE DONE")   
                else:
                    print("FORE FAILED")
                    errorMessage = errorMessage + " FORE Failed: " + error_temp
            else:
                errorMessage = errorMessage + "Forecast File Was Blank, "

        if check_revenue_file:

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
                error_temp = bulk_insert_synxis_cloud_revenue_recap(rev_result)
                if(error_temp == ""):
                    print("REV DONE")   
                else:
                    print("REV FAILED")
                    errorMessage = errorMessage + " REV Failed: " + error_temp
            else:
                errorMessage = errorMessage + "Revenue File Was Blank, "

        if check_monthly_file:

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
                error_temp = bulk_insert_synxis_cloud_monthly_summary(monthly_result)
                if(error_temp == ""):
                    print("MONTHLY DONE")   
                else:
                    print("MONTHLY FAILED")
                    errorMessage = errorMessage + " MONTHLY Failed: " + error_temp
            else:
                errorMessage = errorMessage + "Monthly File Was Blank, "

        
        if(errorMessage==""):
            update_into_pulldate(pullDateId, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
        else:
            errorMessage="Partially Successfull:- "+errorMessage
            update_into_pulldate(pullDateId, ERROR_NOTE=errorMessage, IS_ERROR=True)
    except Exception as e:
        msg = f"[{propertyCode}] failed due to {e}"
        print(msg)
        update_into_pulldate(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)
        return 0


if __name__ == '__main__':

    # Get all property using brand
    PMS_NAME = 'SYNXIS CLOUD'
    print(f"[{PMS_NAME}] SCRIPT IS STARTING...")

    propertycode = None
    reporttype = None
    localfilepath = None

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--propertycode", type=str, required=False, help="Type in the propertycode")
        parser.add_argument("--reporttype", type=str, required=False, help="Type in the reporttype")
        parser.add_argument("--localfilepath", type=str, required=False, help="Type in the localfilepath")
        args = parser.parse_args()
        propertycode = args.propertycode
        reporttype = args.reporttype
        localfilepath = args.localfilepath
        print(f"propertycode :: {propertycode}")
        print(f"reporttype :: {reporttype}")
        print(f"localfilepath :: {localfilepath}")
    except:
        print("Something wents wrong with Inputs!")
        pass

    result = None
    if propertycode is None or localfilepath is None  or reporttype is None:
       print("Please enter all inputs, Recheck!")
       results_as_dict = None
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
            LAST_PULL_DATE_ID = insert_into_pulldate(PROPERTY_CODE, PULLED_DATE, PMS_NAME+"_manual_one_day")

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
                SynxisCloud_Pms(row, reporttype, localfilepath)
            else:
                print("LAST_PULL_DATE_ID is NULL")
    else:
        print(f"Property not available in database!!!")
    print(f"[{PMS_NAME}] SCRIPT STOP!!!")
