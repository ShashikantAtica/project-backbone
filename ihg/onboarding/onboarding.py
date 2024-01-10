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


def bulk_insert_ihg_res(res_list):

    # Add new data of reservation
    print("Data importing...")
    try:
        conn = db_config.get_db_connection()
        conn.execute(db_models.ihg_res_model.insert(), res_list)
        conn.commit()
        conn.close()
        print("Data imported")
    except Exception as e:
        error_message = str(e)
        print(error_message)


def bulk_insert_occ_res(occ_list):

    # Add new data of reservation
    print("Data importing...")
    try:
        conn = db_config.get_db_connection()
        conn.execute(db_models.ihg_occ_model.insert(), occ_list)
        conn.commit()
        conn.close()
        print("Data imported")
    except Exception as e:
        error_message = str(e)
        print(error_message)


def IHG_Pms(row, reporttype, localfilepath):
    pullDateId = row['pullDateId']
    propertyCode = row['propertyCode']
    attachment_format = "../reports"
    
    try:
    # Modification of res report
        reservation_file_path = f'{attachment_format}/{propertyCode}_Reservation_Onboarding.xlsx'
        occupancy_file_path = f'{attachment_format}/{propertyCode}_Occupancy_Onboarding.xlsx'

        if(reporttype == 'Reservation'):
            reservation_file_path = localfilepath
        elif(reporttype == 'Occupancy'):
            occupancy_file_path = localfilepath


        check_reservation_file = os.path.isfile(reservation_file_path)
        check_occupancy_file = os.path.isfile(occupancy_file_path)

        createdAt = "'" + str(arrow.now()) + "'"
        updatedAt = "'" + str(arrow.now()) + "'"
        createdAtEpoch = int(arrow.utcnow().timestamp())
        updatedAtEpoch = int(arrow.utcnow().timestamp())

        errorMessage = ""

        if check_reservation_file:

            # Reservation Data Clean and Insert
            read = pd.read_excel(reservation_file_path)
            read['Arrival Date'] = pd.to_datetime(read['Arrival Date'])
            read.columns = read.columns.str.replace(' ', '', regex=True)
            read.insert(0, column="propertyCode", value=propertyCode)
            read.insert(1, column="pullDateId", value=pullDateId)
            read.insert(2, column="createdAt", value=createdAt)
            read.insert(3, column="updatedAt", value=updatedAt)
            read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
            read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
            read.insert(6, column="uniqueKey", value=read["ConfirmationNumber"].astype(str))
            read.to_csv(f"{attachment_format}{propertyCode}_Reservations.csv", index=False)

            res_result = csv.DictReader(open(f"{attachment_format}{propertyCode}_Reservations.csv", encoding="utf-8"))
            res_result = list(res_result)
            if len(res_result) > 0:
                bulk_insert_ihg_res(res_result)
                print("RES DONE")
            else:
                errorMessage = errorMessage + "Reservation File Was Blank, "

        if check_occupancy_file:
            
            # Occupancy Data Clean and Insert
            read = pd.read_excel(occupancy_file_path)
            read['Date'] = pd.to_datetime(read['Date'])

            read.insert(0, column="propertyCode", value=propertyCode)
            read.insert(1, column="pullDateId", value=pullDateId)
            read.insert(2, column="createdAt", value=createdAt)
            read.insert(3, column="updatedAt", value=updatedAt)
            read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
            read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
            read.insert(6, column="uniqueKey", value=read["propertyCode"].astype(str) + "_" + read['Date'].astype(str)) 


            headers_list = ["propertyCode", "pullDateId", "createdAt", "updatedAt", "createdAtEpoch", "updatedAtEpoch", "uniqueKey", "BlackoutDates", "blank", "ClosedtoArrival", "Date", "DayofWeek",
                            "MaximumLOS", "MinimumLOS", "ReservationGuaranteeRequired", "24Hourhold", "AverageLeadTime",
                            "AverageLOS", "CancelDue", "CancelorNoShow", "DepositDue", "Deposit", "Groupremaining",
                            "RoomslefttoSell", "SpecialEventSpecialRequirement", "Paceasofdate", "AC", "ActualroomssoldLY",
                            "ADR", "BFR", "Groupcommitted", "Groupcontracted", "GroupPickupasofdate", "Grouppickup", "Occ",
                            "OVB", "Paceasofdate1", "Paceasofdate2", "Pickupasofdate", "Pickupasofdate1", "Roomssold",
                            "TotalRoomsCommitted"]
            read.to_csv(f"{attachment_format}{propertyCode}_Occupancy.csv", index=False, header=headers_list)

            occ_result = csv.DictReader(open(f"{attachment_format}{propertyCode}_Occupancy.csv", encoding="utf-8"))
            occ_result = list(occ_result)
            if len(occ_result) > 0:
                bulk_insert_occ_res(occ_result)
                print("OCC DONE")
            else:
                errorMessage = errorMessage + "Occupancy File Was Blank, "

                    
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
    PMS_NAME = "'IHG'"
    print(f"[{PMS_NAME}] SCRIPT IS STARTING...")

    propertycode = None
    reporttype = None
    filename = None
    localfilepath = None

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--propertycode", type=str, required=False, help="Type in the propertycode")
        parser.add_argument("--reporttype", type=str, required=False, help="Type in the reporttype")
        parser.add_argument("--filename", type=str, required=False, help="Type in the filename")
        parser.add_argument("--localfilepath", type=str, required=False, help="Type in the localfilepath")
        args = parser.parse_args()
        propertycode = args.propertycode
        reporttype = args.reporttype
        filename = args.filename
        localfilepath = args.localfilepath
        print(f"propertycode :: {propertycode}")
        print(f"reporttype :: {reporttype}")
        print(f"filename :: {filename}")
        print(f"localfilepath :: {localfilepath}")
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
                IHG_Pms(row, reporttype, localfilepath)
            else:
                print("LAST_PULL_DATE_ID is NULL")
    else:
        print(f"Property not available in database!!!")
    print(f"[{PMS_NAME}] SCRIPT STOP!!!")