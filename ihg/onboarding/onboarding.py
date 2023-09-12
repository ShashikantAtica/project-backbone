import os
import sys

sys.path.append("..")
import arrow
import csv
import pandas as pd

from utils.db import db_config
from utils.db import db_models


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


def bulk_insert_ihg_res(res_list, propertyCode):
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
            f'DELETE from ihg_res where {pullDateId} = {pullDateIdValue};')
        conn.close()
        print("DELETE OLD DATA!!!", pullDateIdValue)
    else:
        print("Not previous data!!!")

    # Add new data of reservation
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.ihg_res_model.insert(), res_list)
    conn.close()
    print("Data imported")


def bulk_insert_occ_res(res_list, propertyCode):
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
            f'DELETE from ihg_occ where {pullDateId} = {pullDateIdValue};')
        conn.close()
        print("DELETE OLD DATA!!!", pullDateIdValue)
    else:
        print("Not previous data!!!")

    # Add new data of reservation
    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.ihg_occ_model.insert(), res_list)
    conn.close()
    print("Data imported")


def IHG_Pms(row):
    pullDateId = row['pullDateId']
    propertyCode = row['propertyCode']

    # Modification of res report
    reservation_file_path = f'{propertyCode}_Reservation.xlsx'
    occupancy_file_path = f'{propertyCode}_Occupancy.xlsx'

    check_reservation_file = os.path.isfile(reservation_file_path)
    check_occupancy_file = os.path.isfile(occupancy_file_path)

    if check_reservation_file and check_occupancy_file:
        # Reservation Data Clean and Insert
        read = pd.read_excel(reservation_file_path)
        read['Arrival Date'] = pd.to_datetime(read['Arrival Date'])
        read.columns = read.columns.str.replace(' ', '', regex=True)
        read.insert(0, column="propertyCode", value=propertyCode)
        read.insert(1, column="pullDateId", value=pullDateId)
        read.to_csv(f"{propertyCode}_Reservations.csv", index=False)

        res_result = csv.DictReader(open(f"{propertyCode}_Reservations.csv", encoding="utf-8"))
        res_result = list(res_result)

        # Occupancy Data Clean and Insert
        read = pd.read_excel(occupancy_file_path)
        read['Date'] = pd.to_datetime(read['Date'])

        read.insert(0, column="propertyCode", value=propertyCode)
        read.insert(1, column="pullDateId", value=pullDateId)

        headers_list = ["propertyCode", "pullDateId", "BlackoutDates", "blank", "ClosedtoArrival", "Date", "DayofWeek",
                        "MaximumLOS", "MinimumLOS", "ReservationGuaranteeRequired", "24Hourhold", "AverageLeadTime",
                        "AverageLOS", "CancelDue", "CancelorNoShow", "DepositDue", "Deposit", "Groupremaining",
                        "RoomslefttoSell", "SpecialEventSpecialRequirement", "Paceasofdate", "AC", "ActualroomssoldLY",
                        "ADR", "BFR", "Groupcommitted", "Groupcontracted", "GroupPickupasofdate", "Grouppickup", "Occ",
                        "OVB", "Paceasofdate1", "Paceasofdate2", "Pickupasofdate", "Pickupasofdate1", "Roomssold",
                        "TotalRoomsCommitted"]
        read.to_csv(f"{propertyCode}_Occupancy.csv", index=False, header=headers_list)

        occ_result = csv.DictReader(open(f"{propertyCode}_Occupancy.csv", encoding="utf-8"))
        occ_result = list(occ_result)

        if len(res_result) > 0 and len(occ_result) > 0:
            bulk_insert_ihg_res(res_result, propertyCode=propertyCode)
            print("RES DONE")

            bulk_insert_occ_res(occ_result, propertyCode=propertyCode)
            print("OCC DONE")

            update_into_pulldate(pullDateId, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
        else:
            print("File was blank!!!")
            update_into_pulldate(pullDateId, ERROR_NOTE="File was blank!!!", IS_ERROR=True)
    else:
        msg = "File Not found!!!"
        update_into_pulldate(pullDateId, ERROR_NOTE=msg, IS_ERROR=True)


if __name__ == '__main__':

    # Get all property using brand
    PMS_NAME = "'IHG'"
    print("SCRIPT STARTED FOR IHG")
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
            IHG_Pms(row)
            print("SCRIPT DONE FOR IHG")
        else:
            print("LAST_PULL_DATE_ID is NULL")
