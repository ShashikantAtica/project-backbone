import argparse
import csv
import os
import sys

sys.path.append("..")
import arrow
from function.forecast import handle_request as forecast_handle_request
from function.reservation import handle_request as reservation_handle_request
from function.realized_activity import handle_request as realized_activity_handle_request
from function.selenium_total_yield import get_total_yield_report_url as total_yield_handle_request

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from utils.db import db_config
from utils.db import db_models


def bulk_insert_marriott_res(propertyCode, res_list, res_before, res_after):
    start_date = "'" + res_before.format("YYYY-MM-DD") + "'"
    end_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    print("start_date :: ", start_date)
    print("end_date :: ", end_date)

    # delete all data
    # current_date = arrow.now()
    # print("current_date :: ", current_date)
    # start_date = current_date.shift(days=-90)
    # print("start_date :: ", start_date)
    db_propertyCode = "'" + propertyCode + "'"
    # current_date = "'" + res_after.format("YYYY-MM-DD") + "'"
    # start_date = "'" + res_before.format("YYYY-MM-DD") + "'"

    # Delete existing data of reservation (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(
        f'DELETE from marriott_reservation where "BookingDate" between {start_date} and {end_date} and "propertyCode" = {db_propertyCode};')
    conn.close()

    # Add new data of reservation (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(db_models.marriott_reservation_model.insert(), res_list)
    conn.close()


def bulk_insert_marriott_fore(propertyCode, fore_list, fore_before, fore_after):
    start_date = "'" + fore_before.format("YYYY-MM-DD") + "'"
    print("fore start_date :: ", start_date)

    end_date = "'" + fore_after.format("YYYY-MM-DD") + "'"
    print("fore end_date :: ", end_date)
    db_propertyCode = "'" + propertyCode + "'"

    # Delete existing data of occ (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(
        f'DELETE FROM marriott_forecast where "ArrivalDate" between {start_date} and {end_date} and "propertyCode" = {db_propertyCode};')
    conn.close()

    # Add new data of occ (up to 90 Days)
    conn = db_config.get_db_connection()
    conn.execute(db_models.marriott_forecast_model.insert(), fore_list)
    conn.close()


def bulk_insert_marriott_realized_activity(propertyCode, realized_activity_list, start_date):
    date_arrow = arrow.get(start_date)
    result_date = date_arrow.shift(days=90).format("YYYY-MM-DD")

    print("realized start : ", date_arrow.date())
    print("realized end : ", result_date)

    conn = db_config.get_db_connection()
    conn.execute(
        f"""DELETE FROM marriott_realized_activity where "ArrivalDate" between '{start_date.date()}' and '{result_date}' and "propertyCode" = '{propertyCode}';""")
    conn.close()

    print("Data importing...")
    conn = db_config.get_db_connection()
    conn.execute(db_models.marriott_realized_activity_model.insert(), realized_activity_list)
    conn.close()
    print("Data imported")


def bulk_insert_marriott_total_yield(propertyCode, total_yield_list, start_date, end_date):
    print("Yeild start : ", start_date.date())
    print("Yeild end : ", end_date.date())

    conn = db_config.get_db_connection()
    conn.execute(
        f"""DELETE FROM marriott_total_yield where "Date" between '{start_date.date()}' and '{end_date.date()}' and "propertyCode" = '{propertyCode}';""")
    conn.close()

    conn = db_config.get_db_connection()
    conn.execute(db_models.marriott_total_yield_model.insert(), total_yield_list)
    conn.close()


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


def main():
    PMS_NAME = "Marriott"
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
            LAST_PULL_DATE_ID = insert_into_pulldate(PROPERTY_CODE, PULLED_DATE,PMS_NAME)

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
                    'fore_before': CURRENT_DATE.shift(days=-OCC_BEFORE),
                    'fore_after': CURRENT_DATE.shift(days=+OCC_AFTER),
                    "propertyCode": PROPERTY_CODE,
                    "pullDateId": LAST_PULL_DATE_ID,
                    "forecast_platform": "rmsplatform"
                    # "otp_number": ""
                }
                print("row :: ", row)

                data_forecast = forecast_handle_request(row)
                if type(data_forecast) is Exception:
                    update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=str(data_forecast), IS_ERROR=True)
                    return 0

                data_reservation = reservation_handle_request(row)
                if type(data_reservation) is Exception:
                    update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=str(data_reservation), IS_ERROR=True)
                    return 0

                data_realized = realized_activity_handle_request(row)
                if type(data_realized) is Exception:
                    update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=str(data_realized), IS_ERROR=True)
                    return 0

                total_yield = total_yield_handle_request(row)
                if type(total_yield) is Exception:
                    update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=str(total_yield), IS_ERROR=True)
                    return 0

                print(f"SCRIPT DONE FOR {PMS_NAME}")

                print(f"Sending Report to Database for {PMS_NAME}")
                folder_name = "./reports/"
                propertyCode = row['external_property_code']

                reservation_file_path = f'{folder_name}{propertyCode}_Reservation.csv'
                forecast_file_path = f'{folder_name}{propertyCode}_Forecast.csv'
                realized_activity_file_path = f'{folder_name}{propertyCode}_Realized_Activity.csv'
                total_yield_file_path = f'{folder_name}{propertyCode}_Total_Yield.csv'

                check_reservation_file = os.path.isfile(reservation_file_path)
                check_forecast_file = os.path.isfile(forecast_file_path)
                check_realized_activity_file = os.path.isfile(realized_activity_file_path)
                check_total_yield_file = os.path.isfile(total_yield_file_path)

                error_msg = ""
                fileCount=0

                if not check_reservation_file:
                    error_msg = error_msg + " Reservation file - N/A"

                if not check_forecast_file:
                    error_msg = error_msg + " Forecast file - N/A"

                if not check_realized_activity_file:
                    error_msg = error_msg + " Realized Activity file - N/A"

                if not check_total_yield_file:
                    error_msg = error_msg + " Total Yield file - N/A"


                if check_reservation_file:

                    fileCount=fileCount+1
                    res_result = csv.DictReader(open(reservation_file_path, encoding="utf-8"))
                    res_result = list(res_result)
                    print(len(res_result))
                    if len(res_result) > 0:
                        bulk_insert_marriott_res(row['propertyCode'], res_result, row['res_before'], row['res_after'])
                        print("RES DONE")
                    else:
                        error_msg = error_msg + "Reservation File Was Blank, "

                if check_forecast_file:

                    fileCount=fileCount+1
                    fore_result = csv.DictReader(open(forecast_file_path, encoding="utf-8"))
                    fore_result = list(fore_result)
                    print(len(fore_result))
                    if len(fore_result) > 0:
                        bulk_insert_marriott_fore(row['propertyCode'], fore_result, row['fore_before'], row['fore_after'])
                        print("FORE DONE")
                    else:
                        error_msg = error_msg + "Forecast File Was Blank, "

                if check_realized_activity_file:

                    fileCount=fileCount+1
                    realized_activity_result = csv.DictReader(open(realized_activity_file_path, encoding="utf-8"))
                    realized_activity_result = list(realized_activity_result)
                    print(len(realized_activity_result))
                    if len(realized_activity_result) > 0:
                        bulk_insert_marriott_realized_activity(row['propertyCode'], realized_activity_result, row['fore_before'])
                        print("REALIZED ACTIVITY DONE")
                    else:
                        error_msg = error_msg + "Realized Activity File Was Blank, "

                if check_total_yield_file:
                    
                    fileCount=fileCount+1
                    total_yield_result = csv.DictReader(open(total_yield_file_path, encoding="utf-8"))
                    total_yield_result = list(total_yield_result)
                    print(len(total_yield_result))
                    if len(total_yield_result) > 0:
                        bulk_insert_marriott_total_yield(row['propertyCode'], total_yield_result, row['fore_before'], row['fore_after'])
                        print("TOTAL YIELD DONE")
                    else:
                        error_msg = error_msg + "Total Yeild File Was Blank, "

                if (fileCount==4):
                    if(error_msg==""):
                        update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE="Successfully Finished", IS_ERROR=False)
                    else:
                        error_msg="Partially Successfull:- "+error_msg
                        update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=error_msg, IS_ERROR=True)
                else:
                    if (fileCount==0):
                        error_msg = "All File Not Found"
                    else:
                        error_msg="Partially Successfull:- "+error_msg
                    update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE=error_msg, IS_ERROR=True)
            else:
                print("LAST_PULL_DATE_ID is NULL")
                update_into_pulldate(LAST_PULL_DATE_ID, ERROR_NOTE="LAST_PULL_DATE_ID is NULL", IS_ERROR=True)
    else:
        print(f"Property not available in database!!!")
    print(f"[{PMS_NAME}] SCRIPT STOP!!!")

if __name__ == "__main__":
    main()
