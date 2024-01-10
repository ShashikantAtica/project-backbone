import argparse
import os
import sys

from bs4 import BeautifulSoup
from sqlalchemy import text

sys.path.append("../../")
import arrow
import csv
import xml.etree.ElementTree as Xet
import pandas as pd

from utils.db import db_config
from utils.db import db_models
from sqlalchemy.dialects.postgresql import insert



def insert_into_pulldate(PROPERTY_CODE, PULLED_DATE,PMS_NAME):
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


def bulk_insert_opera_cloud_res(res_list):

    # Add new data of reservation
    print("Data importing...")
    try:
        conn = db_config.get_db_connection()
        conn.execute(db_models.opera_res_model.insert(), res_list)
        conn.commit()
        conn.close()
        print("Data imported")
    except Exception as e:
        error_message = str(e)
        print(error_message)


def bulk_insert_opera_cloud_occ(occ_list):
  
    # Add new data of reservation
    print("Data importing...")
    try:
        conn = db_config.get_db_connection()
        conn.execute(db_models.opera_occ_model.insert(), occ_list)
        conn.commit()
        conn.close()
        print("Data imported")
    except Exception as e:
        error_message = str(e)
        print(error_message)


def bulk_insert_opera_cloud_arrival(arrival_list):

    # Add new data of reservation
    print("Data importing...")
    try:
        conn = db_config.get_db_connection()
        conn.execute(db_models.opera_arrival_model.insert(), arrival_list)
        conn.commit()
        conn.close()
        print("Data imported")
    except Exception as e:
        error_message = str(e)
        print(error_message)

def bulk_insert_opera_cloud_rbrc(rbrc_list):
    # Add new data of RBRC
    print("Data importing...")
    try:
        conn = db_config.get_db_connection()
        conn.execute(db_models.opera_rbrc_model.insert(), rbrc_list)
        conn.commit()
        conn.close()
        print("Data imported")
    except Exception as e:
        error_message = str(e)
        print(error_message)



def OperaCloud_Pms(row, reporttype, localfilepath):
    pullDateId = row['pullDateId']
    propertyCode = row['propertyCode']
    attachment_format = "../reports"

    try:
        # Modification of res report
        reservation_file_path = f'{attachment_format}/{propertyCode}_Reservation_Onboarding.xml'
        occupancy_file_path = f'{attachment_format}/{propertyCode}_Occupancy_Onboarding.xml'
        arrival_file_path = f'{attachment_format}/{propertyCode}_Arrival_Onboarding.xml'
        rbrc_file_path = f'{attachment_format}/{propertyCode}_RBRC_Onboarding.xml'


        if(reporttype == 'Reservation'):
            reservation_file_path = localfilepath
        elif(reporttype == 'Occupancy'):
            occupancy_file_path = localfilepath
        elif(reporttype == 'Arrival'):
            arrival_file_path = localfilepath
        elif(reporttype == 'RBRC'):
            rbrc_file_path = localfilepath


        check_reservation_file = os.path.isfile(reservation_file_path)
        check_occupancy_file = os.path.isfile(occupancy_file_path)
        check_arrival_file = os.path.isfile(arrival_file_path)
        check_rbrc_file = os.path.isfile(rbrc_file_path)


        createdAt = "'" + str(arrow.now()) + "'"
        updatedAt = "'" + str(arrow.now()) + "'"
        createdAtEpoch =  int(arrow.utcnow().timestamp())
        updatedAtEpoch =  int(arrow.utcnow().timestamp())

        errorMessage = ""

        if check_reservation_file:

            # Start Reservation Report
            cols = ["RESV_NAME_ID", "GUARANTEE_CODE", "RESV_STATUS", "ROOM", "FULL_NAME", "DEPARTURE", "PERSONS",
                    "GROUP_NAME",
                    "NO_OF_ROOMS", "ROOM_CATEGORY_LABEL", "RATE_CODE", "INSERT_USER", "INSERT_DATE", "GUARANTEE_CODE_DESC",
                    "COMPANY_NAME", "TRAVEL_AGENT_NAME", "ARRIVAL", "NIGHTS", "COMP_HOUSE_YN", "SHARE_AMOUNT", "C_T_S_NAME",
                    "SHORT_RESV_STATUS", "SHARE_AMOUNT_PER_STAY"]
            rows = []

            # Parsing the XML file
            xmlparse = Xet.parse(reservation_file_path)
            root = xmlparse.getroot()
            try:
                for i in root[0][0][2][0][2]:
                    RESV_NAME_ID = i.find("RESV_NAME_ID").text
                    GUARANTEE_CODE = i.find("GUARANTEE_CODE").text
                    RESV_STATUS = i.find("RESV_STATUS").text
                    ROOM = i.find("ROOM").text
                    FULL_NAME = i.find("FULL_NAME").text
                    DEPARTURE = i.find("DEPARTURE").text
                    PERSONS = i.find("PERSONS").text
                    GROUP_NAME = i.find("GROUP_NAME").text
                    NO_OF_ROOMS = i.find("NO_OF_ROOMS").text
                    ROOM_CATEGORY_LABEL = i.find("ROOM_CATEGORY_LABEL").text
                    RATE_CODE = i.find("RATE_CODE").text
                    INSERT_USER = i.find("INSERT_USER").text
                    INSERT_DATE = i.find("INSERT_DATE").text
                    GUARANTEE_CODE_DESC = i.find("GUARANTEE_CODE_DESC").text
                    COMPANY_NAME = i.find("COMPANY_NAME").text
                    TRAVEL_AGENT_NAME = i.find("TRAVEL_AGENT_NAME").text
                    ARRIVAL = i.find("ARRIVAL").text
                    NIGHTS = i.find("NIGHTS").text
                    COMP_HOUSE_YN = i.find("COMP_HOUSE_YN").text
                    SHARE_AMOUNT = i.find("SHARE_AMOUNT").text
                    C_T_S_NAME = i.find("C_T_S_NAME").text
                    SHORT_RESV_STATUS = i.find("SHORT_RESV_STATUS").text
                    SHARE_AMOUNT_PER_STAY = i.find("SHARE_AMOUNT_PER_STAY").text
                    #
                    rows.append({"RESV_NAME_ID": RESV_NAME_ID,
                                "GUARANTEE_CODE": GUARANTEE_CODE,
                                "RESV_STATUS": RESV_STATUS,
                                "ROOM": ROOM,
                                "FULL_NAME": FULL_NAME,
                                "DEPARTURE": DEPARTURE,
                                "PERSONS": PERSONS,
                                "GROUP_NAME": GROUP_NAME,
                                "NO_OF_ROOMS": NO_OF_ROOMS,
                                "ROOM_CATEGORY_LABEL": ROOM_CATEGORY_LABEL,
                                "RATE_CODE": RATE_CODE,
                                "INSERT_USER": INSERT_USER,
                                "INSERT_DATE": INSERT_DATE,
                                "GUARANTEE_CODE_DESC": GUARANTEE_CODE_DESC,
                                "COMPANY_NAME": COMPANY_NAME,
                                "TRAVEL_AGENT_NAME": TRAVEL_AGENT_NAME,
                                "ARRIVAL": ARRIVAL,
                                "NIGHTS": NIGHTS,
                                "COMP_HOUSE_YN": COMP_HOUSE_YN,
                                "SHARE_AMOUNT": SHARE_AMOUNT,
                                "C_T_S_NAME": C_T_S_NAME,
                                "SHORT_RESV_STATUS": SHORT_RESV_STATUS,
                                "SHARE_AMOUNT_PER_STAY": SHARE_AMOUNT_PER_STAY})

                df = pd.DataFrame(rows, columns=cols)
                df.insert(0, column="propertyCode", value=propertyCode)
                df.insert(1, column="pullDateId", value=pullDateId)
                df.insert(2, column="createdAt", value=createdAt)
                df.insert(3, column="updatedAt", value=updatedAt)
                df.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                df.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                df.insert(6, column="uniqueKey", value=df["RESV_NAME_ID"].astype(str))
                df['DEPARTURE'] = pd.to_datetime(df['DEPARTURE'])
                df['INSERT_DATE'] = pd.to_datetime(df['INSERT_DATE'])
                df['ARRIVAL'] = pd.to_datetime(df['ARRIVAL'])
                df.to_csv(f"{attachment_format}/{propertyCode}_Reservations.csv", index=False)

                res_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Reservations.csv", encoding="utf-8"))
                res_result = list(res_result)
                
            except Exception:
                res_result = []
                print("Reservation Data not available")

            print("RES RESULT")
            # print(res_result) #This can be uncommented to test/see the result of parsed data
            if len(res_result) > 0:
                bulk_insert_opera_cloud_res(res_result)
                print("RES DONE")
            else:
                errorMessage = errorMessage + " Reservation File Was Blank,"
            # End Reservation Report
            
        if check_occupancy_file:

            # Start Occupancy Report
            cols = ['REVENUE', 'NO_ROOMS', 'IND_DEDUCT_ROOMS', 'IND_NON_DEDUCT_ROOMS', 'GRP_DEDUCT_ROOMS',
                    'GRP_NON_DEDUCT_ROOMS',
                    'NO_PERSONS', 'ARRIVAL_ROOMS', 'DEPARTURE_ROOMS', 'COMPLIMENTARY_ROOMS', 'HOUSE_USE_ROOMS',
                    'DAY_USE_ROOMS',
                    'NO_SHOW_ROOMS', 'INVENTORY_ROOMS', 'CONSIDERED_DATE', 'CHAR_CONSIDERED_DATE', 'IND_DEDUCT_REVENUE',
                    'IND_NON_DEDUCT_REVENUE', 'GRP_NON_DEDUCT_REVENUE', 'GRP_DEDUCT_REVENUE', 'OWNER_ROOMS', 'FF_ROOMS',
                    'CF_OOO_ROOMS', 'CF_CALC_OCC_ROOMS', 'CF_CALC_INV_ROOMS', 'CF_AVERAGE_ROOM_RATE', 'CF_OCCUPANCY',
                    'CF_IND_DED_REV', 'CF_IND_NON_DED_REV', 'CF_BLK_DED_REV', 'CF_BLK_NON_DED_REV']
            rows = []

            # Parsing the XML file
            xmlparse = Xet.parse(occupancy_file_path)
            root = xmlparse.getroot()
            if root[0][0][1][0][1].text == 'Forecast':
                for i in root[0][0][1][0][2]:
                    REVENUE = i.find('REVENUE').text
                    NO_ROOMS = i.find('NO_ROOMS').text
                    IND_DEDUCT_ROOMS = i.find('IND_DEDUCT_ROOMS').text
                    IND_NON_DEDUCT_ROOMS = i.find('IND_NON_DEDUCT_ROOMS').text
                    GRP_DEDUCT_ROOMS = i.find('GRP_DEDUCT_ROOMS').text
                    GRP_NON_DEDUCT_ROOMS = i.find('GRP_NON_DEDUCT_ROOMS').text
                    NO_PERSONS = i.find('NO_PERSONS').text
                    ARRIVAL_ROOMS = i.find('ARRIVAL_ROOMS').text
                    DEPARTURE_ROOMS = i.find('DEPARTURE_ROOMS').text
                    COMPLIMENTARY_ROOMS = i.find('COMPLIMENTARY_ROOMS').text
                    HOUSE_USE_ROOMS = i.find('HOUSE_USE_ROOMS').text
                    DAY_USE_ROOMS = i.find('DAY_USE_ROOMS').text
                    NO_SHOW_ROOMS = i.find('NO_SHOW_ROOMS').text
                    INVENTORY_ROOMS = i.find('INVENTORY_ROOMS').text
                    CONSIDERED_DATE = i.find('CONSIDERED_DATE').text
                    CHAR_CONSIDERED_DATE = i.find('CHAR_CONSIDERED_DATE').text
                    IND_DEDUCT_REVENUE = i.find('IND_DEDUCT_REVENUE').text
                    IND_NON_DEDUCT_REVENUE = i.find('IND_NON_DEDUCT_REVENUE').text
                    GRP_NON_DEDUCT_REVENUE = i.find('GRP_NON_DEDUCT_REVENUE').text
                    GRP_DEDUCT_REVENUE = i.find('GRP_DEDUCT_REVENUE').text
                    OWNER_ROOMS = i.find('OWNER_ROOMS').text
                    FF_ROOMS = i.find('FF_ROOMS').text
                    CF_OOO_ROOMS = i.find('CF_OOO_ROOMS').text
                    CF_CALC_OCC_ROOMS = i.find('CF_CALC_OCC_ROOMS').text
                    CF_CALC_INV_ROOMS = i.find('CF_CALC_INV_ROOMS').text
                    CF_AVERAGE_ROOM_RATE = i.find('CF_AVERAGE_ROOM_RATE').text
                    CF_OCCUPANCY = i.find('CF_OCCUPANCY').text
                    CF_IND_DED_REV = i.find('CF_IND_DED_REV').text
                    CF_IND_NON_DED_REV = i.find('CF_IND_NON_DED_REV').text
                    CF_BLK_DED_REV = i.find('CF_BLK_DED_REV').text
                    CF_BLK_NON_DED_REV = i.find('CF_BLK_NON_DED_REV').text

                    rows.append({
                        'REVENUE': REVENUE,
                        'NO_ROOMS': NO_ROOMS,
                        'IND_DEDUCT_ROOMS': IND_DEDUCT_ROOMS,
                        'IND_NON_DEDUCT_ROOMS': IND_NON_DEDUCT_ROOMS,
                        'GRP_DEDUCT_ROOMS': GRP_DEDUCT_ROOMS,
                        'GRP_NON_DEDUCT_ROOMS': GRP_NON_DEDUCT_ROOMS,
                        'NO_PERSONS': NO_PERSONS,
                        'ARRIVAL_ROOMS': ARRIVAL_ROOMS,
                        'DEPARTURE_ROOMS': DEPARTURE_ROOMS,
                        'COMPLIMENTARY_ROOMS': COMPLIMENTARY_ROOMS,
                        'HOUSE_USE_ROOMS': HOUSE_USE_ROOMS,
                        'DAY_USE_ROOMS': DAY_USE_ROOMS,
                        'NO_SHOW_ROOMS': NO_SHOW_ROOMS,
                        'INVENTORY_ROOMS': INVENTORY_ROOMS,
                        'CONSIDERED_DATE': CONSIDERED_DATE,
                        'CHAR_CONSIDERED_DATE': CHAR_CONSIDERED_DATE,
                        'IND_DEDUCT_REVENUE': IND_DEDUCT_REVENUE,
                        'IND_NON_DEDUCT_REVENUE': IND_NON_DEDUCT_REVENUE,
                        'GRP_NON_DEDUCT_REVENUE': GRP_NON_DEDUCT_REVENUE,
                        'GRP_DEDUCT_REVENUE': GRP_DEDUCT_REVENUE,
                        'OWNER_ROOMS': OWNER_ROOMS,
                        'FF_ROOMS': FF_ROOMS,
                        'CF_OOO_ROOMS': CF_OOO_ROOMS,
                        'CF_CALC_OCC_ROOMS': CF_CALC_OCC_ROOMS,
                        'CF_CALC_INV_ROOMS': CF_CALC_INV_ROOMS,
                        'CF_AVERAGE_ROOM_RATE': CF_AVERAGE_ROOM_RATE,
                        'CF_OCCUPANCY': CF_OCCUPANCY,
                        'CF_IND_DED_REV': CF_IND_DED_REV,
                        'CF_IND_NON_DED_REV': CF_IND_NON_DED_REV,
                        'CF_BLK_DED_REV': CF_BLK_DED_REV,
                        'CF_BLK_NON_DED_REV': CF_BLK_NON_DED_REV
                    })
                df = pd.DataFrame(rows, columns=cols)
                df.insert(0, column="propertyCode", value=propertyCode)
                df.insert(1, column="pullDateId", value=pullDateId)
                df.insert(2, column="createdAt", value=createdAt)
                df.insert(3, column="updatedAt", value=updatedAt)
                df.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                df.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                df['CONSIDERED_DATE'] = pd.to_datetime(df['CONSIDERED_DATE'])
                df['CHAR_CONSIDERED_DATE'] = pd.to_datetime(df['CHAR_CONSIDERED_DATE'])
                df.insert(6, column="uniqueKey", value=df["propertyCode"].astype(str) + "_" + df['CHAR_CONSIDERED_DATE'].astype(str)) 
                df.to_csv(f"{attachment_format}/{propertyCode}_Occupancy.csv", index=False)

                occ_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Occupancy.csv", encoding="utf-8"))
                occ_result = list(occ_result)
            elif root[0][0][1][1][1].text == 'Forecast':
                for i in root[0][0][1][1][2]:
                    REVENUE = i.find('REVENUE').text
                    NO_ROOMS = i.find('NO_ROOMS').text
                    IND_DEDUCT_ROOMS = i.find('IND_DEDUCT_ROOMS').text
                    IND_NON_DEDUCT_ROOMS = i.find('IND_NON_DEDUCT_ROOMS').text
                    GRP_DEDUCT_ROOMS = i.find('GRP_DEDUCT_ROOMS').text
                    GRP_NON_DEDUCT_ROOMS = i.find('GRP_NON_DEDUCT_ROOMS').text
                    NO_PERSONS = i.find('NO_PERSONS').text
                    ARRIVAL_ROOMS = i.find('ARRIVAL_ROOMS').text
                    DEPARTURE_ROOMS = i.find('DEPARTURE_ROOMS').text
                    COMPLIMENTARY_ROOMS = i.find('COMPLIMENTARY_ROOMS').text
                    HOUSE_USE_ROOMS = i.find('HOUSE_USE_ROOMS').text
                    DAY_USE_ROOMS = i.find('DAY_USE_ROOMS').text
                    NO_SHOW_ROOMS = i.find('NO_SHOW_ROOMS').text
                    INVENTORY_ROOMS = i.find('INVENTORY_ROOMS').text
                    CONSIDERED_DATE = i.find('CONSIDERED_DATE').text
                    CHAR_CONSIDERED_DATE = i.find('CHAR_CONSIDERED_DATE').text
                    IND_DEDUCT_REVENUE = i.find('IND_DEDUCT_REVENUE').text
                    IND_NON_DEDUCT_REVENUE = i.find('IND_NON_DEDUCT_REVENUE').text
                    GRP_NON_DEDUCT_REVENUE = i.find('GRP_NON_DEDUCT_REVENUE').text
                    GRP_DEDUCT_REVENUE = i.find('GRP_DEDUCT_REVENUE').text
                    OWNER_ROOMS = i.find('OWNER_ROOMS').text
                    FF_ROOMS = i.find('FF_ROOMS').text
                    CF_OOO_ROOMS = i.find('CF_OOO_ROOMS').text
                    CF_CALC_OCC_ROOMS = i.find('CF_CALC_OCC_ROOMS').text
                    CF_CALC_INV_ROOMS = i.find('CF_CALC_INV_ROOMS').text
                    CF_AVERAGE_ROOM_RATE = i.find('CF_AVERAGE_ROOM_RATE').text
                    CF_OCCUPANCY = i.find('CF_OCCUPANCY').text
                    CF_IND_DED_REV = i.find('CF_IND_DED_REV').text
                    CF_IND_NON_DED_REV = i.find('CF_IND_NON_DED_REV').text
                    CF_BLK_DED_REV = i.find('CF_BLK_DED_REV').text
                    CF_BLK_NON_DED_REV = i.find('CF_BLK_NON_DED_REV').text

                    rows.append({
                        'REVENUE': REVENUE,
                        'NO_ROOMS': NO_ROOMS,
                        'IND_DEDUCT_ROOMS': IND_DEDUCT_ROOMS,
                        'IND_NON_DEDUCT_ROOMS': IND_NON_DEDUCT_ROOMS,
                        'GRP_DEDUCT_ROOMS': GRP_DEDUCT_ROOMS,
                        'GRP_NON_DEDUCT_ROOMS': GRP_NON_DEDUCT_ROOMS,
                        'NO_PERSONS': NO_PERSONS,
                        'ARRIVAL_ROOMS': ARRIVAL_ROOMS,
                        'DEPARTURE_ROOMS': DEPARTURE_ROOMS,
                        'COMPLIMENTARY_ROOMS': COMPLIMENTARY_ROOMS,
                        'HOUSE_USE_ROOMS': HOUSE_USE_ROOMS,
                        'DAY_USE_ROOMS': DAY_USE_ROOMS,
                        'NO_SHOW_ROOMS': NO_SHOW_ROOMS,
                        'INVENTORY_ROOMS': INVENTORY_ROOMS,
                        'CONSIDERED_DATE': CONSIDERED_DATE,
                        'CHAR_CONSIDERED_DATE': CHAR_CONSIDERED_DATE,
                        'IND_DEDUCT_REVENUE': IND_DEDUCT_REVENUE,
                        'IND_NON_DEDUCT_REVENUE': IND_NON_DEDUCT_REVENUE,
                        'GRP_NON_DEDUCT_REVENUE': GRP_NON_DEDUCT_REVENUE,
                        'GRP_DEDUCT_REVENUE': GRP_DEDUCT_REVENUE,
                        'OWNER_ROOMS': OWNER_ROOMS,
                        'FF_ROOMS': FF_ROOMS,
                        'CF_OOO_ROOMS': CF_OOO_ROOMS,
                        'CF_CALC_OCC_ROOMS': CF_CALC_OCC_ROOMS,
                        'CF_CALC_INV_ROOMS': CF_CALC_INV_ROOMS,
                        'CF_AVERAGE_ROOM_RATE': CF_AVERAGE_ROOM_RATE,
                        'CF_OCCUPANCY': CF_OCCUPANCY,
                        'CF_IND_DED_REV': CF_IND_DED_REV,
                        'CF_IND_NON_DED_REV': CF_IND_NON_DED_REV,
                        'CF_BLK_DED_REV': CF_BLK_DED_REV,
                        'CF_BLK_NON_DED_REV': CF_BLK_NON_DED_REV
                    })
                df = pd.DataFrame(rows, columns=cols)
                df.insert(0, column="propertyCode", value=propertyCode)
                df.insert(1, column="pullDateId", value=pullDateId)
                df.insert(2, column="createdAt", value=createdAt)
                df.insert(3, column="updatedAt", value=updatedAt)
                df.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                df.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                df['CONSIDERED_DATE'] = pd.to_datetime(df['CONSIDERED_DATE'])
                df['CHAR_CONSIDERED_DATE'] = pd.to_datetime(df['CHAR_CONSIDERED_DATE'])
                df.insert(6, column="uniqueKey", value=df["propertyCode"].astype(str) + "_" + df['CHAR_CONSIDERED_DATE'].astype(str)) 
                df.to_csv(f"{attachment_format}/{propertyCode}_Occupancy.csv", index=False)

                occ_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Occupancy.csv", encoding="utf-8"))
                occ_result = list(occ_result)
            else:
                occ_result = []
                print("Occupancy Data not available")
            
            print("OCC RESULT")
            # print(occ_result)  #This can be uncommented to test/see the result of parsed data
            if len(occ_result) > 0:
                bulk_insert_opera_cloud_occ(occ_result)
                print("OCC DONE")
            else:
                errorMessage = errorMessage + " Occupancy File Was Blank,"
            # End Occupancy Report

        if check_arrival_file:

            # Start Arrival Report
            arrival_dataframe = []

            with open(arrival_file_path, 'r') as f:
                read = f.read()
                soup_data = BeautifulSoup(read, "xml")
                column_names = soup_data.find_all('G_RESERVATION')

                for column_name in column_names:
                    data_dict = {}
                    for element in column_name:
                        tag = element.name
                        text = element.get_text(strip=True)
                        data_dict[tag] = text
                    arrival_dataframe.append(data_dict)

            arrival_data_concat = pd.DataFrame(arrival_dataframe)
            headers = arrival_data_concat.columns[1:]
            max_length = 255

            for column in ['LIST_G_MEM_TYPE_LEVEL', 'LIST_G_INV_ITEMS', 'LIST_G_BILL_RESV', 'LIST_G_COMMENT_NAME_ID',
                        'LIST_G_RESERV_PROMO', 'LIST_G_DEPT_ID', 'LIST_G_DEP_DATE_CHANGE', 'LIST_G_COMMENT_RESV_NAME_ID',
                        'LIST_G_FIXED_CHARGES', 'LIST_G_AWARDS']:
                arrival_data_concat[column] = arrival_data_concat[column].astype(str).str[:max_length]
            
            final_df = arrival_data_concat[headers]
            final_df.insert(0, column="propertyCode", value=propertyCode)
            final_df.insert(1, column="pullDateId", value=pullDateId)
            final_df.insert(2, column="createdAt", value=createdAt)
            final_df.insert(3, column="updatedAt", value=updatedAt)
            final_df.insert(4, column="createdAtEpoch", value=createdAtEpoch)
            final_df.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
            final_df.insert(6, column="uniqueKey", value=final_df["RESV_NAME_ID"].astype(str))
            final_df['UPDATE_DATE'] = pd.to_datetime(final_df['UPDATE_DATE'])
            final_df['TRUNC_BEGIN'] = pd.to_datetime(final_df['TRUNC_BEGIN'])
            final_df['TRUNC_END'] = pd.to_datetime(final_df['TRUNC_END'])
            final_df['ARRIVAL'] = pd.to_datetime(final_df['ARRIVAL'])
            final_df['DEPARTURE'] = pd.to_datetime(final_df['DEPARTURE'])
            final_df['BEGIN_DATE'] = pd.to_datetime(final_df['BEGIN_DATE'])
            final_df.to_csv(f"{attachment_format}/{propertyCode}_Arrival.csv", index=False)

            arrival_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_Arrival.csv", encoding="utf-8"))
            arrival_result = list(arrival_result)

            print("ARRIVAL RESULT")
            # print(arrival_result)  #This can be uncommented to test/see the result of parsed data
            if len(arrival_result) > 0:
                bulk_insert_opera_cloud_arrival(arrival_result)
                print("ARRIVAL DONE")
            else:
                errorMessage = errorMessage + " Arrival File Was Blank,"

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

            date_set = set()
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
                            date_set.add(BUSINESS_DATE)
                        
                df = pd.DataFrame(rows, columns=cols)
                df.insert(0, column="propertyCode", value=propertyCode)
                df.insert(1, column="pullDateId", value=pullDateId)
                df.insert(2, column="createdAt", value=createdAt)
                df.insert(3, column="updatedAt", value=updatedAt)
                df.insert(4, column="createdAtEpoch", value=createdAtEpoch)
                df.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
                df['BUSINESS_DATE'] = pd.to_datetime(df['BUSINESS_DATE'])
                df['CHAR_BUSINESS_DATE'] = pd.to_datetime(df['CHAR_BUSINESS_DATE'])
                df.insert(6, column="uniqueKey", value=df["propertyCode"].astype(str) + "_" + df['BUSINESS_DATE'].astype(str) + "_" + df['MASTER_VALUE'].astype(str))            
                df.to_csv(f"{attachment_format}/{propertyCode}_RBRC.csv", index=False)
                rbrc_result = csv.DictReader(open(f"{attachment_format}/{propertyCode}_RBRC.csv", encoding="utf-8"))
                rbrc_result = list(rbrc_result)
            except Exception:
                rbrc_result = []
                print("Reservation Data not available")
            
            # End RBRC Report

            print("RBRC RESULT")
            # print(rbrc_result) #This can be uncommented to test/see the result of parsed data
            if len(rbrc_result) > 0:
                bulk_insert_opera_cloud_rbrc(rbrc_result)
                print("RBRC DONE")
            else:
                errorMessage = errorMessage + " RBRC File Was Blank,"
            # End Arrival Report

        
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

    PMS_NAME = "OperaCloud"
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
                OperaCloud_Pms(row, reporttype, localfilepath)
            else:
                print("LAST_PULL_DATE_ID is NULL")
    else:
        print(f"Property not available in database!!!")
    print(f"[{PMS_NAME}] SCRIPT STOP!!!")