import argparse
import os
import sys
import tempfile
from io import BytesIO

from sqlalchemy import text

sys.path.append("../../")
import re
import time
import arrow
import pandas as pd
import requests
from datetime import datetime

from utils.secrets.SecretManager import get_secret_from_api
from bs4 import BeautifulSoup
import csv
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

from utils.db import db_config
from utils.db import db_models
from sqlalchemy.dialects.postgresql import insert


def insert_into_tblproperties(property_object):
    print("Data importing...")
    conn = db_config.get_db_connection()
    stmt = insert(db_models.tbl_properties_model).values(property_object)
    stmt = stmt.on_conflict_do_nothing(index_elements=['uniqueKey'])
    # Execute the insert statement
    conn.execute(stmt)
    conn.commit()
    conn.close()
    print("Imported successfully!!!")

    

if __name__ == '__main__':

    
    print(f"[TBL_PROPERTIES_INSERT] SCRIPT IS STARTING...")

    resAfter = 0 
    resBefore = 1
    occAfter = 365
    occBefore = 0
    createdAt = "'" + str(arrow.now()) + "'"
    updatedAt = "'" + str(arrow.now()) + "'"
    marriott_json = None
    propertyName = ""
    propertyCode = ""
    externalPropertyCode = ""
    propertySecret = None
    pmsName = None
    timezone = "America/Chicago"

    propertycode = None
    propertyname = None
    pmsname = None

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--propertycode", type=str, required=False, help="Type in the propertycode")
        parser.add_argument("--propertyname", type=str, required=False, help="Type in the propertyname")
        parser.add_argument("--pmsname", type=str, required=False, help="Type in the pmsname")
        args = parser.parse_args()
        propertycode = args.propertycode
        propertyname = args.propertyname
        pmsname = args.pmsname
        propertyname = propertyname.replace("#", " ")
        propertyname = propertyname.replace("@", "&")
        pmsname = pmsname.replace("#", " ")
        print(f"propertycode :: {propertycode}")
        print(f"propertyname :: {propertyname}")
        print(f"pmsname :: {pmsname}")
    except:
        print("Something wents wrong with Inputs!")
        pass

    propertyName = propertyname
    propertyCode = propertycode
    pmsName = pmsname
    uniqueKey = pmsName + "_" + propertyCode

    property_object = {
    "propertyName": propertyName,
    "propertyCode": propertyCode,
    "externalPropertyCode": externalPropertyCode,
    "propertySecret": propertySecret,
    "pmsName": pmsName,
    "timezone": timezone,
    "resAfter": resAfter,
    "resBefore": resBefore,
    "occAfter": occAfter,
    "occBefore": occBefore,
    "createdAt": createdAt,
    "updatedAt": updatedAt,
    "marriott_json": marriott_json,
    "uniqueKey": uniqueKey
    }


    
    if propertycode is None or propertyname is None or pmsname is None:
       print("Please enter all inputs, Recheck!")
    else:
        print(f"{propertycode} property inserting to tbl_properties")

        insert_into_tblproperties(property_object)

        print(f"{propertycode} property inserted to tbl_properties successfully")