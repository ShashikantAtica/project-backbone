import json
import os
import sys
import tempfile
from io import BytesIO

sys.path.append("..")
import re
import time
import arrow
import pandas as pd
import requests
from utils.secrets.SecretManager import get_secret_from_api as get_secret_dict
from bs4 import BeautifulSoup
import csv
from utils.db import db_config
from utils.db import db_models
import arrow
from sqlalchemy.dialects.postgresql import insert


data_to_insert = [{
    'propertyCode': 'test322',
    'pullDateId': '2233',
    'ACCOUNT': '1234',
    'AUTH_STATUS': 'decline',
    'CREATED_AT': "'" + str(arrow.now()) + "'",
    'UPDATED_AT': "'" + str(arrow.now()) + "'"
},{
    'propertyCode': 'test456',
    'pullDateId': '2233',
    'ACCOUNT': '6789',
    'AUTH_STATUS': 'approved',
    'CREATED_AT': "'" + str(arrow.now()) + "'",
    'UPDATED_AT': "'" + str(arrow.now()) + "'"
}]

conn = db_config.get_db_connection()
    # 'ON CONFLICT' clause to handle conflicts on the 'ACCOUNT' column
stmt = insert(db_models.choice_noshow_model).values(data_to_insert)
stmt = stmt.on_conflict_do_nothing(index_elements=['ACCOUNT'])

# insert
conn.execute(stmt)
# conn = db_config.get_db_connection()
# conn.execute(db_models.choice_noshow_model.insert(), data_to_insert)
# conn.close()