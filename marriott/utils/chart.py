import sys

sys.path.append("..")
from utils.db import db_config
import ast
import pandas as pd
def upload_chart(secret_name, PROPERTY_CODE):
    PROPERTY_CODE = "'" + str(PROPERTY_CODE) + "'"
    query_string = f'SELECT marriott_json FROM public.tbl_properties where "propertyCode" = {PROPERTY_CODE};'
    conn = db_config.get_db_connection()
    result = conn.execute(query_string)
    conn.close()
    blank = []
    for i in result:
        chart = i['marriott_json']
    new = ast.literal_eval(chart)
    df = pd.DataFrame(new)
    blank.append(df['A'][2])
    blank.append(df['J'][4])
    blank.append(df['K'][3])
    username = secret_name
    return blank

    # table = DB.MARRIOTT_CHART_LOOKUPS_TABLE
    # db.delete(table=table, where={'name': username})
    # db.insert(table=table, values=values, return_column='name')
