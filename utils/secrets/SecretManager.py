import json
from google.cloud import secretmanager
from google.api_core.exceptions import NotFound
import pandas as pd

from utils.secrets.creds import CREDS_MODE

project_id = "625884015372"
parent = f"projects/{project_id}"


def _latest_secret_name(secret_id):
    return f"{parent}/secrets/{secret_id}/versions/latest"


def get_secret(secret_id):
    if secret_id is None or secret_id == "":
        return "{'msg':'secret_id is none or empty!!!'}"
    print("CREDS_MODE :: ",CREDS_MODE)
    if CREDS_MODE == "GOOGLE_SECRET_MANAGER":
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(request={"name": _latest_secret_name(secret_id)})
        print("response :: ", response)
        return response.payload.data.decode("UTF-8")
    elif CREDS_MODE == "GOOGLE_SHEET":
        SHEET_ID = '1gpnqKRVtC6i71AtPTxu6kjUaLmHxwjsmX0xsffNrMHE'
        SHEET_NAME = 'creds'
        url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
        df = pd.read_csv(url)
        df = df[['pms', 'u', 'p']]
        df_new = df[df['pms'] == secret_id]
        json_data = df_new.to_json(orient='records')
        json_data = json.loads(json_data)
        if len(json_data) > 0:
            json_data = json_data[0]
        else:
            json_data = {}
        print(type(json_data))
        print(json_data)
        json_data = json.dumps(json_data)
        return json_data


def get_secret_dict(secret_id):
    return json.loads(get_secret(secret_id))


def secret_exists(secret_id):
    try:
        get_secret(secret_id)
        return True
    except NotFound:
        return False


def add_secret_version(secret_id, secret):
    secret = json.dumps(secret).encode('UTF-8')
    client = secretmanager.SecretManagerServiceClient()
    response = client.add_secret_version(parent=f"{parent}/secrets/{secret_id}", payload={'data': secret})

    return response


if __name__ == '__main__':
    print("Google Secret Manager")
    mydata = get_secret("choice-skytouch-si-ozarks-US000009")
    print("mydata :: ", mydata)
