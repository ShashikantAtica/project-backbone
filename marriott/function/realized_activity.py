import os
import sys
import pandas as pd

sys.path.append("..")

from marriott.utils.login import get_session, FailedLoginException
from requests.exceptions import ConnectionError
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type


class RetryException(Exception):
    pass


class InvalidRequestException(Exception):
    pass


class InvalidReportException(Exception):
    pass


@retry(stop=(stop_after_attempt(2)),
       wait=wait_fixed(5),
       retry=(retry_if_exception_type((RetryException, ConnectionError))),
       reraise=True)
def get_realized_activity_report(session, external_property_code, platform, start_date):
    print(f'get realized activity report for {external_property_code}')
    propertycode = external_property_code

    property_url = (f'https://salesnetcloud.marriott.com/{platform}/oneyield'
                    f'/OysController/signIn/submit?accessLevel=property'
                    f'&entityCode={propertycode}&updatePrivilege=false'
                    f'&requestProfileName=home&updatePrivilege=false'
                    f'&requestProfileName=home&accessLevelRadio=property'
                    f'&propertyCodeText={propertycode}'
                    f'&propertyRecentAccessComboBox={propertycode}')
    get_property_url = session.get(property_url, timeout=120)

    get_realized_activity_data = {
        "month": "",
        "period": "",
        "propertyCode": propertycode,
        "arrivalDate": start_date.format('M/D/YYYY'),
        "dayOfWeek": "ALL",
        "daysOut": "90",
        "roomCategory": "ALL",
        "rateCategory": "ALL",
        "displayType": "2"
    }
    get_realized_activity_url = (f'https://salesnetcloud.marriott.com/{platform}/oneyield/OysController/analysis/realizedActivityActuate')
    get_realized_activity = session.post(get_realized_activity_url, data=get_realized_activity_data)

    if get_realized_activity.status_code >= 400:
        raise RetryException(f"Server error")

    if not get_realized_activity.ok:
        raise RetryException(f"Total Yield report failed")

    return get_realized_activity.content


def process_report(session, payload):
    property_code = payload['external_property_code']
    print(f"start for {property_code}")
    start_date = payload['fore_before']
    folder_name = "./reports/"

    # Realized Activity Start
    realized_activity_report = get_realized_activity_report(session, property_code, payload['forecast_platform'], start_date)
    filename = f'{folder_name}{property_code}_Realized_Activity.xls'
    open(filename, "wb").write(realized_activity_report)
    read = pd.read_excel(filename, skipfooter=7)
    read.insert(0, column="propertyCode", value=payload['propertyCode'])
    read.insert(1, column="pullDateId", value=payload['pullDateId'])
    read['Arrival Date'] = pd.to_datetime(read['Arrival Date']).dt.date
    headers_list = ["propertyCode", "pullDateId", "ArrivalDate", "DOW", "EV_LO", "TransSold", "GroupSold", "TotalRoomsSold_Num",
                    "TotalRoomsSold_Occ_Per", "ArvlAddlDem", "NoShows_Tran", "NoShows_Grp", "Cancels_TranRMS", "Cancels_TranLOS",
                    "Cancels_TranTotal", "Cancels_Grp", "SameDayCheck_ins", "UnexpectedStaythroughs_Tran", "UnexpectedStaythroughs_Grp",
                    "EarlyCheck_outs_Tran", "EarlyCheck_outs_Grp"]
    read.to_csv(f'{folder_name}{property_code}_Realized_Activity.csv', index=False, header=headers_list)
    if os.path.exists(filename):
        os.remove(filename)
    print(f"{property_code} Realized Activity file write success")
    # Realized Activity End


# cloud function entry point
def handle_request(request):
    payload = request

    session = None
    try:
        session = get_session(payload['gcp_secret'], payload['external_property_code'], payload['propertyCode'])
        process_report(session, payload)

    except (InvalidRequestException, InvalidReportException, FailedLoginException) as e:
        reason = str(e)
        print(reason)

    return session
