import json
import os
import arrow

import pandas as pd
from dotenv.main import load_dotenv
from marriott.utils.login import get_session, FailedLoginException
from marriott.utils.mrdw import export, download


class RetryException(Exception):
    pass


class InvalidRequestException(Exception):
    pass


class InvalidReportException(Exception):
    pass


def process_report(session, payload):
    property_code = payload['external_property_code']
    print(f"start for {property_code}")
    start_date = payload['res_before']
    end_date = payload['res_after']

    createdAt = "'" + str(arrow.now()) + "'"
    updatedAt = "'" + str(arrow.now()) + "'"
    createdAtEpoch =  int(arrow.utcnow().timestamp())
    updatedAtEpoch =  int(arrow.utcnow().timestamp())

    # Read report URL from JSON file

    load_dotenv()
    PROJECT_PATH = os.environ['PROJECT_PATH']
    JSON_FOLDER_PATH = f"{PROJECT_PATH}\marriott\\function"
    report_url = ""

    with open(f"{JSON_FOLDER_PATH}\\marriott_master.json") as f:
        data = json.load(f)
        for i in data:
            if i['property_code'] == "USFL230802":
                report_url = i['report_url']
    print("Report URL : ", report_url)
    payload['reservation_report_url'] = report_url
    report_query, download_uri = export(session, payload['reservation_report_url'], start_date, end_date)
    if download_uri is None:
        raise Exception('Report could not be exported!')
    csv = download(session, report_query, download_uri)
    folder_name = "./reports/"

    reservation_file = f'{folder_name}{property_code}_Reservation.csv'
    if os.path.exists(reservation_file):
        os.remove(reservation_file)

    filename = f'{folder_name}{property_code}_Reservation.xlsx'
    open(filename, "wb").write(csv)
    read = pd.read_excel(filename)
    read.insert(0, column="propertyCode", value=payload['propertyCode'])
    read.insert(1, column="pullDateId", value=payload['pullDateId'])
    read.insert(2, column="createdAt", value=createdAt)
    read.insert(3, column="updatedAt", value=updatedAt)
    read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
    read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
    read['Stay Date'] = pd.to_datetime(read['Stay Date']).dt.date
    read['Booking Date'] = pd.to_datetime(read['Booking Date']).dt.date
    headers_list = ['propertyCode', 'pullDateId', 'createdAt', 'updatedAt', 'createdAtEpoch', 'updatedAtEpoch', 'StayYearCalendar', 'StayYYYYMM', 'CustomWDWE', 'StayDOWText', 'StayDate', 'ArrivalIndicator', 'BookingDate',
                    'AVPName', 'BrandName', 'HtlComparableText', 'PropertyCountryRegion', 'HotelTypeName', 'ManagementType', 'MARSHACode', 'PublicClusterName',
                    'GlobalRegionName', 'GlobalDivision', 'MarketCategory', 'MarketSegmentNameforReport', 'MarketPrfxNameforReport', 'RateProgramCode',
                    'RateProgramName', 'RateProgramTier', 'RateCategoryCode', 'MarketCode', 'OpportunityNum', 'QuoteNum', 'LengthofAccomLOA',
                    'LOATierID', 'NightsOut', 'NightsOutTier', 'ChannelAggregateName', 'ChannelDetailName', 'ChannelTypeName', 'ChannelSiteName',
                    'ChannelPartnerName', 'BookingSourceType', 'BookingOffice', 'IntermediaryTypeCode', 'Intermediary', 'RoomCategoryCode', 'RoomPoolCode',
                    'MARSHARateNet', 'RewardsLevelText', 'RoomNights', 'ADRNet', 'RevenueNet', 'AverageRoomNights', 'AverageRevenue']
    read.to_csv(f'{folder_name}{property_code}_Reservation.csv', index=False, header=headers_list)
    if os.path.exists(filename):
        os.remove(filename)
    print(f"{property_code} Reservation file write success")


def handle_request(request):
    payload = request

    session = None
    try:
        session = get_session(payload['gcp_secret'], payload['external_property_code'], payload['propertyCode'])
        print("SESSOIN : :", session)
        if type(session) is Exception:
            return session
        process_report(session, payload)

    except (InvalidRequestException, InvalidReportException, FailedLoginException) as e:
        reason = str(e)
        print(reason)

    return session
