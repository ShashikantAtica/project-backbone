import os

import arrow
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
def get_report(session, external_property_code, platform, start_date, end_date):
    print(f'get_report for {external_property_code}')
    propertycode = external_property_code

    property_url = (f'https://salesnetcloud.marriott.com/{platform}/oneyield'
                    f'/OysController/signIn/submit?accessLevel=property'
                    f'&entityCode={propertycode}&updatePrivilege=false'
                    f'&requestProfileName=home&updatePrivilege=false'
                    f'&requestProfileName=home&accessLevelRadio=property'
                    f'&propertyCodeText={propertycode}'
                    f'&propertyRecentAccessComboBox={propertycode}')

    c = session.get(property_url, timeout=120)

    e_payload = {
        "startdate": start_date.format("YYYY-MM-DD"),
        "enddate": end_date.format("YYYY-MM-DD"),
        "datadate": arrow.now().shift(days=-1).format("YYYY-MM-DD")
    }
    extract_url = (f'https://salesnetcloud.marriott.com/{platform}/forecastui/api'
                   f'/oyextract/v2/properties/{propertycode}/runs/latest/stays')

    extract = session.get(extract_url, params=e_payload, timeout=240)

    if extract.status_code >= 400:
        raise RetryException(f"Server error")

    if not extract.ok:
        raise RetryException(f"Forecast report failed")

    if len(extract.content) == 0 or extract.headers['Content-Type'] != 'application/vnd.ms-excel':
        raise RetryException(f"Report pulled with 0kb from {platform}. Retrying..")

    return extract.content


def process_report(session, payload):
    property_code = payload['external_property_code']
    print(f"start for {property_code}")
    start_date = payload['fore_before']
    end_date = payload['fore_after']
    csv = get_report(session, property_code, payload['forecast_platform'], start_date, end_date)
    folder_name = "./reports/"
    filename = f'{folder_name}{property_code}_Forecast.xlsx'
    open(filename, "wb").write(csv)
    read = pd.read_excel(filename, skiprows=8)
    read.insert(0, column="propertyCode", value=payload['propertyCode'])
    read.insert(1, column="pullDateId", value=payload['pullDateId'])
    read['Arrival Date'] = pd.to_datetime(read['Arrival Date']).dt.date
    headers_list = ['propertyCode', 'pullDateId', 'ArrivalDate', 'ArrivalDay', 'WEindicator', 'Capacity', 'DaysOut', 'SysRemDem', 'RemDemSTLY', 'AppliedRemDem', 'UserOverride',
                    'Event', 'OYv2TotalHotelOccupancy', 'TotalAvailableTransientSupply', 'TotalTransientBooked', 'TotalTransientBookedSTLY', 'AdditionalDemand', 'AdditionalDemandSTLY',
                    'TotalTransientDemand', 'TotalTransientDemandSTLY', 'AllotmentForecast', 'AllotmentRoomsSold', 'OutofOrder', 'GroupRoomBlockAuthorized', 'GroupBooked',
                    'OYv2GroupProj', 'ContractRoomBlockAuthorized', 'ContractBooked', 'OYv2ContractProj', 'ComplimentaryBookedTY', 'RemDemPremiumRetailTY', 'RemDemPremiumRetailTYPer',
                    'PremiumRetailTY', 'RemDemPremiumRetailSTLY', 'RemDemPremiumRetailSTLYPer', 'PremiumRetailSTLY', 'RemDemStandardRetailTY', 'RemDemStandardRetailTYPer',
                    'StandardRetailTY', 'RemDemStandardRetailSTLY', 'RemDemStandardRetailSTLYPer', 'StandardRetailSTLY', 'RemDemSpecialCorporateLRATY', 'RemDemSpecialCorporateLRATYPer',
                    'SpecialCorporateLRATY', 'RemDemSpecialCorporateLRASTLY', 'RemDemSpecialCorporateLRASTLYPer', 'SpecialCorporateLRASTLY', 'RemDemRewardRedemptionTY',
                    'RemDemRewardRedemptionTYPer', 'RewardRedemptionTY', 'RemDemRewardRedemptionSTLY', 'RemDemRewardRedemptionSTLYPer', 'RewardRedemptionSTLY', 'RemDemPolicyDrivenTY',
                    'RemDemPolicyDrivenTYPer', 'PolicyDrivenTY', 'RemDemPolicyDrivenSTLY', 'RemDemPolicyDrivenSTLYPer', 'PolicyDrivenSTLY', 'RemDemGovtMilitaryTY',
                    'RemDemGovtMilitaryTYPer', 'GovtMilitaryTY', 'RemDemGovtMilitarySTLY', 'RemDemGovtMilitarySTLYPer', 'GovtMilitarySTLY', 'RemDemRetailTiedDiscountTY',
                    'RemDemRetailTiedDiscountTYPer', 'RetailTiedDiscountTY', 'RemDemRetailTiedDiscountSTLY', 'RemDemRetailTiedDiscountSTLYPer', 'RetailTiedDiscountSTLY',
                    'RemDemFixedDiscountTY', 'RemDemFixedDiscountTYPer', 'FixedDiscountTY', 'RemDemFixedDiscountSTLY', 'RemDemFixedDiscountSTLYPer', 'FixedDiscountSTLY',
                    'RoomCollection1HurdleRevenue', 'RoomCollection1TransientOTBTY', 'RoomCollection1TransientOTBSTLY', 'RoomCollection1RemDemTY', 'RoomCollection1RemDemTYPer',
                    'RoomCollection1RemDemSTLY', 'RoomCollection1RemDemSTLYPer', 'RoomCollection2HurdleRevenue', 'RoomCollection2TransientOTBTY', 'RoomCollection2TransientOTBSTLY',
                    'RoomCollection2RemDemTY', 'RoomCollection2RemDemTYPer', 'RoomCollection2RemDemSTLY', 'RoomCollection2RemDemSTLYPer']
    read.to_csv(f'{folder_name}{property_code}_Forecast.csv', index=False, header=headers_list)
    if os.path.exists(filename):
        os.remove(filename)
    print(f"{property_code} Forecast file write success")


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
