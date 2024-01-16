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
    forecast_file = f'{folder_name}{property_code}_Forecast.csv'
    if os.path.exists(forecast_file):
        os.remove(forecast_file)
    filename = f'{folder_name}{property_code}_Forecast.xlsx'
    open(filename, "wb").write(csv)
    read = pd.read_excel(filename, skiprows=8)
    createdAt = "'" + str(arrow.now()) + "'"
    updatedAt = "'" + str(arrow.now()) + "'"
    createdAtEpoch =  int(arrow.utcnow().timestamp())
    updatedAtEpoch =  int(arrow.utcnow().timestamp())

    columns = ['ArrivalDate', 'ArrivalDay', 'WEindicator', 'Capacity', 'DaysOut', 'SysRemDem', 'RemDemSTLY', 'AppliedRemDem', 'UserOverride',
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
               'RoomCollection2RemDemTY', 'RoomCollection2RemDemTYPer', 'RoomCollection2RemDemSTLY', 'RoomCollection2RemDemSTLYPer',
               'RoomCollection3HurdleRevenue', 'RoomCollection3TransientOTBTY', 'RoomCollection3TransientOTBSTLY', 'RoomCollection3RemDemTY', 'RoomCollection3RemDemTYPer', 'RoomCollection3RemDemSTLY', 'RoomCollection3RemDemSTLYPer']

    compairing_columns = ['Arrival Date', 'Arrival Day', 'WE indicator  ', 'Capacity', 'Days Out',
                          'Sys Rem Dem', 'Rem Dem STLY', 'Applied Rem Dem', 'User Override',
                          'Event', 'OYv2 Total Hotel Occupancy',
                          'Total Available Transient Supply', 'Total Transient Booked',
                          'Total Transient Booked STLY', 'Additional Demand',
                          'Additional Demand STLY', 'Total Transient Demand',
                          'Total Transient Demand STLY', 'Allotment Forecast',
                          'Allotment Rooms Sold', 'Out of Order', 'Group Room Block Authorized',
                          'Group Booked', 'OYv2 Group Proj', 'Contract Room Block Authorized',
                          'Contract Booked', 'OYv2 Contract Proj', 'Complimentary Booked TY',
                          'Rem Dem Premium Retail TY #', 'Rem Dem Premium Retail TY %',
                          'Premium Retail TY', 'Rem Dem Premium Retail STLY #',
                          'Rem Dem Premium Retail STLY %', 'Premium Retail STLY',
                          'Rem Dem Standard Retail TY #', 'Rem Dem Standard Retail TY %',
                          'Standard Retail TY', 'Rem Dem Standard Retail STLY #',
                          'Rem Dem Standard Retail STLY %', 'Standard Retail STLY',
                          'Rem Dem Special Corporate (LRA) TY #',
                          'Rem Dem Special Corporate (LRA) TY %', 'Special Corporate (LRA) TY',
                          'Rem Dem Special Corporate (LRA) STLY #',
                          'Rem Dem Special Corporate (LRA) STLY %',
                          'Special Corporate (LRA) STLY', 'Rem Dem Reward Redemption TY #',
                          'Rem Dem Reward Redemption TY %', 'Reward Redemption TY',
                          'Rem Dem Reward Redemption STLY #', 'Rem Dem Reward Redemption STLY %',
                          'Reward Redemption STLY', 'Rem Dem Policy Driven TY #',
                          'Rem Dem Policy Driven TY %', 'Policy Driven TY',
                          'Rem Dem Policy Driven STLY #', 'Rem Dem Policy Driven STLY %',
                          'Policy Driven STLY', 'Rem Dem Govt/Military TY #',
                          'Rem Dem Govt/Military TY %', 'Govt/Military TY',
                          'Rem Dem Govt/Military STLY #', 'Rem Dem Govt/Military STLY %',
                          'Govt/Military STLY', 'Rem Dem Retail Tied Discount  TY #',
                          'Rem Dem Retail Tied Discount  TY %', 'Retail Tied Discount  TY',
                          'Rem Dem Retail Tied Discount  STLY #',
                          'Rem Dem Retail Tied Discount  STLY %', 'Retail Tied Discount  STLY',
                          'Rem Dem Fixed Discount TY #', 'Rem Dem Fixed Discount TY %',
                          'Fixed Discount TY', 'Rem Dem Fixed Discount STLY #',
                          'Rem Dem Fixed Discount STLY %', 'Fixed Discount STLY',
                          'Room Collection 1 Hurdle Revenue',
                          'Room Collection 1 Transient OTB TY',
                          'Room Collection 1 Transient OTB STLY', 'Room Collection 1 Rem DemTY #',
                          'Room Collection 1 Rem Dem TY %', 'Room Collection 1 Rem Dem STLY #',
                          'Room Collection 1 Rem Dem STLY %', 'Room Collection 2 Hurdle Revenue',
                          'Room Collection 2 Transient OTB TY',
                          'Room Collection 2 Transient OTB STLY', 'Room Collection 2 Rem DemTY #',
                          'Room Collection 2 Rem Dem TY %', 'Room Collection 2 Rem Dem STLY #',
                          'Room Collection 2 Rem Dem STLY %', 'Room Collection 3 Hurdle Revenue',
                          'Room Collection 3 Transient OTB TY',
                          'Room Collection 3 Transient OTB STLY', 'Room Collection 3 Rem DemTY #',
                          'Room Collection 3 Rem Dem TY %', 'Room Collection 3 Rem Dem STLY #',
                          'Room Collection 3 Rem Dem STLY %']
    for column in compairing_columns:
        if column not in read.columns:
            read[column] = 0

    read.columns = columns
    read.dropna(subset=['ArrivalDate'], inplace=True)
    read.insert(0, column="propertyCode", value=payload['propertyCode'])
    read.insert(1, column="pullDateId", value=payload['pullDateId'])
    read.insert(2, column="createdAt", value=createdAt)
    read.insert(3, column="updatedAt", value=updatedAt)
    read.insert(4, column="createdAtEpoch", value=createdAtEpoch)
    read.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
    read['ArrivalDate'] = pd.to_datetime(read['ArrivalDate']).dt.date
    read.insert(6, column="uniqueKey", value=read["propertyCode"].astype(str) + "_" + read['ArrivalDate'].astype(str)) 
    read.to_csv(f'{folder_name}{property_code}_Forecast.csv', index=False)
    if os.path.exists(filename):
        os.remove(filename)
    print(f"{property_code} Forecast file write success")


# cloud function entry point
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
