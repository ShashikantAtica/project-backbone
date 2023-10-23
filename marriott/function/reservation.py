import os

import pandas as pd

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
    payload[
        'reservation_report_url'] = "https://mrdw.ca.analytics.ibm.com/bi/v1/disp?updatedco=false&run.outputFormat=spreadsheetML&p_dRP=1&p_PubC=&p_dCorN=1&p_dADRS=0&p_fNO=&p_dRC=1&p_ADR=1&p_dIT=1&p_dRL=1&p_dADRF=&p_PP=Phys&p_RNS=1&p_dCD=1&p_dBKD=1&p_dLOA=1&p_fAST=&p_dRtC=1&p_dCA=1&p_dProp=1&p_dIID=1&p_dTNO=1&p_dBST=1&p_RptS=DAT3&p_fADmR=&p_dADRFO=&p_dSD=YR&p_dSD=MN&p_dSD=WC&p_dSD=DOW&p_dSD=SD&p_dTRtp=1&p_fWC=&p_dBO=1&p_security=1&p_AVP=&p_dSeg=1&p_dRTMF=&p_fCS=&p_fCP=&p_BDS=&p_MktCat=100&p_dBrand=1&p_fCT=&b_action=cognosViewer&p_dTLOA=1&p_REV=1&p_CDE1=20240331&p_dQN=1&p_fMT=&p_dAST=1&p_dHtlType=1&cv.toolbar=false&p_fPCtry=&p_dNO=1&p_WE1=6&p_fRP=KING&p_fRP=KSTE&p_fRP=QNQN&p_fRP=QQST&p_WE2=7&p_dRVF=&p_TFName=&p_fRL=&p_EID=bpate289&p_Prop=DFWFI&p_Currency=Local&p_dRVS=0&run.prompt=false&p_fIT=&p_PorM=&p_dRVFO=&p_dRNF=&p_fRC=&p_ARV=1&p_BDE=&p_dON=1&p_fCA=&p_dPubC=1&p_dRNFO=&p_dADMF=&p_dADMS=0&p_fBrand=&p_dTMFO=&p_AsOfDate=&p_dRNS=0&p_TD=0&p_dPCtry=1&encoding=UTF-8&p_Seg=Rtp&p_NetGross=Net&p_CDS1=20200101&p_dGblReg=1&p_dWC=0&p_ARN=1&p_fTLOA=&p_dGblDiv=1&cv.header=false&p_dAVP=1&p_TDS=0&p_CT2=11&p_CT3=29&p_CT1=4&p_fTRtp=&p_dADmR=0&p_dCT=1&p_dADMFO=&p_dCS=1&p_dRpn=1&p_dCP=1&p_fTNO=&ui.object=%2fcontent%2ffolder%5b%40name%3d%27MRDW%27%5d%2ffolder%5b%40name%3d%27Release%27%5d%2ffolder%5b%40name%3d%27Demand%27%5d%2ffolder%5b%40name%3d%27Analysis%27%5d%2freport%5b%40name%3d%27DAT3%27%5d&p_ADmnd=0&ui.action=run&p_fGblReg=&p_fRtC=&p_fBST=&p_fHtlType=&p_dSRT=0&p_dMarRt=1&p_dMT=1&p_fCorN=&p_fGblDiv=&p_fLOA=&p_fMarRtFrm=0&p_fMarRtTo=999999999&p_fSeg=&p_CWED=0&p_fDOW=&p_dMC=1&p_ropt=xls&p_MyC="
    report_query, download_uri = export(session, payload['reservation_report_url'], start_date, end_date)
    if download_uri is None:
        raise Exception('Report could not be exported!')
    csv = download(session, report_query, download_uri)
    folder_name = "./reports/"
    filename = f'{folder_name}{property_code}_Reservation.xlsx'
    open(filename, "wb").write(csv)
    read = pd.read_excel(filename)
    read.insert(0, column="propertyCode", value=payload['propertyCode'])
    read.insert(1, column="pullDateId", value=payload['pullDateId'])
    read['Stay Date'] = pd.to_datetime(read['Stay Date']).dt.date
    read['Booking Date'] = pd.to_datetime(read['Booking Date']).dt.date
    headers_list = ['propertyCode', 'pullDateId', 'StayYearCalendar', 'StayYYYYMM', 'CustomWDWE', 'StayDOWText', 'StayDate', 'ArrivalIndicator', 'BookingDate',
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
        session = get_session(payload['gcp_secret'], payload['external_property_code'])
        process_report(session, payload)

    except (InvalidRequestException, InvalidReportException, FailedLoginException) as e:
        reason = str(e)
        print(reason)

    return session
