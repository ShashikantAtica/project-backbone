from bs4 import BeautifulSoup as bs
from utils.log import log
import arrow

logger = log.get_logger('marriott.queue')
log.set_context('marriott.queue')


def is_reservation_ready(session):
    domain_name = "https://extranetcloud.marriott.com/MRDWWebApp/mrdw"
    # home
    a = session.get(f"{domain_name}/mrdwHome.do")
    logger.info(f'{a.url} [{a.status_code}]')
    # # demand tab
    b = session.get(f"{domain_name}/mrdwDemand.do")
    logger.info(f'{b.url} [{b.status_code}]')
    # # demand analysis
    c = session.get(f"{domain_name}/demSegHome.action")
    logger.info(f'{c.url} [{c.status_code}]')
    # add new property report
    # browser 'Add New Property Report' button calls /demSegPropReportSelect_input.action
    # and redirects here
    date_check_url = f"{domain_name}/demSegPropReportSelect_input.action"
    d = session.get(date_check_url)
    logger.info(f'{d.url} [{d.status_code}]')

    soup = bs(d.content, "html.parser")
    try:
        date_text = soup.find(id="searchForm")
        step_1 = date_text.find("fieldset")
        step_2 = step_1.find_all("div")[0]
        step_3 = step_2.find_all("div")[1]
        step_4 = step_3.find_all("label")[1].string
    except:
        raise Exception('Date element not found')

    # step_4 - November 21, 2022
    captured_date = arrow.get(step_4, "MMMM D, YYYY")
    logger.info(captured_date.format('YYYY-MM-DD'))
    yesterday_date = arrow.now('America/Chicago').shift(days=-1)
    logger.info(yesterday_date.format('YYYY-MM-DD'))

    return captured_date.date() == yesterday_date.date()


def is_forecast_ready(session, spider_property_code):
    propertycode = spider_property_code[2:]
    property_url = (f'https://salesnet.marriott.com/rmsplatform/oneyield'
                    f'/OysController/signIn/submit?accessLevel=property'
                    f'&entityCode={propertycode}&updatePrivilege=false'
                    f'&requestProfileName=home&accessLevelRadio=property'
                    f'&propertyCodeText={propertycode}'
                    f'&propertyRecentAccessComboBox={propertycode}')
    a = session.get(property_url)

    soup = bs(a.content, "html.parser")
    date_b_tag = soup.find_all("td", {"nowrap": True})

    printed_date = list(date_b_tag[0].strings)[3]
    final_date = printed_date.split(",")[1]

    # Tue 11-22-2022
    captured_date = arrow.get(final_date.strip(), "ddd MM-DD-YYYY")
    today = arrow.now()

    return captured_date.date() == today.date()
