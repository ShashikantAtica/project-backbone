import re
import json
import time
import arrow
from bs4 import BeautifulSoup as bs
import urllib.parse as urlparse


def export(session, report_uri, start_date, end_date):
    report_query = _new_report_query(report_uri, start_date, end_date)

    disp = _get_disp(session, report_query)

    if disp['login'] == True:
        legacy_url = _get_legacy_login(session, disp['login_uri'])
        legacy_auth = _post_bi_login(session, legacy_url)
        oauth_ref, oauth_code = _get_authorization_oauth2(session, legacy_auth)
        _get_bi_login(session, oauth_ref, oauth_code, report_query)
        disp = _get_disp(session, report_query)

    if "m_sTracking" in disp['state']:
        poll_status, download_uri = _get_disp_poll(session, disp['state'])
        print("First poll status: " + poll_status)

        while poll_status in ["working", "stillWorking"]:
            print("Waiting for report...")
            time.sleep(1)
            poll_status, download_uri = _get_disp_poll(session, disp['state'])
            print("Next poll status: " + poll_status)
            if download_uri is not None:
                print("Download ready: " + download_uri)
            time.sleep(1)

        return report_query, download_uri
    else:
        return report_query, None


def download(session, report_query, download_uri):
    url = "https://mrdw.ca.analytics.ibm.com" + download_uri

    ref = "https://mrdw.ca.analytics.ibm.com/bi/v1/disp?" \
          + urlparse.urlencode(report_query, doseq=True)

    headers = {
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'Upgrade-Insecure-Requests': '1',
        'accept-language': 'en-US,en;q=0.9',
        'referer': ref,
        'dnt': '1'
    }

    r = session.get(url, stream=True, headers=headers)
    print("GET /bi/v1/disp?b_action=rc [" + str(r.status_code) + "]")
    return r.content


def _get_disp(session, report_query):
    url = "https://mrdw.ca.analytics.ibm.com/bi/v1/disp"

    headers = {
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-US,en;q=0.9'
    }

    r = session.get(url, headers=headers, params=report_query)
    print("GET /bi/v1/disp [" + str(r.status_code) + "]")

    login_search = re.search('content="0; URL=([^"]+)"', r.text)

    if login_search == None:
        soup = bs(r.content, "html.parser")
        scripts = soup.find_all('script')

        json_a = re.compile(r'oCV\.initViewer\(\{.*?\}\);')
        matches_a = json_a.search(str(scripts))

        json_b = re.compile(r'\{.*?\}')
        matches_b = json_b.search(matches_a.group(0))

        json_str = re.sub(r'\\<', r'<', matches_b.group(0))
        json_str = re.sub(r'\\>', r'>', json_str)

        return {
            'login': False,
            'login_uri': None,
            'state': json.loads(json_str + " }")
        }
    else:
        return {
            'login': True,
            'login_uri': login_search[1],
            'state': None
        }


def _get_legacy_login(session, uri):
    url = "https://mrdw.ca.analytics.ibm.com" + uri

    headers = {
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-US,en;q=0.9'
    }

    r = session.get(url, headers=headers)
    print("GET /bi/?legacyLogin [" + str(r.status_code) + "]")

    return url


def _post_bi_login(session, legacy_url):
    url = "https://mrdw.ca.analytics.ibm.com/bi/v1/login"

    headers = {
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-US,en;q=0.9',
        'referer': legacy_url,
        'origin': 'https://mrdw.ca.analytics.ibm.com',
        'content-type': 'application/json; charset=UTF-8',
        'x-xsrf-token': session.cookies.get('XSRF-TOKEN', domain='mrdw.ca.analytics.ibm.com', path='/bi'),
        'x-requested-with': 'XMLHttpRequest'
    }

    r = session.post(url, headers=headers, data='{}')
    print("POST /bi/v1/login [" + str(r.status_code) + "]")

    return json.loads(r.text)


def _get_authorization_oauth2(session, legacy_auth):
    url = legacy_auth['promptInfo']['displayObjects'][0]['value']

    headers = {
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-US,en;q=0.9',
        'referer': 'https://mrdw.ca.analytics.ibm.com/',
    }

    r = session.get(url, headers=headers)
    print("GET /userauth/as/authorization.oauth2 [" + str(r.status_code) + "]")

    code = re.search('id="authCode" type="text/template">([^<]+)</script>', r.text)[1]

    return r.url, code


def _get_bi_login(session, ref, code, report_query):
    redirect_url = "https://mrdw.ca.analytics.ibm.com/bi/v1/disp?" \
                   + urlparse.urlencode(report_query, doseq=True)

    url = "https://mrdw.ca.analytics.ibm.com/bi/v1/login" \
          + "?h_CAM_action=logonAs&CAMNamespace=Ping2&isMobile=false&legacyLogin=true" \
          + "&redirectUrl=" + urlparse.quote(redirect_url, safe='') \
          + "&code=" + code

    headers = {
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
        'accept': '*/*',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'accept-language': 'en-US,en;q=0.9',
        'referer': ref,
        'x-requested-with': 'XMLHttpRequest',
        'x-xsrf-token': session.cookies.get('XSRF-TOKEN', domain='mrdw.ca.analytics.ibm.com', path='/bi'),
    }

    r = session.get(url, headers=headers)
    print("GET /bi/v1/login [" + str(r.status_code) + "]")


def _get_disp_poll(session, disp_state):
    url = "https://mrdw.ca.analytics.ibm.com/bi/v1/disp"

    params = {
        "b_action": "cognosViewer",
        "cv.actionState": disp_state["m_sTracking"],
        "cv.catchLogOnFault": "true",
        "cv.header": "false",
        "cv.id": "_NS_",
        "cv.objectPermissions": "execute read traverse",
        "cv.responseFormat": "data",
        "cv.showFaultPage": "true",
        "cv.toolbar": "false",
        "executionParameters": disp_state["m_sParameters"],
        "m_tracking": disp_state["m_sTracking"],
        "ui.action": "wait",
        "ui.cafcontextid": disp_state["m_sCAFContext"],
        "ui.conversation": disp_state["m_sConversation"],
        "ui.object": ("/content/folder[@name='MRDW']"
                      "/folder[@name='Release']/folder[@name='Demand']"
                      "/folder[@name='Analysis']/report[@name='DAT3']"),
        "ui.objectClass": "report",
        "ui.primaryAction": "run",
        "ui.routingServerGroup": "DAT_MRDW"
    }

    r = session.post(url, data=params)
    print("GET /bi/v1/disp [" + str(r.status_code) + "]")

    soup = bs(r.content, "html.parser")

    # State
    state_str = soup.xml.state.string
    state_str = re.sub(r'\\<', r'<', state_str)
    state_str = re.sub(r'\\>', r'>', state_str)
    state = json.loads(state_str)

    if state['m_sStatus'] == "complete":
        scripts = soup.find_all('script')
        json_a = re.compile(r"var sURL = '.*?';")
        json_b = re.compile(r"'.*?'")

        matches_a = json_a.search(str(scripts))
        matches_b = json_b.search(matches_a.group(0))

        return state['m_sStatus'], matches_b.group(0)[1:-1]
    else:
        return state['m_sStatus'], None


def _new_report_query(original_uri, start_date, end_date):
    parsed = urlparse.urlparse(original_uri)
    query = urlparse.parse_qs(parsed.query)

    query["p_CDS1"] = [start_date.format('YYYYMMDD')]
    query["p_CDE1"] = [end_date.format('YYYYMMDD')]

    return query
