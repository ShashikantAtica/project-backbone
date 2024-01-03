import requests
import re
import sys
import ast

from sqlalchemy import text

sys.path.append("..")
import arrow
from utils.secrets.SecretManager import get_secret_from_api as get_secret_dict
from bs4 import BeautifulSoup as bs
from utils.db import db_config


class FailedLoginException(Exception):
    pass


def get_session(gcp_secret, propertyCode, atica_propertyCode):
    session = _login(gcp_secret, propertyCode, atica_propertyCode)
    if type(session) is Exception:
        return session
    return session


def _login(secret_name, propertyCode, atica_propertyCode):
    try:
        platform = "PMS"
        secret = get_secret_dict(atica_propertyCode, platform)
        if secret is None:
            raise Exception("API response is none")
        start = arrow.now()
        session, challenge = _submit_login(secret['u'], secret['p'])

        if challenge == None:
            raise FailedLoginException("Failed to produce login challenge")
        else:
            # ex. challenge['lookups'] -> ['[C3]', '[E4]', '[J7]']
            security_code = lookup(challenge['lookups'], propertyCode)
            print("security_code :", security_code)

        if security_code is None:
            raise FailedLoginException(f"Could not produce security code from {challenge}")

        _login_challenge(session, challenge['path'], security_code)
        return session
    except Exception as e:
        return e


def lookup(challenges, propertyCode):
    propertyCode = "'" + propertyCode + "'"

    query_string = f'SELECT marriott_json FROM public.tbl_properties where "externalPropertyCode" = {propertyCode};'
    conn = db_config.get_db_connection()
    res = conn.execute(text(query_string))
    result = res.fetchall()
    columns = res.keys()
    results_as_dict = [dict(zip(columns, row)) for row in result]
    conn.close()
    chart = []
    for i in results_as_dict:
        chart.append(i['marriott_json'])

    chart_dict = ast.literal_eval(chart[0])
    values_of_challenges = []

    for challenge in challenges:
        letter = challenge[0]  # Extract the letter part of the challenge
        number = int(challenge[1]) - 1  # Extract the number part and adjust to zero-based index
        value = chart_dict[letter][str(number)]  # Retrieve the value from chart_dict
        values_of_challenges.append(value)

    print("Values of challenges:", values_of_challenges)
    return values_of_challenges


def _submit_login(user_id, password):
    with requests.session() as session:
        _get_slash_mrdw(session)
        sso_params = _get_sso_ping(session)
        ping_uri = _post_idp_sso(session, sso_params)
        challenge = _post_sso(session, user_id, password, ping_uri)

        return session, challenge


def _login_challenge(session, challenge_path, lookup_results):
    _post_sso_table(session, challenge_path, lookup_results)
    _get_mrdw_fsso(session)


def _get_slash_mrdw(session):
    url = 'https://extranet.marriott.com/mrdw/'

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
    print("GET /mrdw/ [" + str(r.status_code) + "]")


def _get_sso_ping(session):
    url = 'https://extranet.marriott.com/userauth/sp/startSSO.ping?PartnerIdpId=ISAM-EXT-IDP-CONN-MFA&TargetResource=https%3A%2F%2Fextranet.marriott.com%2Fmrdw'

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
        'Referer': 'https://extranet.marriott.com/mrdw',
        'accept-language': 'en-US,en;q=0.9'
    }

    r = session.get(url, headers=headers)
    print("GET /userauth/sp/startSSO.ping [" + str(r.status_code) + "]")

    saml = re.search('name="SAMLRequest" value="([^"]+)"', r.text)[1]
    relay = re.search('name="RelayState" value="([^"]+)"', r.text)[1]

    return {
        'SAMLRequest': saml,
        'RelayState': relay
    }


# return login form action generated for each request
def _post_idp_sso(session, sso_params):
    url = 'https://extranet.marriott.com/userauth/idp/SSO.saml2'

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
        'Referer': 'https://extranet.marriott.com/',
        'accept-language': 'en-US,en;q=0.9'
    }

    r = session.post(url, headers=headers, data=sso_params)
    print("POST /userauth/idp/SSO.saml2 [" + str(r.status_code) + "]")

    soup = bs(r.content, "html.parser")
    scripts = soup.find_all('script')

    ping_uri = ''
    for script in scripts:
        try:
            ping_uri = script.string.split('var PingbaseURL = PingURL.concat("', 1)[-1].split('");', 1)[0]
        except Exception as e:
            print(e)
            ping_uri = ''

        # ex /idp/N2C95/resumeSAML20/idp/SSO.ping
        if 'resumeSAML' in ping_uri:
            break

    return ping_uri


def _post_sso(session, user_id, password, ping_uri):
    url = "https://extranetcloud.marriott.com/userauth" + ping_uri
    print(url)

    headers = {
        'Sec-Ch-Ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document',
        'accept-language': 'en-US,en;q=0.9',
        'Referer': 'https://extranetcloud.marriott.com/2FAmarrsso/idp/eyJ2c2lkIjoidXJuOm1nc2Nsb3VkLm1hcnJpb3R0LmNvbSJ9/SSO.saml2',
        'Origin': 'https://extranetcloud.marriott.com'
    }

    params = {
        "pf.username": user_id,
        "pf.pass": password,
        "pf.ok": "",
        "pf.cancel": "",
        "pf.alternateAuthnSystem": "",
        "pf.adapterId": "HTMLFormAuthAdapter"
    }

    r = session.post(url, headers=headers, data=params)

    print("POST /userauth/idp/wYn9b/resumeSAML20/idp/SSO.ping [" + str(r.status_code) + "]")
    if 'Authentication failed' in r.text:
        raise FailedLoginException('Authentication failed')
    elif 'temporarily locked' in r.text:
        raise FailedLoginException('Temporary account lock')

    soup = bs(r.content, "html.parser")
    chlng = soup.find_all('b')
    formURL = soup.find('form', {'id': 'authenticate', 'method': 'post'})
    action_value = formURL.get('action')
    # url_re = re.findall(r'<b>', str(soup))
    # print(url_re)
    if chlng is None:
        raise FailedLoginException('Cannot find challenge path')

    path = [i.text for i in chlng]
    path.remove('Printed Grid:')
    challenge = {
        'serial_no': user_id,
        'lookups': path,
        'path': action_value
    }
    print(challenge)
    # try:
    #     message_ps = soup.find_all("p", {"id": "message"})
    # except Exception as e:
    #     print(e)
    #     raise FailedLoginException('Failed login')

    # determine auth method
    # try:
    #     b_tags = soup.find_all("b")[2]
    # except Exception as e:
    #     print(e)
    #     raise FailedLoginException('Failed login')

    # 2FA / OTP
    # if b_tags.text == "Passcode:":
    #     challenge['lookups'] = ''
    #
    # # Lookups
    # elif len(message_ps) > 0:
    #     letters = soup.find_all("div", id=re.compile(r"challenge[\d]"))
    #     challenge['lookups'] = [letter.string.strip() for letter in letters]
    #
    # # Fail
    # else:
    #     challenge = None

    return challenge


def _post_sso_table(session, challenge_path, lookup_results):
    result = ''.join(lookup_results)
    url = "https://extranetcloud.marriott.com" + challenge_path

    headers = {
        'Sec-Ch-Ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document',
        'Accept-Language': 'en-US,en;q=0.9'
    }

    params = {
        "passcode": result
    }

    r = session.post(url, headers=headers, data=params)
    print("POST /userauth/idp/wYn9b/resumeSAML20/idp/SSO.ping [" + str(r.status_code) + "]")


def _get_mrdw_fsso(session):
    url = 'https://extranetcloud.marriott.com/MRDWWebApp/fsso.html'

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://extranetcloud.marriott.com/mrdw',
        'Sec-Ch-Ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    }

    session.max_redirects = 10
    try:
        r = session.get(url, headers=headers)
    except requests.exceptions.TooManyRedirects as e:
        r = e.response
        for path in r.history:
            print(f'{path.url} [{path.status_code}]')
        raise e

    # set back to default
    session.max_redirects = 30
    print("GET /mrdw/MRDWWebApp/fsso.html [" + str(r.status_code) + "]")
    return r
