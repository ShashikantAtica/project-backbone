import os
import pandas as pd
from bs4 import BeautifulSoup as bs
import re
import time
import arrow
from marriott.utils.login import lookup
from utils.secrets.SecretManager import get_secret_from_api as get_secret_dict
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service


def get_total_yield_report_url(payload):
    try:
        platform = "PMS"
        secret = get_secret_dict(payload['propertyCode'], platform)
        if secret is None:
            raise Exception("API response is none")
        username = secret['u']
        password = secret['p']
        external_property_code = payload['external_property_code']
        platform = payload['forecast_platform']
        start_date = payload['fore_before']

        folder_name = "./reports/"
        save_dir = os.path.abspath('reports/')
        driver = None
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--hide-scrollbars')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--window-size=1280,1000")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": save_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        service = Service('../chromedriver.exe')
        driver = webdriver.Chrome(options=chrome_options, service=service)
        driver.maximize_window()

        url = "https://mgs.marriott.com/"
        print('getting home')
        driver.get(url)
        WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.XPATH, '/html/body/table/tbody/tr[3]/td/div/form/div[1]/a')))
        driver.find_element(By.XPATH, '/html/body/table/tbody/tr[3]/td/div/form/div[1]/a').click()
        print('entering creds')
        WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.ID, 'username')))
        driver.find_element(By.ID, 'username').send_keys(username)
        driver.find_element(By.ID, 'password').send_keys(password)
        driver.find_element(By.ID, 'SMBT').click()
        print("login form success")
        time.sleep(5)

        if driver.current_url.split("/")[3] == "userauth":
            time.sleep(5)
            content = driver.page_source
            if 'Authentication failed' in content:
                raise Exception('Authentication failed')
            elif 'temporarily locked' in content:
                raise Exception('Temporary account lock')

            soup = bs(content, "html.parser")
            chlng = soup.find_all('b')
            formURL = soup.find('form', {'id': 'authenticate', 'method': 'post'})

            path = [i.text for i in chlng]
            path.remove('Printed Grid:')
            print("path : ", path)
            lookup_results = lookup(path, external_property_code)

            inputs = driver.find_elements(By.XPATH, '//input[@id="text"]')
            for i, j in zip(inputs, lookup_results):
                i.send_keys(j)

            driver.find_element(By.XPATH, '//button[@id="sign-on"]').click()
            print("login chart Success")
            time.sleep(15)

            # Total Yield Start
            print("Getting the One Yield Page")
            driver.get(f'https://salesnet.marriott.com/{platform}/oneyield/OysController/signIn')

            print("Signing in the Property")
            WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.ID, 'propertyCodeText')))
            driver.find_element(By.ID, 'propertyCodeText').click()
            driver.find_element(By.ID, 'propertyCodeText').send_keys(external_property_code)
            driver.find_element(By.ID, 'propertyCodeText').send_keys(Keys.ENTER)
            time.sleep(1)

            print("Getting the Total Yield Report")
            WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.LINK_TEXT, 'Forecast')))
            driver.find_element(By.LINK_TEXT, 'Forecast').click()
            WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.LINK_TEXT, 'Total Hotel Calendar')))
            driver.find_element(By.LINK_TEXT, 'Total Hotel Calendar').click()
            WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.ID, 'ty_thc_startDate')))
            time.sleep(10)
            driver.find_element(By.ID, 'ty_thc_startDate').send_keys(Keys.ENTER)
            driver.find_element(By.ID, 'ty_thc_startDate').send_keys(Keys.CONTROL + "A")
            driver.find_element(By.ID, 'ty_thc_startDate').clear()
            driver.find_element(By.ID, 'ty_thc_startDate').send_keys(start_date.format('DD MMM YYYY'))
            time.sleep(1)
            driver.find_element(By.ID, 'ty_thc_startDate_submit').click()
            WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.ID, 'availability.ty.thc.hotelview.summaryGrid')))

            print("Total Yield Report Generated")
            filepath = os.path.join(f'{folder_name}Report.xls')
            if os.path.exists(filepath):
                os.remove(filepath)
            driver.find_element(By.ID, 'availability.thc.hotelview.exportButton').click()
            while not os.path.exists(filepath):
                time.sleep(1)
            time.sleep(5)
            print("Total Yield Report Exported Successfully")
            driver.quit()

            createdAt = "'" + str(arrow.now()) + "'"
            updatedAt = "'" + str(arrow.now()) + "'"
            createdAtEpoch =  int(arrow.utcnow().timestamp())
            updatedAtEpoch =  int(arrow.utcnow().timestamp())

            print("Total Yield Report Modification")
            df = pd.read_excel(filepath, header=None, skiprows=5)
            pivoted_df = df.T
            pivoted_df = pivoted_df.dropna(axis=1, how='all')
            pivoted_df.iloc[0, 0] = "Date"
            pivoted_df.insert(0, column="propertyCode", value=payload['propertyCode'])
            pivoted_df.insert(1, column="pullDateId", value=payload['pullDateId'])
            pivoted_df.insert(2, column="createdAt", value=createdAt)
            pivoted_df.insert(3, column="updatedAt", value=updatedAt)
            pivoted_df.insert(4, column="createdAtEpoch", value=createdAtEpoch)
            pivoted_df.insert(5, column="updatedAtEpoch", value=updatedAtEpoch)
            # pivoted_df.insert(6, column="uniqueKey", value=pivoted_df["propertyCode"].astype(str) + "_" + pivoted_df['Date'].astype(str)) 
            headers = ["propertyCode", "pullDateId", "createdAt", "updatedAt", "createdAtEpoch", "updatedAtEpoch", "Date", "Sleeping_Room_Occ_Per", "Function_Space_Occ_Per", "Sleeping_Rooms_Projected", "Transient", "inContract",
                       "Definite", "Tentative_1", "Tentative_2", "Hold", "Prospect", "To_Be_s", "Out_of_Order_Rooms",
                       "MARSHA_Booked_Group", "MARSHA_Blocked_Group", "Group_Restrictions_Hotel", "Restriction_Threshold_Hotel"]
            pivoted_df.columns = headers
            final_df = pivoted_df.iloc[1:]
            final_df.reset_index(drop=True, inplace=True)
            final_df.loc[:, 'Date'] = pd.to_datetime(final_df['Date'].replace('\n', ' '), format='%a %b %d').dt.strftime(f'{arrow.now().format("YYYY")}-%m-%d')
            final_df.insert(6, column="uniqueKey", value=final_df["propertyCode"].astype(str) + "_" + final_df['Date'].astype(str)) 
            final_df.to_csv(os.path.join(f'{folder_name}{external_property_code}_Total_Yield.csv'), index=False)

            print("Total Yield Report Modified Successfully")
            if os.path.exists(filepath):
                os.remove(filepath)
        return "Success"
            #  Total Yield End
    except Exception as e:
        return e
