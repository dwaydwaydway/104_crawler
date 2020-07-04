import argparse
import logging
import traceback
import sys
import subprocess
import threading
import json
import copy
import warnings
from pathlib import Path
from datetime import date
from multiprocessing import Pool

import pandas as pd
from box import Box
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as expected
from bs4 import BeautifulSoup
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
warnings.filterwarnings("ignore")
error_log = []
visualize = False
threadLocal = threading.local()


def get_driver_wait():
    driver = getattr(threadLocal, 'driver', None)
    wait = getattr(threadLocal, 'wait', None)
    if driver is None or wait is None:
        if visualize:
            driver = webdriver.Firefox()
        else:
            options = Options()
            options.add_argument('-headless')
            driver = webdriver.Firefox(options=options)
        driver.get('https://www.google.com')
        wait = WebDriverWait(driver, timeout=100)
        setattr(threadLocal, 'driver', driver)
        setattr(threadLocal, 'wait', wait)
    return driver, wait


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config', dest='config_path',
        default='./config.yaml', type=Path,
        help='the path of config file')
    args = parser.parse_args()
    return vars(args)


def get_job_urls(page_source):
    soup = BeautifulSoup(page_source, "html.parser")
    job_urls = []
    for job in soup.find_all('article', "js-job-item"):
        job_url = "https://" + job.find("a", "js-job-link")['href'][2:]
        job_urls.append(job_url)
    return job_urls


def parse_job(job_url):
    try:
        driver, wait = get_driver_wait()
        previous_page = driver.page_source
        driver.get(job_url)
        wait.until(
            driver.page_source != previous_page and
            expected.visibility_of_element_located(
                (By.CLASS_NAME, "job-description__content")))
        soup = BeautifulSoup(driver.page_source, "html.parser")
        data = {
            '公司名稱': '無', '工作職稱': '無', '工作內容': '無',
            '職務類別': '無', '工作待遇': '無', '工作性質': '無', '上班地點': '無', '管理責任': '無', '出差外派': '無', '上班時段': '無', '休假制度': '無', '可上班日': '無', '需求人數': '無',
            '接受身份': '無', '工作經歷': '無', '學歷要求': '無', '科系要求': '無', '語文條件': '無', '擅長工具': '無', '工作技能': '無', '其他條件': '無',
            '公司福利': '無', '聯絡人': '無', '聯絡方式': '無', '連結路徑': '無'
        }
        data['公司名稱'] = soup.find(
            "a", attrs={"data-gtm-head": "公司名稱"})['title'].strip()
        data['工作職稱'] = soup.find(
            "div", attrs={"class": "job-header__title"}).find("h1")['title'].strip()
        data['工作內容'] = soup.find(
            "p", attrs={"class": "job-description__content"}).text.strip()

        description_set = {'職務類別', '工作待遇', '工作性質', '上班地點',
                           '管理責任', '出差外派', '上班時段', '休假制度', '可上班日', '需求人數'}
        for item in soup.find("div", attrs={"class": "job-description-table row"}).find_all("div", attrs={"class": "row mb-2"}):
            col = item.find("h3").text.strip()
            if col in description_set:
                if col == '職務類別':
                    data[col] = [description.text.strip()
                                 for description in item.find_all('u')]
                elif col == '工作待遇':
                    data[col] = (" ").join([description.text.strip()
                                            for description in item.find_all('p')])
                else:
                    data[col] = item.find('p').text.strip()

        requirement_set = {'接受身份', '工作經歷', '學歷要求',
                           '科系要求', '語文條件', '擅長工具', '工作技能'}
        for item in soup.find("div", attrs={"class": "job-requirement-table row"}).find_all("div", attrs={"class": "row mb-2"}):
            col = item.find("h3").text.strip()
            if col in requirement_set:
                if col == '接受身份':
                    data[col] = [description.text.strip() for description in item.find_all(
                        'span') if description.text.strip() != '、']
                else:
                    data[col] = item.find('p').text.strip()

        try:
            data['其他條件'] = soup.find("div", attrs={
                                     "class": "job-requirement col opened"}).find("p", attrs={"class": "m-0 r3"}).text.strip()
        except:
            pass
        try:
            data['公司福利'] = soup.find(
                "div", attrs={"class": "row benefits-description"}).find("p").text.strip()
        except:
            pass

        try:
            contact_set = ['聯絡人', '聯絡方式']
            contact_table = soup.find(
                "div", attrs={"class": "row job-contact-table"})
            info = [x.text.strip() for x in contact_table.find_all(
                "div", attrs={"class": "col p-0 job-contact-table__data t3 mb-0 text-break"})]
            for i, _ in enumerate(info):
                data[contact_set[i]] = info[i]
        except:
            pass

        data['連結路徑'] = job_url
        return data

    except Exception as e:
        error_class = e.__class__.__name__
        detail = e.args[0]
        _, _, tb = sys.exc_info()
        lastCallStack = traceback.extract_tb(tb)[-1]
        fileName = lastCallStack[0]
        lineNum = lastCallStack[1]
        funcName = lastCallStack[2]
        errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(
            fileName, lineNum, funcName, error_class, detail)
        error_log.append([job_url, errMsg])
        return None


def main(config_path):
    config = Box.from_yaml(config_path.open())

    logging.info('[-] Setting Browser...')
    today = date.today().strftime("%m%d")
    global visualize
    visualize = config.visualize
    if visualize:
        driver = webdriver.Firefox()
    else:
        options = Options()
        options.add_argument('-headless')
        driver = webdriver.Firefox(options=options)

    driver.get("https://www.104.com.tw/jobs/main/")

    logging.info(f'[-] Searching for {config.search}...')
    ikeyword = driver.find_element_by_id('ikeyword')
    ikeyword.send_keys(config.search)
    ikeyword.send_keys(Keys.ENTER)
    wait = WebDriverWait(driver, timeout=100)

    wait.until(
        expected.visibility_of_element_located((By.CLASS_NAME, "js-job-link")) and
        expected.visibility_of_element_located((By.CLASS_NAME, "js-next-page")))

    soup = BeautifulSoup(driver.page_source, "html.parser")

    n_pages = int(soup.find("select", attrs={
                  "class": "page-select"}).find("option").text.split(' ')[-2])
    logging.info(
        f'[-] {n_pages} pages of jobs found. Collecting search result page sources...')

    page_sources = []
    for _ in tqdm(range(n_pages - 2), desc="Page", leave=False):
        page_sources.append(driver.page_source)
        driver.find_element_by_class_name('js-next-page').click()
        wait.until(driver.current_url != page_sources[-1]
                   and expected.visibility_of_element_located((By.CLASS_NAME, "js-next-page")))
    page_sources.append(driver.page_source)

    driver.quit()

    logging.info(f'[-] Collecting job page urls...')
    with Pool(processes=config.n_process) as p:
        result = list(tqdm(p.imap(get_job_urls, page_sources),
                           total=len(page_sources), leave=False))
        job_urls = [url for pack in result for url in pack]

    logging.info(f'[-] Parsing data...')
    with Pool(processes=config.n_process) as p:
        data = list(tqdm(p.imap(parse_job, job_urls),
                         total=len(job_urls), leave=False))

    logs = copy.deepcopy(error_log)
    if len(logs) > 0:
        logging.info(f'[-] Retry error data...')
        for log in logs:
            data.append(parse_job(log[0]))

    log = {'Total Data Count': len(data)}
    data = [job for job in data if job is not None]
    log['Succesfully Parsed Data Count'] = len(data)

    logging.info(
        f'[-] Filtering data')
    for filter_col in config.filters:
        if config.filters[filter_col]['includes'] is not None:
            for token in config.filters[filter_col]['includes']:
                data = [job for job in data if token in job[filter_col]]
        if config.filters[filter_col]['excludes'] is not None:
            for token in config.filters[filter_col]['excludes']:
                data = [job for job in data if token not in job[filter_col]]

    log['Filtered Data Count'] = len(data)

    company_set = set()
    for job in data:
        company_set.add(job['公司名稱'])
    log[f'% of Company'] = len(company_set)

    logging.info(f'[-] log: {log}')

    dump_path = Path(f'data/{today}')
    if not dump_path.is_dir():
        dump_path.mkdir(parents=True)

    data = pd.DataFrame(data)
    data.to_csv(dump_path / f'{config.search}.csv',
                index=None, encoding='utf_8_sig')

    logging.info(
        f'[*] Dumped crawling results @ {dump_path}/{config.search}.csv!')

    with open(dump_path / f"{config.search}_log.json", 'w') as outfile:
        json.dump(log, outfile, indent=4)
    logging.info(f'[*] Dumped log @ {dump_path / "log.json"}')

    subprocess.run(["cp", f"{config_path}", str(
        dump_path / f"{config.search}_config.yaml")])
    logging.info(
        f'[*] Dumped configeration file @ {str(dump_path / f"{config.search}_config.yaml")}')


if __name__ == "__main__":
    args = parse_args()
    main(**args)
    subprocess.run(["killall", "-9", "firefox-bin"])
