import datetime
# TODO get this as parameter

# state/city
location = "berlin/berlin"
location = "baden-wuerttemberg/pforzheim"
location = "niedersachsen/lueneburg-kreis"
location = "hessen/bergstrasse-kreis"
location = "hessen/darmstadt-dieburg-kreis"
location = "baden-wuerttemberg"
location = "baden-wuerttemberg/mannheim"
location = "bayern/fuerth-kreis"

actualDate = datetime.date.today()
actualDate = actualDate.strftime('%Y-%m-%d')
city = location.split('/')[1] if len(location.split('/')) > 1 else location
file_name_csv = 'immobilienscout_result_' + city + '_' + actualDate + '.csv'
file_folder = 'data'
file_folder_raw = file_folder + '/raw/'
file_folder_interim = file_folder + '/interim/'
file_path_raw = file_folder_raw + file_name_csv
file_path_interim = file_folder_interim + file_name_csv

# location = "sachsen-anhalt/magdeburg"
propertyType = 'wohnung'
# url_to_crawl = 'https://www.immobilienscout24.de/Suche/de/rheinland-pfalz/alzey-worms-kreis/wohnung-kaufen?enteredFrom=result_list'
# TODO build URLS that can also check 50km around
# https://www.immobilienscout24.de/Suche/radius/wohnung-kaufen?centerofsearchaddress=Heidelberg;;;1276001014;Baden-W%C3%BCrttemberg;&price=-150000.0&geocoordinates=49.40589;8.68357;50.0&enteredFrom=one_step_search

price_max = '150000'

# TOOD get more precise URLs
url_to_crawl = 'https://www.immobilienscout24.de/Suche/de/' + location + '/'+propertyType+'-kaufen?' # price=-'+price_max+'&enteredFrom=result_list'


import urllib.request
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy import Selector
import selenium
from selenium import webdriver
import chromedriver_binary
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import pandas as pd
pd.set_option('display.max_columns', 100)
pd.set_option('display.max_colwidth', 200)
import requests
import json
import re
import numpy as np
from crochet import setup

import time

import itertools
import random

temp_str= 'en-US,en;q=0.9,ar-LB;q=0.8,ar;q=0.7'
lst= ['en-US', 'ar', 'ar-LB']
accept_language= []
language_array= []

for i in range(1, len(lst)+1):
    els = [list(x) for x in itertools.combinations(lst, i)]
    language_array.extend(els)

for el in language_array:
    var_count= len(el)
    test= ';q={},'.join(el).rstrip(',').format(*(round(random.uniform(0, 1), 1) for _ in range(var_count)))
    print(test)

temp_str= 'en-US,en;q=0.9,ar-LB;q=0.8,ar;q=0.7'
lst= ['af', 'sq', 'ar', 'ar-dz', 'ar-bh', 'ar-eg', 'ar-iq', 'ar-jo', 'ar-kw', 'ar-lb', 'ar-ly', 'ar-ma', 'ar-om', 'ar-qa', 'ar-sa', 'ar-sy', 'ar-tn', 'ar-ae', 'ar-ye', 'ar', 'hy', 'as', 'ast', 'az', 'eu', 'bg', 'be', 'bn', 'bs', 'br', 'bg', 'my', 'ca', 'ch', 'ce', 'zh', 'zh-hk', 'zh-cn', 'zh-sg', 'zh-tw', 'cv', 'co', 'cr', 'hr', 'cs', 'da', 'nl', 'nl-be', 'en', 'en-au', 'en-bz', 'en-ca', 'en-ie', 'en-jm', 'en-nz', 'en-ph', 'en-za', 'en-tt', 'en-gb', 'en-us', 'en-zw', 'eo', 'et', 'fo', 'fa', 'fj', 'fi', 'fr', 'fr-be', 'fr-ca', 'fr-fr', 'fr-lu', 'fr-mc', 'fr-ch', 'fy', 'fur', 'gd', 'gd-ie', 'gl', 'ka', 'de', 'de-at', 'de-de', 'de-li', 'de-lu', 'de-ch', 'el', 'gu', 'ht', 'he', 'hi', 'hu', 'is', 'id', 'iu', 'ga', 'it', 'it-ch', 'ja', 'kn', 'ks', 'kk', 'km', 'ky', 'tlh', 'ko', 'ko-kp', 'ko-kr', 'la', 'lv', 'lt', 'lb', 'mk', 'ms', 'ml', 'mt', 'mi', 'mr', 'mo', 'nv', 'ng', 'ne', 'no', 'nb', 'nn', 'oc', 'or', 'om', 'fa', 'fa-ir', 'pl', 'pt', 'pt-br', 'pa', 'pa-in', 'pa-pk', 'qu', 'rm', 'ro', 'ro-mo', 'ru', 'ru-mo', 'sz', 'sg', 'sa', 'sc', 'gd', 'sd', 'si', 'sr', 'sk', 'sl', 'so', 'sb', 'es', 'es-ar', 'es-bo', 'es-cl', 'es-co', 'es-cr', 'es-do', 'es-ec', 'es-sv', 'es-gt', 'es-hn', 'es-mx', 'es-ni', 'es-pa', 'es-py', 'es-pe', 'es-pr', 'es-es', 'es-uy', 'es-ve', 'sx', 'sw', 'sv', 'sv-fi', 'sv-sv', 'ta', 'tt', 'te', 'th', 'tig', 'ts', 'tn', 'tr', 'tk', 'uk', 'hsb', 'ur', 've', 'vi', 'vo', 'wa', 'cy', 'xh', 'ji', 'zu']

accept_language= []
language_array= []

for i in range(1, len(lst)-1):  # range(1, len(lst)-180)
    els = [list(x) for x in itertools.combinations(lst[:20], i)]
    language_array.extend(els)
for el in language_array:
    var_count= len(el)
    test= ';q={},'.join(el).rstrip(',').format(*(round(random.uniform(0, 1), 1) for _ in range(var_count)))
    accept_language.append(test)

raw_columns = ['url', 'title', 'address', 'region', 'contact_person', 'telephone', 'mobile', 'fax', 'price_purchase', 'estimated_monthly_rate','rooms', 'living_area_m2', 'furnishing', 'flat_type', 'floor', 'floor_total', 'bedrooms', 'commission_buyer_rate', 'maintenance_cost', 'construction_year', 'object_state', 'heating_type', 'energy_efficiency_class', 'location', 'other_information']
df_raw = pd.DataFrame(columns = raw_columns)


from fake_useragent import UserAgent

# options = Options()
ua = UserAgent()
userAgent = ua.random
# print(userAgent)
# options.add_argument(f'user-agent={userAgent}')



# options = Options()
# options.headless = True
# options = webdriver.ChromeOptions()
# options.add_argument('window-size=1600x900')
# options.add_argument('--headless')
# options.add_experimental_option("excludeSwitches", ["enable-automation"])
# options.add_experimental_option('useAutomationExtension', False)

crawler_name = 'scrapper_immobilienscout'

base_uri = 'immobilienscout24.de'
base_url = 'https://www.immobilienscout24.de'
# Modified

# headers= {
# "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
# "Accept-Encoding": "gzip, deflate, br",
# "Accept-Language": "en-US,en;q=0.8,ar;q=0.7",

# "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36",
# }


# driver = webdriver.Chrome(options=options, executable_path=ChromeDriverManager().install())
# driver = webdriver.Chrome(executable_path=ChromeDriverManager().install())

# Modified

# OLD configuration
#driver = webdriver.Chrome(options=options, executable_path="chromedriver.exe")
import random
import itertools



# accept_language= ["en-US,en;q=0.9,ar-LB;q=0.8,ar;q=0.7", "en-US,en;q=0.9,ar-SA;q=0.8,ar;q=0.7", "en-US,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", \
#                  "en-US,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", "en-AU,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", "en-PH,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", \
#                  "en-US,en;q=0.9,ar-MA;q=0.8,ar;q=0.7", "en-BZ,en;q=0.9,ar-LB;q=0.8,ar;q=0.7", "en-ZA,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", \
#                  "en-US,en;q=0.9,ar-OM;q=0.8,ar;q=0.7", "en-CA,en;q=0.9,ar-SY;q=0.8,ar;q=0.7", "en-TT,en;q=0.9,ar-LY;q=0.8,ar;q=0.7",
#                  "en-US,en;q=0.9,ar-QA;q=0.8,ar;q=0.7", "en-AU,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", "en-GB,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", \
#                  "en-US,en;q=0.9,ar-SY;q=0.8,ar;q=0.7", "en-US,en;q=0.9,ar-QA;q=0.8,ar;q=0.7", "en-ZW,en;q=0.9,ar-LY;q=0.8,ar;q=0.7",
#                  "en-AU,en;q=0.9,ar-LB;q=0.8,ar;q=0.7", "en-US,en;q=0.9,ar-SA;q=0.8,ar;q=0.7", "en-US,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", \
#                  "en-BZ,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", "en-AU,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", "en-PH,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", \
#                  "en-CA,en;q=0.9,ar-MA;q=0.8,ar;q=0.7", "en-BZ,en;q=0.9,ar-LB;q=0.8,ar;q=0.7", "en-ZA,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", \
#                  "en-CA,en;q=0.9,ar-OM;q=0.8,ar;q=0.7", "en-CA,en;q=0.9,ar-SY;q=0.8,ar;q=0.7", "en-TT,en;q=0.9,ar-LY;q=0.8,ar;q=0.7",
#                  "en-US,en;q=0.9,ar-QA;q=0.8,ar;q=0.7", "en-AU,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", "en-GB,en;q=0.9,ar-LY;q=0.8,ar;q=0.7", \
#                  "en-US,en;q=0.9,ar-SY;q=0.8,ar;q=0.7", "en-US,en;q=0.9,ar-QA;q=0.8,ar;q=0.7", "en-ZW,en;q=0.9,ar-LY;q=0.8,ar;q=0.7"]


class Scrap_immobilienscout(scrapy.Spider):
    download_delay = 0.1
    name = crawler_name

    def start_requests(self):
        global accept_language
        language= random.choice(accept_language)
        req = urllib.request.Request(
              url_to_crawl,
              data=None,
              headers= {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "deflate, br",
                "Accept-Language": language,
                "User-Agent": ua.random,
                }

        )

        first_page = urllib.request.urlopen(req)
        time.sleep(1.4)
        #first_page = urllib.request.urlopen(url_to_crawl)

        max_pages = first_page.read()
        max_pages = max_pages.decode("utf8")
        first_page.close()

        max_pages_selector = Selector(text = max_pages)
        max_pages = max_pages_selector.xpath('//select[@aria-label="Seitenauswahl"]/option[last()]/@value').extract_first()
        max_pages = int(max_pages) if max_pages is not None else 1

        base_url = url_to_crawl.replace('enteredFrom=one_step_search', '')
        print(f'@@{max_pages}@@')
        for page_number in range(max_pages):
            url = base_url + '&pagenumber=' + str(page_number + 1)
            print (url)
            language= random.choice(accept_language)
            print(language)
            #headers =  {
            #'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.84 Safari/537.36',
            #'Accept': 'application/json,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            #'Accept-Encoding': 'gzip, deflate, sdch',
            #'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4',
            #}

            #headers =  {
            #'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.84 Safari/537.36'
            #}

            #
#             headers = {
# #             'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0',
#             'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36',
#             'Accept': 'application/json,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
#             'Accept-Encoding': 'gzip, deflate, sdch',
#             'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4',}
            headers= {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": language,
            "User-Agent": ua.random,
            }


#             yield scrapy.Request(url, headers = headers, callback = self.parse_properties_list)
            yield scrapy.Request(url, headers = headers, callback = self.parse_properties_list)
#             yield scrapy.Request(url, headers = headers)

    def parse_properties_list(self, response):
        global accept_language
        print('######################################################################')
        property_urls = response.xpath('//a[contains(@class, "result-list-entry__brand-title-container")]/@href').extract()
        print(property_urls, response)
        for url in property_urls:
            print('********************', 3)
            language= random.choice(accept_language)
            print(language)
            if base_uri not in url:
                headers= {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": language,
                "Referer": base_uri,
                "User-Agent": ua.random,

#                 "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36",
                }
                print(headers)
                url = base_url + url + '#/'
                print(f'^^^^^^^^^^^^^^^^^^^^^^^^^^^{url}^^^^^^^^^^^^^^^^^^')
#                 driver.get(url)
#                 src = driver.page_source
                session= requests.Session()
                src= session.get(url, headers= headers, allow_redirects=True).text
                if 'robot' in src.lower():
                    print('++++++++++++++++++++++++++++++++++++++detected+++++++++++++++++++++++++++++++++++++++++++++')
#                     print(src)
#                 time.sleep(10)
                self.scrap_property(src, url)


    def scrap_property(self, src, url):
        print('********************', 1)
        sel = Selector(text = src)
        title = sel.css('h1#expose-title ::text').extract_first()
        address = sel.xpath('//div[@class="address-block"]//span[@class="block font-nowrap print-hide"]//text()').extract_first()
        region = sel.css('span.zip-region-and-country::text').extract_first()
        contact_person = sel.xpath('//div[@data-qa="contactName"]//text()').extract_first()
        phone = sel.xpath('//script//text()').extract_first()
        phone = phone if phone is not None else ''
        telefon = re.findall('"phoneNumber":\{"contactNumber":"(\+?[0-9 ]*)"\}', phone)
        mobil = re.findall('"cellPhoneNumber":\{"contactNumber":"(\+?[0-9 ]*)"\}', phone)
        fax = re.findall('"faxNumber":\{"contactNumber":"(\+?[0-9 ]*)"\}', phone)
        price = sel.xpath('//*[@id="is24-content"]/div[2]/div[1]/div[2]/div[1]/div[1]/div/div[1]/text()').extract_first()
        price = price if price else sel.xpath('//*[@id="is24-content"]/div[3]/div[1]/div[2]/div[1]/div[1]/div/div[1]/text()').extract_first()
        # TODO estimated_monthly_rate gets loaded dynamically we need some wait until its finishing with loading
        # estimated_monthly_rate = sel.xpath('//span[contains(@class, "monthly-rate-result") and contains(@class, "monthly-rate-value")]//text()').extract_first()
        # estimated_monthly_rate = sel.xpath('//*[@id="is24-content"]/div[2]/div[1]/div[2]/div[1]/div[2]/div/div[3]/div[4]/span[3]/text()').extract_first()
        # estimated_monthly_rate = sel.xpath('//*[@id="is24-content"]/div[2]/div[1]/div[2]/div[1]/div[2]/div/div[2]/text()').extract_first()
        estimated_monthly_rate = None
        rooms_count = sel.xpath('//dd[contains(@class,"is24qa-zimmer")]/text()').extract_first()
        living_area = sel.xpath('//dd[contains(@class, "is24qa-wohnflaeche-ca")]/text()').extract_first()
        furnishing = sel.xpath('//div[contains(@class, "criteriagroup") and contains(@class, "boolean-listing")]//span[contains(@class, "palm-hide")]/text()').extract()
        flat_type = sel.xpath('//dd[contains(@class, "is24qa-typ")]/text()').extract_first()
        # TODO parse floor
        floor = sel.xpath('//dd[contains(@class, "is24qa-etage")]//text()').extract_first()
        floor = floor if floor is not None else ''
        floor = re.findall('[0-9]+', floor)
        total_floors = int(floor[1]) if len(floor) > 1 else 0
        floor = int(floor[0]) if len(floor) > 0 else 0
        bedroom = sel.xpath('//dd[contains(@class, "is24qa-schlafzimmer")]//text()').extract_first()
        allowance = sel.xpath('//dd[contains(@class, "is24qa-hausgeld")]//text()').extract_first()
        commission = sel.xpath('//dd[contains(@class, "is24qa-provision")]/text()').extract_first()
        construction_year = sel.xpath('//dd[contains(@class, "is24qa-baujahr")]//text()').extract_first()
        object_state = sel.xpath('//dd[contains(@class, "is24qa-objektzustand")]//text()').extract_first()
        heatingy_type = sel.xpath('//dd[contains(@class, "is24qa-heizungsart")]//text()').extract_first()
        energy_efficiency_class = sel.xpath('//dd[contains(@class, "is24qa-energieeffizienzklasse")]//text()').extract_first()
        location = sel.xpath('//pre[contains(@class, "is24qa-lage")]/text()').extract_first()
        other_information = sel.xpath('//*[@id="is24-content"]/div[2]/div[3]/pre/text()').extract_first()

        lst_row = [url, title, address, region, contact_person, telefon, mobil, fax, price, estimated_monthly_rate, rooms_count, living_area, furnishing, flat_type, floor, total_floors, bedroom, commission, allowance, construction_year, object_state, heatingy_type, energy_efficiency_class, location, other_information]

        df_raw.loc[len(df_raw)] = lst_row
        df_raw.to_csv(file_path_raw, index=False)
        print("File Path Raw:", file_path_raw)

# DEBUG
process = CrawlerProcess()
process.crawl(Scrap_immobilienscout)
process.start(),
# DEBUG
# setup()
# def spider_process(spider):
#     process = CrawlerProcess()
#     process.crawl(spider)
#
# spider_process(Scrap_immobilienscout)
