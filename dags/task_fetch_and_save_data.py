import requests
from bs4 import BeautifulSoup
import pandas as pd 
import json
import os
from datetime import datetime, date
from time import sleep

BASE_URL = "https://autot.tori.fi/veneet/myydaan/moottoriveneet?sivu="
HEADERS = {
        "User-Agent":
          "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0",
      }
current_date = datetime.now().strftime("%Y%m%d")

def fetch_page(page_number: str) -> str:
  """
    Send a request to the url with specific page number and headers.
    If the status is successful, then use BeautifulSoup to parse and return content as a json-like string. 
    Else, raise an exception.
  """
  url = BASE_URL + page_number
  response = requests.get(url, headers = HEADERS)
  if response.status_code == 200:
    soup = BeautifulSoup(response.content, "html.parser")
    data = soup.find(id = "__NEXT_DATA__")
    content = data.contents[0]
    return content
  else:
    raise Exception("Error happened when fetching page", response.status_code)
  json_content = json.loads(content)
  listing_ads = json_content['props']['pageProps']['initialReduxState']['search']['result']['list_ads']
  df = pd.json_normalize(listing_ads)
  return df
  # 

def unpck(item):
    if len(item) > 0:
        for obj in item:
            if 'hv' in obj:
                return obj
            else:
                return 'not specified'

def convert_to_dataframe(content: str):
  """
    Load the json string and get the list of ads from the result key.
    Normalize the content into a dataframe using pandas.
    Write the data frame into a parquet object.
  """
  json_content = json.loads(content)
  listing_ads = json_content['props']['pageProps']['initialReduxState']['search']['result']['list_ads']
  df = pd.json_normalize(listing_ads)
  return df

def fetch_all_and_save(path, number_of_pages):
  """
    Given a number of pages that need to be fetched, the program will first create an empty dataframe,
    fetch each page and concatenate all pages together.
    If a listing was sponsored, it will appeared in every page. As a result, duplicates will be dropped.
  """
  results = pd.DataFrame()
  for page_number in range(1, number_of_pages + 1):
    content = fetch_page(page_number=str(page_number))
    page_df = convert_to_dataframe(content=content)
    results = pd.concat([results, page_df])
    sleep(5)
  results.columns = [column.replace('.','_') for column in results.columns]
  results = results.drop_duplicates(subset=["list_id"], keep="first")
  results['EngineHp'] = results['_app_cardDetails'].apply(unpck)
  columns_to_drop=['_app_vehicleBrandId', '_app_vehicleModelId', '_app_price', 
  '_app_title','_app_url','_app_urlAbsolute', '_app_subtitle', '_app_cardDetails']
  
  results.drop(columns = columns_to_drop, inplace = True)
  if 'polepos_ad' in results.columns:
    results.drop(columns = ['polepos_ad'], inplace = True)
  results[['list_datetime', 'utc_offset']] = results['list_time'].str.split('+', expand=True)
  results['list_datetime'] = pd.to_datetime(results['list_datetime'])
  results['list_date'] = results['list_datetime'].dt.date
  results['list_hour'] = results['list_datetime'].dt.hour
  results['scraping_date'] = date.today()
  ordered_columns = ['subject', 'list_id', 'images', 'list_time', 'status',
       'account_account_ads', 'account_account_id', 'account_name',
       'account_user_id', 'account_eid_verified', 'account_created',
       'category_code', 'category_label', 'list_price_currency',
       'list_price_value', 'location_region_code', 'location_region_label',
       'location_area_code', 'location_area_label', 'location_zipcode_code',
       'location_zipcode_label', 'location_coordinates_lat',
       'location_coordinates_lng', 'location_coordinates_is_precise',
       'type_code', 'type_label', 'ad_details_boat_engine_brand_single_code',
       'ad_details_boat_engine_brand_single_label',
       'ad_details_boat_engine_stroke_single_code',
       'ad_details_boat_engine_stroke_single_label',
       'ad_details_cx_engine_power_single_code',
       'ad_details_cx_engine_power_single_label',
       'ad_details_fuel_single_code', 'ad_details_fuel_single_label',
       'ad_details_general_condition_single_code',
       'ad_details_general_condition_single_label',
       'ad_details_regdate_single_code', 'ad_details_regdate_single_label',
       'store_details_id', 'store_details_name', 'store_details_has_contacts',
       'ad_details_cx_used_hour_single_code',
       'ad_details_cx_used_hour_single_label', 'EngineHp', 'list_datetime',
       'utc_offset', 'list_date', 'list_hour', 'scraping_date']
  results = results[ordered_columns]
  results.to_parquet(f"{path}/data/secondhand-boats-{current_date}.parquet")