import requests
import json
import warnings
import pandas as pd
import os
from dotenv import load_dotenv
import time
import logging

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

log_format = logging.Formatter("%(asctime)s : %(levelname)s : %(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_format)
logger.addHandler(stream_handler)

file_handler = logging.FileHandler("boats24.log")
file_handler.setFormatter(log_format)
logger.addHandler(file_handler)


load_dotenv()

ZENROWS_API_KEY =os.getenv("ZENROWS_API_KEY")

warnings.filterwarnings('ignore')
CSV_FILE = 'boats24.csv'


    

def flatten_dict(d, parent_key='', sep='_'):
    """Recursively flattens a nested dictionary, ensuring only 'url' and 'title' are kept from 'media' lists."""
    flattened = {}

    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if k == 'media' and isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    if 'url' in item:
                        flattened[f"{new_key}{sep}{i}_url"] = item['url']
                    if 'title' in item:
                        flattened[f"{new_key}{sep}{i}_title"] = item['title']

        elif isinstance(v, dict):
            flattened.update(flatten_dict(v, new_key, sep))

        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    # Flatten nested dictionary inside list
                    flattened.update(flatten_dict(item, f"{new_key}{sep}{i}", sep))
                elif isinstance(item, list):
                    # Convert list of non-dict items to a string
                    flattened[f"{new_key}{sep}{i}"] = ", ".join(map(str, item))
                else:
                    # Store primitive list elements as a single string
                    flattened[f"{new_key}{sep}{i}"] = item
        else:
            # Store primitive values directly
            flattened[new_key] = v

    return flattened




def load_data():
    if os.path.exists(CSV_FILE):
        df = pd.read_csv(CSV_FILE)
        data_list = df.to_dict(orient='records')
        logger.info(f'Loaded {len(data_list)} records from {CSV_FILE}')
        return data_list
    else:
        logger.info(f'No data found in {CSV_FILE}')
        return []


def fetch_content(url):
    ZENROWS_BASE_URL = "https://api.zenrows.com/v1/"

    payload = {
        "apikey": ZENROWS_API_KEY,
        "url": url,
        "js_render": "true", # Enable JS rendering for dynamic content
        "original_status":"true" #retrieve the original HTTP status code returned by the target website
    }
    response = requests.get(ZENROWS_BASE_URL, params=payload)
    return response

def main():
    
    page_size = 100
    years = [
        "1900-1950",
        "1950-1960",
        "1960-1970",
        "1970-1980",
        "1980-1990",
        "1990-2000",
        "2000-2005",
        "2005-2010",    
        "2010-2012",
        "2012-2014",
        "2014-2016",
        "2016-2018",
        "2018-2020",
        "2020-2022",
        "2022-2024",
        "2024-2026",
        "2026-2028",
             
             ]
    


    BASE_URL = f"https://www.boatshop24.com/bs24/search/boat?page=PAGENUMBER&facets=countrySubdivision,make,condition,makeModel,type,class,country,countryRegion,countryCity,fuelType,hullMaterial,hullShape,minYear,maxYear,minMaxPercentilPrices,enginesConfiguration,enginesDriveType,numberOfEngines,minTotalHorsepowerPercentil,maxTotalHorsepowerPercentil,minLengthPercentil,maxLengthPercentil&fields=id,make,model,year,featureType,specifications.dimensions.lengths.nominal,location.address,aliases,owner.logos,owner.name,owner.rootName,owner.location.address.city,owner.location.address.country,price.hidden,price.type.amount,portalLink,class,media,isOemModel,isCurrentModel,attributes,previousPrice,mediaCount,cpybLogo&useMultiFacetedFacets=true&enableSponsoredSearch=true&locale=en-US&distance=200mi&pageSize={page_size}&sort=modified-desc&year=YEARRANGE&advantageSort=1"
    
   
    unique_ids ,df_columns= load_unique_ids()
    logger.info(f'Loaded {len(unique_ids)} unique ids from {CSV_FILE}')
        

    first_write = not os.path.exists(CSV_FILE)  # Check if file exists for header handling

    for year in years:
        tries = 0
        page = 1

        while True:
            try:
                url = BASE_URL.replace('PAGENUMBER', str(page))
                url = url.replace('YEARRANGE', year)
                logger.info(f'Fetching Year Range {year} Page {page}...')
                response = fetch_content(url)
                logger.info("Request has been sent")
                response.raise_for_status()
                data = response.json()
                try:
                    logger.info(f"{data['search']['count']} TOTAL RESULTS FOUND FOR THIS YEAR RANGE approximately")
                except:
                    pass


                search_res = data['search']
                test = search_res['records']
                if len(test) == 0:
                    logger.info(f'No more data for year {year}')
                    break
                new_list = [flatten_dict(item) for item in test]

                sponsered = data['sponsored']['records']
                new_list += [flatten_dict(item) for item in sponsered]


                new_data = []
                logger.info("Results found comparing agains previous ids")
                for item in new_list:
                    if item['id'] not in unique_ids:
                        new_data.append(item)
                        unique_ids.add(item['id'])

                df = pd.DataFrame(new_data)
                # logger.info(df_columns)
                missing_columns = set(df_columns) - set(df.columns)
                # logger.info(missing_columns)
                for column in missing_columns:
                    df[column] = pd.NA  # Or use `pd.NA` if you want to have nullable types
                if  len(df_columns) > 0:
                    df = df[list(df_columns)]
                # logger.info(df.columns)
                
                if len(df.columns) <= len(df_columns):    
                    df.to_csv(CSV_FILE, mode='a', header=first_write, index=False)  
                    first_write = False
                    df = pd.DataFrame()
                else:
                    logger.info("Saving Complete data due to headers update will take some time")
                    flattend_data = load_data()
                    flattend_data += new_data
                    df = pd.DataFrame(flattend_data)
                    df.to_csv(CSV_FILE,index=False)
                    df_columns = df.columns
                    df =pd.DataFrame() 
                    first_write = False
                    logger.info("Data Saved") 
                    
                    
                    

                logger.info(f'Page {page} done')
                logger.info(f'Unique ids: {len(unique_ids)}')

                page += 1

                logger.info("sleeping for 2 seconds")
                time.sleep(2)



            except Exception as e:
                logger.info(f'Error: {e}')
                tries += 1
                if tries > 2:
                    break
                time.sleep(2)
                
                continue
  





def load_unique_ids():
    try:
        df = pd.read_csv(CSV_FILE)
        
        return set(df['id']),df.columns
    except:
        return set(),[]




def daily_update():
    logger.info("Running Daily Update for Boat shop ")
    unique_ids,df_columns = load_unique_ids()
    page_size = 100
    
    BASE_URL = f"https://www.boatshop24.com/bs24/search/boat?page=PAGENUMBER&facets=countrySubdivision,make,condition,makeModel,type,class,country,countryRegion,countryCity,fuelType,hullMaterial,hullShape,minYear,maxYear,minMaxPercentilPrices,enginesConfiguration,enginesDriveType,numberOfEngines,minTotalHorsepowerPercentil,maxTotalHorsepowerPercentil,minLengthPercentil,maxLengthPercentil&fields=id,make,model,year,featureType,specifications.dimensions.lengths.nominal,location.address,aliases,owner.logos,owner.name,owner.rootName,owner.location.address.city,owner.location.address.country,price.hidden,price.type.amount,portalLink,class,media,isOemModel,isCurrentModel,attributes,previousPrice,mediaCount,cpybLogo&useMultiFacetedFacets=true&enableSponsoredSearch=true&locale=en-US&distance=200mi&pageSize={page_size}&sort=modified-desc&advantageSort=1"

    
    
    first_write = not os.path.exists(CSV_FILE)  # Check if file exists for header handling


    while True:
        old_id_encountered = False
        page =1
        try:
            url = BASE_URL.replace('PAGENUMBER', str(page))
            logger.info(f'Fetching  Page {page}...')
            response = fetch_content(url)
            logger.info("Request has been sent")
            response.raise_for_status()
            data = response.json()
            search_res = data['search']
            test = search_res['records']
            if len(test) == 0:
                logger.info(f'No more data ')
                break
            new_list = [flatten_dict(item) for item in test]
            
            new_data = []
            logger.info("Results found comparing agains previous ids")
            for item in new_list:
                if item['id'] not in unique_ids:
                    new_data.append(item)
                    unique_ids.add(item['id'])
                elif not old_id_encountered:
                    old_id_encountered = True
            


            df = pd.DataFrame(new_data)
            missing_columns = set(df_columns) - set(df.columns)
            for column in missing_columns:
                df[column] = pd.NA  # Or use `pd.NA` if you want to have nullable types
            if  len(df_columns) > 0:
                df = df[list(df_columns)]
            

            if len(df.columns) <= len(df_columns):    
                df.to_csv(CSV_FILE, mode='a', header=first_write, index=False)  
                first_write = False
                df = pd.DataFrame()
            else:
                logger.info("Saving Complete data due to headers update will take some time")
                flattend_data = load_data()
                flattend_data += new_data
                df = pd.DataFrame(flattend_data)
                df.to_csv(CSV_FILE,index=False)
                df_columns = df.columns
                df =pd.DataFrame() 
                first_write = False
                logger.info("Data Saved") 

            
                
                
                
                
            logger.info(f'Page {page} done')
            logger.info(f" Found {len(new_data)} new listings")
            if old_id_encountered:
                logger.info("All New data scraped")
                break
            page += 1
            logger.info("sleeping for 2 seconds")
            time.sleep(2)

        except Exception as e:
            logger.info(f'Error: {e}')
            time.sleep(2)
            
            continue
  



    pass
if __name__ == '__main__':
    daily_update()