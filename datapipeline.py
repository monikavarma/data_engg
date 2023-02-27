import pandas as pd
import requests
from sqlalchemy import create_engine


def extract() -> dict:
    "get data from source E->EXTRACT"
    url = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(url).json()  # get the data from api and change to api

    return data


def transform(data: dict) -> pd.DataFrame:
    "transform data into desired structure and filter T->TRANSFRORM"
    df = pd.DataFrame(data)
    print('toptal uni', len(data))

    # filter universities which contain california
    df = df[df["name"].str.contains("California")]
    print('toptal uni in california', len(df))
    for l in df['domains']:
        df['domains'] = ','.join(map(str, l))
    for l in df['web_pages']:
        df['web_pages'] = ','.join(map(str, l))
    df = df.reset_index(drop=True)  # filktering to remove index
    return df[['domains', 'country', 'web_pages', 'name']]


def load(df: pd.DataFrame) -> None:
    # loads data into a sqlite database

    disk_engine = create_engine('sqlite:///my_lite_store.db')
    df.to_sql('cal_uni', disk_engine, if_exists='replace')


# make function calls
data = extract()
df = transform(data)
load(df)
