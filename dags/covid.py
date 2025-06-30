from contextlib import nullcontext
from datetime import datetime,timedelta


import wget
import os
import pandas as pd

from pandas.core.interchange.dataframe_protocol import DataFrame
from sqlalchemy import create_engine

#create superset -- list all available columns and add File_Date for identifying files
superset_columns=[
    "FIPS","Admin2","Province_State","Country_Region","Last_Update",
    "Lat","Long_","Confirmed","Deaths","Recovered","Active",
    "Combined_Key","Incidence_Rate","Case_Fatality_Ratio",
    "File_Date"
    ]
#rename columns
New_Name={
    "Province/State":"Province_State",
    "Country/Region":"Country_Region",
    "Last Update":"Last_Update",
    "Incident Rate":"Incidence_Rate",
    "Latitude":"Lat",
    "Longitude":"Long_"
    }

required_columns=["Province_State", "Country_Region", "Last_Update", "Confirmed","Deaths", "Recovered","Active","File_Date"]

start_date=datetime(2021,1,1)
end_date=datetime(2021,1,4)
days=(end_date - start_date).days + 1  # use the .days to get the diff of the number of days the iteration will run through

def reshape_schema(df_input):
    df_input=df_input.rename(columns=lambda c: New_Name.get(c, c))
    for col in superset_columns:
        if col not in df.columns:
            df_input[col]=pd.NA
    return df_input[superset_columns]

def validate(df_input, fname):
    missing = set(required_columns) - set(df_input.columns)
    if missing:
        raise ValueError(f"{fname}: missing columns {missing}")
    if (df[["Confirmed","Deaths"]].fillna(0) < 0).any().any():
        raise ValueError(f"{fname}: negative values in Confirmed/Deaths")

engine =create_engine("postgresql://admin:admin@localhost:5432/covid")

all_df=[]

for day in range(days):
    single_date=start_date + timedelta(day) # timedelta adds number in terms of days . changing the date into number
    date_str=single_date.strftime("%m-%d-%Y")
    url_f=f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/refs/heads/master/csse_covid_19_data/csse_covid_19_daily_reports/{date_str}.csv"
    try:
        csv_files = wget.download(url_f)
        df = pd.read_csv(csv_files)
        os.remove(csv_files)  # remove csv file after reading and appending to list
        df = reshape_schema(df)
        df["File_Date"] = single_date.date()
        validate(df,f"{date_str}.csv")
        all_df.append(df)

    except Exception as e:
        continue

if len(all_df) > 0:
    final_df=pd.concat(all_df, ignore_index=True)
    final_df=final_df.drop_duplicates(keep="first")
    print(final_df.head())
    final_df.to_csv("covid_data.csv", index=False)
    engine.connect()
    final_df.to_sql("row_covid_data", engine=engine, index=False, if_exists="append", method="multi")

# create a normalizing function that can assign the columns a standard name