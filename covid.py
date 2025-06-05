import wget
import pandas as pd


url="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/refs/heads/master/csse_covid_19_data/csse_covid_19_daily_reports/01-02-2023.csv"
table=wget.download(url)
data=pd.read_csv(table)
print(data.head())