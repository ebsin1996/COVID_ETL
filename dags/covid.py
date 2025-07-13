from contextlib import nullcontext
from datetime import datetime,timedelta
import requests
import os
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import DAG
from airflow.operators.python import PythonOperator


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
        if col not in df_input.columns:
            df_input[col]=pd.NA
    return df_input[superset_columns]

def validate(df_input, fname):
    missing = set(required_columns) - set(df_input.columns)
    if missing:
        raise ValueError(f"{fname}: missing columns {missing}")
    if (df_input[["Confirmed","Deaths"]].fillna(0) < 0).any().any():
        raise ValueError(f"{fname}: negative values in Confirmed/Deaths")

engine=create_engine("postgresql://admin:admin@covid_postgres:5432/covid")

all_df=[]

def extract_covid_data():
    for day in range(days):
        single_date=start_date + timedelta(day) # timedelta adds number in terms of days . changing the date into number
        date_str=single_date.strftime("%m-%d-%Y")
        url_f=f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/refs/heads/master/csse_covid_19_data/csse_covid_19_daily_reports/{date_str}.csv"
        try:
            local_path = f"/opt/airflow/dags/{date_str}.csv"
            response = requests.get(url_f, timeout=30)
            response.raise_for_status()
            with open(local_path, 'wb') as f:
                f.write(response.content)
            # csv_files = wget.download(url_f)
            df = pd.read_csv(local_path)
            os.remove(local_path)  # remove csv file after reading and appending to list
            df = reshape_schema(df)
            df["File_Date"] = single_date.date()
            validate(df,f"{date_str}.csv")
            all_df.append(df)

        except Exception as e:
            continue

def load_to_postgres():
    print(f"[load_to_postgres] {len(all_df)} dataframes to write")
    if len(all_df) > 0:
        final_df=pd.concat(all_df, ignore_index=True)
        final_df=final_df.drop_duplicates(keep="first")
        print(final_df.head())
        final_df.to_csv("covid_data.csv", index=False)
        with engine.begin() as conn:
            final_df.to_sql("raw_covid_data",
                            con=conn,
                            if_exists="append" ,
                            index=False,
                            method="multi")
        print("covid data has has been written to Postgres")


dag = DAG(
        dag_id="covid_data",
        start_date=datetime(2025, 6, 27),
        schedule="@daily",
        catchup=False
)

with  dag:
    extract_task = PythonOperator(
        task_id="extract_covid_data",
        python_callable=extract_covid_data,
        op_kwargs={"name": "Airflow"},
    )
    process_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres ,
        dag=dag
    )

    extract_task >> process_task

    # dbt_run = DockerOperator(
    #     task_id="dbt_run",
    #     image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
    #     api_version="auto",
    #     auto_remove="success",
    #     mount_tmp_dir=False,
    #     network_mode="host",
    #     mounts=[
    #         # Mount the named volume 'dbt_project' (backed by your host folder)
    #         Mount(source="dbt_project", target="/usr/app/dbt", type="volume"),
    #
    #     ],
    #     command=(
    #         "bash -c \""
    #         "cd /usr/app/dbt && "
    #         "dbt deps && "
    #         "dbt source freshness --profiles-dir . && "
    #         "dbt run --profiles-dir ."
    #         "\""
    #     ),
    #
    # )
    #
    # dbt_test = DockerOperator(
    #     task_id="dbt_test",
    #     image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
    #     api_version="auto",
    #     auto_remove="success",
    #     mount_tmp_dir=False,
    #     network_mode="host",
    #     mounts=[
    #         Mount(source="dbt_project", target="/usr/app/dbt", type="volume"),
    #
    #     ],
    #     command=(
    #         "bash -c \""
    #         "cd /usr/app/dbt && "
    #         "dbt test --profiles-dir ."
    #         "\""
    #     ),



