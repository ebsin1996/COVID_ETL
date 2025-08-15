from contextlib import nullcontext
from datetime import datetime,timedelta
import requests
import os
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import tempfile
import logging


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
end_date=datetime(2021,2,28)  # Extended to full month for meaningful data
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

def extract_covid_data(**context):
    logger = logging.getLogger(__name__)
    all_df = []
    failed_dates = []
    
    for day in range(days):
        single_date=start_date + timedelta(day) # timedelta adds number in terms of days . changing the date into number
        date_str=single_date.strftime("%m-%d-%Y")
        url_f=f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/refs/heads/master/csse_covid_19_data/csse_covid_19_daily_reports/{date_str}.csv"
        
        try:
            # Use temporary directory instead of hardcoded path
            with tempfile.NamedTemporaryFile(mode='w+b', suffix='.csv', delete=False) as temp_file:
                local_path = temp_file.name
                
            response = requests.get(url_f, timeout=30)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                f.write(response.content)
                
            df = pd.read_csv(local_path)
            os.remove(local_path)  # remove csv file after reading
            
            df = reshape_schema(df)
            df["File_Date"] = single_date.date()
            validate(df, f"{date_str}.csv")
            all_df.append(df)
            logger.info(f"Successfully processed data for {date_str}: {len(df)} rows")

        except Exception as e:
            failed_dates.append(date_str)
            logger.error(f"Failed to process data for {date_str}: {str(e)}")
            # Clean up temp file if it exists
            if 'local_path' in locals() and os.path.exists(local_path):
                os.remove(local_path)
            continue
    
    logger.info(f"Extraction completed: {len(all_df)} successful, {len(failed_dates)} failed")
    if failed_dates:
        logger.warning(f"Failed dates: {failed_dates}")
    
    # Convert dataframes to JSON for XCom serialization
    serialized_data = []
    for df in all_df:
        serialized_data.append(df.to_json(orient='records', date_format='iso'))
    
    # Push data to XCom for the next task
    context['task_instance'].xcom_push(key='covid_data', value=serialized_data)
    context['task_instance'].xcom_push(key='total_records', value=sum(len(df) for df in all_df))
    
    return f"Extracted {len(all_df)} datasets with {sum(len(df) for df in all_df)} total records"

def load_to_postgres(**context):
    logger = logging.getLogger(__name__)
    
    # Pull data from XCom
    serialized_data = context['task_instance'].xcom_pull(key='covid_data', task_ids='extract_covid_data')
    total_records = context['task_instance'].xcom_pull(key='total_records', task_ids='extract_covid_data')
    
    if not serialized_data:
        logger.warning("No data received from extraction task")
        return "No data to load"
    
    logger.info(f"[load_to_postgres] Received {len(serialized_data)} datasets with {total_records} total records")
    
    # Reconstruct dataframes from JSON
    all_df = []
    for json_data in serialized_data:
        df = pd.read_json(json_data, orient='records')
        # Convert File_Date back to date objects
        df['File_Date'] = pd.to_datetime(df['File_Date']).dt.date
        all_df.append(df)
    
    if len(all_df) > 0:
        final_df = pd.concat(all_df, ignore_index=True)
        final_df = final_df.drop_duplicates(keep="first")
        
        logger.info(f"Final dataset shape: {final_df.shape}")
        logger.info(f"Sample data:\n{final_df.head()}")
        
        # Save to CSV for debugging
        csv_path = "/tmp/covid_data.csv"
        final_df.to_csv(csv_path, index=False)
        logger.info(f"Data saved to {csv_path} for debugging")
        
        try:
            with engine.begin() as conn:
                rows_inserted = final_df.to_sql("raw_covid_data",
                                con=conn,
                                if_exists="append",
                                index=False,
                                method="multi",
                                chunksize=5000)
                logger.info(f"Successfully inserted {len(final_df)} rows into raw_covid_data table")
                
        except Exception as e:
            logger.error(f"Failed to insert data into postgres: {str(e)}")
            raise
            
        return f"Successfully loaded {len(final_df)} records to postgres"
    else:
        logger.warning("No valid dataframes to load")
        return "No valid data to load"


dag = DAG(
        dag_id="covid_data",
        start_date=datetime(2025, 8, 15),
        schedule="@daily",
        catchup=False,
        default_args={
        "execution_timeout": timedelta(seconds=300)
        }
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



