FROM apache/airflow:3.0.1

#use root to install package
USER root

#system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    vim \
    git \
    libpq-dev \
    && apt-get autoremove -y --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


#Return to airflow user for security
USER airflow
WORKDIR /opt/airflow



#install python dependencies using pip since that's what you're using
COPY requirements.txt .
RUN pip install --upgrade pip  \
    && pip install --no-cache-dir -r requirements.txt


#Copy DAGs and other necesary files
COPY dags/ /opt/airflow/dags
COPY plugins/ /opt/airflow/plugins
COPY config/ /opt/airflow/config
COPY dbt/covid_transform/    /opt/airflow/dbt/covid_transform/