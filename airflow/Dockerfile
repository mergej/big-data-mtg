FROM marcelmittelstaedt/airflow:latest

ADD ./src /home/airflow/airflow/dags/

ADD ./requirements.txt /home/tmp/python/

WORKDIR /home/tmp/python/
RUN apt-get update -y
RUN apt-get install libsasl2-dev -y
RUN apt-get install libmysqlclient-dev -y
RUN apt-get install python-mysqldb -y
RUN pip3 install -r requirements.txt

WORKDIR /

# Expose Airflow Web Service Port
EXPOSE 8080

# Start startup Script
ENTRYPOINT ["/startup.sh"]