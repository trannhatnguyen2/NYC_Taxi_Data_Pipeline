FROM apache/airflow:2.7.1

USER root

# Install Java
RUN apt-get update && apt-get install -y openjdk-11-jdk

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
ENV MINIO_ENDPOINT minio:9000
ENV MINIO_ACCESS_KEY minio_access_key
ENV MINIO_SECRET_KEY minio_secret_key

RUN export JAVA_HOME

RUN apt-get update && \
    apt-get install -y git && \
    apt-get install -y libgomp1 

USER airflow
# This is to fix a bug in Airflow with PostgreSQL connection
RUN pip install git+https://github.com/mpgreg/airflow-provider-great-expectations.git@87a42e275705d413cd4482134fc0d94fa1a68e6f


COPY ./requirements.txt /requirements.txt
RUN pip install --user -r /requirements.txt