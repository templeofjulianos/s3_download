FROM debian

ADD s3_download.py .

RUN apt-get -y update && apt-get -y upgrade
RUN apt-get -y install python3-pip
RUN pip3 install boto3 && pip3 install pyyaml