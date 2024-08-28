FROM python:3.8
COPY requirements.txt /tmp
RUN pip3 install --upgrade pip
RUN pip install -r /tmp/requirements.txt
RUN pip install mysqlclient
RUN apt-get update 
COPY tema2.py /tema2/
WORKDIR /tema2
EXPOSE 5000

CMD python3 tema2.py
