FROM osgeo/gdal:latest

RUN apt-get update
ENV DISPLAY=:0
RUN apt-get install python python3-pip \
    libpq-dev postgresql postgresql-contrib \
    unixodbc-dev python3-tk -y


WORKDIR /usr/src
RUN ls


COPY /src .
COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["python3", "./main.py"]
