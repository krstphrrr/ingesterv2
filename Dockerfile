FROM osgeo/gdal:latest

RUN apt-get update

RUN apt-get install python python3-pip \
    libpq-dev postgresql postgresql-contrib \
    unixodbc-dev python3-tk -y


WORKDIR /usr/src
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY / .
# RUN mkdir dimas
COPY /dimas /dimas


# RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["python3", "main.py"]
# winpty docker container run -it -v //C/Users/://external ingester
