FROM python:alpine

WORKDIR /usr/src/app

COPY /src .
COPY requirements.txt .

RUN apk add --update --no-cache postgresql-client
RUN apk add --update --no-cache --virtual .tmp-build-deps \
     gcc libc-dev linux-headers postgresql-dev musl-dev zlib-dev \
     && apk add --no-cache --virtual .postgis-rundeps-edge \
        --repository http://dl-cdn.alpinelinux.org/alpine/edge/testing \
        --repository http://dl-cdn.alpinelinux.org/alpine/edge/main \
        geos \
        gdal \
        proj-util
ENV GDAL_VERSION=3.1.2-r0
RUN pip install --no-binary fiona 
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "./src/main.py"]
