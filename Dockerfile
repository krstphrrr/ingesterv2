FROM osgeo/gdal:latest

RUN apt-get update

RUN apt-get install python python3-pip \
    libpq-dev postgresql postgresql-contrib \
    unixodbc-dev python3-tk -y
    # curl apt-transport-https

# RUN apt-get install unixodbc odbcinst1debian2
# RUN curl -s https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
#     curl -s https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
#     apt-get update && ACCEPT_EULA=Y apt-get -y install msodbcsql17

WORKDIR /usr/src
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY / .
# RUN mkdir dimas
COPY /dimas /dimas

# RUN apt-get install -y mdbtools
RUN apt-get install git -y
RUN apt-get install libtool automake autoconf glib2.0 -y
RUN apt-get install bison flex -y
RUN git clone https://github.com/mdbtools/mdbtools.git

RUN cd mdbtools\
  && autoreconf -i -f\
  && ./configure --prefix=/out --disable-man --with-unixodbc=/usr\
  && make\
  && make install
RUN ldconfig

# COPY mdbtools-drv.ini /etc/odbcinst.ini
# RUN pip3 install --no-cache-dir -r requirements.txt

# CMD ["python3", "main.py"]

# winpty docker container run -it -v //C/Users/://external ingester
