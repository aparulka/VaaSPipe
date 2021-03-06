FROM python:3.7.0-alpine

RUN apk update \
        && apk add --progress musl-dev \
        && apk add --progress gcc \
        && apk add --progress postgresql-client \
        && apk add --progress postgresql-dev  \
	&& apk add --progress openssh \
	&& apk add --progress python2 \
	&& apk add --progress python2-dev \
	&& apk add --progress py2-pip 

# Python2 release in Alpine does not support pip upgrades, so skipping

RUN python3 -m pip install --upgrade pip \
#	&& python2 -m pip install PyYAML requests python-dateutil pytz psycopg2 \
	&& python3 -m pip install PyYAML requests python-dateutil pytz psycopg2

# Clean-up dependencies
RUN apk del gcc musl-dev

RUN mkdir /VaaSPipe
WORKDIR /VaaSPipe
COPY . /VaaSPipe
#VOLUME /VaaSPipe
ENTRYPOINT /bin/sh
 
