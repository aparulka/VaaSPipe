## What is VaaSPipe

VaaSPipe is a flexible module to extract datasets (KPIs, Dimension) from VaaS customer systems.
It is written in Python3 and takes in 4 YAML arguments to chain the various components of each data pipeline:

- Service Configuration: service configuration and main parameters
- Transformation: defines rules to modify the output from API calls
- Notification: specify the details of the STMP server and a list of email addresses to send CSV (tab-separated) outputs attached to an email
- Data Source: specify the details of the source system. It supports nG1, nGP and nG1 Postgres data sources

## Running VaaSPipe:

VaaSPipe is designed to be run inside a container with all required Python dependencies. The image is based off Alpine OS and as of this writting takes about 300MB.

One can also run VaaSPipe on any system with Python and this requirements installed:

```
/VaaSPipe # python3 -m pip list
Package         Version
--------------- ---------
certifi         2018.8.24
chardet         3.0.4
idna            2.7
pip             18.0
psycopg2        2.7.5
python-dateutil 2.7.3
pytz            2018.5
PyYAML          3.13
requests        2.19.1
setuptools      40.2.0
six             1.11.0
urllib3         1.23
wheel           0.31.1

```

Typical command:

```
/VaaSPipe # python3 vaaspipe.py -s service_configuration/applications/service_applications_daily.yml -t transformations/transformations_apps.yml -n global_config/notifications.yml -d global_config/datasource.yml
```

VaaSPipe help:

```
/VaaSPipe # python3 vaaspipe.py -h
usage: vaaspipe -service service.yml -transformations transformations.yml -notification notification.yml -datasource datasource.yml

VaaS Data Extraction for nGenius by NETSCOUT

optional arguments:
  -h, --help            show this help message and exit
  -s SERVICE, -service SERVICE
  -t TRANSFORMATIONS, -transformations TRANSFORMATIONS
  -n NOTIFICATIONS, -notifications NOTIFICATIONS
  -d DATASOURCE, -datasource DATASOURCE
```

## Developing for VaaSPipe:

If you want to merge any code into VaaSPipe, you'll need a pull request, or email eduardo.rodriguez@netscout.com.

The typical development workflow used in this project is this:

- Select a directory to host your development environment

