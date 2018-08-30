## What is VaaSPipe

VaaSPipe is a flexible module to extract datasets (KPIs, Dimension) from VaaS customer systems.
It is written in Python3 and takes in 4 YAML arguments to chain the various components of each data pipeline:

- Service Configuration: service configuration and main parameters
- Transformation: defines rules to modify the output from API calls
- Notification: specify the details of the STMP server and a list of email addresses to send CSV (tab-separated) outputs attached to an email
- Data Source: specify the details of the source system. It supports nG1, nGP and nG1 Postgres data sources

## Running VaaSPipe:

Typical command:

```
/VaaSPipe # python3 vaaspipe.py -s service_configuration/applications/service_applications_daily.yml -t transformations/transformations_apps.yml -n global_config/notifications.yml -d global_config/datasource.yml```
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

