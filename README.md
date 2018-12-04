## What is VaaSPipe

VaaSPipe is a flexible module to extract datasets (KPIs, Dimensions) from VaaS customer systems.
It is written in Python3 and takes in 4 YAML arguments to chain the various components of each data pipeline:

- Service Configuration: service configuration and main parameters
- Transformation: defines rules to modify the output from API calls
- Notification: specify the details of the STMP server and a list of email addresses to send CSV (tab-separated) outputs attached to an email
- Data Source: specify the details of the source system. It supports nG1, nGP and nG1 Postgres data sources

## Building and Running VaaSPipe:

VaaSPipe is designed to be run inside a container with all required Python dependencies. The image is based off Alpine OS and as of this writting takes about 300MB.

The lastest production & development ready image can be fetched from http://vaas-git.labs.netscout.com/erodrigu/docker_images.git , VaaSPipe/Dockerfile
( http://172.20.237.165/erodrigu/docker_images/tree/master/VaaSPipe ).  Build your image/container by following the instructions in the corresponding README.md

One can also run VaaSPipe on any system with Python and these requirements installed:

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

NOTE: Before you run a sample command, make sure notification.yml has been edited to include only your target email address.


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
```
- Select a directory to host your development environment
- git init # setup git in development directory
- git clone http://vaas-git.labs.netscout.com/erodrigu/VaaSPipe.git # clone VaaSPipe
- git remote add origin http://vaas-git.labs.netscout.com/erodrigu/VaaSPipe.git # setup origin to simplify commands
- git fetch --all
- git reset --hard origin/master
- git checkout -b your-branch-name master
- git branch # ensure you're in the right branch
<< Work on your code and configuration changes >>
- git add -A :/ # stage changes
- git commit –a # commit changes locally.  Do not forget to add a commit message
- git push -u origin your-branch-name # push changes to GitLab server
```

After testing the feature and getting approval, you'll be ready to merge to master to produce a new release

```
- git checkout master
if you need to discard any local changes on that branch, issue
- git checkout master -f
- git pull origin master
- git merge first_updates
- git push origin master
```

## Resource utilization

To monitor VaaSPipe, use docker stats command.  Typically, VaaSPipe execution last a few seconds (<10 seconds) and use around 20MB of RAM. In idle state, the memory footprint of VaaSPipe is only 644KBytes.

```
docker stats vaaspipe
```

