import csv, json, yaml
from io import StringIO
import requests
import logging
import datetime
from dateutil.parser import parse
from dateutil.tz import gettz
from dateutil.relativedelta import relativedelta
import pytz

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # https://stackoverflow.com/questions/27981545/suppress-insecurerequestwarning-unverified-https-request-is-being-made-in-pytho

import sys, traceback


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

with open('global_config/vaas_lib.yml', 'rb') as input:
			vaas_lib=yaml.load(input)

			
			
dbONE_API_headers= {'Content-Type': 'text/xml;charset=UTF-8'}

service_mappings_separator=vaas_lib['Output_Separator']
output_separator=vaas_lib['Output_Separator']


# Available timezones are stored in pytz.all_timezones

tz = pytz.timezone(vaas_lib['Timezone'])


# https://stackoverflow.com/questions/38470781/how-to-get-timezone-from-datetime-string-in-python

tzinfos= dict()
for code, timezone in vaas_lib['Tzinfos'].items(): 
	tzinfos[code] = gettz(timezone)
	

def query_dbONE(host, port, query, username, password, headers=dbONE_API_headers, api_version=None, verify=False, conversion='true',DT='csv', encrypted='false'):
	'''
	Builds DBONE API query.
	Format as of nG1 6.1 is of type: https://192.168.99.18:8443/dbonequerydata/?username=svc-dBONE/password=F34Mu93S7Rv6/encrypted=false/conversion=true/DT=csv 
	
	Output Format:
	['serviceId\ttargetTime\tfailedTransactions\ttotalTransactions\tresponseTime\tfailedPercentage\tserviceId_String\ttargetTime_String', '122029775\t1535256000000\t0\t67407\t139304.6454063840\t0.0000000000\tO365 Authentication (Pune)\tSun Aug 26 00:00:00 EDT 2018', '122030298\t1535256000000\t0\t714182\t291171.5849273787\t0.0000000000\tO365 Exchange (Pune)\tSun Aug 26 00:00:00 EDT 2018', '148578737\t1535256000000\t0\t38039\t317043.3394678725\t0.0000000000\tSalesForce (Pune)\tSun Aug 26 00:00:00 EDT 2018', '94846774\t1535256000000\t0\t8865\t46995.6806848213\t0.0000000000\tO365 SharepointOnline (San Jose)\tSun Aug 26 00:00:00 EDT 2018', '122029802\t1535256000000\t0\t106665\t73553.7996398695\t0.0000000000\tO365 Authentication (San Jose)\tSun Aug 26 00:00:00 EDT 2018', '143019858\t1535256000000\t0\t63500\t59822.7504096811\t0.0000000000\tO365 Exchange (San Jose)\tSun Aug 26 00:00:00 EDT 2018', '154069910\t1535256000000\t0\t23268\t9597.5654082906\t0.0000000000\tO365 DNS - San Jose\tSun Aug 26 00:00:00 EDT 2018', '160829939\t1535256000000\t0\t10384\t108296.9919417476\t0.0000000000\tMS - Oracle Web Service (San Jose)\tSun Aug 26 00:00:00 EDT 2018', '146302544\t1535256000000\t0\t112729\t45417.2753807910\t0.0000000000\tSalesForce (San Jose)\tSun Aug 26 00:00:00 EDT 2018', '150305546\t1535256000000\t0\t4320\t117166.7053240741\t0.0000000000\tSalesForce (Dublin)\tSun Aug 26 00:00:00 EDT 2018', '150890489\t1535256000000\t0\t21609\t81929.8512155592\t0.0000000000\tO365 Authentication (Dublin)\tSun Aug 26 00:00:00 EDT 2018', '150890611\t1535256000000\t0\t6625\t21468.1678743961\t0.0000000000\tO365 Yammer (Dublin)\tSun Aug 26 00:00:00 EDT 2018', '150890195\t1535256000000\t0\t74874\t169611.7007146197\t0.0000000000\tO365 Exchange (Dublin)\tSun Aug 26 00:00:00 EDT 2018', '150890800\t1535256000000\t0\t6462\t100479.6674407803\t0.0000000000\tO365 SharePoint Online (Dublin)\tSun Aug 26 00:00:00 EDT 2018', '160829875\t1535256000000\t0\t8619\t109075.1588242128\t0.0000000000\tMS - Oracle Web Service (Dublin)\tSun Aug 26 00:00:00 EDT 2018', '149342864\t1535256000000\t0\t239052\t14331.4416870124\t0.0000000000\tOracle Web Service (Westford)\tSun Aug 26 00:00:00 EDT 2018', '164944267\t1535256000000\t0\t145920\t23786.7653852749\t0.0000000000\tOracle HTTP Westford\tSun Aug 26 00:00:00 EDT 2018', '130075227\t1535256000000\t0\t62153\t18566.1742623453\t0.0000000000\tO365 DNS (Westford)\tSun Aug 26 00:00:00 EDT 2018', '74789789\t1535256000000\t619\t42856\t4241.3775836352\t1.4443718195\tOracle DB (DEV)\tSun Aug 26 00:00:00 EDT 2018', '121963273\t1535256000000\t2\t299075\t67590.1085916253\t0.0006687286\tO365 Authentication (WST)\tSun Aug 26 00:00:00 EDT 2018', '147541832\t1535256000000\t0\t2046906\t118518.7814007372\t0.0000000000\tO365 Exchange (Westford)\tSun Aug 26 00:00:00 EDT 2018', '147542093\t1535256000000\t0\t76975\t64820.7045221033\t0.0000000000\tO365 SharePointOnline (Westford)\tSun Aug 26 00:00:00 EDT 2018', '148485149\t1535256000000\t0\t2159047\t68781.6154765379\t0.0000000000\tSalesForce (Westford)\tSun Aug 26 00:00:00 EDT 2018', '147542280\t1535256000000\t0\t59647\t34953.7453136011\t0.0000000000\tO365 Yammer (Westford)\tSun Aug 26 00:00:00 EDT 2018', '147542030\t1535256000000\t0\t16943\t30232.1714572390\t0.0000000000\tO365 Online (Westford)\tSun Aug 26 00:00:00 EDT 2018', '174471288\t1535256000000\t0\t568629\t40821.9890067947\t0.0000000000\tSalesForce (Allen)\tSun Aug 26 00:00:00 EDT 2018', '174469762\t1535256000000\t0\t13328\t23962.5794802055\t0.0000000000\tO365 SharePointOnline (Allen)\tSun Aug 26 00:00:00 EDT 2018', '174470370\t1535256000000\t0\t486939\t126330.4470779381\t0.0000000000\tO365 Exchange (Allen)\tSun Aug 26 00:00:00 EDT 2018', '174470316\t1535256000000\t0\t1308\t7626.7706422018\t0.0000000000\tO365 DNS (Allen)\tSun Aug 26 00:00:00 EDT 2018', '174469821\t1535256000000\t0\t82966\t114643.3308956400\t0.0000000000\tO365 Authentication (Allen)\tSun Aug 26 00:00:00 EDT 2018', '174469663\t1535256000000\t0\t1708\t19632.9841920375\t0.0000000000\tO365 Online (Allen)\tSun Aug 26 00:00:00 EDT 2018']
	'''
	
	query_url='https://'+host+':'+str(port)+'/dbonequerydata/?username='+username+'/password='+password+'/encrypted='+encrypted+'/conversion='+conversion+'/DT='+DT
	
	logging.info(query_url)
	
	api_response = requests.post(query_url, headers=headers, data=query, verify=verify)
	
	reader = csv.DictReader(StringIO(api_response.text), delimiter=",", quotechar='"')
	
	response=StringIO()
	writer = csv.DictWriter(response, reader.fieldnames, delimiter=output_separator)
	writer.writeheader()
	writer.writerows(reader)
	return response.getvalue().strip().split("\r\n")
	#return response.text.strip().replace(output_separator, " ").replace(",", output_separator).split("\r\n")
	
def query_nGPulse_availability(datasource, query, version=None):
	'''
	Builds nGPulse API query
	'''

	# Setup
	
	hostname = get_hostname(datasource['host'], datasource['port'])
	print(hostname)
	nGP_Service_Test_List = query['nGP_Service_Test_List']
	nGP_Location_List = query['nGP_Location_List']

	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}
	kpi_filter_params = query['kpi_filter_params']

	# Time calculations
	now = datetime.datetime.now(tz)	
	start_time = (now + relativedelta(**kpi_filter_params['start']['relativedelta'])).replace(**kpi_filter_params['start']['replace'])
	end_time = (now + relativedelta(**kpi_filter_params['end']['relativedelta'])).replace(**kpi_filter_params['end']['replace'])
	start_time_ms = int(start_time.timestamp())
	end_time_ms = int(end_time.timestamp())
	output_datestamp =  start_time.strftime("%d-%m-%Y %H:%M:%S")
	
	kpi_filter_params['start'] = start_time_ms
	kpi_filter_params['end'] = end_time_ms
	
	logging.info("start_time: "+start_time.strftime('%d-%m-%Y %H:%M:%S'))
	logging.info("end_time: "+end_time.strftime('%d-%m-%Y %H:%M:%S'))
	
	
	# Get the Acces Token
	url = 'http://' + hostname + '/ipm/auth/login'
	data = {'emailOrUsername' : datasource['emailOrUsername'], 'password' : datasource['password']}
	response = requests.post(url, data=data)
	authentication_json = json.loads(response.text)
	token =  authentication_json['accessToken']
	token_string = ('Access %s' %token)
	auth_headers = {'ngp-authorization' : token_string}
	
	# Get a list of service tests 
	url = 'http://' + hostname + '/ipm/v1/admin/tests'
	params = {'query' : '{"status":"Running"}'}
	
	response = requests.get(url, params=params, headers=auth_headers)
	services_json = json.loads(response.text)

	services = {}

	for index, item in enumerate(services_json):
		name =  services_json[index]['name']
		id =  services_json[index]['_id']
		services[name] = id

	
	url = 'http://' + hostname + '/query/table'
	
	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	# Writing header as a Transformation.
	# header=['Date','Location','Service Test','Availability (%)']
	# writer.writerow(header)
	
	for nGP_Service_Test in nGP_Service_Test_List:
		id = services[nGP_Service_Test]
		
		kpi_filter_params['test'] = id
		
		response = requests.get(url, params=kpi_filter_params, headers=auth_headers)
		
		parsed_json = json.loads(response.text)


	 
		for index, item in enumerate(parsed_json['data']):
			nPoint =  parsed_json['data'][index]['agent']['name'].replace("\t", " ")
			if nPoint in nGP_Location_List:  
				availability =  parsed_json['data'][index]['availPercent'] 
				writer.writerow([output_datestamp,nPoint,nGP_Service_Test,availability, start_time_ms, end_time_ms])

	
	return parsed_response.getvalue().strip().split("\r\n")
 
def query_nGPulse_server(datasource, query, version=None):
	'''
	Builds nGPulse API query
	'''

	# Setup
	
	hostname = get_hostname(datasource['host'], datasource['port'])

	
	# Include VMs and HyperVisors in nGP_Server_List
	nGP_Server_List = query['nGP_Server_List']
	nGP_Router_List = query['nGP_Router_List']
	nGP_AccessPoint_List = query['nGP_AccessPoint_List']
	nGP_Switch_List = query['nGP_Switch_List']
	nGP_WirelessController_List = query['nGP_WirelessController_List']
	nGP_GenericDevice_List = query['nGP_GenericDevice_List']

	infra = {}

	infra['server'] = nGP_Server_List or []
	infra['router'] = nGP_Router_List or []
	infra['accesspoint'] = nGP_AccessPoint_List or []
	infra['switch'] = nGP_Switch_List or []
	infra['wirelesscontroller'] = nGP_WirelessController_List or []
	infra['generic'] = nGP_GenericDevice_List or []	
	
	
	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}
	kpi_filter_params = query['kpi_filter_params']

	# Time calculations
	now = datetime.datetime.now(tz)	
	start_time = (now + relativedelta(**kpi_filter_params['start']['relativedelta'])).replace(**kpi_filter_params['start']['replace'])
	end_time = (now + relativedelta(**kpi_filter_params['end']['relativedelta'])).replace(**kpi_filter_params['end']['replace'])
	start_time_ms = int(start_time.timestamp())
	end_time_ms = int(end_time.timestamp())
	output_datestamp =  start_time.strftime("%d-%m-%Y %H:%M:%S")
	
	kpi_filter_params['start'] = start_time_ms
	kpi_filter_params['end'] = end_time_ms
	
	logging.info("start_time: "+start_time.strftime('%d-%m-%Y %H:%M:%S'))
	logging.info("end_time: "+end_time.strftime('%d-%m-%Y %H:%M:%S'))
	
	
	# Get the Acces Token
	url = 'http://' + hostname + '/ipm/auth/login'
	data = {'emailOrUsername' : datasource['emailOrUsername'], 'password' : datasource['password']}
	response = requests.post(url, data=data)
	authentication_json = json.loads(response.text)
	token =  authentication_json['accessToken']
	token_string = ('Access %s' %token)
	auth_headers = {'ngp-authorization' : token_string}

	# Infrastructure querying
	
	url = 'http://' + hostname + '/query/table'

	headers = {'ngp-authorization' : token_string}

	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	for type in kpi_filter_params['type']:
		kpi_filter_params['type'] = type
		response = requests.get(url, params=kpi_filter_params, headers=headers) 
		parsed_json = json.loads(response.text)
		
		for index, item in enumerate(parsed_json['data']):
			service =  parsed_json['data'][index][type]['deviceType']
			infrastructureId =  parsed_json['data'][index][kpi_filter_params['type']]['name']
			if infrastructureId in infra[service]:  
				green =  parsed_json['data'][index]['status']['green'] 
				yellow =  parsed_json['data'][index]['status']['yellow'] 
				orange =  parsed_json['data'][index]['status']['orange'] 
				red =  parsed_json['data'][index]['status']['red'] 
				gray =  parsed_json['data'][index]['status']['gray'] 
				count =  parsed_json['data'][index]['status']['count'] 	
				writer.writerow([output_datestamp,service.replace(output_separator, " "),infrastructureId.replace(output_separator, " "),green,yellow,orange,red,gray,count, start_time_ms, end_time_ms])
		
	# for index, item in enumerate(parsed_json['data']):
	  # server =  parsed_json['data'][index]['server']['name']
	  # if server in nGP_Server_List:  
	   # green =  parsed_json['data'][index]['status']['green'] 
	   # yellow =  parsed_json['data'][index]['status']['yellow'] 
	   # orange =  parsed_json['data'][index]['status']['orange'] 
	   # red =  parsed_json['data'][index]['status']['red'] 
	   # gray =  parsed_json['data'][index]['status']['gray'] 
	   # count =  parsed_json['data'][index]['status']['count'] 
	   # writer.writerow([output_datestamp,server,green,yellow,orange,red,gray,count])
	   
	return parsed_response.getvalue().strip().split("\r\n")

def query_nGPulse_voip(datasource, query, version=None):
	'''
	Builds nGPulse API query
	'''

	# Setup
	
	hostname = get_hostname(datasource['host'], datasource['port'])
	print(hostname)

	nGP_Service_Test_List = query['nGP_Service_Test_List']
	
	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}
	kpi_filter_params = query['kpi_filter_params']

	# Time calculations
	now = datetime.datetime.now(tz)	
	start_time = (now + relativedelta(**kpi_filter_params['start']['relativedelta'])).replace(**kpi_filter_params['start']['replace'])
	end_time = (now + relativedelta(**kpi_filter_params['end']['relativedelta'])).replace(**kpi_filter_params['end']['replace'])
	start_time_ms = int(start_time.timestamp())
	end_time_ms = int(end_time.timestamp())
	output_datestamp =  start_time.strftime("%d-%m-%Y %H:%M:%S")
	
	kpi_filter_params['start'] = start_time_ms
	kpi_filter_params['end'] = end_time_ms
	
	logging.info("start_time: "+start_time.strftime('%d-%m-%Y %H:%M:%S'))
	logging.info("end_time: "+end_time.strftime('%d-%m-%Y %H:%M:%S'))
	
	
	# Get the Acces Token
	url = 'http://' + hostname + '/ipm/auth/login'
	data = {'emailOrUsername' : datasource['emailOrUsername'], 'password' : datasource['password']}
	response = requests.post(url, data=data)
	authentication_json = json.loads(response.text)
	token =  authentication_json['accessToken']
	token_string = ('Access %s' %token)
	auth_headers = {'ngp-authorization' : token_string}
	
	# Get a list of service tests 
	url = 'http://' + hostname + '/ipm/v1/admin/tests'
	params = {'query' : '{"status":"Running"}'}
	
	response = requests.get(url, params=params, headers=auth_headers)
	services_json = json.loads(response.text)

	service_dict = {}

	#debug only
	#print "Service Tests available are: \n"

	for index, item in enumerate(services_json):
	 name =  services_json[index]['name']
	 id =  services_json[index]['_id']
	 service_dict[name] = id

	
	url = 'http://' + hostname + '/query/table'

	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	for nGP_Service_Test in nGP_Service_Test_List:
	 id = service_dict[nGP_Service_Test]
	 kpi_filter_params['test'] = id
	 headers = {'ngp-authorization' : token_string}
	 response = requests.get(url, params=kpi_filter_params, headers=headers) 
	 parsed_json = json.loads(response.text)

	 for index, item in enumerate(parsed_json['data']):
	  nPoint =  parsed_json['data'][index]['agent']['name'].replace(output_separator, " ")
	  availability =  parsed_json['data'][index]['availPercent']
	  caller_mos =  parsed_json['data'][index]['avgLqmosRx'] 
	  callee_mos =  parsed_json['data'][index]['avgLqmosTx'] 
	  count  =  parsed_json['data'][index]['count'] 
	  writer.writerow([output_datestamp,nGP_Service_Test.replace(output_separator, " "),nPoint,availability,caller_mos,callee_mos,count, start_time_ms, end_time_ms])

	
	return parsed_response.getvalue().strip().split("\r\n")

def query_nGPulse_latency(datasource, query, version=None):
	'''
	Builds nGPulse API query
	'''

	# Setup
	
	hostname = get_hostname(datasource['host'], datasource['port'])
	print(hostname)

	# populate a dict of service tests per service
	service_app = {}
	service_app = query['Services']
	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}
	kpi_filter_params = query['kpi_filter_params']

	# Time calculations
	now = datetime.datetime.now(tz)	
	start_time = (now + relativedelta(**kpi_filter_params['start']['relativedelta'])).replace(**kpi_filter_params['start']['replace'])
	end_time = (now + relativedelta(**kpi_filter_params['end']['relativedelta'])).replace(**kpi_filter_params['end']['replace'])
	start_time_ms = int(start_time.timestamp())
	end_time_ms = int(end_time.timestamp())
	output_datestamp =  start_time.strftime("%d-%m-%Y %H:%M:%S")
	
	kpi_filter_params['start'] = start_time_ms
	kpi_filter_params['end'] = end_time_ms
	
	logging.info("start_time: "+start_time.strftime('%d-%m-%Y %H:%M:%S'))
	logging.info("end_time: "+end_time.strftime('%d-%m-%Y %H:%M:%S'))
	
	
	# Get the Acces Token
	url = 'http://' + hostname + '/ipm/auth/login'
	data = {'emailOrUsername' : datasource['emailOrUsername'], 'password' : datasource['password']}
	response = requests.post(url, data=data)
	authentication_json = json.loads(response.text)
	token =  authentication_json['accessToken']
	token_string = ('Access %s' %token)
	auth_headers = {'ngp-authorization' : token_string}
	
	# Get a list of service tests 
	url = 'http://' + hostname + '/ipm/v1/admin/tests'
	params = {'query' : '{"status":"Running"}'}
	
	response = requests.get(url, params=params, headers=auth_headers)
	services_json = json.loads(response.text)

	service_dict = {}

	for service in service_app.keys():
		for ping_test in service_app[service]:
			 # get the service_id
			for index, item in enumerate(services_json):
			 name =  services_json[index]['name']
			 id =  services_json[index]['_id']
			 service_dict[name] = id


	url = 'http://' + hostname + '/query/table'

	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	for service in service_app.keys():
	 for ping_test in service_app[service]:
	  kpi_filter_params['test'] = service_dict[ping_test]

	  headers = {'ngp-authorization' : token_string}

	  response = requests.get(url, params=kpi_filter_params, headers=headers)
	 
	  parsed_json = json.loads(response.text)

	  for index, item in enumerate(parsed_json['data']):
	   nPoint =  parsed_json['data'][index]['agent']['name']
	   availability =  parsed_json['data'][index]['availPercent'] 
	   Avg_Latency =  parsed_json['data'][index]['avgavg']
	   Best_Latency =  parsed_json['data'][index]['avgbest']
	   Worst_Latency =  parsed_json['data'][index]['avgworst']
	   count = parsed_json['data'][index]['count'] 
	   writer.writerow([output_datestamp,service.replace(output_separator, " "),ping_test.replace(output_separator, " "),nPoint.replace(output_separator, " "),availability,Avg_Latency,Best_Latency,Worst_Latency,count, start_time_ms, end_time_ms])
	
	return parsed_response.getvalue().strip().split("\r\n")
 	
def query_nGPulse_ping(datasource, query, version=None):
	'''
	Builds nGPulse API query
	'''

	# Setup
	
	hostname = get_hostname(datasource['host'], datasource['port'])
	print(hostname)

	# populate a dict of service tests per service
	service_app = {}
	service_app = query['Services']
	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}
	kpi_filter_params = query['kpi_filter_params']

	# Time calculations
	now = datetime.datetime.now(tz)	
	start_time = (now + relativedelta(**kpi_filter_params['start']['relativedelta'])).replace(**kpi_filter_params['start']['replace'])
	end_time = (now + relativedelta(**kpi_filter_params['end']['relativedelta'])).replace(**kpi_filter_params['end']['replace'])
	start_time_ms = int(start_time.timestamp())
	end_time_ms = int(end_time.timestamp())
	output_datestamp =  start_time.strftime("%d-%m-%Y %H:%M:%S")
	
	kpi_filter_params['start'] = start_time_ms
	kpi_filter_params['end'] = end_time_ms
	
	logging.info("start_time: "+start_time.strftime('%d-%m-%Y %H:%M:%S'))
	logging.info("end_time: "+end_time.strftime('%d-%m-%Y %H:%M:%S'))
	
	
	# Get the Acces Token
	url = 'http://' + hostname + '/ipm/auth/login'
	data = {'emailOrUsername' : datasource['emailOrUsername'], 'password' : datasource['password']}
	response = requests.post(url, data=data)
	authentication_json = json.loads(response.text)
	token =  authentication_json['accessToken']
	token_string = ('Access %s' %token)
	auth_headers = {'ngp-authorization' : token_string}
	
	# Get a list of service tests 
	url = 'http://' + hostname + '/ipm/v1/admin/tests'
	params = {'query' : '{"status":"Running"}'}
	
	response = requests.get(url, params=params, headers=auth_headers)
	services_json = json.loads(response.text)

	service_dict = {}

	for service in service_app.keys():
		for ping_test in service_app[service]:
			 # get the service_id
			for index, item in enumerate(services_json):
			 name =  services_json[index]['name']
			 id =  services_json[index]['_id']
			 service_dict[name] = id


	url = 'http://' + hostname + '/query/table'

	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	for service in service_app.keys():
	 for ping_test in service_app[service]:
	  kpi_filter_params['test'] = service_dict[ping_test]

	  headers = {'ngp-authorization' : token_string}

	  response = requests.get(url, params=kpi_filter_params, headers=headers)
	 
	  parsed_json = json.loads(response.text)

	  for index, item in enumerate(parsed_json['data']):
		   nPoint =  parsed_json['data'][index]['agent']['name']
		   availability =  parsed_json['data'][index]['availPercent'] 
		   Avg_Ping_Latency =  parsed_json['data'][index]['avgping_results']
		   count = parsed_json['data'][index]['count'] 
		   writer.writerow([output_datestamp,service.replace(output_separator, " "),ping_test.replace(output_separator, " "),nPoint.replace(output_separator, " "),availability,Avg_Ping_Latency,count, start_time_ms, end_time_ms])
	
	return parsed_response.getvalue().strip().split("\r\n")
 	
def query_nGPulse_web(datasource, query, version=None):
	'''
	Builds nGPulse API query
	'''

	# Setup
	
	hostname = get_hostname(datasource['host'], datasource['port'])
	print(hostname)

	# populate a dict of service tests per service
	service_app = {}
	service_app = query['Services']
	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}
	kpi_filter_params = query['kpi_filter_params']

	# Time calculations
	now = datetime.datetime.now(tz)	
	start_time = (now + relativedelta(**kpi_filter_params['start']['relativedelta'])).replace(**kpi_filter_params['start']['replace'])
	end_time = (now + relativedelta(**kpi_filter_params['end']['relativedelta'])).replace(**kpi_filter_params['end']['replace'])
	start_time_ms = int(start_time.timestamp())
	end_time_ms = int(end_time.timestamp())
	output_datestamp =  start_time.strftime("%d-%m-%Y %H:%M:%S")
	
	kpi_filter_params['start'] = start_time_ms
	kpi_filter_params['end'] = end_time_ms
	
	logging.info("start_time: "+start_time.strftime('%d-%m-%Y %H:%M:%S'))
	logging.info("end_time: "+end_time.strftime('%d-%m-%Y %H:%M:%S'))
	
	
	# Get the Acces Token
	
	url = 'http://' + hostname + '/ipm/auth/login'
	
	data = {'emailOrUsername' : datasource['emailOrUsername'], 'password' : datasource['password']}
	response = requests.post(url, data=data)
	authentication_json = json.loads(response.text)
	token =  authentication_json['accessToken']
	token_string = ('Access %s' %token)
	auth_headers = {'ngp-authorization' : token_string}
	
	# Get a list of service tests 
	url = 'http://' + hostname + '/ipm/v1/admin/tests'
	params = {'query' : '{"status":"Running"}'}
	
	response = requests.get(url, params=params, headers=auth_headers)
	services_json = json.loads(response.text)

	service_dict = {}

	for service in service_app.keys():
		for ping_test in service_app[service]:
			 # get the service_id
			for index, item in enumerate(services_json):
			 name =  services_json[index]['name']
			 id =  services_json[index]['_id']
			 service_dict[name] = id


	url = 'http://' + hostname + '/query/table'

	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	for service in service_app.keys():
	 for ping_test in service_app[service]:
	  kpi_filter_params['test'] = service_dict[ping_test]

	  headers = {'ngp-authorization' : token_string}

	  response = requests.get(url, params=kpi_filter_params, headers=headers)
	 
	  parsed_json = json.loads(response.text)

	  for index, item in enumerate(parsed_json['data']):
		   nPoint =  parsed_json['data'][index]['agent']['name']
		   availability =  parsed_json['data'][index]['availPercent'] 
		   Avg_Response =  parsed_json['data'][index]['avgResponse']
		   count = parsed_json['data'][index]['count'] 
		   writer.writerow([output_datestamp,service.replace(output_separator, " "),ping_test.replace(output_separator, " "),nPoint.replace(output_separator, " "),availability,Avg_Response,count, start_time_ms, end_time_ms])
	
	return parsed_response.getvalue().strip().split("\r\n")
 	
def get_hostname(hostname, port):	
	if port is not None:
		return hostname + ":" + port
	else:
		return hostname
	
def transformation(text, output_headers, transformations):
	'''
	Provides a CSV dictionary object that matches expected Service Output format.
	It includes empty headers for columnns not present in the input text
	
	text: a single string that contains a CSV file, with '\n' as line separator and ',' as field separator. 1st row is a header with column names
	header: a list containing the name of columns expected in the output
	e.g.
	text= 'serviceId,targetTime,failedTransactions,totalTransactions,responseTime,failedPercentage,serviceId_String,targetTime_String\n122029775,1529208000000,0,63203,146333.6818896887,0.0,"O365 Authentication (Pune)","Sun Jun 17 00:00:00 EDT 2018"\n122030298,1529208000000,0,414200,280154.0962761872,0.0,"O365 Exchange (Pune)","Sun Jun 17 00:00:00 EDT 2018"\n148578737,1529208000000,0,60359,285514.9159168117,0.0,"SalesForce (Pune)","Sun Jun 17 00:00:00 EDT 2018"\n94846774,1529208000000,0,37689,73932.96659587379,0.0,"O365 SharepointOnline (San Jose)","Sun Jun 17 00:00:00 EDT 2018"\n143019858,1529208000000,0,30682,82367.30223308883,0.0,"O365 Exchange (San Jose)","Sun Jun 17 00:00:00 EDT 2018"\n122029802,1529208000000,24,98316,80757.40459491647,0.024411082,"O365 Authentication (San Jose)","Sun Jun 17 00:00:00 EDT 2018"\n146302544,1529208000000,0,34668,43192.169221000375,0.0,"SalesForce (San Jose)","Sun Jun 17 00:00:00 EDT 2018"'
	 
	headers = ['Customer', 'Service', 'Location', 'Date', 'mosBucket3In', 'mosBucket2In', 'mosBucket1In', 'ucServiceId', 'Total_Transactions', 'Avg_Good_Mos', 'Time', 'ucServiceId_String', 'targetTime_String']
	
	'''

	result_set = text
	logging.debug(">>>======RESULT SET========")
	logging.debug(result_set)
	logging.debug("======RESULT SET========<<<<")
	
	Header=True if 'Header' in transformations.keys() else False
	
	if Header:
		if 'add_header' in transformations['Header'].keys():
			# Adding a header to the result_set
			result_set.insert(0, output_separator.join(transformations['Header']['add_header']))
			
		if 'modify_header' in transformations['Header'].keys():
			# Assumes result_set contains header. Rename certain fields as per 'modify_header' mapping configuration
			header=result_set[0].split(output_separator)
			mapping=transformations['Header']['modify_header']
			for key, value in mapping.items():
				header[header.index(key)]=value		
			
			result_set[0]=output_separator.join(header)
	else:
		logging.debug("No changes made to Header")
	
	logging.debug(result_set)
	api_headers= result_set.pop(0).split(output_separator)
	logging.debug(api_headers)
	api_body = list(map(lambda x: x.replace('"','').split(output_separator),result_set)) 
	
	
	transformed = []
	transformed.append(output_headers)
	
	Transformations=True if 'Transformations' in transformations.keys() else False
	
	logging.debug(Transformations)
	
	if Transformations:
		transformations = transformations['Transformations']
	
		for result in api_body:
			eRow = [] # Enhanced Record
			logging.info(result)
			for out_field in output_headers:
				try:
					index = api_headers.index(out_field)
					eRow.append(result[index])
				except ValueError:
					if transformations[out_field]['type'] == 'simple':
						eRow.append(transformation_simple(result, api_headers, out_field, transformations[out_field]))
					if 	transformations[out_field]['type'] == 'date':
						eRow.append(transformation_date(result, api_headers, out_field, transformations[out_field]))
					if 	transformations[out_field]['type'] == 'date_injection':
						eRow.append(transformation_date_injection(result, api_headers, out_field, transformations[out_field]))							
			logging.info(eRow)			
			transformed.append(eRow)
	else:
		transformed = transformed + api_body
		
	logging.debug(transformed)
	
	response = StringIO()
	writer = csv.writer(response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	writer.writerows(transformed)
	
	#print("\n".join(map(lambda x: print(x), transformed)))
	#return "\n".join(map(lambda x: ",".join(x), transformed))
	return response.getvalue()

def transformation_simple(record, headers, mapping, transformations):
	logging.info("A simple transformation: "+ mapping)
	
	try:
		with open(transformations['mapping_file'], 'r') as mappings:
			lookups = list(csv.DictReader(mappings, delimiter=service_mappings_separator))
		lookup_column = headers.index(transformations['lookup_column'])
		
		for lookup in lookups:
			if lookup[transformations['lookup_column']] in record:
				return lookup[mapping]
		return transformations['default']
	except KeyError:
		return transformations['default']	
	
def transformation_date(record, headers, mapping, transformations):

	try:
		logging.info("A date transformation: "+ mapping)
		
		lookup_column = headers.index(transformations['lookup_column'])	
		
		return datetime.date.strftime(parse( record[lookup_column], tzinfos=tzinfos ) , transformations['date_format'])
	except:
		return "00-0-0000 00:00:00"

def transformation_date_injection(record, headers, mapping, transformations):
	try:
		logging.info("A date injection transformation: "+ mapping)

		relativedelta_params = transformations['relativedelta']
		replace_params = transformations['replace']
		
		logging.debug(relativedelta_params)
		logging.debug(replace_params)

		server_time = datetime.datetime.now(tz)

		logging.debug("server_time: "+ str(server_time))

		transformed_time = (server_time + relativedelta(**relativedelta_params)).replace(**replace_params)

		logging.debug("transformed_time: "+ str(transformed_time))
	
		return datetime.date.strftime( transformed_time , transformations['date_format'])
	except Exception:
		print("Exception in user code:")
		print("-"*60)
		traceback.print_exc(file=sys.stdout)
		print("-"*60)
		return "00-0-0000 00:00:00"

def send_notification(server, port, from_email, to_email, subject, msg_body, attachment, filename, tls = 0, password = None):
	msg = MIMEMultipart()
	msg['From'] = from_email
	#msg['To'] = ','.join(to_email)
	msg['Subject'] = subject
	
	msg.attach(MIMEText(msg_body, 'plain'))

	part = MIMEBase('application', 'octet-stream')
	part.set_payload(attachment)
	encoders.encode_base64(part)
	part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
	msg.attach(part)
	
	smtp_srv = smtplib.SMTP(server, int(port))
	if tls == 1:
		smtp_srv.starttls()
		smtp_srv.login(msg['From'], password)
		
	for mail in to_email:
		logging.info("Sending mail to: "+ mail)
		smtp_srv.sendmail(msg['From'], mail, msg.as_string()) 
	
	smtp_srv.quit()
	
def get_time(format_str="%Y%m%d_%H%M"):
	return datetime.datetime.now(tz).strftime(format_str)
