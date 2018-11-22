import csv, json, yaml
from io import StringIO
import requests
import logging
import datetime
from dateutil.parser import parse
from dateutil.tz import gettz
from dateutil.relativedelta import relativedelta
import pytz
import psycopg2

import os

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
	

def query_dbONE(host, port, query, username, password, ssl=True, headers=dbONE_API_headers, api_version=None, verify=False, conversion='true',DT='csv', encrypted='false'):
	'''
	Builds DBONE API query.
	Format as of nG1 6.1 is of type: https://192.168.99.18:8443/dbonequerydata/?username=svc-dBONE/password=F34Mu93S7Rv6/encrypted=false/conversion=true/DT=csv 
	
	Output Format:
	['serviceId\ttargetTime\tfailedTransactions\ttotalTransactions\tresponseTime\tfailedPercentage\tserviceId_String\ttargetTime_String', '122029775\t1535256000000\t0\t67407\t139304.6454063840\t0.0000000000\tO365 Authentication (Pune)\tSun Aug 26 00:00:00 EDT 2018', '122030298\t1535256000000\t0\t714182\t291171.5849273787\t0.0000000000\tO365 Exchange (Pune)\tSun Aug 26 00:00:00 EDT 2018', '148578737\t1535256000000\t0\t38039\t317043.3394678725\t0.0000000000\tSalesForce (Pune)\tSun Aug 26 00:00:00 EDT 2018']
	'''
	
	# defaults to SSL
	protocol = get_protocol(ssl)
	print(protocol)
	
	query_url= protocol+host+':'+str(port)+'/dbonequerydata/?username='+username+'/password='+password+'/encrypted='+encrypted+'/conversion='+conversion+'/DT='+DT
	
	logging.info(query_url)
	
	api_response = requests.post(query_url, headers=headers, data=query, verify=verify)
	
	reader = csv.DictReader(StringIO(api_response.text), delimiter=",", quotechar='"')
	
	response=StringIO()
	writer = csv.DictWriter(response, reader.fieldnames, delimiter=output_separator)
	writer.writeheader()
	writer.writerows(reader)
	return response.getvalue().strip().split("\r\n")

def query_psql(host,user,password,dbname,sql):

	conn = psycopg2.connect(host=host,user=user,password=password,dbname=dbname)
	cur = conn.cursor()
	query_file=open(sql, 'r')
	query = query_file.read()

	cur.execute(query)
	all = cur.fetchall()
	
	#all=[(58841411, '050PLUS', '050Plus', 'UGP@050Plus', 'WEB', 'TCP'), (58835929, '30th Activ', '30th Active Directory', '30th Active Directory', 'NONE', 'TCP'),(58836025, '30th Intra', '30th Intra-Farm Services', '30th Intra-Farm Services', 'NONE', 'TCP')]
	
	myList = []
	
	for row in all:
		myList.append(output_separator.join(map(str,row)))
 
	return myList
 
def query_nGPulse_server(datasource, query, version=None, ssl=False):
	'''
	Builds nGPulse API query
	'''

	# ------- Common Setup -------------	
	
	protocol = get_protocol(ssl)
	hostname = get_hostname(datasource['host'], datasource['port'])
	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}
	kpi_filter_params = query['kpi_filter_params']
	kpi_filter_params = _start_to_end_time_ms(kpi_filter_params)
	
	logging.info("start_time: "+ kpi_filter_params['start_str'])
	logging.info("end_time: "+ kpi_filter_params['end_str'])
	
	token = _nGPulse_token(datasource['emailOrUsername'], 
							datasource['password'],
							protocol,
							hostname)
	
	auth_headers = _nGPulse_auth_headers(token)
	
	output_datestamp =  kpi_filter_params['start_str']	
	start_time_ms = kpi_filter_params['start']
	end_time_ms = kpi_filter_params['end']	
	# ------- Common Setup -------------	

	# ------- Test-specific setup -------------	
	
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
	
	# ------- Test-specific setup -------------	
	
	# ------- Test-specific query and data processing -------------
	
	url = protocol + hostname + '/query/table'

	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	for type in kpi_filter_params['type']:
		kpi_filter_params['type'] = type
		response = requests.get(url, params=kpi_filter_params, headers=auth_headers) 
		parsed_json = json.loads(response.text)
		
		for index, item in enumerate(parsed_json['data']):
			service =  parsed_json['data'][index][type]['deviceType']
			infrastructureId =  parsed_json['data'][index][kpi_filter_params['type']]['name']	
			try:
				locationId = parsed_json['data'][index][type]['sites'][0]['name']
			except (IndexError,KeyError):
				locationId = 'Unknown'
			if (infrastructureId in infra[service] or infra[service] == []):
				green =  parsed_json['data'][index]['status']['green'] 
				yellow =  parsed_json['data'][index]['status']['yellow'] 
				orange =  parsed_json['data'][index]['status']['orange'] 
				red =  parsed_json['data'][index]['status']['red'] 
				gray =  parsed_json['data'][index]['status']['gray'] 
				count =  parsed_json['data'][index]['status']['count'] 	
				writer.writerow([output_datestamp,service.replace(output_separator, " "),locationId.replace(output_separator, " "),infrastructureId.replace(output_separator, " "),green,yellow,orange,red,gray,count, start_time_ms, end_time_ms])
	   
	return parsed_response.getvalue().strip().split("\r\n")

def query_nGPulse_voip(datasource, query, version=None, ssl=False):
	'''
	Builds nGPulse API query
	'''
	# ------- Common Setup -------------	
	
	protocol = get_protocol(ssl)
	hostname = get_hostname(datasource['host'], datasource['port'])
	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}
	kpi_filter_params = query['kpi_filter_params']
	kpi_filter_params = _start_to_end_time_ms(kpi_filter_params)
	
	logging.info("start_time: "+ kpi_filter_params['start_str'])
	logging.info("end_time: "+ kpi_filter_params['end_str'])
	
	token = _nGPulse_token(datasource['emailOrUsername'], 
							datasource['password'],
							protocol,
							hostname)
	
	auth_headers = _nGPulse_auth_headers(token)
	
	output_datestamp =  kpi_filter_params['start_str']	
	start_time_ms = kpi_filter_params['start']
	end_time_ms = kpi_filter_params['end']	
	# ------- Common Setup -------------	

	# ------- Test-specific setup -------------	
	
	nGP_Service_Test_List = query['nGP_Service_Test_List'] or []
	group = 'VoIP'
	service_type_name = 'VoipPulse'

	# ------- Test-specific setup -------------	
	
	# ------- Test-specific query and data processing -------------
	
	service_dict = _nGPulse_get_tests(protocol, hostname, auth_headers, group, service_type_name)

	url = protocol + hostname + '/query/table'

	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	
	for nGP_Service_Test, item in service_dict.items():
		
		id = service_dict[nGP_Service_Test]
		# check if this service test is on our list or if the list is null (meaning get all service tests)
		if (nGP_Service_Test in nGP_Service_Test_List or nGP_Service_Test_List  == []):
			kpi_filter_params['test'] = id
			headers = auth_headers
			response = requests.get(url, params=kpi_filter_params, headers=headers) 
			parsed_json = json.loads(response.text)
			
			if ('trends' not in kpi_filter_params):
				# ------- Query does not relate to trends -------
		
				# get the data from all the npoints
				for index, item in enumerate(parsed_json['data']):
					nPoint =  parsed_json['data'][index]['agent']['name'].replace(output_separator, " ")
					availability =  parsed_json['data'][index]['availPercent']
					caller_mos =  parsed_json['data'][index]['avgLqmosRx'] 
					callee_mos =  parsed_json['data'][index]['avgLqmosTx'] 
					count  =  parsed_json['data'][index]['count'] 
					writer.writerow([output_datestamp,nGP_Service_Test.replace(output_separator, " "),nPoint,availability,caller_mos,callee_mos,count, start_time_ms, end_time_ms])

	
			else:
				# ------- Query is for trends -------
				# get the data from all the npoints
				
				# ------- Get trend data for kpi#1 (availability)
				
				
				for index, item in enumerate(parsed_json['data']):
					nPoint =  parsed_json['data'][index]['agent']['name']
					
					kpi1_trend_dict = {}
					kpi2_trend_dict = {}
					kpi3_trend_dict = {}
					
					for index1, item1 in enumerate(parsed_json['data'][index]['trends']['availability']['data']):
						
						availability =  parsed_json['data'][index]['trends']['availability']['data'][index1]['value']
						str = parsed_json['data'][index]['trends']['availability']['data'][index1]['str']
						# ------- Handle 'str' format: 2018-Oct-30_11:09 -------
						time = datetime.datetime.strptime(str,'%Y-%b-%d_%H:%M')
						if ('count' in parsed_json['data'][index]['trends']['availability']['data'][index1]):
							kpi1_trend_dict[time] = availability
						
						
					for index1, item1 in enumerate(parsed_json['data'][index]['trends']['avgLqmosRx']['data']):
						
						caller_mos =  parsed_json['data'][index]['trends']['avgLqmosRx']['data'][index1]['value']
						str = parsed_json['data'][index]['trends']['avgLqmosRx']['data'][index1]['str']
						# ------- Handle 'str' format: 2018-Oct-30_11:09 -------
						time = datetime.datetime.strptime(str,'%Y-%b-%d_%H:%M')
						if ('count' in parsed_json['data'][index]['trends']['avgLqmosRx']['data'][index1]):
							kpi2_trend_dict[time] = caller_mos
							
							
					for index1, item1 in enumerate(parsed_json['data'][index]['trends']['avgLqmosTx']['data']):
						
						callee_mos =  parsed_json['data'][index]['trends']['avgLqmosTx']['data'][index1]['value']
						str = parsed_json['data'][index]['trends']['avgLqmosTx']['data'][index1]['str']
						# ------- Handle 'str' format: 2018-Oct-30_11:09 -------
						time = datetime.datetime.strptime(str,'%Y-%b-%d_%H:%M')
						if ('count' in parsed_json['data'][index]['trends']['avgLqmosTx']['data'][index1]):
							kpi3_trend_dict[time] = callee_mos	
						
					count = 1	
					
					for key in kpi1_trend_dict:
						try:
							caller_mos = kpi2_trend_dict[key]
						except KeyError:
							caller_mos = ''
						try:
							callee_mos = kpi3_trend_dict[key]
						except KeyError:
							callee_mos = ''
							
						writer.writerow(
							[key,
							nGP_Service_Test.replace(output_separator, " "),
							nPoint.replace(output_separator, " "),
							kpi1_trend_dict[key],
							caller_mos,
							callee_mos,
							count,
							start_time_ms,
							end_time_ms])

	return parsed_response.getvalue().strip().split("\r\n")
	
	
	

def query_nGPulse_latency(datasource, query, version=None, ssl=False):
	'''
	Builds nGPulse API query
	'''
	# ------- Common Setup -------------	
	
	protocol = get_protocol(ssl)
	hostname = get_hostname(datasource['host'], datasource['port'])
	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}
	kpi_filter_params = query['kpi_filter_params']
	kpi_filter_params = _start_to_end_time_ms(kpi_filter_params)
	
	logging.info("start_time: "+ kpi_filter_params['start_str'])
	logging.info("end_time: "+ kpi_filter_params['end_str'])
	
	token = _nGPulse_token(datasource['emailOrUsername'], 
							datasource['password'],
							protocol,
							hostname)
	
	auth_headers = _nGPulse_auth_headers(token)
	
	output_datestamp =  kpi_filter_params['start_str']	
	start_time_ms = kpi_filter_params['start']
	end_time_ms = kpi_filter_params['end']	
	# ------- Common Setup -------------	
	# ------- Test-specific setup -------------	

	nGP_Service_Test_List = query['nGP_Service_Test_List'] or []
	group = 'latency'
	service_type_name = 'latency'
	# ------- Test-specific setup -------------	

	# ------- Test-specific query and data processing -------------

	service_dict = _nGPulse_get_tests(protocol, hostname, auth_headers, group, service_type_name)

	url = protocol + hostname + '/query/table'

	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	
	for nGP_Service_Test, item in service_dict.items():
		id = service_dict[nGP_Service_Test]
		# check if this service test is on our list or if the list is null (meaning get all service tests)
		if (nGP_Service_Test in nGP_Service_Test_List or nGP_Service_Test_List  == []):
			kpi_filter_params['test'] = id
			headers = auth_headers
			response = requests.get(url, params=kpi_filter_params, headers=headers) 
			parsed_json = json.loads(response.text)
		
			# get the data from all the npoints
			for index, item in enumerate(parsed_json['data']):
				nPoint =  parsed_json['data'][index]['agent']['name']
				availability =  parsed_json['data'][index]['availPercent'] 
				Avg_Latency =  parsed_json['data'][index]['avgavg']
				Best_Latency =  parsed_json['data'][index]['avgbest']
				Worst_Latency =  parsed_json['data'][index]['avgworst']
				count = parsed_json['data'][index]['count'] 
				writer.writerow([output_datestamp,nGP_Service_Test.replace(output_separator, " "),nPoint.replace(output_separator, " "),availability,Avg_Latency,Best_Latency,Worst_Latency,count, start_time_ms, end_time_ms])
				
	return parsed_response.getvalue().strip().split("\r\n")
	
def query_nGPulse_ping(datasource, query, version=None, ssl=False):
	'''
	Builds nGPulse API query
	'''
	# ------- Common Setup -------------	
	
	protocol = get_protocol(ssl)
	hostname = get_hostname(datasource['host'], datasource['port'])
	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}	kpi_filter_params = query['kpi_filter_params']
	kpi_filter_params = query['kpi_filter_params']
	kpi_filter_params = _start_to_end_time_ms(kpi_filter_params)
	
	logging.info("start_time: "+ kpi_filter_params['start_str'])
	logging.info("end_time: "+ kpi_filter_params['end_str'])
	
	token = _nGPulse_token(datasource['emailOrUsername'], 
							datasource['password'],
							protocol,
							hostname)
	
	auth_headers = _nGPulse_auth_headers(token)
	
	output_datestamp =  kpi_filter_params['start_str']	
	start_time_ms = kpi_filter_params['start']
	end_time_ms = kpi_filter_params['end']	
	# ------- Common Setup -------------	
	# ------- Test-specific setup -------------	
	nGP_Service_Test_List = query['nGP_Service_Test_List'] or []
	group = 'ping'
	service_type_name = 'ping'
	# ------- Test-specific setup -------------		

	# ------- Test-specific query and data processing -------------

	service_dict = _nGPulse_get_tests(protocol, hostname, auth_headers, group, service_type_name)
	
	url = protocol + hostname + '/query/table'

	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	for nGP_Service_Test, item in service_dict.items():
		id = service_dict[nGP_Service_Test]
		# check if this service test is on our list or if the list is null (meaning get all service tests)
		if (nGP_Service_Test in nGP_Service_Test_List or nGP_Service_Test_List  == []):
			kpi_filter_params['test'] = id
			response = requests.get(url, params=kpi_filter_params, headers=auth_headers) 
			parsed_json = json.loads(response.text)
		
			# get the data from all the npoints
			for index, item in enumerate(parsed_json['data']):
				nPoint =  parsed_json['data'][index]['agent']['name']
				availability =  parsed_json['data'][index]['availPercent'] 
				Avg_Ping_Latency =  parsed_json['data'][index]['avgping_results']
				count = parsed_json['data'][index]['count'] 
				writer.writerow([output_datestamp,nGP_Service_Test.replace(output_separator, " "),nPoint.replace(output_separator, " "),availability,Avg_Ping_Latency,count, start_time_ms, end_time_ms])
			
				
	return parsed_response.getvalue().strip().split("\r\n")
 	
def query_nGPulse_web(datasource, query, version=None, ssl=False):
	'''
	Builds nGPulse API query
	'''
	# ------- Common Setup -------------	
	
	protocol = get_protocol(ssl)
	hostname = get_hostname(datasource['host'], datasource['port'])
	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}
	kpi_filter_params = query['kpi_filter_params']
	kpi_filter_params = _start_to_end_time_ms(kpi_filter_params)
	
	logging.info("start_time: "+ kpi_filter_params['start_str'])
	logging.info("end_time: "+ kpi_filter_params['end_str'])
	
	token = _nGPulse_token(datasource['emailOrUsername'], 
							datasource['password'],
							protocol,
							hostname)
	
	auth_headers = _nGPulse_auth_headers(token)
	
	output_datestamp =  kpi_filter_params['start_str']	
	start_time_ms = kpi_filter_params['start']
	end_time_ms = kpi_filter_params['end']	
	# ------- Common Setup -------------
	# ------- Test-specific setup -------------	
	nGP_Service_Test_List = query['nGP_Service_Test_List'] or []
	group = 'Web'
	service_type_name = 'Web'
	# ------- Test-specific setup -------------
	# ------- Test-specific query and data processing -------------

	service_dict = _nGPulse_get_tests(protocol, hostname, auth_headers, group, service_type_name)

	url = protocol + hostname + '/query/table'

	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	
	for nGP_Service_Test, item in service_dict.items():
		id = service_dict[nGP_Service_Test]
		# check if this service test is on our list or if the list is null (meaning get all service tests)
		if (nGP_Service_Test in nGP_Service_Test_List or nGP_Service_Test_List  == []):
			kpi_filter_params['test'] = id
			response = requests.get(url, params=kpi_filter_params, headers=auth_headers) 
			parsed_json = json.loads(response.text)
		
			# get the data from all the npoints
			for index, item in enumerate(parsed_json['data']):
				nPoint =  parsed_json['data'][index]['agent']['name']
				availability =  parsed_json['data'][index]['availPercent'] 
				Avg_Response =  parsed_json['data'][index]['avgResponse']
				count = parsed_json['data'][index]['count'] 
				writer.writerow([output_datestamp,nGP_Service_Test.replace(output_separator, " "),nPoint.replace(output_separator, " "),availability,Avg_Response,count, start_time_ms, end_time_ms])
			
				
	return parsed_response.getvalue().strip().split("\r\n")
	
def query_nGPulse_o365_onedrive(datasource, query, version=None, ssl=False):
	'''
	Builds nGPulse API query
	'''
	# ------- Common Setup -------------	
	
	protocol = get_protocol(ssl)
	hostname = get_hostname(datasource['host'], datasource['port'])
	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}
	kpi_filter_params = query['kpi_filter_params']
	kpi_filter_params = _start_to_end_time_ms(kpi_filter_params)
	
	logging.info("start_time: "+ kpi_filter_params['start_str'])
	logging.info("end_time: "+ kpi_filter_params['end_str'])
	
	token = _nGPulse_token(datasource['emailOrUsername'], 
							datasource['password'],
							protocol,
							hostname)
	
	auth_headers = _nGPulse_auth_headers(token)
	
	output_datestamp =  kpi_filter_params['start_str']	
	start_time_ms = kpi_filter_params['start']
	end_time_ms = kpi_filter_params['end']	
	# ------- Common Setup -------------
	# ------- Test-specific setup -------------	
	nGP_Service_Test_List = query['nGP_Service_Test_List'] or []
	group = 'o365AccountOneDrive'
	service_type_name = 'o365AccountOneDrive'
	# ------- Test-specific setup -------------
	# ------- Test-specific query and data processing -------------

	service_dict = _nGPulse_get_tests(protocol, hostname, auth_headers, group, service_type_name)
	
	url = protocol + hostname + '/query/table'

	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	
	for nGP_Service_Test, item in service_dict.items():
		id = service_dict[nGP_Service_Test]
		# check if this service test is on our list or if the list is null (meaning get all service tests)
		if (nGP_Service_Test in nGP_Service_Test_List or nGP_Service_Test_List  == []):
			kpi_filter_params['test'] = id

			response = requests.get(url, params=kpi_filter_params, headers = auth_headers) 
			parsed_json = json.loads(response.text)
			
			if ('trends' not in kpi_filter_params):
				# ------- Query does not relate to trends -------
		
				# get the data from all the npoints
				for index, item in enumerate(parsed_json['data']):
					nPoint =  parsed_json['data'][index]['agent']['name']
					availability =  parsed_json['data'][index]['availPercent'] 
					maxupload_time =  parsed_json['data'][index]['maxupload_time']
					count = parsed_json['data'][index]['count'] 
					writer.writerow([output_datestamp,nGP_Service_Test.replace(output_separator, " "),nPoint.replace(output_separator, " "),availability,maxupload_time,count, start_time_ms, end_time_ms])
				
			
			else:
				# ------- Query is for trends -------
				# get the data from all the npoints
				
				# ------- Get trend data for kpi#1 (availability)
				
				
				for index, item in enumerate(parsed_json['data']):
					nPoint =  parsed_json['data'][index]['agent']['name']
					
					kpi1_trend_dict = {}
					kpi2_trend_dict = {}
					
					for index1, item1 in enumerate(parsed_json['data'][index]['trends']['availability']['data']):
						
						availability =  parsed_json['data'][index]['trends']['availability']['data'][index1]['value']
						str = parsed_json['data'][index]['trends']['availability']['data'][index1]['str']
						# ------- Handle 'str' format: 2018-Oct-30_11:09 -------
						time = datetime.datetime.strptime(str,'%Y-%b-%d_%H:%M')
						if ('count' in parsed_json['data'][index]['trends']['availability']['data'][index1]):
							kpi1_trend_dict[time] = availability
						
						
					for index1, item1 in enumerate(parsed_json['data'][index]['trends']['maxupload_time']['data']):
						
						maxupload_time =  parsed_json['data'][index]['trends']['maxupload_time']['data'][index1]['value']
						str = parsed_json['data'][index]['trends']['maxupload_time']['data'][index1]['str']
						# ------- Handle 'str' format: 2018-Oct-30_11:09 -------
						time = datetime.datetime.strptime(str,'%Y-%b-%d_%H:%M')
						if ('count' in parsed_json['data'][index]['trends']['maxupload_time']['data'][index1]):
							kpi2_trend_dict[time] = maxupload_time
						
						
					count = 1	
					
					for key in kpi1_trend_dict:
						try:
							maxupload_time = kpi2_trend_dict[key]
						except KeyError:
							maxupload_time = ''
						writer.writerow(
							[key,
							nGP_Service_Test.replace(output_separator, " "),
							nPoint.replace(output_separator, " "),
							kpi1_trend_dict[key],
							maxupload_time,
							count,
							start_time_ms,
							end_time_ms])

	return parsed_response.getvalue().strip().split("\r\n")
 	
def query_nGPulse_o365_outlook(datasource, query, version=None, ssl=False):
	'''
	Builds nGPulse API query
	'''
	# ------- Common Setup -------------	
	
	protocol = get_protocol(ssl)
	hostname = get_hostname(datasource['host'], datasource['port'])
	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}
	kpi_filter_params = query['kpi_filter_params']
	kpi_filter_params = _start_to_end_time_ms(kpi_filter_params)
	
	logging.info("start_time: "+ kpi_filter_params['start_str'])
	logging.info("end_time: "+ kpi_filter_params['end_str'])
	
	token = _nGPulse_token(datasource['emailOrUsername'], 
							datasource['password'],
							protocol,
							hostname)
	
	auth_headers = _nGPulse_auth_headers(token)
	
	output_datestamp =  kpi_filter_params['start_str']	
	start_time_ms = kpi_filter_params['start']
	end_time_ms = kpi_filter_params['end']	
	# ------- Common Setup -------------
	# ------- Test-specific setup -------------	
	nGP_Service_Test_List = query['nGP_Service_Test_List'] or []
	group = 'o365AccountOutlook'
	service_type_name = 'o365AccountOutlook'
	# ------- Test-specific setup -------------
	# ------- Test-specific query and data processing -------------

	service_dict = _nGPulse_get_tests(protocol, hostname, auth_headers, group, service_type_name)
	
	url = protocol + hostname + '/query/table'

	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=output_separator,quoting=csv.QUOTE_MINIMAL)
	
	
	
	for nGP_Service_Test, item in service_dict.items():
		id = service_dict[nGP_Service_Test]
		# check if this service test is on our list or if the list is null (meaning get all service tests)
		if (nGP_Service_Test in nGP_Service_Test_List or nGP_Service_Test_List  == []):
			kpi_filter_params['test'] = id
			
			response = requests.get(url, params=kpi_filter_params, headers= auth_headers) 
			parsed_json = json.loads(response.text)
			
			if ('trends' not in kpi_filter_params):
				# ------- Query does not relate to trends -------
		
				# get the data from all the npoints
				for index, item in enumerate(parsed_json['data']):
					nPoint =  parsed_json['data'][index]['agent']['name']
					availability =  parsed_json['data'][index]['availPercent'] 
					maxresp_time =  parsed_json['data'][index]['maxresp_time']
					count = parsed_json['data'][index]['count'] 
					writer.writerow([output_datestamp,nGP_Service_Test.replace(output_separator, " "),nPoint.replace(output_separator, " "),availability,maxresp_time,count, start_time_ms, end_time_ms])
			

			else:
				# ------- Query is for trends -------
				# get the data from all the npoints
				
				# ------- Get trend data for kpi#1 (availability)
				
				
				for index, item in enumerate(parsed_json['data']):
					nPoint =  parsed_json['data'][index]['agent']['name']
					
					kpi1_trend_dict = {}
					kpi2_trend_dict = {}
					
					for index1, item1 in enumerate(parsed_json['data'][index]['trends']['availability']['data']):
						
						availability =  parsed_json['data'][index]['trends']['availability']['data'][index1]['value']
						str = parsed_json['data'][index]['trends']['availability']['data'][index1]['str']
						# ------- Handle 'str' format: 2018-Oct-30_11:09 -------
						time = datetime.datetime.strptime(str,'%Y-%b-%d_%H:%M')
						if ('count' in parsed_json['data'][index]['trends']['availability']['data'][index1]):
							kpi1_trend_dict[time] = availability
						
						
					for index1, item1 in enumerate(parsed_json['data'][index]['trends']['maxresp_time']['data']):
						
						maxresp_time =  parsed_json['data'][index]['trends']['maxresp_time']['data'][index1]['value']
						str = parsed_json['data'][index]['trends']['maxresp_time']['data'][index1]['str']
						# ------- Handle 'str' format: 2018-Oct-30_11:09 -------
						time = datetime.datetime.strptime(str,'%Y-%b-%d_%H:%M')
						if ('count' in parsed_json['data'][index]['trends']['maxresp_time']['data'][index1]):
							kpi2_trend_dict[time] = maxresp_time
						
						
					count = 1	
					
					for key in kpi1_trend_dict:
						try:
							maxresp_time = kpi2_trend_dict[key]
						except KeyError:
							maxresp_time = ''
						writer.writerow(
							[key,
							nGP_Service_Test.replace(output_separator, " "),
							nPoint.replace(output_separator, " "),
							kpi1_trend_dict[key],
							maxresp_time,
							count,
							start_time_ms,
							end_time_ms])

	return parsed_response.getvalue().strip().split("\r\n")
				
def _nGPulse_token( emailOrUsername, password, protocol, hostname ):
	'''

	'''
	endpoint= '/ipm/auth/login'
	
	url = protocol + hostname + endpoint
	
	data = {'emailOrUsername' : emailOrUsername, 'password' : password}
	response = requests.post(url, data= data)
	
	authentication_json = json.loads(response.text)
	token =  authentication_json['accessToken']
	
	return token

def _nGPulse_auth_headers(token):
	'''
	'''
	token_string = ('Access %s' %token)
	auth_headers = {'ngp-authorization' : token_string}

	return auth_headers
	
def _start_to_end_time_ms(kpi_filter_params):	
	'''
	'''
	
	date_str_format='%d-%m-%Y %H:%M:%S'
	
	now = datetime.datetime.now(tz)	
	start_time = (now + relativedelta(**kpi_filter_params['start']['relativedelta'])).replace(**kpi_filter_params['start']['replace'])

	end_time = (now + relativedelta(**kpi_filter_params['end']['relativedelta'])).replace(**kpi_filter_params['end']['replace'])
	
	start_time_ms = int(start_time.timestamp())
	end_time_ms = int(end_time.timestamp())
	
	kpi_filter_params['start'] = start_time_ms
	kpi_filter_params['end'] = end_time_ms
	
	kpi_filter_params['start_str'] = start_time.strftime(date_str_format)
	kpi_filter_params['end_str'] = end_time.strftime(date_str_format)
	
	return kpi_filter_params
			
def _nGPulse_get_tests(protocol, hostname, auth_headers, group, service_type_name):

	url = protocol + hostname + '/ipm/v1/admin/testTypes'
	params = {'query' : '{"status":"Running","group":"'+group+'"}'}
	
	response = requests.get(url, params = params, headers = auth_headers)
	service_type_json = json.loads(response.text)
	
	for index, item in enumerate(service_type_json):
		if (service_type_json[index]['name'] == service_type_name):
			service_type_id =  service_type_json[index]['_id']
	
	# Get a list of service tests 
	url = protocol + hostname + '/ipm/v1/admin/tests'
	params = {'query' : '{"status":"Running"}'}
	
	response = requests.get(url, params = params, headers = auth_headers)
	services_json = json.loads(response.text)

	service_dict = {}
	
	for index, item in enumerate(services_json):
		name =  services_json[index]['name']
		id =  services_json[index]['_id']
		type = services_json[index]['type']
		if (type == service_type_id):
			service_dict[name] = id
			
	return service_dict

def _nGPulse_query_table():
	return True
	
def get_hostname(hostname, port):	
	if port is not None:
		return hostname + ":" + port
	else:
		return hostname

def get_protocol(ssl=True):
	return 'https://' if ssl in [None, True] else 'http://'
		
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
	
def csv_to_disk(content,filename,directory):

	output = os.path.join(directory,filename)
	logging.info("========== Writing to local CSV %s ==========", output)
    
	file = open(output,"w")
	file.write(content)
	file.close()
	
def get_time(format_str):
	return datetime.datetime.now(tz).strftime(format_str)
