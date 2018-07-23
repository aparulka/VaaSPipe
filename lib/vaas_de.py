import csv
import json
from io import StringIO
import requests
import logging
import datetime
from dateutil.parser import parse
from dateutil.tz import gettz
from dateutil.relativedelta import relativedelta

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # https://stackoverflow.com/questions/27981545/suppress-insecurerequestwarning-unverified-https-request-is-being-made-in-pytho

import sys, traceback


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

dbONE_API_headers= {'Content-Type': 'text/xml;charset=UTF-8'}
service_mappings_separator="\t"
tzinfos = {"EDT": gettz("America/Boston")}


def query_dbONE(host, port, query, username, password, headers=dbONE_API_headers, api_version=None, verify=False, conversion='true',DT='csv', encrypted='false'):
	'''
	Builds DBONE API query.
	Format as of nG1 6.1 is of type: https://192.168.99.18:8443/dbonequerydata/?username=svc-dBONE/password=F34Mu93S7Rv6/encrypted=false/conversion=true/DT=csv 
	'''
	
	query_url='https://'+host+':'+str(port)+'/dbonequerydata/?username='+username+'/password='+password+'/encrypted='+encrypted+'/conversion='+conversion+'/DT='+DT
	
	logging.info(query_url)
	
	response = requests.post(query_url, headers=headers, data=query, verify=verify)
	
	return response.text
	
	
def query_nGPulse(datasource, query, version=None):
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
	now = datetime.datetime.now()	
	start_time = (now + relativedelta(**kpi_filter_params['start']['relativedelta'])).replace(**kpi_filter_params['start']['replace'])
	end_time = (now + relativedelta(**kpi_filter_params['end']['relativedelta'])).replace(**kpi_filter_params['end']['replace'])
	start_time_ms = int(start_time.strftime("%s")) * 1000
	end_time_ms = int(end_time.strftime("%s")) * 1000
	output_datestamp =  end_time.strftime("%Y-%m-%d")
	
	kpi_filter_params['start'] = start_time_ms
	kpi_filter_params['end'] = end_time_ms
	
	logging.info("start_time: "+start_time.strftime('%d-%m-%Y %H:%M:%S'))
	logging.info("end_time: "+end_time.strftime('%d-%m-%Y %H:%M:%S'))
	
	
	# Get the Acces Token
	url = 'http://' + hostname + '/ipm/auth/login'
	data = {'emailOrUsername' : datasource['emailOrUsername'], 'password' : datasource['password']}
	response = requests.post(url, data=data)
	authentication_json = json.loads(response.text)
	print(authentication_json)
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
	writer = csv.writer(parsed_response,delimiter=',',quoting=csv.QUOTE_MINIMAL)
	
	# Writing header as a Transformation.
	# header=['Date','Location','Service Test','Availability (%)']
	# writer.writerow(header)
	
	for nGP_Service_Test in nGP_Service_Test_List:
		id = services[nGP_Service_Test]
		
		kpi_filter_params['test'] = id
		
		response = requests.get(url, params=kpi_filter_params, headers=auth_headers)
		
		parsed_json = json.loads(response.text)


	 
		for index, item in enumerate(parsed_json['data']):
			nPoint =  parsed_json['data'][index]['agent']['name']
			if nPoint in nGP_Location_List:  
				availability =  parsed_json['data'][index]['availPercent'] 
				writer.writerow([output_datestamp,nPoint,nGP_Service_Test,availability])

	
	return parsed_response.getvalue()
 
def query_nGPulse_server(datasource, query, version=None):
	'''
	Builds nGPulse API query
	'''

	# Setup
	
	hostname = get_hostname(datasource['host'], datasource['port'])
	print(hostname)
	nGP_Server_List = query['nGP_Server_List']
	
	# kpi_filter_params = {'metrics': 'availPercent', 'type': 'test,agent', 'end': 'end_time_ms', 'start': 'start_time_ms', 'test': 'id', 'rowLimit': 100}
	kpi_filter_params = query['kpi_filter_params']

	# Time calculations
	now = datetime.datetime.now()	
	start_time = (now + relativedelta(**kpi_filter_params['start']['relativedelta'])).replace(**kpi_filter_params['start']['replace'])
	end_time = (now + relativedelta(**kpi_filter_params['end']['relativedelta'])).replace(**kpi_filter_params['end']['replace'])
	start_time_ms = int(start_time.strftime("%s")) * 1000
	end_time_ms = int(end_time.strftime("%s")) * 1000
	output_datestamp =  end_time.strftime("%Y-%m-%d")
	
	kpi_filter_params['start'] = start_time_ms
	kpi_filter_params['end'] = end_time_ms
	
	logging.info("start_time: "+start_time.strftime('%d-%m-%Y %H:%M:%S'))
	logging.info("end_time: "+end_time.strftime('%d-%m-%Y %H:%M:%S'))
	
	
	# Get the Acces Token
	url = 'http://' + hostname + '/ipm/auth/login'
	data = {'emailOrUsername' : datasource['emailOrUsername'], 'password' : datasource['password']}
	response = requests.post(url, data=data)
	authentication_json = json.loads(response.text)
	print(authentication_json)
	token =  authentication_json['accessToken']
	token_string = ('Access %s' %token)
	auth_headers = {'ngp-authorization' : token_string}

	# Infrastructure querying
	
	url = 'http://' + hostname + '/query/table'


	# f.write ('Date,VaaS_Customer,Service,Infrastructure_ID,Normal (%),Degraded (%),Excessive (%),Critical (%),No Data (%),Count\n')

	headers = {'ngp-authorization' : token_string}

	response = requests.get(url, params=kpi_filter_params, headers=headers)
	 
	parsed_json = json.loads(response.text)

	
	parsed_response = StringIO()
	writer = csv.writer(parsed_response,delimiter=',',quoting=csv.QUOTE_MINIMAL)
	
	for index, item in enumerate(parsed_json['data']):
	  server =  parsed_json['data'][index]['server']['name']
	  if server in nGP_Server_List:  
	   green =  parsed_json['data'][index]['status']['green'] 
	   yellow =  parsed_json['data'][index]['status']['yellow'] 
	   orange =  parsed_json['data'][index]['status']['orange'] 
	   red =  parsed_json['data'][index]['status']['red'] 
	   gray =  parsed_json['data'][index]['status']['gray'] 
	   count =  parsed_json['data'][index]['status']['count'] 
	   writer.writerow([output_datestamp,server,green,yellow,orange,red,gray,count])
	   
	return parsed_response.getvalue()
 
 
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

	result_set = text.strip().split("\r\n")
	logging.debug(">>>======RESULT SET========")
	logging.debug(result_set)
	logging.debug("======RESULT SET========<<<<")
	
	
	
	Header=True if 'Header' in transformations.keys() else False
	
	if Header:
		if 'add_header' in transformations['Header'].keys():
			# Adding a header to the result_set
			result_set.insert(0, ",".join(transformations['Header']['add_header']))
			
		if 'modify_header' in transformations['Header'].keys():
			# Assumes result_set contains header. Rename certain fields as per 'modify_header' mapping configuration
			header=result_set[0].split(',')
			mapping=transformations['Header']['modify_header']
			for key, value in mapping.items():
				header[header.index(key)]=value		
			
			result_set[0]=",".join(header)
	else:
		logging.debug("No changes made to Header")
	
	logging.debug(result_set)
	api_headers= result_set.pop(0).split(',')
	logging.debug(api_headers)
	api_body = list(map(lambda x: x.replace('"','').split(','),result_set)) 
	
	
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
	writer = csv.writer(response,delimiter=',',quoting=csv.QUOTE_MINIMAL)
	
	writer.writerows(transformed)
	
	#print("\n".join(map(lambda x: print(x), transformed)))
	#return "\n".join(map(lambda x: ",".join(x), transformed))
	return response.getvalue()


def process_header(csv_input, transformations):

	return "header,header,header"

def get_body(csv_input):

	return"body,body,body"
	
def transformation_simple(record, headers, mapping, transformations):
	logging.info("A simple transformation: "+ mapping)
	
	try:
		with open(transformations['mapping_file'], 'r') as mappings:
			lookups = list(csv.DictReader(mappings, delimiter=service_mappings_separator))
		lookup_column = headers.index(transformations['lookup_column'])
		
		for lookup in lookups:
			if lookup[transformations['lookup_column']] in record:
				return lookup[mapping]

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

		server_time = datetime.datetime.now()

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
	return datetime.datetime.now().strftime(format_str)