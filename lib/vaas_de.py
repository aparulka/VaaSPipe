import csv
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
	
	return response
	
	
def query_nGPulse(host, port, query, username, password, version=None):
	'''
	Builds nGPulse API query
	'''
	
	return ''
 

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

	result_set = text.strip().split("\n")
	logging.debug(">>>======RESULT SET========")
	logging.debug(result_set)
	logging.debug("======RESULT SET========<<<<")
	
	api_headers= result_set.pop(0).split(',')
	api_body = list(map(lambda x: x.replace('"','').split(','),result_set)) 
	
	
	transformed = []
	transformed.append(output_headers)
	
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
				# eRow.append( "transformation_"+transformations[out_field]['type']() )		
				
				
		logging.info(eRow)			
		transformed.append(eRow)
		
	logging.debug(transformed)
	
	response = StringIO()
	writer = csv.writer(response,delimiter=',',quoting=csv.QUOTE_MINIMAL)
	
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