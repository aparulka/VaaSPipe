import yaml
import argparse
import logging
import lib.vaas_de as vaas_de

# vaaspipe -service service.yml -transformations transformations.yml -notification notification.yml -datasource datasource.yml
parser = argparse.ArgumentParser(description='VaaS Data Extraction for nGenius by NETSCOUT', 
                                 usage='vaaspipe -service service.yml -transformations transformations.yml -notification notification.yml -datasource datasource.yml', prog='vasspipe')



parser.add_argument('-s','-service', action="store", dest="service")
parser.add_argument('-t','-transformations', action="store", dest="transformations")
parser.add_argument('-n','-notifications', action="store", dest="notifications")
parser.add_argument('-d','-datasource', action="store", dest="datasource")

pipe_setup=parser.parse_args()

datasource=yaml.load(open(pipe_setup.datasource,"r"))
transformations=yaml.load(open(pipe_setup.transformations,"r"))
service=yaml.load(open(pipe_setup.service,"r"))
notification=yaml.load(open(pipe_setup.notifications,"r"))


logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s]:[%(levelname)s]:%(message)s', 
                    datefmt='%m/%d/%Y %I:%M:%S %p', filename=service['Service']['logging'])


logging.info("Query File: "+service['Service']['query_file'])
query_file=open(service['Service']['query_file'], 'rb')
with open(service['Service']['query_file'], 'rb') as input:
			query=yaml.load(input)


			
api_response=None
if service['Service']['Service_Category'] in ['Applications', 'Links', 'Service Enablers', 'Unified Communications']:
	api_response = vaas_de.query_dbONE(datasource.get('nG1_API').get('host'), 
									   datasource.get('nG1_API').get('port'), 
									   query_file,
									   datasource.get('nG1_API').get('user'), 
									   datasource.get('nG1_API').get('password')
									   ,ssl=datasource.get('nG1_API').get('ssl')
									   )
elif service['Service']['Service_Category'] in ['Infrastructure']:
	api_response = vaas_de.query_nGPulse_server(datasource['nGPulse'],query['Query'], ssl=datasource.get('nGPulse').get('ssl'))
elif service['Service']['Service_Category'] in ['VoIP Test']:
	api_response = vaas_de.query_nGPulse_voip(datasource['nGPulse'],query['Query'], ssl=datasource.get('nGPulse').get('ssl'))
elif service['Service']['Service_Category'] in ['Latency Test']:
	api_response = vaas_de.query_nGPulse_latency(datasource['nGPulse'],query['Query'], ssl=datasource.get('nGPulse').get('ssl'))
elif service['Service']['Service_Category'] in ['Ping Test']:
	api_response = vaas_de.query_nGPulse_ping(datasource['nGPulse'],query['Query'], ssl=datasource.get('nGPulse').get('ssl'))
elif service['Service']['Service_Category'] in ['Web Test']:
	api_response = vaas_de.query_nGPulse_web(datasource['nGPulse'],query['Query'], ssl=datasource.get('nGPulse').get('ssl'))
elif service['Service']['Service_Category'] in ['O365 OneDrive Test']:	
	api_response = vaas_de.query_nGPulse_o365_onedrive(datasource['nGPulse'],query['Query'])
elif service['Service']['Service_Category'] in ['O365 Outlook Test']:	
	api_response = vaas_de.query_nGPulse_o365_outlook(datasource['nGPulse'],query['Query'])
elif service['Service']['Service_Category'] in ['Dimensions']:
	api_response = vaas_de.query_psql(datasource.get('postGres').get('host'),datasource.get('postGres').get('user'),datasource.get('postGres').get('password'),datasource.get('postGres').get('dbname'),service.get('Service').get('query_file'))
elif service['Service']['Service_Category'] in ['Dimension_Device-Interface']:
	api_response = vaas_de.device_extraction(datasource.get('nG1_API').get('host'), 
               datasource.get('nG1_API').get('port'), 
               query['Query'],
               datasource.get('nG1_API').get('user'), 
               datasource.get('nG1_API').get('password'),                      
               ssl=datasource.get('nG1_API').get('ssl')
               )
	else:
	raise Exception(service['Service_Category']+' is not a valid Service Category')
								   								   
								   
query_file.close()
logging.info("=========== Start Transformations ======")

result =   vaas_de.transformation(api_response, service['Service']['output_format'],
                                  transformations) 

timestamp=vaas_de.get_time()
								  
attachment_name = service['Service']['filename']+timestamp+'.csv'
subject = service['Service']['Key']+";"+timestamp


vaas_de.send_notification(notification['Notifications']['smtp_server'],
                          notification['Notifications']['port'],
						  notification['Notifications']['from'],
						  notification['Notifications']['receiver'],
						  subject,
						  service['Service']['Description'], 
						  result, attachment_name)

