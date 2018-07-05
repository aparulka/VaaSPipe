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

api_response = vaas_de.query_dbONE(datasource.get('nG1_API').get('host'), 
                                   datasource.get('nG1_API').get('port'), 
								   #datasource.get('nG1_API').get('query'), 
								   query_file,
								   datasource.get('nG1_API').get('user'), 
								   datasource.get('nG1_API').get('password'))

query_file.close()
logging.info("=========== Start Transformations ======")

result =   vaas_de.transformation(api_response.text, service['Service']['output_format'],
                                  transformations['Transformations']) 

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