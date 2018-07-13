import unittest
import yaml
import sys
import logging
logging.basicConfig(level=logging.DEBUG, format=	'[%(asctime)s]:[%(levelname)s]:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='tests_vaaspipe.log')

sys.path.insert(0, '../lib/')
import vaas_de as vaas_de

logging.captureWarnings(True)
				
''' https://dzone.com/articles/tdd-python-5-minutes
'''

class VaaSPipeTests(unittest.TestCase):
	
	def test_dbONE_api_query(self):
		
		with open("test_datasource.yml","r") as input:
			datasource=yaml.load(input)
		
		with open("test_service_configuration.yml","r") as input:
			service=yaml.load(input)

		query_file=open(service['Service']['query_file'], 'rb')
				
		api_response = vaas_de.query_dbONE(datasource.get('nG1_API').get('host'), 
                                   datasource.get('nG1_API').get('port'),
								   query_file,
								   datasource.get('nG1_API').get('user'), 
								   datasource.get('nG1_API').get('password'))
		
		self.assertEqual(api_response.status_code, 200 )

		query_file.close()
		
	def test_transformations(self):
		self.maxDiff=None
		with open("test_transformation.yml","r") as input:
			transformations=yaml.load(input)
		with open("test_service_configuration.yml","r") as input:
			service=yaml.load(input)
		
		response=open("api_response.txt","r")

		result = vaas_de.transformation(response.read(),service['Service']['output_format'], transformations)
		
		
		with open("result_file__transformation___test.txt", "w", newline='\r\n') as rs:
			rs.write(result)	
		
		
		reference_file=open('transformation_reference.txt', 'r',newline='') #https://stackoverflow.com/questions/5144382/preserve-end-of-line-style-when-working-with-files-in-python
		
		reference_result = reference_file.read()

		self.assertEqual(result, reference_result)
		
		response.close()
		reference_file.close()

	def test_transformation_add_header(self):
		
		with open("transformations_add_header.yml","r") as input:
			transformations=yaml.load(input)
		with open("test_add_header_service_configuration.yml","r") as input:
			service=yaml.load(input)
		
		response=open("api_response_no_header.txt","r")

		result = vaas_de.transformation(response.read(),service['Service']['output_format'], transformations)
		
		with open("result_file_____test.txt", "w", newline='\r\n') as rs:
			rs.write(result)
		
		reference_file=open('transformation_reference_add_header.txt', 'r', newline='') #https://stackoverflow.com/questions/5144382/preserve-end-of-line-style-when-working-with-files-in-python
		
		reference_result = reference_file.read()

		self.assertEqual(result, reference_result)
		
		response.close()
		reference_file.close()

	def test_transformation_modify_header(self):
		self.maxDiff=None
		with open("transformations_modify_header.yml","r") as input:
			transformations=yaml.load(input)
		with open("test_modify_header_service_configuration.yml","r") as input:
			service=yaml.load(input)
		
		response=open("api_response.txt","r")

		result = vaas_de.transformation(response.read(),service['Service']['output_format'], transformations)
		
		with open("result_file_____test.txt", "w", newline='\r\n') as rs:
			rs.write(result)
		
		reference_file=open('transformation_reference_modify_header.txt', 'r', newline='') #https://stackoverflow.com/questions/5144382/preserve-end-of-line-style-when-working-with-files-in-python
		
		reference_result = reference_file.read()

		self.assertEqual(result, reference_result)
		
		response.close()
		reference_file.close()
			
	
	
if __name__ == '__main__':

    unittest.main()