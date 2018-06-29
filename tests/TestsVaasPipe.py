import unittest
import yaml
import argparse
sys.path.insert(0, '../lib/')
import vaas_de


''' https://dzone.com/articles/tdd-python-5-minutes
'''

class MyFirstTests(unittest.TestCase):
	def test_hello(self):
			self.assertEqual(hello_world(), 'hello world')
			
	
	
	