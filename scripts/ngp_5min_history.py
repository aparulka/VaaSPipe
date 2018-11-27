import subprocess
import argparse

# ------- assume initial state is -0 days -12 hours, -0 days -0 hours

parser = argparse.ArgumentParser()

parser.add_argument("--service", dest="service", help="service type: O365_outlook|O365_onedrive|VoIP|fileServer")
parser.add_argument("--days", dest="days", type=int, help="1-30")

args = parser.parse_args()

if (args.service == "O365_outlook"):
     
	sc_file='service_configuration/service_tests/O365_outlook/O365_outlook_5min_trend.yml'
	t_file='transformations/transformations_O365_outlook.yml'
	query_file='nGP_queries/service_tests/O365_outlook/O365_outlook_query_5min_trend.yml'
	temp_query_file='nGP_queries/service_tests/O365_outlook/O365_outlook_query_5min_trend_temp.yml'

elif (args.service == "O365_onedrive"):

	sc_file='service_configuration/service_tests/O365_onedrive/O365_onedrive_5min_trend.yml'
	t_file='transformations/transformations_O365_onedrive.yml'
	query_file='nGP_queries/service_tests/O365_onedrive/O365_onedrive_query_5min_trend.yml'
	temp_query_file='nGP_queries/service_tests/O365_onedrive/O365_onedrive_query_5min_trend_temp.yml'
	
elif (args.service == "VoIP"):

	sc_file='service_configuration/service_tests/voip/voip_5min_trend.yml'
	t_file='transformations/transformations_voip.yml'
	query_file='nGP_queries/service_tests/voip/voip_query_5min_trend.yml'
	temp_query_file='nGP_queries/service_tests/voip/voip_query_5min_trend_temp.yml'
	
elif (args.service == "fileServer"):

	sc_file='service_configuration/service_tests/file_server/file_server_5min_trend.yml'
	t_file='transformations/transformations_file_server.yml'
	query_file='nGP_queries/service_tests/file_server/file_server_query_5min_trend.yml'
	temp_query_file='nGP_queries/service_tests/file_server/file_server_query_5min_trend_temp.yml'
	


max = (args.days * 2)

for i in range (0,max):
	start = i * 12
	new_start = start + 12
	start_days=new_start//24
	start_hours=(new_start - (new_start//24*24))
	
	end = start - 12
	new_end = end + 12 
	end_days=new_end//24
	end_hours=(new_end - (new_end//24*24))
	
	#debug="Range is from -%i -%i to -%i -%i" % (start_hours, start_days, end_hours, end_days)
	#print(debug)
	command="sed -e '8s/hours: -12/hours: -%i/' -e '9s/days: -0/days: -%i/' -e '17s/hours: -0/hours: -%i/' -e '18s/days: -0/days: -%i/' %s > %s" %(start_hours, start_days, end_hours, end_days, query_file, temp_query_file)
	subprocess.call([command],shell=True)
	
	
	command="python3 vaaspipe.py -s %s -t %s -n global_config/notifications.yml -d global_config/ngpulse.yml" %(sc_file, t_file)

	subprocess.call([command],shell=True)
	
	


