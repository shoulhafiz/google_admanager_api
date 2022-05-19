# Import the library.
from googleads import ad_manager
from datetime import datetime, timedelta
import traceback
import tempfile
from pathlib import Path
import gzip
import pandas as pd
import os

# Initialize a client object, by default uses the credentials in ~/googleads.yaml.
client = ad_manager.AdManagerClient.LoadFromStorage()

# Initialize a service.
network_service = client.GetService('NetworkService', version='v202202')

# Make a request.
current_network = network_service.getCurrentNetwork()

print ('Found network %s (%s)!' % (current_network['displayName'],current_network['networkCode']))


def get_report():
	# statement = (ad_manager.StatementBuilder(version='v202202')
	# 			.Where('id = :id')
	# 			.WithBindVariable('id', int(10))
	# 			.Limit(None)
	# 			.Offset(None))

	# end_date = datetime.now().date()
	# start_date = end_date - timedelta(days=60)

	report_job = {
		'reportQuery': {
			# 'dimensions': ['MONTH_AND_YEAR','WEEK','DATE','HOUR','LINE_ITEM_TYPE','AD_UNIT_NAME','DEVICE_CATEGORY_NAME','TARGETING'],
			# 'dimensions': ['MONTH_AND_YEAR','WEEK','DATE','HOUR','AD_UNIT_NAME'],
			'dimensions':['WEEK','DEVICE_CATEGORY_NAME','DEVICE_CATEGORY_ID'],
			# 'dimensionAttributes':['LINE_ITEM_LABELS'],
			# 'customFieldIds':['ytdevice','ytvrallowed','yt_channel_id'],
			# 'statement': statement.ToStatement(),
			'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS','TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'],
			'dateRangeType': 'LAST_3_MONTHS',
			# 'startDate':start_date,
			# 'endDate':end_date
		}
	}

	report_downloader = client.GetDataDownloader(version='v202202')

	try:
		report_job_id = report_downloader.WaitForReport(report_job)
	except errors.AdManagerReportError as e:
		print(f'Failed to generate report. Error was: {e}')

	export_format = 'CSV_DUMP'

	report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz',delete=False)

	report_downloader.DownloadReportToFile(
		report_job_id, export_format, report_file)

	report_file.close()

	print(f'Report job with id {report_job_id} downloaded to: \n{report_file.name}')

	with open(report_file.name,'rb') as report:
		temp = gzip.GzipFile(fileobj=report)
		df = pd.read_csv(temp,sep=',')

	print(df)

	os.remove(report_file.name)
	# with open(report_file.name, 'rb') as report:
	# 	report_reader = csv.reader(report)
	# 	for row in report_reader:
	# 		process_row(row)

def main():
	get_report()

if __name__ == '__main__':
	main()