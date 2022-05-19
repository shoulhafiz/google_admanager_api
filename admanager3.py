# Import the library.
from googleads import ad_manager, errors
from datetime import datetime
from datetime import date
import tempfile
import gzip
import pandas as pd
import os
from google.cloud import bigquery
from google.oauth2 import service_account

BQ_KEY_LOCATION = '/app/mpd_bq_key/mpdbq1-bq-key.json'
credentials = service_account.Credentials.from_service_account_file(BQ_KEY_LOCATION)

client_mpd = bigquery.Client(credentials=credentials, project="mpdbq1", location="US")

# Initialize a client object, by default uses the credentials in ~/googleads.yaml.
client = ad_manager.AdManagerClient.LoadFromStorage()

# Initialize a service.
network_service = client.GetService('NetworkService', version='v202202')

# Make a request.
current_network = network_service.getCurrentNetwork()

print('Found network %s (%s)!' % (current_network['displayName'], current_network['networkCode']))


def get_inventory_type_week():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    report_job = {
        'reportQuery': {
            'dimensions': ['WEEK', 'AD_UNIT_NAME', 'AD_UNIT_ID'],
            'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS',
                        'TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'],
            'dateRangeType': 'CUSTOM_DATE',
            'startDate': start_date,
            'endDate': end_date
        }
    }

    report_downloader = client.GetDataDownloader(version='v202202')

    try:
        report_job_id = report_downloader.WaitForReport(report_job)
    except errors.AdManagerReportError as e:
        print(f'Failed to generate report. Error was: {e}')

    export_format = 'CSV_DUMP'

    report_file = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)

    report_downloader.DownloadReportToFile(
        report_job_id, export_format, report_file)

    report_file.close()

    print(f'Report job with id {report_job_id} downloaded to: \n{report_file.name}')

    with open(report_file.name, 'rb') as report:
        temp = gzip.GzipFile(fileobj=report)
        df = pd.read_csv(temp, sep=',')

    df.rename(columns={"Dimension.WEEK": "WEEK", "Dimension.AD_UNIT_NAME": "AD_UNIT_NAME",
                       "Dimension.AD_UNIT_ID": "AD_UNIT_ID",
                       "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE": "TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE"}, inplace=True)

    # print(df)

    # insert into bigquery
    # name of table and its schema
    table_id = "admanager.Inventory_Type_Week"
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("WEEK", "STRING"),
        bigquery.SchemaField("AD_UNIT_NAME", "STRING"),
        bigquery.SchemaField("AD_UNIT_ID", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_CLICKS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE", "INTEGER")]

    client_mpd.delete_table(table_id, not_found_ok=True)

    job = client_mpd.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    job.result()

    os.remove(report_file.name)


def get_inventory_type_month_and_year():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    report_job = {
        'reportQuery': dict(dimensions=['MONTH_AND_YEAR', 'AD_UNIT_NAME', 'AD_UNIT_ID'],
                            columns=['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS',
                                     'TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'], dateRangeType='CUSTOM_DATE',
                            startDate=start_date, endDate=end_date)
    }

    report_downloader = client.GetDataDownloader(version='v202202')

    try:
        report_job_id = report_downloader.WaitForReport(report_job)
    except errors.AdManagerReportError as e:
        print(f'Failed to generate report. Error was: {e}')

    export_format = 'CSV_DUMP'

    report_file = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)

    report_downloader.DownloadReportToFile(
        report_job_id, export_format, report_file)

    report_file.close()

    print(f'Report job with id {report_job_id} downloaded to: \n{report_file.name}')

    with open(report_file.name, 'rb') as report:
        temp = gzip.GzipFile(fileobj=report)
        df = pd.read_csv(temp, sep=',')

    df.rename(columns={"Dimension.MONTH_AND_YEAR": "MONTH_AND_YEAR", "Dimension.AD_UNIT_NAME": "AD_UNIT_NAME",
                       "Dimension.AD_UNIT_ID": "AD_UNIT_ID",
                       "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE": "TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE"}, inplace=True)

    # print(df)

    # insert into bigquery
    # name of table and its schema
    table_id = "admanager.Inventory_Type_Month_and_Year"
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("MONTH_AND_YEAR", "STRING"),
        bigquery.SchemaField("AD_UNIT_NAME", "STRING"),
        bigquery.SchemaField("AD_UNIT_ID", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_CLICKS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE", "INTEGER")]

    client_mpd.delete_table(table_id, not_found_ok=True)

    job = client_mpd.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    job.result()

    os.remove(report_file.name)


def get_inventory_date():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    report_job = {
        'reportQuery': dict(dimensions=['DATE', 'AD_UNIT_NAME', 'AD_UNIT_ID'],
                            columns=['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS',
                                     'TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'], dateRangeType='CUSTOM_DATE',
                            startDate=start_date, endDate=end_date)
    }

    report_downloader = client.GetDataDownloader(version='v202202')

    try:
        report_job_id = report_downloader.WaitForReport(report_job)
    except errors.AdManagerReportError as e:
        print(f'Failed to generate report. Error was: {e}')

    export_format = 'CSV_DUMP'

    report_file = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)

    report_downloader.DownloadReportToFile(
        report_job_id, export_format, report_file)

    report_file.close()

    print(f'Report job with id {report_job_id} downloaded to: \n{report_file.name}')

    with open(report_file.name, 'rb') as report:
        temp = gzip.GzipFile(fileobj=report)
        df = pd.read_csv(temp, sep=',')

    df.rename(columns={"Dimension.DATE": "DATE", "Dimension.AD_UNIT_NAME": "AD_UNIT_NAME",
                       "Dimension.AD_UNIT_ID": "AD_UNIT_ID",
                       "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE": "TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE"}, inplace=True)

    # print(df)

    # insert into bigquery
    # name of table and its schema
    table_id = "admanager.Inventory_Type_Date"
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("DATE", "STRING"),
        bigquery.SchemaField("AD_UNIT_NAME", "STRING"),
        bigquery.SchemaField("AD_UNIT_ID", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_CLICKS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE", "INTEGER")]

    client_mpd.delete_table(table_id, not_found_ok=True)

    job = client_mpd.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    job.result()

    os.remove(report_file.name)


def main():
    get_inventory_type_week()
    get_inventory_type_month_and_year()
    get_inventory_date()


if __name__ == '__main__':
    main()
