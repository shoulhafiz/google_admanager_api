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
import config

BQ_KEY_LOCATION = '/app/mpd_bq_key/mpdbq1-bq-key.json'
# BQ_KEY_LOCATION = config.BQ_KEY_LOCATION
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

    print(f'Table {table_id} successfully refreshed on:', datetime.now())

    os.remove(report_file.name)


def get_inventory_type_month_and_year():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    report_job = {
        'reportQuery': {
            'dimensions': ['MONTH_AND_YEAR', 'AD_UNIT_NAME', 'AD_UNIT_ID'],
            'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS',
                        'TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'], 'dateRangeType': 'CUSTOM_DATE',
            'startDate': start_date, 'endDate': end_date}
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

    print(f'Table {table_id} successfully refreshed on:', datetime.now())

    os.remove(report_file.name)


def get_inventory_date():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    report_job = {
        'reportQuery': {
            'dimensions': ['DATE', 'AD_UNIT_NAME', 'AD_UNIT_ID'],
            'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS',
                        'TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'], 'dateRangeType': 'CUSTOM_DATE',
            'startDate': start_date, 'endDate': end_date}
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

    print(f'Table {table_id} successfully refreshed on:', datetime.now())

    os.remove(report_file.name)


def inventory_line_item_type_month_and_year():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    # change the dimensions & columns according the report
    report_job = {
        'reportQuery': {
            'dimensions': ['MONTH_AND_YEAR', 'LINE_ITEM_TYPE'],
            'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS',
                        'TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'], 'dateRangeType': 'CUSTOM_DATE',
            'startDate': start_date, 'endDate': end_date}
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

    # change the columns name according to the dimensions & columns in reportQuery
    df.rename(columns={"Dimension.MONTH_AND_YEAR": "MONTH_AND_YEAR", "Dimension.LINE_ITEM_TYPE": "LINE_ITEM_TYPE",
                       "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE": "TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE"}, inplace=True)

    # print(df)

    # insert into bigquery
    # change name of table and its schema
    table_id = "admanager.Inventory_Line_Item_Type_Month_and_Year"  # change table name & schema field
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("MONTH_AND_YEAR", "STRING"),
        bigquery.SchemaField("LINE_ITEM_TYPE", "STRING"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_CLICKS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE", "INTEGER")]

    client_mpd.delete_table(table_id, not_found_ok=True)

    job = client_mpd.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    job.result()

    print(f'Table {table_id} successfully refreshed on:', datetime.now())

    os.remove(report_file.name)


def inventory_line_item_type_date():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    # change the dimensions & columns according the report
    report_job = {
        'reportQuery': {
            'dimensions': ['DATE', 'LINE_ITEM_TYPE'],
            'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS',
                        'TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'], 'dateRangeType': 'CUSTOM_DATE',
            'startDate': start_date, 'endDate': end_date}
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

    # change the columns name according to the dimensions & columns in reportQuery
    df.rename(columns={"Dimension.DATE": "DATE", "Dimension.LINE_ITEM_TYPE": "LINE_ITEM_TYPE",
                       "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE": "TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE"}, inplace=True)

    # print(df)

    # insert into bigquery
    # change name of table and its schema
    table_id = "admanager.Inventory_Line_Item_Type_Date"  # change table name & schema field
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("DATE", "STRING"),
        bigquery.SchemaField("LINE_ITEM_TYPE", "STRING"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_CLICKS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE", "INTEGER")]

    client_mpd.delete_table(table_id, not_found_ok=True)

    job = client_mpd.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    job.result()

    print(f'Table {table_id} successfully refreshed on:', datetime.now())

    os.remove(report_file.name)


def inventory_line_item_type_week():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    # change the dimensions & columns according the report
    report_job = {
        'reportQuery': {
            'dimensions': ['WEEK', 'LINE_ITEM_TYPE'],
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

    # change the columns name according to the dimensions & columns in reportQuery
    df.rename(columns={"Dimension.WEEK": "WEEK", "Dimension.LINE_ITEM_TYPE": "LINE_ITEM_TYPE",
                       "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE": "TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE"},
              inplace=True)

    # print(df)

    # insert into bigquery
    # change name of table and its schema
    table_id = "admanager.Inventory_Line_Item_Type_Week"  # change table name & schema field
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("WEEK", "STRING"),
        bigquery.SchemaField("LINE_ITEM_TYPE", "STRING"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_CLICKS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE", "INTEGER")]

    client_mpd.delete_table(table_id, not_found_ok=True)

    job = client_mpd.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    job.result()

    print(f'Table {table_id} successfully refreshed on:', datetime.now())

    os.remove(report_file.name)


def inventory_ad_format_week():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    # change the dimensions & columns according the report
    report_job = {
        'reportQuery': {
            'dimensions': ['WEEK', 'POSITION_OF_POD'],
            'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS'],
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

    # change the columns name according to the dimensions & columns in reportQuery
    df.rename(columns={"Dimension.WEEK": "WEEK", "Dimension.POSITION_OF_POD": "POSITION_OF_POD",
                       "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                       },
              inplace=True)

    # print(df)

    # insert into bigquery
    # change name of table and its schema
    table_id = "admanager.Inventory_Ad_Format_Week"  # change table name & schema field
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("WEEK", "STRING"),
        bigquery.SchemaField("POSITION_OF_POD", "STRING"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_CLICKS", "INTEGER")
    ]

    client_mpd.delete_table(table_id, not_found_ok=True)

    job = client_mpd.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    print(job.result())
    print(f'Table {table_id} successfully refreshed on:', datetime.now())

    os.remove(report_file.name)


def inventory_ad_format_month_and_year():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    # change the dimensions & columns according the report
    report_job = {
        'reportQuery': {
            'dimensions': ['MONTH_AND_YEAR', 'POSITION_OF_POD'],
            'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS'],
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

    # change the columns name according to the dimensions & columns in reportQuery
    df.rename(columns={"Dimension.MONTH_AND_YEAR": "MONTH_AND_YEAR", "Dimension.POSITION_OF_POD": "POSITION_OF_POD",
                       "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                       },
              inplace=True)

    # print(df)

    # insert into bigquery
    # change name of table and its schema
    table_id = "admanager.Inventory_Ad_Format_Month_and_Year"  # change table name & schema field
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("MONTH_AND_YEAR", "STRING"),
        bigquery.SchemaField("POSITION_OF_POD", "STRING"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_CLICKS", "INTEGER")
    ]

    client_mpd.delete_table(table_id, not_found_ok=True)

    job = client_mpd.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    print(job.result())
    print(f'Table {table_id} successfully refreshed on:', datetime.now())

    os.remove(report_file.name)


def inventory_ad_format_date():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    # change the dimensions & columns according the report
    report_job = {
        'reportQuery': {
            'dimensions': ['DATE', 'POSITION_OF_POD'],
            'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS'],
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

    # change the columns name according to the dimensions & columns in reportQuery
    df.rename(columns={"Dimension.DATE": "DATE", "Dimension.POSITION_OF_POD": "POSITION_OF_POD",
                       "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                       },
              inplace=True)

    # print(df)

    # insert into bigquery
    # change name of table and its schema
    table_id = "admanager.Inventory_Ad_Format_Date"  # change table name & schema field
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("DATE", "STRING"),
        bigquery.SchemaField("POSITION_OF_POD", "STRING"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_CLICKS", "INTEGER")
    ]

    client_mpd.delete_table(table_id, not_found_ok=True)

    job = client_mpd.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    print(job.result())
    print(f'Table {table_id} successfully refreshed on:', datetime.now())

    os.remove(report_file.name)


def inventory_device_category_week():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    # change the dimensions & columns according the report
    report_job = {
        'reportQuery': {
            'dimensions': ['WEEK', 'DEVICE_CATEGORY_NAME', 'DEVICE_CATEGORY_ID'],
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

    # change the columns name according to the dimensions & columns in reportQuery
    df.rename(columns={"Dimension.WEEK": "WEEK", "Dimension.DEVICE_CATEGORY_NAME": "DEVICE_CATEGORY_NAME",
                       "Dimension.DEVICE_CATEGORY_ID": "DEVICE_CATEGORY_ID",
                       "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE": "TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE"},
              inplace=True)

    # print(df)

    # insert into bigquery
    # change name of table and its schema
    table_id = "admanager.Inventory_Device_Category_Week"  # change table name & schema field
    job_config = bigquery.LoadJobConfig()
    job_config.schema = {
        bigquery.SchemaField("WEEK", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_NAME", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_ID", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_CLICKS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE", "INTEGER")
    }

    client_mpd.delete_table(table_id, not_found_ok=True)

    job = client_mpd.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    print(job.result())
    print(f'Table {table_id} successfully refreshed on:', datetime.now())

    os.remove(report_file.name)


def main():
    # get_inventory_type_week()
    # get_inventory_type_month_and_year()
    # get_inventory_date()
    # inventory_line_item_type_month_and_year()
    # inventory_line_item_type_date()
    # inventory_line_item_type_week()
    # inventory_ad_format_week()
    # inventory_ad_format_month_and_year()
    # inventory_ad_format_date()
    inventory_device_category_week()


if __name__ == '__main__':
    main()
