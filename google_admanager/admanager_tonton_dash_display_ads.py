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

# BQ_KEY_LOCATION = '/app/mpd_bq_key/mpdbq1-bq-key.json'
BQ_KEY_LOCATION = config.BQ_KEY_LOCATION
credentials = service_account.Credentials.from_service_account_file(BQ_KEY_LOCATION)

client_mpd = bigquery.Client(credentials=credentials, project="mpdbq1", location="US")

# Initialize a client object, by default uses the credentials in ~/googleads.yaml.
client = ad_manager.AdManagerClient.LoadFromStorage()

# Initialize a service.
network_service = client.GetService('NetworkService', version='v202202')

# Make a request.
current_network = network_service.getCurrentNetwork()

print('Found network %s (%s)!' % (current_network['displayName'], current_network['networkCode']))


def get_inventory_device_category_week():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    report_job = {
        'reportQuery': {
            'dimensions': ['WEEK', 'DEVICE_CATEGORY_NAME', 'DEVICE_CATEGORY_ID', 'LINE_ITEM_NAME', 'PARENT_AD_UNIT_ID',
                           'AD_UNIT_NAME', 'AD_UNIT_ID'],
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

    # filter report
    df.replace(',', '', regex=True, inplace=True)

    parent_ad_unit_list = ['183717733', '22235807415', '21676101156', '21872721896', '21872873235']

    df = df[df['Dimension.LINE_ITEM_NAME'].str.contains('tonton', case=False, na=False)
            & df['Dimension.PARENT_AD_UNIT_ID'].isin(parent_ad_unit_list)]

    df.rename(columns={"Dimension.WEEK": "WEEK", "Dimension.DEVICE_CATEGORY_NAME": "DEVICE_CATEGORY_NAME",
                       "Dimension.DEVICE_CATEGORY_ID": "DEVICE_CATEGORY_ID",
                       "Dimension.LINE_ITEM_NAME": "LINE_ITEM_NAME", "Dimension.PARENT_AD_UNIT_ID": "PARENT_AD_UNIT_ID",
                       "Dimension.AD_UNIT_NAME": "AD_UNIT_NAME", "Dimension.AD_UNIT_ID": "AD_UNIT_ID",
                       "Dimension.LINE_ITEM_ID": "LINE_ITEM_ID",
                       "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE": "TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE"}, inplace=True)

    print(df)

    # insert into bigquery
    # name of table and its schema
    table_id = "admanager.Display_ads_Inventory_Device_Category_Week"
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("WEEK", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_NAME", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_ID", "INTEGER"),
        bigquery.SchemaField("LINE_ITEM_NAME", "STRING"),
        bigquery.SchemaField("LINE_ITEM_ID", "INTEGER"),
        bigquery.SchemaField("PARENT_AD_UNIT_ID", "STRING"),
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


def get_inventory_device_category_month_and_year():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    report_job = {
        'reportQuery': {
            'dimensions': ['MONTH_AND_YEAR', 'DEVICE_CATEGORY_NAME', 'DEVICE_CATEGORY_ID', 'LINE_ITEM_NAME',
                           'PARENT_AD_UNIT_ID',
                           'AD_UNIT_NAME', 'AD_UNIT_ID'],
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

    # filter report
    df.replace(',', '', regex=True, inplace=True)

    parent_ad_unit_list = ['183717733', '22235807415', '21676101156', '21872721896', '21872873235']

    df = df[df['Dimension.LINE_ITEM_NAME'].str.contains('tonton', case=False, na=False)
            & df['Dimension.PARENT_AD_UNIT_ID'].isin(parent_ad_unit_list)]

    df.rename(
        columns={"Dimension.MONTH_AND_YEAR": "MONTH_AND_YEAR", "Dimension.DEVICE_CATEGORY_NAME": "DEVICE_CATEGORY_NAME",
                 "Dimension.DEVICE_CATEGORY_ID": "DEVICE_CATEGORY_ID",
                 "Dimension.LINE_ITEM_NAME": "LINE_ITEM_NAME", "Dimension.PARENT_AD_UNIT_ID": "PARENT_AD_UNIT_ID",
                 "Dimension.AD_UNIT_NAME": "AD_UNIT_NAME", "Dimension.AD_UNIT_ID": "AD_UNIT_ID",
                 "Dimension.LINE_ITEM_ID": "LINE_ITEM_ID",
                 "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                 "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                 "Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE": "TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE"}, inplace=True)

    print(df)

    # insert into bigquery
    # name of table and its schema
    table_id = "admanager.Display_ads_Inventory_Device_Category_Month_and_Year"
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("MONTH_AND_YEAR", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_NAME", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_ID", "INTEGER"),
        bigquery.SchemaField("LINE_ITEM_NAME", "STRING"),
        bigquery.SchemaField("LINE_ITEM_ID", "INTEGER"),
        bigquery.SchemaField("PARENT_AD_UNIT_ID", "STRING"),
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


def get_inventory_device_category_date():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    report_job = {
        'reportQuery': {
            'dimensions': ['DATE', 'DEVICE_CATEGORY_NAME', 'DEVICE_CATEGORY_ID', 'LINE_ITEM_NAME',
                           'PARENT_AD_UNIT_ID', 'AD_UNIT_NAME', 'AD_UNIT_ID'],
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

    # filter report
    df.replace(',', '', regex=True, inplace=True)

    parent_ad_unit_list = ['183717733', '22235807415', '21676101156', '21872721896', '21872873235']

    df = df[df['Dimension.LINE_ITEM_NAME'].str.contains('tonton', case=False, na=False)
            & df['Dimension.PARENT_AD_UNIT_ID'].isin(parent_ad_unit_list)]

    df.rename(
        columns={"Dimension.DATE": "DATE", "Dimension.DEVICE_CATEGORY_NAME": "DEVICE_CATEGORY_NAME",
                 "Dimension.DEVICE_CATEGORY_ID": "DEVICE_CATEGORY_ID",
                 "Dimension.LINE_ITEM_NAME": "LINE_ITEM_NAME", "Dimension.PARENT_AD_UNIT_ID": "PARENT_AD_UNIT_ID",
                 "Dimension.AD_UNIT_NAME": "AD_UNIT_NAME", "Dimension.AD_UNIT_ID": "AD_UNIT_ID",
                 "Dimension.LINE_ITEM_ID": "LINE_ITEM_ID",
                 "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                 "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                 "Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE": "TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE"}, inplace=True)

    # print(df)

    # insert into bigquery
    # name of table and its schema
    table_id = "admanager.Display_ads_Inventory_Device_Category_Date"
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("DATE", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_NAME", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_ID", "INTEGER"),
        bigquery.SchemaField("LINE_ITEM_NAME", "STRING"),
        bigquery.SchemaField("LINE_ITEM_ID", "INTEGER"),
        bigquery.SchemaField("PARENT_AD_UNIT_ID", "STRING"),
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


def get_inventory_campaign_type_week():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    report_job = {
        'reportQuery': {
            'dimensions': ['WEEK', 'DEVICE_CATEGORY_NAME', 'DEVICE_CATEGORY_ID', 'LINE_ITEM_TYPE',
                           'LINE_ITEM_NAME', 'LINE_ITEM_ID', 'PARENT_AD_UNIT_ID', 'AD_UNIT_NAME',
                           'AD_UNIT_ID'],
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

    # filter report
    df.replace(',', '', regex=True, inplace=True)

    parent_ad_unit_list = ['183717733', '22235807415', '21676101156', '21872721896', '21872873235']

    df = df[df['Dimension.LINE_ITEM_NAME'].str.contains('tonton', case=False, na=False)
            & df['Dimension.PARENT_AD_UNIT_ID'].isin(parent_ad_unit_list)]

    # Create calculated revenue column
    df = df.assign(REVENUE=lambda x: round((x['Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'] / 1000000), 2))

    # drop columns
    df.drop(['Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'], axis=1, inplace=True)

    df.rename(columns={"Dimension.WEEK": "WEEK", "Dimension.DEVICE_CATEGORY_NAME": "DEVICE_CATEGORY_NAME",
                       "Dimension.DEVICE_CATEGORY_ID": "DEVICE_CATEGORY_ID",
                       "Dimension.LINE_ITEM_TYPE": "LINE_ITEM_TYPE",
                       "Dimension.LINE_ITEM_NAME": "LINE_ITEM_NAME", "Dimension.PARENT_AD_UNIT_ID": "PARENT_AD_UNIT_ID",
                       "Dimension.AD_UNIT_NAME": "AD_UNIT_NAME", "Dimension.AD_UNIT_ID": "AD_UNIT_ID",
                       "Dimension.LINE_ITEM_ID": "LINE_ITEM_ID",
                       "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS"}, inplace=True)

    # print(df)

    # insert into bigquery
    # name of table and its schema
    table_id = "admanager.Display_ads_Inventory_Campaign_Type_Week"
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("WEEK", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_NAME", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_ID", "INTEGER"),
        bigquery.SchemaField("LINE_ITEM_TYPE", "STRING"),
        bigquery.SchemaField("LINE_ITEM_NAME", "STRING"),
        bigquery.SchemaField("LINE_ITEM_ID", "INTEGER"),
        bigquery.SchemaField("PARENT_AD_UNIT_ID", "STRING"),
        bigquery.SchemaField("AD_UNIT_NAME", "STRING"),
        bigquery.SchemaField("AD_UNIT_ID", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_CLICKS", "INTEGER"),
        bigquery.SchemaField("REVENUE", "FLOAT")]

    client_mpd.delete_table(table_id, not_found_ok=True)

    job = client_mpd.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    job.result()

    print(f'Table {table_id} successfully refreshed on:', datetime.now())

    os.remove(report_file.name)


def get_inventory_campaign_type_month_and_year():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    report_job = {
        'reportQuery': {
            'dimensions': ['MONTH_AND_YEAR', 'DEVICE_CATEGORY_NAME', 'DEVICE_CATEGORY_ID', 'LINE_ITEM_TYPE',
                           'LINE_ITEM_NAME', 'LINE_ITEM_ID', 'PARENT_AD_UNIT_ID', 'AD_UNIT_NAME',
                           'AD_UNIT_ID'],
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

    # filter report
    df.replace(',', '', regex=True, inplace=True)

    parent_ad_unit_list = ['183717733', '22235807415', '21676101156', '21872721896', '21872873235']

    df = df[df['Dimension.LINE_ITEM_NAME'].str.contains('tonton', case=False, na=False)
            & df['Dimension.PARENT_AD_UNIT_ID'].isin(parent_ad_unit_list)]

    # Create calculated revenue column
    df = df.assign(REVENUE=lambda x: round((x['Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'] / 1000000), 2))

    # drop columns
    df.drop(['Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'], axis=1, inplace=True)

    df.rename(
        columns={"Dimension.MONTH_AND_YEAR": "MONTH_AND_YEAR", "Dimension.DEVICE_CATEGORY_NAME": "DEVICE_CATEGORY_NAME",
                 "Dimension.DEVICE_CATEGORY_ID": "DEVICE_CATEGORY_ID",
                 "Dimension.LINE_ITEM_TYPE": "LINE_ITEM_TYPE",
                 "Dimension.LINE_ITEM_NAME": "LINE_ITEM_NAME", "Dimension.PARENT_AD_UNIT_ID": "PARENT_AD_UNIT_ID",
                 "Dimension.AD_UNIT_NAME": "AD_UNIT_NAME", "Dimension.AD_UNIT_ID": "AD_UNIT_ID",
                 "Dimension.LINE_ITEM_ID": "LINE_ITEM_ID",
                 "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                 "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS"}, inplace=True)

    # print(df)

    # insert into bigquery
    # name of table and its schema
    table_id = "admanager.Display_ads_Inventory_Campaign_Type_Month_and_Year"
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("MONTH_AND_YEAR", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_NAME", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_ID", "INTEGER"),
        bigquery.SchemaField("LINE_ITEM_TYPE", "STRING"),
        bigquery.SchemaField("LINE_ITEM_NAME", "STRING"),
        bigquery.SchemaField("LINE_ITEM_ID", "INTEGER"),
        bigquery.SchemaField("PARENT_AD_UNIT_ID", "STRING"),
        bigquery.SchemaField("AD_UNIT_NAME", "STRING"),
        bigquery.SchemaField("AD_UNIT_ID", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_CLICKS", "INTEGER"),
        bigquery.SchemaField("REVENUE", "FLOAT")]

    client_mpd.delete_table(table_id, not_found_ok=True)

    job = client_mpd.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    job.result()

    print(f'Table {table_id} successfully refreshed on:', datetime.now())

    os.remove(report_file.name)


def get_inventory_campaign_type_date():
    end_date = datetime.now().date()
    start_date = date(date.today().year, 1, 1)

    report_job = {
        'reportQuery': {
            'dimensions': ['DATE', 'DEVICE_CATEGORY_NAME', 'DEVICE_CATEGORY_ID', 'LINE_ITEM_TYPE',
                           'LINE_ITEM_NAME', 'LINE_ITEM_ID', 'PARENT_AD_UNIT_ID', 'AD_UNIT_NAME',
                           'AD_UNIT_ID'],
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

    # filter report
    df.replace(',', '', regex=True, inplace=True)

    parent_ad_unit_list = ['183717733', '22235807415', '21676101156', '21872721896', '21872873235']

    df = df[df['Dimension.LINE_ITEM_NAME'].str.contains('tonton', case=False, na=False)
            & df['Dimension.PARENT_AD_UNIT_ID'].isin(parent_ad_unit_list)]

    # format columns
    df = df.assign(REVENUE=lambda x: round((x['Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'] / 1000000), 2))

    # drop columns
    df.drop(['Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'], axis=1, inplace=True)

    df.rename(columns={"Dimension.DATE": "DATE", "Dimension.DEVICE_CATEGORY_NAME": "DEVICE_CATEGORY_NAME",
                       "Dimension.DEVICE_CATEGORY_ID": "DEVICE_CATEGORY_ID",
                       "Dimension.LINE_ITEM_TYPE": "LINE_ITEM_TYPE",
                       "Dimension.LINE_ITEM_NAME": "LINE_ITEM_NAME", "Dimension.PARENT_AD_UNIT_ID": "PARENT_AD_UNIT_ID",
                       "Dimension.AD_UNIT_NAME": "AD_UNIT_NAME", "Dimension.AD_UNIT_ID": "AD_UNIT_ID",
                       "Dimension.LINE_ITEM_ID": "LINE_ITEM_ID",
                       "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                       "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "TOTAL_LINE_ITEM_LEVEL_CLICKS"}, inplace=True)

    # print(df)

    # insert into bigquery
    # name of table and its schema
    table_id = "admanager.Display_ads_Inventory_Campaign_Type_Date"
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("DATE", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_NAME", "STRING"),
        bigquery.SchemaField("DEVICE_CATEGORY_ID", "INTEGER"),
        bigquery.SchemaField("LINE_ITEM_TYPE", "STRING"),
        bigquery.SchemaField("LINE_ITEM_NAME", "STRING"),
        bigquery.SchemaField("LINE_ITEM_ID", "INTEGER"),
        bigquery.SchemaField("PARENT_AD_UNIT_ID", "STRING"),
        bigquery.SchemaField("AD_UNIT_NAME", "STRING"),
        bigquery.SchemaField("AD_UNIT_ID", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS", "INTEGER"),
        bigquery.SchemaField("TOTAL_LINE_ITEM_LEVEL_CLICKS", "INTEGER"),
        bigquery.SchemaField("REVENUE", "FLOAT")]

    client_mpd.delete_table(table_id, not_found_ok=True)

    job = client_mpd.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    job.result()

    print(f'Table {table_id} successfully refreshed on:', datetime.now())

    os.remove(report_file.name)


def main():
    # get_inventory_device_category_week()
    # get_inventory_device_category_month_and_year()
    # get_inventory_device_category_date()
    get_inventory_campaign_type_week()
    get_inventory_campaign_type_month_and_year()
    get_inventory_campaign_type_date()


if __name__ == '__main__':
    main()
