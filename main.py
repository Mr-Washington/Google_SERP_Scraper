#!/usr/bin/env python
#import yaml

import os
import json
import random
import string

from google.cloud import storage
from google.cloud import bigquery
from typing import List, Dict

import yaml
from flask import Flask, Request, render_template, request
from pydantic import BaseSettings
import uuid
from datetime import datetime, timezone
import time
from random import randrange
from requests import Response
import requests
from bs4 import BeautifulSoup
# import beautifulsoup4
import re
#from requests_html import HTMLSession
#from google.cloud import storage

SOURCE_BUCKET_NAME = 'queries-to-execute'
DESTINATION_VID_ID_BUCKET_NAME = 'dev1-yt-video-ids-to-scrape'
DESTINATION_SEARCH_TO_RESULT_BUCKET_NAME = 'dev1-bing-search-query-result-map-to-bq-upload'
DESTINATION_BUCKET_NAME = 'raw-yt-webpage-init-data'
ALREADY_SCRAPED_IDS_BUCKET_NAME = 'dev1-already-scraped-yt-video-ids'
YT_ID_PREFIX_CHARACTERS = list('_-abcdefghijklmnopqrstuvwxyz1234567890')
REGIONS = ['de', 'us', 'gb', 'fr']

proxy_host = "proxy.zyte.com"
proxy_port = "8011"
proxy_auth = "77ed9b40ef784f29bc1e17fbe7a90fb2:"  # Make sure to include ':' at the end
proxies = {
    "https": f"http://{proxy_auth}@{proxy_host}:{proxy_port}/",
    "http": f"http://{proxy_auth}@{proxy_host}:{proxy_port}/"
}
zyte_crt_path = 'zyte-proxy-ca.crt'

HEADERS = {
    "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
    'X-Crawlera-Profile': 'desktop',
    'X-Crawlera-Cookies': 'disable',
    'X-Crawlera-Region': 'fr',
    'X-Crawlera-Error': "1",
    'Accept-Encoding': 'gzip, deflate',
    'Upgrade-Insecure-Requests': "1"
}

def new_entry(req):
    print("Hello World")
    #load_data()
    """hello_gcs(event, context)
    bucket_metadata("bucket2413")
    upload_blob("bucket2413", "bucket.txt", "new_project_id")"""
    etl_settings = load_settings_from_yaml("etl-settings.yaml")
    bq_client = bigquery.Client()
    for etl in etl_settings.ETLS:  # TODO: make parallel execution
        [gs, bucket] = etl.get('source_storage_uri').split("//")
        search_buckets(bucket)
    # new_bigquery()
    return 'ok'



def search_buckets(bucket_name):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"
    file_name = []
    date = []
    runID = []
    blob_name = []
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        blob_name.append(blob.name)
        if blob.name.count("/") > 1:
            [temp_date, temp_runid, temp_name] = blob.name.split("/")   # TODO: handle any subdirectories
            result = file_name.count(temp_name)
            if not result:
                file_name.append(temp_name)
            result = date.count(temp_date)
            if not result:
                date.append(temp_date)
            result = runID.count(temp_runid)
            if not result:
                runID.append(temp_runid)

    for time in date:
        for run in runID:
            pre = time + "/" + run
            check_done = pre + ".DONE"
            check_PROCESSED = pre + ".PROCESSED"
            prefix = pre + "/*"
            print(pre)
            #matching = [s for s in blob_name if pre in s]
            #for match in matching:
                #print(match)
            if any(check_PROCESSED in s for s in blob_name):
                print('Folder Processed')
                continue

            elif any(check_done in s for s in blob_name):
                send_bigquery(bucket_name, prefix)
                dest = pre + '.PROCESSED'
                delete = pre + '.DONE'
                create_delete(bucket_name, 'flag.PROCESSED', dest, delete)
            #blobs_specific = list(storage_client.list_blobs(prefix=pre))
        #if blob.name.endswith('.txt'):

        #else:
         #   print("Wrong extension")


# ----------------------------------------------START OF GITHUB CODE -------------------------------------
class CloudFunctionSettings(BaseSettings):
    # Enable debug output (logging, additional messages, etc)
    DEBUG: bool = False
    ETLS: List[Dict[str, str]] = []


def send_bigquery(bucket_name, prefix):
    etl_settings = load_settings_from_yaml("etl-settings.yaml")
    bq_client = bigquery.Client()
    # for etl in etl_settings.ETLS:  # TODO: make parallel execution
    bucket_src = [s for s in etl_settings.ETLS if bucket_name in s.get('source_storage_uri')]
    load_individual_table_data(bucket_src[0].get('source_storage_uri'), bucket_src[0].get('destination_table'), bq_client, prefix)
    print(etl_settings)
    return 'ok'


def load_individual_table_data(source: str, dest: str, bq_client, prefix):
    # uri = "gs://yt-videos-to-bq-upload"

    uri = "%s/" % source  # we want to load all files found in the folder
    uri = uri + prefix
    # table_id = "song-sleuth-alpha:youtube_data.yt_videos"
    table_id = dest

    # [_, table_name] = table_id.split(".")
    # schema_name = table_name + '.json'
    job_config = bigquery.LoadJobConfig(
        # schema=schema_name,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_APPEND"
    )
    print(uri)
    print(table_id)
    load_job = bq_client.load_table_from_uri(
        uri,
        table_id,
        location="us-central1",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)
    print("Loaded %s rows from %s to %s" % (destination_table.num_rows, uri, table_id))


def load_settings_from_yaml(yaml_path: str) -> CloudFunctionSettings:
    with open(yaml_path) as yaml_file:
        yaml_env = yaml.safe_load(yaml_file)
        return CloudFunctionSettings(**yaml_env)

# ---------------------------------- END OF GITHUB CODE --------------------------------------------


def create_delete(bucket_name, source_file_name, destination_blob_name, delete_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string('')

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

    blob = bucket.blob(delete_blob_name)
    blob.delete()

    print("Blob {} deleted.".format(delete_blob_name))

def new_bigquery():
    client = bigquery.Client()
    query_job = client.query(
        """
        SELECT
          CONCAT(
            'https://stackoverflow.com/questions/',
            CAST(id as STRING)) as url,
          view_count
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE tags like '%google-bigquery%'
        ORDER BY view_count DESC
        LIMIT 10"""
    )

    results = query_job.result()  # Waits for job to complete.
    for row in results:
        print("{} : {} views".format(row.url, row.view_count))


def create_bucket_class_location(bucket_name):
    """Create a new bucket in specific location with storage class"""
    # bucket_name = "your-new-bucket-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "COLDLINE"
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )
#initialize storage client
#initilaize bucket
#initilaizee blob
#read/write blob


def bucket_metadata(bucket_name):
    """Adds a bucket's metadata to a file."""
    # bucket_name = 'your-bucket-name'
    file_open = open("bucket.txt", "a")

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    file_open.write("ID: {}".format(bucket.id))
    file_open.write("Name: {}".format(bucket.name))
    file_open.write("Location: {}".format(bucket.location))
    file_open.close()


def hello_pubsub(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
                        event. The `@type` field maps to
                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                        The `data` field maps to the PubsubMessage data
                        in a base64-encoded string. The `attributes` field maps
                        to the PubsubMessage attributes if any is present.
         context (google.cloud.functions.Context): Metadata of triggering event
                        including `event_id` which maps to the PubsubMessage
                        messageId, `timestamp` which maps to the PubsubMessage
                        publishTime, `event_type` which maps to
                        `google.pubsub.topic.publish`, and `resource` which is
                        a dictionary that describes the service API endpoint
                        pubsub.googleapis.com, the triggering topic's name, and
                        the triggering event type
                        `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
    Returns:
        None. The output is written to Cloud Logging.
    """
    import base64

    print("""This Function was triggered by messageId {} published at {} to {}
    """.format(context.event_id, context.timestamp, context.resource["name"]))

    if 'data' in event:
        name = base64.b64decode(event['data']).decode('utf-8')
    else:
        name = 'World'
    print('Hello {}!'.format(name))


def hello_gcs(event, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This generic function logs relevant data when a file is changed.

    Args:
        event (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """
    read_file('bucket.txt')
    """print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('PROCESSEDd: {}'.format(event['PROCESSEDd']))"""
    return 'ok!'


def read_file(filename):
    print('Reading the full file contents:\n')
    gcs_file = storage.open(filename)
    contents = gcs_file.read()
    gcs_file.close()
    #self.response.write(contents)


def load_data():
    blob_name = []
    client = bigquery.Client()
    storage_client = storage.Client()
    print("within load data")
    blobs = storage_client.list_blobs("dev1-yt-license-details-to-bq-upload")
    pre = "2021-06-03/85fbb1d2-0592-44f9-beae-64fec9a491e4/"
    for blob in blobs:
        # print(blob.name)
        blob_name.append(blob.name)
    matching = [s for s in blob_name if pre in s]

    for match in matching:
        job_config = bigquery.LoadJobConfig(
            # schema=schema_name,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition="WRITE_APPEND"
        )

        uri = "gs://dev1-yt-license-details-to-bq-upload/" + match
        print(uri)
        table_id = 'youtube_data.yt_license_details'
        load_job = client.load_table_from_uri(
            uri,
            table_id,
            location="us-central1",  # Must match the destination dataset location.
            job_config=job_config,
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        destination_table = client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))


# -----------------------BEGINNING OF SERP SCRAPER CODE----------------------------
def scraper_entry(req):
    # TODO: Potential new paramaters to add to the blob objects..
    #   -metadata: results returned
    #   -blobdata: google query parameters
    #   -blobdata: failed results returned
    #   -metadata: date posted of the YT_URL/ date of the most recent video
    bucket_name = SOURCE_BUCKET_NAME
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    flag = 0
    for new_blob in blobs:
        blob = new_blob
        flag += 1
        if flag == 2:
            break
    blob.patch()  # populates metadata
    run_uuid = blob.metadata['run_uuid']
    run_date = blob.metadata['run_date']
    event_json_str = blob.download_as_string()
    event_json = json.loads(event_json_str)

    query_term = event_json['query_string']
    query_uuid = event_json['query_uuid']
    # search_endpoint = event_json['search_endpoint']
    # search_results = []
    search_results = execute_search_with_google(query_term)
    write_files(query_term, query_uuid, search_results, storage_client, run_uuid, run_date)
    return 'ok'


def execute_search_with_google(query):
    # TODO: Potential aspects to keep track of....
    #   -total amount of variations for a specific query string
    #   -results returned when you set a keyword(~), or you set it as necessary in the results
    query = query.replace(" ", "+")
    session = requests.Session()
    scraped_vids = []
    query_params = '&num=99&start=0&as_sitesearch=youtube.com'     # String of parameters to search 100 pages results
    # TODO: Explore other possible query parameters i.e. as_qdr=x (search in a range of dates), cr=countryXX limits
    #  results to certain countries, %2Bterm returns results with a specific term without any synonyms.
    url = 'https://www.google.com/search?q=' + query + query_params
    query_url = url
    for character in YT_ID_PREFIX_CHARACTERS:   # Do the seseme street logic and loop through alphabet
        if url == 0:
            url = query_url
        [query_str, google_flags] = url.split("&", 1)
        url = '%s+inurl:watch?v=%s&%s' % (query_str, character, google_flags)
        while url != 0:
            response = retried_request(session, url)
            soup = BeautifulSoup(response.text, 'lxml', from_encoding='utf-8')
            for a in soup.find_all('a', href=True):
                if "https://www.youtube.com/watch?v=" in a['href']:
                    scraped_vids.append({"url": a['href']})
                    print("Found the URL:", a['href'])
            url = get_url(soup, url)

    """ result = re.search('<a href="https://www.youtube.com(.*)" data-ved="', response.text)
    print(result.group(1))"""
    # print(response.text)
    print("That's all folks!!")
    return scraped_vids


def get_url(soup, url):
    # while (any(check_PROCESSED in s for s in blob_name)):
    # TODO: Potential new metrics that can track...
    #   -Create a new metric where we keep track of which google parameters we are collecting
    #   -Potentially add a list of parameters that we would like to search for in the google blob
    #   -{"YT_URL":youtube.com/url, "Google_Params": {"inurl":"watch?v=", ... , "sitesearch":"youtube.com"}, ...}
    for a in soup.find_all('td', class_='d6cvqb'):
        for child in a.children:
            if child.name == 'a':
                print("Found the URL:", child['href'])
                num = re.search('&start=(.+?)&', child['href'])
                prev_page = re.search('&start=(.+?)&', url)
                # [new_url, num] = child['href'].split("&start=")
                # [past_url, prev_page] = url.split("&start=")

                print("tried previous page number is %s and tried next page number is %s" % (str(prev_page.group(1)), str(num.group(1))))
                if int(num.group(1)) > int(prev_page.group(1)):
                    print("ACCEPTED previous page number is %s and ACCEPTED next page number is %s" % (str(prev_page.group(1)), str(num.group(1))))
                    print("The next page number is: ", num.group(1))
                    url = 'https://www.google.com' + child['href']
                    return url
    return 0


def retried_request(session, url, retries=3, sleep_range=15) -> Response:
    # TODO: potential outputs for this function that can be tracked is...
    #     -results returned for a given query {"vidID":vidID, "query":query, .... , "URLs_returned": XX}
    #     -all the results that failed {"vidID":failed videoID, "retries":retries, "region_range": REGIONS}
    req_start_time = time.perf_counter()
    response = session.get(url, proxies=proxies, verify=zyte_crt_path, headers=HEADERS, timeout=600)
    req_end_time = time.perf_counter()
    time_delta = str(req_end_time - req_start_time)
    for retry in range(retries):
        print('making call to %s, took %s' % (url, time_delta))
        if response.status_code == 200:
            response.raise_for_status()
            return response
        if response.status_code in [403, 413]:
            response.raise_for_status()
            return response
        else:
            # for helping avoid zyte concurrent request limit
            base_sleep = int(sleep_range / retries)
            sleep = (base_sleep * retry) + randrange(base_sleep)
            print('%s : %s on retry number %s sleeping %s' % (url, response.status_code, str(retry), str(sleep)))
            print('Response error message is %s' % response.headers['X-Crawlera-Error'])
            time.sleep(sleep)
            headers = {
                "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
                'X-Crawlera-Profile': 'desktop',
                'X-Crawlera-Error': "1",
                'X-Crawlera-Cookies': 'disable',
                'X-Crawlera-Region': REGIONS[retry],
                'Accept-Encoding': 'gzip, deflate',
                'Upgrade-Insecure-Requests': "1"
            }
        req_start_time = time.perf_counter()
        if retry != 0:
            response = session.get(url, proxies=proxies, verify=zyte_crt_path, headers=headers, timeout=600)
        else:
            response = session.get(url, proxies=proxies, verify=zyte_crt_path, headers=HEADERS, timeout=600)
        req_end_time = time.perf_counter()
        time_delta = str(req_end_time - req_start_time)
    if response.status_code in [429]:
        # raise error for now will trigger retry which is what we want
        pass
    return response


def write_files(query_term: str, query_uuid, search_results: List[Dict], storage_client, run_uuid, run_date):
    vid_id_bucket = storage_client.bucket(DESTINATION_VID_ID_BUCKET_NAME)
    query_result_map_bucket = storage_client.bucket(DESTINATION_SEARCH_TO_RESULT_BUCKET_NAME)
    already_scraped_vid_bucket = storage_client.bucket(ALREADY_SCRAPED_IDS_BUCKET_NAME)
    query_and_result_strings = []
    for search_result in search_results:
        search_result_url = search_result['url']
        try:
            search_result_video_id = get_chars_after_substring(search_result_url, '/watch?v=')
        except ValueError:
            try:
                search_result_video_id = get_chars_after_substring(search_result_url, '/watch/')
            except ValueError:
                print('%s skipped as it is not identified as a video page' % search_result_url)
                continue
        if already_scraped_vid_bucket.blob(search_result_video_id).exists():
            continue
        msg = {"search_query": query_term, "yt_url": search_result['url']}
        # data = json.dumps(msg).encode('utf-8')
        data_str = json.dumps(msg)
        blob = vid_id_bucket.blob("%s/%s/%s" % (run_date, run_uuid, search_result_video_id))
        blob.upload_from_string(data_str)
        blob.metadata = {"run_uuid": run_uuid, "run_date": run_date}
        blob.patch()

        search_result_uuid = str(uuid.uuid4())
        json_str = json.dumps(
            build_db_obj_for_query_string_and_result(query_uuid, search_result_video_id, search_result_uuid),
            default=str)
        query_and_result_strings.append(json_str)
        # we want to spread out putting queries on the queue. we have 1000 results and 540 seconds max
        # this ends up being having a ~1.8 queries per second limit. we choose 2.5 qps to give buffer (setup time, etc.)
        # we may want to even create a function that looks at / infers our zyte load and emits these messages when room
        time.sleep(.2)
    search_query_blob = query_result_map_bucket.blob("%s/%s/%s" % (run_date, run_uuid, query_uuid))
    new_line_blobs = '\n'.join(query_and_result_strings)
    if new_line_blobs:
        search_query_blob.upload_from_string(new_line_blobs)
        search_query_blob.metadata = {'run_uuid': run_uuid, "run_date": run_date}
        search_query_blob.patch()
    return 'ok'
    """run_uuid = query_uuid
    run_date = "2021-06-28"
    storage_blob_prefix = "%s/%s/" % (run_date, run_uuid)
    # file_content_to_write: str = json.loads(source_filenames_to_write)
    # content_by_line: List[str] = file_content_to_write.splitlines()
    storage_client = storage.Client()
    destination_bucket = storage_client.bucket(bucket_name)

    for line in vid_list:
        list_id = ''.join(random.choice(letters) for i in range(10))
        blob = destination_bucket.blob("%s-%s" % (storage_blob_prefix, list_id))
        blob.upload_from_string(str(line))
        # blob.metadata = source_metadata
        blob.patch()"""
    # return 'ok'

# --------------------------- START OF GITHUB HELPER FUNCTIONS -------------------------------------
def build_db_obj_for_query_string_and_result(query_uuid, search_result_video_id, search_result_uuid) -> Dict[str, any]:
    now = datetime.now(tz=timezone.utc)
    query_obj = {
        'Search_Result_UID': search_result_uuid,
        'Query_UID': query_uuid,
        'YT_Video_ID': search_result_video_id,
        'Created_Date': now,
        'Last_Modified': now
    }
    return query_obj


def get_chars_after_substring(text: str, substring: str):
    substring_index = text.index(substring)
    return text[substring_index + len(substring):]
# -------------------------END OF SERP SCRAPER CODE---------------------------------------------