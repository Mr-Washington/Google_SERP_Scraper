import base64
from google.cloud import storage
from google.cloud import bigquery
import mock
import base64
import os
import subprocess
import uuid

import requests
from requests.packages.urllib3.util.retry import Retry
import main


mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'
mock_context.resource = {
    'name': 'projects/song-sleuth-dev1/topics/song-sleuth-dev1-topic',
    'service': 'pubsub.googleapis.com',
    'type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage',
}


def unit_test_file_system(capsys):
    data = {}
    bucket_name = "dev1-yt-license-details-to-bq-upload"
    destination_blob_name = "2021-06-03/85fbb1d2-0592-44f9-beae-64fec9a491e4.DONE"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob_name = []
    blob.upload_from_string('')

    # Call tested function
    main.new_entry(data, mock_context)
    out, err = capsys.readouterr()
    blobs = storage_client.list_blobs("dev1-yt-license-details-to-bq-upload")
    for blob in blobs:
        blob_name.append(blob.name)
    processed = "2021-06-03/85fbb1d2-0592-44f9-beae-64fec9a491e4.PROCESSED"
    assert any(processed in s for s in blob_name)


def test_print_name():
    name = str(uuid.uuid4())
    port = 8088  # Each running framework instance needs a unique port

    encoded_name = base64.b64encode(name.encode('utf-8')).decode('utf-8')
    pubsub_message = {
        'data': {'data': encoded_name}
    }

    process = subprocess.Popen(
      [
        'functions-framework',
        '--target', 'new_entry',
        '--signature-type', 'event',
        '--port', str(port)
      ],
      cwd=os.path.dirname(__file__),
      stdout=subprocess.PIPE
    )

    # Send HTTP request simulating Pub/Sub message
    # (GCF translates Pub/Sub messages to HTTP requests internally)
    url = f'http://localhost:{port}/'

    retry_policy = Retry(total=6, backoff_factor=1)
    retry_adapter = requests.adapters.HTTPAdapter(
      max_retries=retry_policy)

    session = requests.Session()
    session.mount(url, retry_adapter)

    response = session.post(url, json=pubsub_message)

    assert response.status_code == 200

    # Stop the functions framework process
    process.kill()
    process.wait()
    out, err = process.communicate()

    print(out, err, response.content)

    assert f'Hello {name}!' in str(out)