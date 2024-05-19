import json
import os
from flask import jsonify
from google.cloud import storage

def upload(file_path, content):
  """
  Uploads to Cloud Storage
  Args:
     file_path (str): Path in the bucket to store the content
     content (str): Content to upload
  Returns:
     nothing
  """

  storage_client = storage.Client()
  # BUCKET_NAME is env variable
  bucket_name = os.getenv('BUCKET_NAME')
  bucket = storage_client.bucket(bucket_name)
  new_blob = bucket.blob(file_path)
  new_blob.upload_from_string(content)


def process(request):
  """
  Cloud function that processes an event and
  stores it in Cloud Storage
  Args:
      request (flask object): event information 
  Returns:
      json: event id
  """
  # read request
  payload = request.get_json(silent=True) 

  # convert request info into a dictionary
  eventos = {}
  eventos = {
    'eventId': payload['eventId'],
    'eventTime': payload['eventTime'],
    'processTime': payload['processTime'],
    'resourceId': payload['resourceId'],
    'useId': payload['userId'],
    'countryCode': payload['countryCode'],
    'duration': payload['duration'],
    'itemPrice': payload['itemPrice']
   }
  
  # convert processed info into json
  payload_as_string = json.dumps(eventos)
  evento = eventos['eventId']

  # upload to Cloud Storage
  upload(f'events/event-{evento}.json', payload_as_string)

  return jsonify({'external_id': evento})

