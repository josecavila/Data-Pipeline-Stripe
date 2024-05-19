import json
from flask import jsonify
from google.cloud import firestore


db = firestore.Client()

def create_resource(request):
  """
  Cloud function to create a new resource 
  and store it in Firestore
  Args:
     request (flask object)
  Returns:
     string: external id  
  """

  # get info from request
  payload = request.get_json(silent=True)
  id = payload['id'] 
  name = payload['name']
  categoryId = payload['categoryId'] 
  providerId = payload['providerId'] 
  promotion = payload['promotion']

  # add a document into collection
  doc_ref = db.collection('resources').document(id)
  doc_ref.set({
    'id': id,
    'name': name,
    'categoryId': categoryId,
    'providerId': providerId,
    'promotion': promotion
  })
  
  return jsonify({ 'external_id': id })