import json
from flask import jsonify
from google.cloud import firestore


db = firestore.Client()

def create_user(request):
  """
  Cloud function to create a new user 
  and store it in Firestore
  Args:
     request (flask object)
  Returns:
     string: external id  
  """
 # get info from request
  payload = request.get_json(silent=True)
  email = payload['email'] 
  name = payload['name'] 
  age = payload['age'] 

  # add a document into collection
  doc_ref = db.collection('users').document(email)
  doc_ref.set({
    'email': email,
    'name':name,
    'age': age
  })
  
  return jsonify({ 'external_id': email })