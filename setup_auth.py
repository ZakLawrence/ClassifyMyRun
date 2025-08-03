from requests_oauthlib import OAuth2Session
from dotenv import load_dotenv
import webbrowser 
import os 
import json

load_dotenv()
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
redirect_url = "http://127.0.0.1:5000/authorization"

session = OAuth2Session(client_id=client_id,redirect_uri=redirect_url)

auth_base_url = "https://www.strava.com/oauth/authorize"

session.scope = "read_all,activity:read_all,activity:write,profile:read_all"

auth_link = session.authorization_url(auth_base_url)
print(auth_link[0])
webbrowser.open(auth_link[0])

redirect_response = input("Paste redirect url here:")

token_url = "https://www.strava.com/api/v3/oauth/token"
session.fetch_token(
    token_url=token_url,
    client_id=client_id,
    client_secret=client_secret,
    authorization_response=redirect_response,
    include_client_id=True
)

#response = session.get("https://www.strava.com/api/v3/athlete")
#
#print("\n")
#print(f"Response Status: {response.status_code}")
#print(f"Response Reason: {response.reason}")
#print(f"Response Text: \n{response.text}")


token = session.token.copy()
token.pop("athlete", None)

with open("token.json", "w") as f:
    json.dump(token, f)