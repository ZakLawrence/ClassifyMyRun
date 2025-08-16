from requests_oauthlib import OAuth2Session
from dotenv import load_dotenv
from functools import partial
from pathlib import Path
import os 
import json

parent_folder = Path(__file__).resolve().parent.parent.parent
env_path =  parent_folder / ".env"
load_dotenv(dotenv_path=env_path)
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
token_url = "https://www.strava.com/api/v3/oauth/token"
token_path = parent_folder / "token.json"


def load_token(token_file:str=token_path) -> dict:
    with open(token_file,"r") as f:
        return json.load(f)

def save_token(token,token_file:str=token_path) -> None:
    with open(token_file,"w") as f:
        json.dump(token,f)

def get_session(**kwargs):
    if kwargs.get("token_file",None):
        token = load_token(token_file=kwargs.get("token_file",token_path))
        save_func = partial(save_token,token_file=kwargs.get("token_file",token_path))
    else:
        token = load_token()
        save_func = save_token
    
    return OAuth2Session(
        client_id=client_id,
        token=token,
        auto_refresh_url=token_url,
        auto_refresh_kwargs={
            "client_id":client_id,
            "client_secret":client_secret,
        },
        token_updater= save_func    
    )
