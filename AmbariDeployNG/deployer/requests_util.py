import requests
import json
from log_utils import get_logger

logger = get_logger(__name__)

def get_api_call(req_url):
    api_resp = requests.get(req_url,headers=set_ambari_headers(),auth=('admin','admin'))
    logger.info(api_resp)
    return api_resp


def post_api_call(req_url,json_file):
    api_resp = requests.post(req_url, headers=set_ambari_headers(), auth=('admin','admin'), data=open(json_file, 'rb'))
    logger.info(api_resp)
    return api_resp


def post_api_call_using_json_string(req_url,json_obj):
    api_resp = requests.post(req_url, headers=set_ambari_headers(), auth=('admin','admin'), data=json.dump(json_obj))
    logger.info(api_resp)
    return api_resp

def set_ambari_headers():
    headers = {'X-Requested-By': 'ambari'}
    return headers

