import requests
import json
import datetime


debug = False
http_timeout = 120

username = "shayes_compass"
password = "Infa2024!"

baseURL = "https://dmp-us.informaticacloud.com"
api_baseURL = "https://idmc-api.dmp-us.informaticacloud.com"



def login():
    # we login using the old api for now

    global iics_username
    iics_username = username
    global iics_password
    iics_password = password

    if baseURL.endswith('/'):
        baseURL = baseURL[:-len('/')]

    loginURL = baseURL + "/saas/public/core/v3/login"

    loginData = {'username': username, 'password': password}
    loginData_debug = {'userName': username, 'password': 'xxxx'}
    headers = {'content-type': 'application/json'}
    if debug:
        print("DEBUG(login) URL:", loginURL, "Header", headers, "Data", json.dumps(loginData_debug))

    response = requests.post(loginURL, headers=headers, data=json.dumps(loginData), timeout=http_timeout)
    if debug:
        print("DEBUG(Login) Response:", response.text)

    try:
        data = json.loads(response.text)

        # retrieve the sessionID
        thisSessionID = data['userInfo']['sessionId']
        global sessionID
        sessionID = thisSessionID

        # retrieve the orgID
        thisOrgID = data['userInfo']['orgId']
        global orgID
        orgID = thisOrgID
    except:
        print("ERROR logging in: ")
        print("DEBUG(login) URL:", loginURL, "Header", headers, "Data", json.dumps(loginData_debug))
        print("DEBUG(Login) Response:", response.text)
        quit()

    # Raw Base URL:
    ## RawURL = data['products'][0]['baseApiUrl']
    RawURL = data['products'][0]['baseApiUrl']
    if debug:
        print("DEBUG(login): RawURL: " + RawURL)

    global iics_URL
    if RawURL.endswith('/saas'):
        iics_URL = RawURL[:-len('/saas')]
    else:
        iics_URL = baseURL

    if debug:
        print("DEBUG(login): Setting iics_URL to " + iics_URL)

    global auth_header_xml
    auth_header_xml = {'content-type': 'application/xml', 'Accept': 'application/xml', 'INFA-SESSION-ID': sessionID,
                       'IDS-SESSION-ID': sessionID, 'icSessionId': sessionID}
    global auth_header_json
    ## auth_header_json = {'content-type':'application/json', 'Accept':'application/json', 'INFA-SESSION-ID':sessionID,'IDS-SESSION-ID':sessionID, 'icSessionId':sessionID}
    auth_header_json = {'Accept': 'application/json', 'INFA-SESSION-ID': sessionID, 'IDS-SESSION-ID': sessionID,
                        'icSessionId': sessionID}
    global auth_header_file
    ## auth_header_file = {'Content-Type':'multipart/form-data',  'Accept-Encoding':'gzip, deflate, br', 'Accept':'application/json', 'INFA-SESSION-ID':sessionID,'IDS-SESSION-ID':sessionID, 'icSessionId':sessionID}
    auth_header_file = {'Accept': 'application/json', 'INFA-SESSION-ID': sessionID, 'IDS-SESSION-ID': sessionID,
                        'icSessionId': sessionID}

    loginURL = baseURL+"/identity-service/api/v1/jwt/Token?client_id=cdlg_app&nonce=g3t69BWB49BHHNn&access_code="
    if debug:
        print("DEBUG(login): Catalog LoginUrl: "+loginURL)

    if debug:
        print("DEBUG(login): Catalog Auth_Header_Json: ",auth_header_json)

    # catresponse = requests.post(loginURL, auth_header_json, catdata=json.dumps(loginData),timeout=http_timeout)
    catresponse = requests.post(loginURL, headers=auth_header_json, data=json.dumps(loginData),timeout=http_timeout)

    if debug:
        print("DEBUG(login): Catalog Response: "+catresponse.text)

    try:
        catdata = json.loads(catresponse.text)

        # retrieve the sessionID
        thisTokenID = catdata['jwt_token']
        global jwt_token
        jwt_token = thisTokenID

    except:
        print("ERROR Getting Token in: ")
        print("DEBUG(login) URL:",loginURL,"Header",auth_header_json,"Data",json.dumps(loginData_debug))
        print("DEBUG(Login) Response:",catresponse.text)
        quit()
    return sessionID



def execute_export_job():
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    this_token = login()

    url = api_baseURL+"/data360/search/export/v1/assets?knowledgeQuery=Resources&segments=all&fileName=Export_Resources_"+timestamp+"&summaryViews=all"

    payload = json.dumps({
    "from": 0,
    "size": 10000
    })
    headers = {
    'X-INFA-ORG-ID': '010TIJ',
    'Content-Type': 'application/json',
    'Authorization': 'Bearer '+this_token
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)
