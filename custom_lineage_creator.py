import sys
import difflib
import pandas
import csv
import glob
import os
import datetime
import zipfile
import warnings
import re
import openpyxl
import json
import io
import requests
import getpass
import time
import configparser
from string import Template
from requests_toolbelt.multipart.encoder import MultipartEncoder
import argparse
import ast
warnings.simplefilter("ignore")

version = 20250319
print(f"INFO: custom_lineage_creator {version}")

help_message = '''

Usage. Execute the script.
By default, it'll prompt for which config file to use, and credential information.

Optionally, you can set parameters:

   --default_user
        You can specify the username to use. if you do now specify one, it will look in the
        ~/.informatica_cdgc/credentials file (as shown below)
        Example:
            --default_user=shayes_compass

   --default_pwd
        You can specify the password to use. if you do now specify one, it will look in the
        ~/.informatica_cdgc/credentials file (as shown below)
        Example:
            --default_pwd=12345

   --default_pod
        You can specify the pod to use. if you do now specify one, it will look in the
        ~/.informatica_cdgc/credentials file (as shown below)
        Typically this "pod" can be shown in the url: for example: "dm-us"
        Example:
            --default_pwd=dm-us  

   --prompt_for_login_info            
        Is set to False, it will not prompt to confirm credentials, unless needed.
        So, if a [default] profile is set in credentials file, or if credentials
        provided on the command line, it won't prompt.
        This defaults to True
        Example:
            --prompt_for_login_info=False

   --config_file
        You can specify an exact config file to use. This csv file should exist in the same
        directory as the py or exe file that was executed.
        Example:
            --config_file="config - My Lineage.csv"

   --config_file_path
        You can specify an exact config file to use, including the full path. Similar to config_file 
        setting, but you can specify a full path of the config file. For windows, use linux forward slashes.
        Example:
            --config_file_path="c:/junk/config - My Lineage.csv"            

    --pause_before_loading            
        You can force it to not pause, and just go ahead and load, if set to False
        This defaults to True
        Example:
            --pause_before_loading=False

    --pause_when_done
        You can set the behavior on whether or not the script pauses when it's complete.
        (So you can review the information on the window, before it disappears)
        Example:
            --pause_when_done=False

    --use_api
        You can set the script to not use the API. If you set this to False, then 
        additional requirements will be needed. The script will not make any connection
        to your env, or use any APIs. For this reason, you'll need to manually export the assets 
        (Within Data Governance and Catalog perform a search for "resources" and export including children)
        Place this file in the same location as the script/exe file
        If using ETL, you'll also need to download the templates, and models that you'll be using.
        (Within Metadata Command Center, go to customize, metadata Models, then Download Template 
        Models are json files, Metadata Templates are zip files)
        place these files in the <script_location>/data/templates folder.
        If using this option, the script will not create resources, or execute.
        So you'll need to manually create the resource(s) and execute them.
        Example:
            --use_api=False

    --directory_with_assets_export
        If you set the --use_api=False, you can specify which directory contains the export file zip/xlsx file
        It will default to the <script_location> and will look for the latest zip file or xlsx file in that directory.
        Example:
            --directory_with_assets_export=/my_files/assets

    --directory_to_write_links_file
        You can specify a different location to write the Lineage Resource zip files.
        It will default to <script_location>/links 
        Example:
            --directory_to_write_links_file=/my_files/links

    --directory_with_templates
        You can specify a different location to reading/write the template and model files.
        It will default to <script_location>/data/templates 
        Example:
            --directory_with_templates=/my_files/data/templates

    --directory_to_write_resource_files
        You can specify a different location to write the ETL Resource zip files.
        It will default to <script_location>/resources 
        Example:
            --directory_to_write_resource_files=/my_files/resources

    --extracts_folder
        You can specify a different location to write the temporary files that the api downloads.
        These files are raw collection of resources, and assets (in the case of using the api)
        It will default to <script_location>/data 
        Example:
            --extracts_folder=/my_files/data

   --models_to_download
        You can specify any additional models to download for your ETL. By default
        it will download "com.infa.odin.models.IICS.V2","com.infa.odin.models.Script", and "core"
        so no need to specify those. But if you are using an additional model, you can update the script
        or you can specify the model in an array format. Note that typically, any templates you're going
        to download, you'll want to get that model as well.
        Example:
            --models_to_download="['custom.etl', 'custom.myScript']"

   --templates_to_download
        You can specify any additional models to download for your ETL. By default
        it will download "com.infa.odin.models.IICS.V2" and "com.infa.odin.models.Script"
        so no need to specify those. But if you are using an additional model, you can update the script
        or you can specify the model in an array format. Note that typically, any templates you're going
        to download, you'll want to get that model as well.
        Example:
            --templates_to_download="['custom.etl', 'custom.myScript']"

Config file Names:            
    The names of the Lineage Resource will be derived from the config file name
    Name the config files like this:
        config - <Lineage Resource Name>.csv
            or
        config_<Lineage Resource Name>.csv

Python prerequisites:
    If needed, install the python prerequisites:  pip install pandas datetime openpyxl Requests requests_toolbelt
    If executing using the Windows exe, all python prerequisites should be covered.
    Ensure you have write access to the folder which the pythong script/binary resides

Credentials file:
    Optionally, you can create a "credentials" file in ~/.informatica_cdgc (or c:/users/xxx/.informatica_cdgc)
    If it finds a default, it'll use it.
    Otherwise it'll prompt for which profile to use.
    Format example (without extra preceding tabs):
        [default]
        pod = dmp-us
        user = shayes_example
        pwd = xyz

        [shayes_compass]
        pod = dmp-us
        user = shayes_compass
        pwd = abc

        [reinvent]
        pod = dm-us
        user = reinvent01
        pwd = zyx
'''

## Set this to pause before exiting
pause_when_done = True

## Script will use Informatica "internal" APIs
## If this is false, then you'll need to export
## the resources / datasets / elements to xlsx or zip file.
## And manually download the models and templates.
## If set to true, it'll do all the work for you.
use_api = True

## If using ETL in your lineage, besure to include models
## and templates for the objects you'll be using
## Included here are pretty common models.
models_to_download = ['com.infa.odin.models.IICS.V2', 'com.infa.odin.models.Script', 'core']
templates_to_download = ['com.infa.odin.models.IICS.V2', 'com.infa.odin.models.Script']

##########################################################################
## Leave this empty, and we'll look up or create a custom datatype
default_resource_type = ""

error_quietly = False
force_attributes_not_in_template = True
default_config_file = ""
config_file = default_config_file
## Leave this empty, and it'll prompt.

## if len(sys.argv) > 1:
    ## default_config_file = sys.argv[1]

## Default Locations for where to download or create items
script_location = os.path.dirname(os.path.abspath(sys.executable if getattr(sys, 'frozen', False) else __file__))

config_file_path = script_location+"/"+default_config_file
directory_to_write_links_file = script_location+"/links"
directory_with_templates = script_location+"/data/templates"
directory_to_write_resource_files = script_location+"/resources"

#################################################################
## For Using API
#################################################################
pause_before_loading = True
show_raw_errors = False
default_user = ""
default_pwd = ""
default_pod = ""

extracts_folder = script_location+'/data'

## If this is set to True, then it'll always prompt
## Otherwise, if it can find the credentials, it won't prompt
prompt_for_login_info = True
#################################################################

#################################################################
## For Using Export File (no API)
#################################################################
directory_with_assets_export = script_location+""
catalog_source_sheet = "Catalog Source"
catalog_refid_column = "Reference ID"
catalog_name_column = "Name"

technical_data_element_sheet = "Technical Data element"
element_name_column = "Name"
element_parent_column = "Parent: Technical Data Set"
element_refid_column = "Reference ID"
element_hierarchical_column = "HierarchicalPath"


technical_data_set_sheet = "Technical Data Set"
dataset_name_column = "Name"
dataset_refid_column = "Reference ID"
dataset_hierarchical_column = "HierarchicalPath"
#################################################################

timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
default_lineage_resource_name = "Lineage"


searches = [
    {
        'search_name': 'All Resources',
        'save_filename': 'resources.json',
        'elastic_search': {
                "from": 0,
                "size": 10000,
                "query": {
                    "term": {
                    "core.classType": "core.Resource"
                    }
                },
                "sort": [
                    {
                    "com.infa.ccgf.models.governance.scannedTime": {
                        "order": "desc"
                    }
                    }
                ]
        }
    },
     {
        'search_name': 'Assets in a Resource',
        'save_filename': 'assets.json',
        'elastic_search': {
                "from": 0,
                "size": 10000,
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"core.origin": "${core_origin}" }},
                            {"term": {"elementType": "OBJECT" }} 
                        
                        ]
                    }
                },
                "sort": [
                    {
                        "com.infa.ccgf.models.governance.scannedTime": {
                            "order": "desc"
                        }
                    }
                ]
            }
    }  
]

def parse_parameters():
    # Check for --help first
    if '--help' in sys.argv:
        print(help_message)
        programPause = input("Press the <ENTER> key to exit...")
        sys.exit(0)

    parser = argparse.ArgumentParser(description="Dynamically set variables from command-line arguments.")
    args, unknown_args = parser.parse_known_args()

    for arg in unknown_args:
        if arg.startswith("--") and "=" in arg:
            key, value = arg[2:].split("=", 1)  # Remove "--" and split into key and value
            try:
                # Safely parse value as Python object (list, dict, etc.)
                value = ast.literal_eval(value)
            except (ValueError, SyntaxError):
                pass  # Leave value as-is if parsing fails

            # Handle appending to arrays or updating dictionaries
            if key in globals():
                existing_value = globals()[key]
                if isinstance(existing_value, list) and isinstance(value, list):
                    ## If what was passed is an array, we'll append to the array
                    existing_value.extend(value)  # Append to the existing array
                elif isinstance(existing_value, dict) and isinstance(value, dict):
                    ## If what was passed is a dict, we'll add to the dict
                    existing_value.update(value)  # Add or update keys in the dictionary
                else:
                    ## Otherwise, it's an ordinary variable. replace it
                    globals()[key] = value  # Replace for other types
            else:
                ## It's a new variable. Create an ordinary variable.
                globals()[key] = value  # Set as a new variable

def select_recent_csv(directory):
    """
    Lists CSV files in a given directory, sorted by most recent modification time,
    prompts the user to select one, and returns the path for the selected file.

    Args:
        directory (str): The directory to search for CSV files.

    Returns:
        str: The full path of the selected CSV file, or None if no valid file is selected.
    """
    # Expand user directory if ~ is used
    directory = os.path.expanduser(directory)

    # Check if the directory exists
    if not os.path.isdir(directory):
        print(f"Directory not found: {directory}")
        return None

    # List all CSV files in the directory
    csv_files = [
        os.path.join(directory, file)
        for file in os.listdir(directory)
        if file.endswith('.csv')
    ]

    # Check if any CSV files were found
    if not csv_files:
        print(f"No CSV files found in the directory: {directory}")
        return None

    # Sort the files by modification time (most recent first)
    csv_files.sort(key=os.path.getmtime, reverse=True)

    # Display the files to the user with their modification times
    print("Select a CSV file:")
    for i, file in enumerate(csv_files, start=1):
        ## mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(file))
        lineage_resource_name = set_lineage_resource_name(filepath=os.path.basename(file), set_name=False)
        if use_api:
            print(f"    {i}. {os.path.basename(file)} - {lineage_resource_name}")
        else:
            print(f"    {i}. {os.path.basename(file)}")

    # Prompt the user to select a file
    while True:
        try:
            choice = int(input(f"Enter the number of the file to select (1-{len(csv_files)}): "))
            if 1 <= choice <= len(csv_files):
                selected_file = csv_files[choice - 1]
                return selected_file
            else:
                print(f"Invalid choice. Please select a number between 1 and {len(csv_files)}.")
        except ValueError:
            print("Invalid input. Please enter a number.")

def set_lineage_resource_name(filepath=default_config_file, set_name=True):
    global default_lineage_resource_name
    # Get the filename from the filepath
    filename = os.path.basename(filepath)
    filename_no_ext = os.path.splitext(filename)[0]
    cleaned_filename = re.sub(r'(?i)config[-_ ]*', '', filename_no_ext).strip()

    
    if len(cleaned_filename) < 2:
        if set_name:
            default_lineage_resource_name = f"Lineage ({timestamp})"
        else:
            return f"Lineage ({timestamp})"
    else:
        if set_name:
            default_lineage_resource_name = cleaned_filename
        else:
            return cleaned_filename

def cleanup_data():
    os.makedirs(extracts_folder, exist_ok=True)
    for filename in os.listdir(extracts_folder):
        file_path = os.path.join(extracts_folder, filename)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)  # Delete the file
        except Exception as e:
            print(f"ERROR: Error deleting file {file_path}: {e}")


# Define function to read JSON files and create DataFrames
def read_json_files_into_dataframes(assets_file, resources_file):
    # Load JSON files
    with open(assets_file, 'r') as f:
        assets_data = json.load(f)
    with open(resources_file, 'r') as rf:
        resources_data = json.load(rf)

    # Extract data from JSON arrays
    assets = []
    for a in assets_data:
        for b in a['hits']['hits']:
            assets.append(b)

    resources = []
    for r in resources_data:
        for rb in r['hits']['hits']:
            resources.append(rb)

    def get_resource_name_from_id(id_to_check):
        for resource in resources:
            source = resource.get("sourceAsMap", {})
            this_name = source.get("core.name", "")
            this_type = source.get("core.resourceType", "Reference")
            this_id = source.get("core.origin", "")

            if "reference" not in this_type.lower() and this_id == id_to_check:
                return this_name
        return id_to_check

    def get_hierarchical_path(location):
        """
        Function to create hierarchical path from location field
        """
        if not location:
            return ""
        parts = location.split("/")
        # Assuming hierarchical path uses core.externalId of objects after base identifier
        raw_path = "/".join(parts[2:])
        resource_id = raw_path.split("/")[0]
        resource_name = get_resource_name_from_id(resource_id)
        path = raw_path.replace(resource_id, resource_name)
        return path



    test_object = assets[0]
    ## print(f"DEBUGSCOTT: {test_object}")
    ## exit()
    # Create dictionaries to hold dataset and element records
    dataset_records = []
    element_records = []
    resource_records = []

    # Iterate through assets to find datasets and elements
    for asset in assets:
        source = asset.get("sourceAsMap", {})
        ## print(f"DEBUGSCOTT: {source}")
        asset_type = source.get("type", [])
        if "core.DataSet" in asset_type:
            # Extract dataset details
            name = source.get("core.name")
            reference_id = source.get("core.externalId")
            location = source.get("core.location")
            hierarchical_path = get_hierarchical_path(location)
            ## We need to exclude these specific classes, as I can't create lineage for these
            if not reference_id.endswith('~core.DataSet') and not reference_id.endswith('~core.DataElement'):
                dataset_records.append({
                    "Name": name,
                    "Reference ID": reference_id,
                    "HierarchicalPath": hierarchical_path
                })
        elif "core.DataElement" in asset_type:
            # Extract element details
            name = source.get("core.name")
            reference_id = source.get("core.externalId")
            location = source.get("core.location")
            hierarchical_path = get_hierarchical_path(location)
            ## We need to exclude these specific classes, as I can't create lineage for these
            if not reference_id.endswith('~core.DataSet') and not reference_id.endswith('~core.DataElement'):
                element_records.append({
                    "Name": name,
                    "Reference ID": reference_id,
                    "HierarchicalPath": hierarchical_path
                })

    for resource in resources:
        source = resource.get("sourceAsMap", {})
        name = source.get("core.name")
        reference_id = source.get("core.externalId")
        origin = source.get("core.origin")       
        resource_records.append({
            "Name": name,
            "Reference ID": reference_id,
            "Origin": origin
        })
    # Convert records to dataframes
    dataset_df = pandas.DataFrame(dataset_records)
    element_df = pandas.DataFrame(element_records)
    resource_df = pandas.DataFrame(resource_records)

    return resource_df, dataset_df, element_df


def process_search(search_name, dataset_list=[], **tokens):
    getCredentials()
    login()        

    for i in searches:
        if i['search_name'] == search_name:
            this_search_name = i['search_name']
            this_save_filename = i['save_filename']
            this_elastic_search_raw = i['elastic_search']
            query_json_template = json.dumps(this_elastic_search_raw)
            template = Template(query_json_template)
            this_elastic_search = json.loads(template.safe_substitute(tokens))

            this_full_filename_path = os.path.join(extracts_folder, this_save_filename)
            getCredentials()
            login()
            this_header = headers_bearer.copy()
            this_header['X-INFA-SEARCH-LANGUAGE'] = 'elasticsearch'
            
            Result = requests.post(cdgc_url+"/ccgf-searchv2/api/v1/search", headers=this_header, data=json.dumps(this_elastic_search))
            detailResultJson = json.loads(Result.text)

            total_hits_this_search = 0
            try:
                total_hits_this_search = dataset_list and detailResultJson['hits']['totalHits']
            except:
                pass

            if dataset_list and detailResultJson['hits']['totalHits'] > 9999:
                ## If the hits are 10000, then we didn't get everything
                ## the API is limiting us. 
                ## So we're rewrite the elastic search to include the names of the datasets, if possible
                this_updated_elastic_search = this_elastic_search
                should_clause = []
                for value in dataset_list:
                    should_clause.extend([
                        {"wildcard": {"core.name": f"*{value}*"}},
                        {"wildcard": {"core.externalId": f"*{value}*"}}
                    ])
                this_updated_elastic_search['query']['bool']['should'] = should_clause
                this_updated_elastic_search['query']['bool']['minimum_should_match'] = 1

                print(f"WARN: The search is returning a large number. I'll try to limit that down, now")
                ## print(f"{this_updated_elastic_search}")
                Result = requests.post(cdgc_url+"/ccgf-searchv2/api/v1/search", headers=this_header, data=json.dumps(this_updated_elastic_search))
                detailResultJson = json.loads(Result.text)


            os.makedirs(extracts_folder, exist_ok=True)
            if os.path.exists(this_full_filename_path) and os.path.getsize(this_full_filename_path) > 0:
                # Read existing data
                with open(this_full_filename_path, 'r') as file:
                    try:
                        data = json.load(file)  # Load existing JSON array
                    except json.JSONDecodeError:
                        data = []  # Start a new array if the file is not valid JSON
            else:
                data = []  # Start a new array if the file doesn't exist or is empty

            # Append new data to the list
            data.append(detailResultJson)

            # Write the updated array back to the file
            with open(this_full_filename_path, 'w') as file:
                json.dump(data, file, indent=4)

def load_credentials_from_home():
    global default_user, default_pwd, default_pod

    def get_informatica_credentials():
        credentials_path = os.path.join(os.path.expanduser("~"), ".informatica_cdgc", "credentials")
        if not os.path.exists(credentials_path):
            print(f"INFO: Credentials file not found: {credentials_path}")
            return None

        config = configparser.ConfigParser()
        config.read(credentials_path)

        if "default" in config:
            return dict(config["default"])

        # If no default section, list available profiles and prompt user to select one
        profiles = config.sections()
        if not profiles:
            return None

        print("INFO: No 'default' profile found. Please select a profile:")
        for i, profile in enumerate(profiles, start=1):
            print(f"    {i}. {profile}")

        # Prompt user for selection
        while True:
            try:
                choice = int(input("Enter the number of the profile to use: "))
                if 1 <= choice <= len(profiles):
                    selected_profile = profiles[choice - 1]
                    print(f"Using credentials from the '{selected_profile}' profile.")
                    return dict(config[selected_profile])
                else:
                    print(f"INFO: Invalid choice. Please select a number between 1 and {len(profiles)}.")
            except ValueError:
                print("INFO: Invalid input. Please enter a valid number.")

    if len(default_user) < 1 or len(default_pwd) < 1 or len(default_pod) < 1:
        credentials_dict = get_informatica_credentials()
        if credentials_dict:
            default_user = credentials_dict.get('user')
            default_pwd = credentials_dict.get('pwd')
            default_pod = credentials_dict.get('pod')
        else:
            # Define the path to the credentials file in the user's home directory
            credentials_path = os.path.join(os.path.expanduser("~"), ".informatica_cdgc", "credentials.json")
            
            # Check if the file exists
            if os.path.exists(credentials_path):
                with open(credentials_path, 'r') as file:
                    try:
                        # Load the JSON data
                        credentials = json.load(file)
                        
                        # Set each credential individually if it exists in the file
                        if 'default_user' in credentials:
                            default_user = credentials['default_user']
                        if 'default_pwd' in credentials:
                            default_pwd = credentials['default_pwd']
                        if 'default_pod' in credentials:
                            default_pod = credentials['default_pod']
                        
                    except json.JSONDecodeError:
                        pass

def process_json_error(text):
    result_text = text
    if not show_raw_errors:
        try:
            resultJson = json.loads(text)
            result_text = resultJson['message']
        except Exception as e:
            pass
    return result_text

def getCredentials():
    global pod
    global iics_user
    global iics_pwd
    global iics_url
    global cdgc_url
    global idmc_url

    load_credentials_from_home()
    if any(var not in globals() for var in ['pod', 'iics_user', 'iics_pwd', 'iics_url', 'cdgc_url', 'idmc_url']):
        if prompt_for_login_info == True:
            pod = input(f"Enter pod (default: {default_pod}): ") or default_pod
            iics_user = input(f"Enter username (default : {default_user}): ") or default_user
            iics_pwd=getpass.getpass("Enter password: ") or default_pwd   
        else:
            if len(default_pod) > 1:
                pod = default_pod
            else:
                pod = input(f"Enter pod (default: {default_pod}): ") or default_pod
            if len(default_user) > 1:
                iics_user = default_user
            else:
                iics_user = input(f"Enter username (default : {default_user}): ") or default_user
            if len(default_pwd) > 1:
                iics_pwd = default_pwd
            else:
                iics_pwd=getpass.getpass("Enter password: ") or default_pwd   
        iics_url = "https://"+pod+".informaticacloud.com"
        cdgc_url = "https://cdgc-api."+pod+".informaticacloud.com"
        idmc_url = f"https://idmc-api.{pod}.informaticacloud.com"

def login():
    global sessionID
    global orgID
    global headers
    global headers_bearer
    global jwt_token
    global api_url   
    # retrieve the sessionID & orgID & headers
    ## Test to see if I'm already logged in
    if 'jwt_token' not in globals() or len(headers_bearer) < 2:
        loginURL = iics_url+"/saas/public/core/v3/login"
        loginData = {'username': iics_user, 'password': iics_pwd}
        response = requests.post(loginURL, headers={'content-type':'application/json'}, data=json.dumps(loginData))
        try:        
            data = json.loads(response.text)   
            sessionID = data['userInfo']['sessionId']
            orgID = data['userInfo']['orgId']
            api_url = data['products'][0]['baseApiUrl']
            headers = {'Accept':'application/json', 'INFA-SESSION-ID':sessionID,'IDS-SESSION-ID':sessionID, 'icSessionId':sessionID}
        except:
            print("ERROR: logging in: ",loginURL," : ",response.text)
            quit()

        # retrieve the Bearer token
        URL = iics_url+"/identity-service/api/v1/jwt/Token?client_id=cdlg_app&nonce=g3t69BWB49BHHNn&access_code="  
        response = requests.post(URL, headers=headers, data=json.dumps(loginData))
        try:        
            data = json.loads(response.text)
            jwt_token = data['jwt_token']
            headers_bearer = {'content-type':'application/json', 'Accept':'application/json', 'INFA-SESSION-ID':sessionID,'IDS-SESSION-ID':sessionID, 'icSessionId':sessionID, 'Authorization':'Bearer '+jwt_token}        
        except:
            print("ERROR: Getting Token in: ",URL," : ",response.text)
            quit()

def download_template_file(type, name):
    getCredentials()
    login()        

    if not os.path.exists(directory_with_templates):
        os.makedirs(directory_with_templates) 

    if type == "metamodel":

        this_header = headers_bearer.copy()
        ## this_header['X-INFA-SEARCH-LANGUAGE'] = 'elasticsearch'
        result = requests.get(cdgc_url + f"/ccgf-modelv2/api/v2/models/{name}/export?packageName={name}", headers=this_header)

        # Check if the request was successful
        if result.status_code in [200, 201, 202, 204]:
            # Write the binary content to a file
            with open(f'{directory_with_templates}/{name}.json', 'wb') as file:
                file.write(result.content)
        else:
            print(f"ERROR: Unable to download {name}.json: {result.text}")

    elif type == "template":

        this_header = headers_bearer.copy()
        ## this_header['X-INFA-SEARCH-LANGUAGE'] = 'elasticsearch'
        this_payload = {"packageName": name, "format": "csv"}
        result = requests.post(cdgc_url + f"/ccgf-modelv2/api/v2/models/export?packageName={name}&format=csv", headers=this_header, data=json.dumps(this_payload))

        # Check if the request was successful
        if result.status_code in [200, 201, 202, 204]:
            # Write the binary content to a file
            with open(f'{directory_with_templates}/{name}_metadata_template.zip', 'wb') as file:
                file.write(result.content)
        else:
            print(f"ERROR: Unable to download {name}_metadata_template.zip: {result.text}")

def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        # Call the default hook for keyboard interrupts
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    # Here you can log the exception or handle it as you want
    ## print(f"An error occurred: {exc_value}")

if error_quietly:    
    sys.excepthook = handle_exception    

resource_classes = {}
json_objects = {}

def get_reference_prefix(name):
    # Filter the DataFrame for the given name
    result = df_full_export_Resources[df_full_export_Resources[catalog_name_column] == name]
    
    # Check if any results were found
    if not result.empty:
        # Extract the Reference ID
        reference_id = result[catalog_refid_column].values[0]
        # Return the prefix (portion before '://')
        return reference_id.split('://')[0]
    else:
        return f"xxx_{name}_xxx"

def add_df_to_resource_classes(resource_name, class_name, new_row, extra_fields={}, replacement_text={}):
    global resource_classes

    # Use setdefault to initialize the resource_classes if it doesn't exist
    resource_classes.setdefault(resource_name, {})
    
    # Check if the class_name DataFrame exists; if not, initialize it
    if class_name not in resource_classes[resource_name]:
        resource_classes[resource_name][class_name] = pandas.DataFrame()  # Initialize as empty DataFrame

    # Concatenate the existing DataFrame with the new row
    
    this_name = new_row.get('core.name', "NO_NAME")
    for key in extra_fields:
        if ":" in key:
            parts = key.split(":")
            p_for_name = parts[0]
            for r_key in replacement_text:
                p_for_name = p_for_name.replace(r_key, replacement_text[r_key])
            p_name = parts[1]
            if this_name == p_for_name:
                if p_name in resource_classes[resource_name][class_name] or force_attributes_not_in_template:
                    new_value = extra_fields[key]
                    for r_key in replacement_text:
                        new_value = new_value.replace(r_key, replacement_text[r_key]) 
                    if len(new_value) > 0:
                    # Only add if there's a value here. If it's a null value, then skip 
                        extra_field = {p_name: new_value}
                        new_row.update(extra_field)

    new_df = pandas.DataFrame([new_row])  
    resource_classes[resource_name][class_name] = pandas.concat(
        [resource_classes[resource_name][class_name], new_df],
        ignore_index=True
    )

def generate_additional_class(resource_name, parent_path, parent_type, parent_id, current_type, attribute_dict, dataset_name="XXX", element_name="XXX", extra_fields={}, replacement_text={}):
    global resource_links
    ## Fatching the name from the provided 
    this_etl_name = attribute_dict["name"].replace('{name}',dataset_name)
    this_etl_path = parent_path+"/"+this_etl_name
    this_etl_id_work = parent_id+"_"+this_etl_name
    this_etl_id = re.sub(r'[^a-zA-Z0-9]', '_', this_etl_id_work).lower()
    this_association = find_association(parent_type, current_type)
    new_assocation_row = {"Source": parent_id, "Target": this_etl_id , "Association": this_association}

    '''
    replacement_text=dataset_name
    if element_name != 'XXX':
        replacement_text = element_name    
    '''
        
    ## new_assocation_df = pandas.concat([resource_links, pandas.DataFrame([new_assocation_row])], ignore_index=True) 
    ## resource_links = new_assocation_df
    add_df_to_resource_classes(resource_name, "links", new_assocation_row, replacement_text=replacement_text)

    df = load_template(resource_name, current_type)
    new_row = {"core.name": this_etl_name, "core.externalId": this_etl_id}

    ## print(f"DEBUGSCOTT2: element_name is {element_name}")

    add_df_to_resource_classes(resource_name, current_type, new_row, extra_fields=extra_fields, replacement_text=replacement_text)
    ## test new_df = pandas.concat([df, pandas.DataFrame([new_row])], ignore_index=True)       
    ## test resource_classes[resource_name][current_type]  = new_df

    return this_etl_path, current_type, this_etl_id


def generate_resource_path(resource_name,etl_path_string, etl_types_string, dataset_name="XXX", extra_fields={}, replacement_text={}):
    global resource_links

    formatted_etl_path_string = etl_path_string
    for r_key in replacement_text:
        formatted_etl_path_string = formatted_etl_path_string.replace(r_key, replacement_text[r_key])   

    etl_names = formatted_etl_path_string.split('/')
    etl_types = etl_types_string.split('/')

    last_type = ""
    last_id = ""
    for index, value in enumerate(etl_names):
        ## Replacing within values
        new_name_value = value
        for r_key in replacement_text:
            new_name_value = new_name_value.replace(r_key, replacement_text[r_key])

        this_etl_name = new_name_value
        this_etl_type = etl_types[index]
        this_previous_elements = etl_names[:index]
        this_previous_string = "_".join(this_previous_elements)
        this_etl_id_work = ""
        if this_previous_string:
            this_etl_id_work = this_previous_string+"_"+this_etl_name
            this_etl_id = re.sub(r'[^a-zA-Z0-9]', '_', this_etl_id_work).lower()
            previous_id = re.sub(r'[^a-zA-Z0-9]', '_', this_previous_string).lower()
            previous_type = etl_types[index-1]
            this_association = find_association(previous_type, this_etl_type)
            new_assocation_row = {"Source": previous_id, "Target": this_etl_id , "Association": this_association}
            add_df_to_resource_classes(resource_name, "links", new_assocation_row, replacement_text=replacement_text)
        else:
            this_etl_id_work = this_etl_name
            this_etl_id = re.sub(r'[^a-zA-Z0-9]', '_', this_etl_id_work).lower()
            new_assocation_row = {"Source": "$resource", "Target": this_etl_id , "Association": "core.ResourceParentChild"}
            add_df_to_resource_classes(resource_name, "links", new_assocation_row, extra_fields=extra_fields, replacement_text=replacement_text)
      

        df = load_template(resource_name,this_etl_type)
        new_row = {"core.name": this_etl_name, "core.externalId": this_etl_id}
        add_df_to_resource_classes(resource_name, this_etl_type, new_row,extra_fields=extra_fields, replacement_text=replacement_text)

        last_type = this_etl_type
        last_id = this_etl_id
    return resource_name+"/"+formatted_etl_path_string, last_type, last_id

def load_metamodels(search_path=directory_with_templates):
    """
    Searches for JSON files within a directory (including inside zip files) 
    and stores those containing 'packageName' in a global dictionary.

    Parameters:
    search_path (str): The path to search for JSON files.
    """
    for root, dirs, files in os.walk(search_path):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                json_data = load_json_file(file_path)
                if json_data and "packageName" in json_data:
                    json_objects[file_path] = json_data

            elif file.endswith(".zip"):
                zip_path = os.path.join(root, file)
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_file:
                        for zip_info in zip_file.infolist():
                            if zip_info.filename.endswith(".json"):
                                with zip_file.open(zip_info.filename) as json_file:
                                    json_data = load_json_content(json_file)
                                    if json_data and "packageName" in json_data:
                                        json_objects[zip_info.filename] = json_data
                except zipfile.BadZipFile:
                    print(f"Warning: '{zip_path}' is not a valid zip file.")

def find_association(from_class, to_class):
    """
    Searches the global dictionary for associations matching the given 'fromClass' 
    and 'toClass', including matches with superClasses. Returns the concatenated 
    'packageName.name' if a match is found.

    Parameters:
    from_class (str): The 'fromClass' value to search for.
    to_class (str): The 'toClass' value to search for.

    Returns:
    str: Concatenated 'packageName.name' if a match is found, otherwise None.
    """
    # Build a lookup dictionary for class names and their superClasses
    class_hierarchy = build_class_hierarchy()
    
    for json_data in json_objects.values():
        package_name = json_data.get("packageName")
        associations = json_data.get("associations", [])

        for association in associations:
            # Get the classes to match against
            association_from = association.get("fromClass")
            association_to = association.get("toClass")
            isParentChildKind = "core.ParentChild" in association.get("associationKinds")

            if (isParentChildKind) and (is_class_or_superclass(association_from, from_class, class_hierarchy) and
                is_class_or_superclass(association_to, to_class, class_hierarchy)):
                return f"{package_name}.{association.get('name')}"
    return None

def build_class_hierarchy():
    """
    Builds a dictionary mapping class names to their superClasses from the JSON objects.

    Returns:
    dict: A dictionary where keys are class names and values are sets of their superClasses.
    """
    class_hierarchy = {}

    for json_data in json_objects.values():
        packageName = json_data.get('packageName')
        classes = json_data.get("classes", [])
        for cls in classes:
            name = packageName+"."+cls.get("name")
            super_classes = set(cls.get("superClasses", []))
            class_hierarchy[name] = super_classes

    return class_hierarchy

def is_class_or_superclass(class_name, target_class, class_hierarchy):
    """
    Checks if a class name matches the target class directly or through its superClasses.

    Parameters:
    class_name (str): The class name from the association.
    target_class (str): The class name to match against.
    class_hierarchy (dict): The class hierarchy dictionary.

    Returns:
    bool: True if the class matches the target class or one of its superClasses.
    """
    if class_name == target_class:
        return True
    return class_name in class_hierarchy.get(target_class, set())

def load_json_file(file_path):
    """Loads a JSON file from a file path."""
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error reading '{file_path}': {e}")
        return None

def load_json_content(json_file):
    """Loads JSON content from a file-like object."""
    try:
        return json.load(json_file)
    except json.JSONDecodeError as e:
        print(f"Error reading JSON content: {e}")
        return None


def load_template(resource_name,template_name, search_path=directory_with_templates):
    """
    Searches for a CSV file with the given name within the specified directory path, 
    including all subdirectories and any .zip files encountered. Returns a DataFrame
    if the file is found and loaded successfully.
    
    Parameters:
    file_name (str): The name of the CSV file to search for.
    search_path (str): The path to the directory where the search should start.
    
    Returns:
    pd.DataFrame: DataFrame containing the CSV data if found, otherwise None.
    """

    if resource_name in resource_classes and template_name in resource_classes[resource_name]:
        return resource_classes[resource_name][template_name]
    else:
        file_name = template_name+".csv"
        for root, dirs, files in os.walk(search_path):
            # Check for the CSV file in the current directory
            if file_name in files:
                file_path = os.path.join(root, file_name)
                df = pandas.read_csv(file_path)
                resource_classes[resource_name][template_name] = df
                return df
            
            # Check within zip files in the current directory
            for file in files:
                if file.endswith('.zip'):
                    zip_path = os.path.join(root, file)
                    try:
                        with zipfile.ZipFile(zip_path, 'r') as zip_file:
                            if file_name in zip_file.namelist():
                                with zip_file.open(file_name) as csv_file:
                                    df = pandas.read_csv(csv_file)
                                    resource_classes[resource_name][template_name] = df
                                    return df
                    except zipfile.BadZipFile:
                        print(f"Warning: '{zip_path}' is not a valid zip file.")
                    except Exception as e:
                        print(f"Error reading '{file_name}' in '{zip_path}': {e}")
    
    return None



def finish_up():
    if pause_when_done:
        programPause = input("Press the <ENTER> key to exit...")
    exit()


if not os.path.exists(directory_with_assets_export):
    os.makedirs(directory_with_assets_export)

if not os.path.exists(directory_to_write_links_file):
    os.makedirs(directory_to_write_links_file)    

def extract_dataframe_from_zip(zip_file_path, this_sheet_name):
    # List to hold dataframes from all the xlsx files
    all_dataframes = []
    
    # Open the zip file
    with zipfile.ZipFile(zip_file_path, 'r') as z:
        # Loop through each file in the zip archive
        for file_name in z.namelist():
            # Check if the file is an xlsx file
            if file_name.endswith('.xlsx'):
                # Open the xlsx file from the zip without extracting to disk
                with z.open(file_name) as xlsx_file:
                    # Read the Excel file
                    excel_file = pandas.ExcelFile(xlsx_file)
                    
                    if this_sheet_name in excel_file.sheet_names:
                        # Read the "Technical Data Set" sheet into a DataFrame
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            df = pandas.read_excel(excel_file, sheet_name=this_sheet_name)
                            all_dataframes.append(df)
    
    # If dataframes were found, concatenate them into a single dataframe
    if all_dataframes:
        combined_df = pandas.concat(all_dataframes, ignore_index=True)
        return combined_df
    else:
        print(f"No {this_sheet_name} sheets found in any files.")
        finish_up()
        return None

# Read the specific sheet 'Technical Data element'
def find_latest_xlsx(directory):
    # Search for .zip and .xlsx files
    zip_files = glob.glob(os.path.join(directory, "*.zip"))
    xlsx_files = glob.glob(os.path.join(directory, "*.xlsx"))

    latest_file = None

    # Find the latest .zip file
    if zip_files:
        latest_zip = max(zip_files, key=os.path.getmtime)
    else:
        latest_zip = None

    # Find the latest .xlsx file
    if xlsx_files:
        latest_xlsx = max(xlsx_files, key=os.path.getmtime)
    else:
        latest_xlsx = None

    # Compare modification times and return the latest file
    if latest_zip and latest_xlsx:
        # If both exist, return the latest based on modification time
        latest_file = latest_zip if os.path.getmtime(latest_zip) > os.path.getmtime(latest_xlsx) else latest_xlsx
    elif latest_zip:
        latest_file = latest_zip
    elif latest_xlsx:
        latest_file = latest_xlsx

    return latest_file
    
    


def write_csv(df):
    # Get the current timestamp
    file_name = f'{directory_to_write_links_file}/links_{timestamp}.csv'
    # Open the CSV file in append mode
    with open(file_name, mode='a', newline='') as file:
        writer = csv.writer(file)
        
        # Write header if the file is empty or doesn't exist
        if file.tell() == 0:
            writer.writerow(['Source','Target','Association'])
        
        # Write each name with the current timestamp
        print(f"Writing to {file_name}")
        for index, row in df.iterrows():
            writer.writerow([row['src Reference ID'], row['tgt Reference ID'], row['Association']])

def write_links_zip(df, resource_name=default_lineage_resource_name):
    # Get the current timestamp
    cleaned_resource_name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', '_', resource_name)
    zip_file_name = os.path.join(directory_to_write_links_file, f'{cleaned_resource_name}_{timestamp}.zip')
    
    # Create a zip file and write the CSV data directly to it
    try:
        with zipfile.ZipFile(zip_file_name, mode='w') as zf:
            # Create a StringIO buffer to hold the CSV data
            with io.StringIO() as csv_buffer:
                # Write the header
                csv_buffer.write('Source,Target,Association\n')
                
                # Write each row with formatted data
                for index, row in df.iterrows():
                    csv_buffer.write(f"{row['src Reference ID']},{row['tgt Reference ID']},{row['Association']}\n")
                
                # Go back to the start of the StringIO buffer
                csv_buffer.seek(0)
                
                # Write the buffer content to the zip file
                zf.writestr('links.csv', csv_buffer.getvalue())
        
        print(f"Writing to {zip_file_name} containing links.csv")
        return resource_name, zip_file_name


    except Exception as e:
        print(f"An error occurred while writing to the zip file: {e}")


def write_resource_to_zip(resource_name, df_dict) :
    # Create a zip file and write each DataFrame to separate CSV files
    if not os.path.exists(directory_to_write_resource_files):
        os.makedirs(directory_to_write_resource_files)            

    zip_file_path = directory_to_write_resource_files+"/"+resource_name+"_"+timestamp+".zip"
    try:
        with zipfile.ZipFile(zip_file_path, mode='w') as zf:
            for name, df in df_dict.items():
                with io.StringIO() as csv_buffer:
                    # Write the DataFrame to the buffer
                    df_cleaned = df.loc[:, ~df.columns.str.contains('^Unnamed')]  # Remove any "Unnamed" columns if they exist
                    df_cleaned.drop_duplicates(inplace=True)
                    df_cleaned.to_csv(csv_buffer, index=False, header=True)
                    csv_buffer.seek(0)  # Reset the buffer to the beginning
                    
                    # Write the buffer content to the zip file as {name}.csv
                    zf.writestr(f"{name}.csv", csv_buffer.getvalue())
        
        print(f"Successfully written {resource_name} to {zip_file_path}")
        return resource_name, zip_file_path

    except Exception as e:
        print(f"An error occurred while writing to the zip file: {e}")

def write_dataframe_to_csv(df, file_name):
    df_cleaned = df.loc[:, ~df.columns.str.contains('^Unnamed')]  # Remove any "Unnamed" columns if they exist
    df_cleaned.to_csv(file_name, index=False) 


def append_or_create(df, new_data):
    if df is None or df.empty:
        # Create the DataFrame if it's None or empty
        df = pandas.DataFrame(new_data)
    else:
        # Append new data if the DataFrame already exists
        df = pandas.concat([df, new_data])
    return df

def create_extra_fields(row):
    # Define headers to exclude
    excluded_headers = [
        "Source Resource", "Source Dataset", "Source Element",
        "Target Resource", "Target Dataset", "Target Element",
        "Dataset Match Score", "Element Match Score",
        "ETL Resource Name", "ETL Path", "ETL Path Types",
        "ETL Dataset Type", "ETL Dataset Name", "ETL Element Type",
        "ETL Element Name"
    ]

    # Create a dictionary from the row, excluding specified headers
    filtered_dict = {col: row.get(col, "") for col in row.keys() if col not in excluded_headers}

    return filtered_dict

def getObjectsFromApi():
    ## Remove files from the current extracts directory
    cleanup_data()
    ## Get the resources into a json file
    process_search('All Resources')
    ## Get the filename that it saves to:
        
    resource_names = []
    dataset_list = []
    with open(config_file_path) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            this_Source_Resource = row['Source Resource'].split('/')[0]
            this_Target_Resource = row['Target Resource'].split('/')[0]
            this_Source_Dataset = row['Source Dataset']
            this_Target_Dataset = row['Target Dataset']

            if this_Source_Resource not in resource_names:
                resource_names.append(this_Source_Resource)
            if this_Target_Resource not in resource_names:
                resource_names.append(this_Target_Resource)

            if '{' not in this_Source_Dataset:
                cleaned_Source_Dataset = re.sub(r'[^a-zA-Z0-9_ ]', '', this_Source_Dataset)
                if cleaned_Source_Dataset not in dataset_list and len(cleaned_Source_Dataset) > 1:
                   dataset_list.append(cleaned_Source_Dataset)
                   ## We're only going to use this list of datasets if the list of all objects is too large (10000)
                   ## So, I'm ignoring any regex in this string, and if it contains a {name} token or something, we'll ignore it

            if '{' not in this_Target_Dataset:
                cleaned_Target_Dataset = re.sub(r'[^a-zA-Z0-9_ ]', '', this_Target_Dataset)
                if cleaned_Target_Dataset not in dataset_list and len(cleaned_Target_Dataset) > 1:
                   dataset_list.append(cleaned_Target_Dataset)
                   ## We're only going to use this list of datasets if the list of all objects is too large (10000)
                   ## So, I'm ignoring any regex in this string, and if it contains a {name} token or something, we'll ignore it

    resources_file = next( (extracts_folder + "/" + s['save_filename'] for s in searches if s['search_name'] == 'All Resources'),  None)
    with open(resources_file, 'r') as rf:
        resources_data = json.load(rf)

        resources_to_get = []
        for r in resources_data:
            for rb in r['hits']['hits']:
                source = rb.get("sourceAsMap", {})
                this_name = source.get("core.name", "")
                this_id = source.get("core.origin", "")
                if this_name in resource_names and this_id not in resources_to_get:
                    resources_to_get.append(this_id)

    for core_origin in resources_to_get:
        process_search('Assets in a Resource', dataset_list=dataset_list, core_origin=core_origin)

    for model in models_to_download:
        download_template_file('metamodel', model)
    for template in templates_to_download:
        download_template_file('template', template) 

def readConfigAndStart(fileName):

    global final_dataframe
    final_dataframe = None


    with open(fileName) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            this_Source_Resource = row['Source Resource']
            this_Source_Dataset_regex = row['Source Dataset']
            this_Source_Element_regex = row['Source Element']
            this_Target_Resource = row['Target Resource']
            this_Target_Dataset_regex = row['Target Dataset']
            this_Target_Element_regex = row['Target Element']
            this_Dataset_Match_Score = row['Dataset Match Score']
            this_Element_Match_Score = row['Element Match Score']
            this_ETL_Resource_Name = row.get('ETL Resource Name', "")
            this_ETL_Path = row.get('ETL Path', "")
            this_ETL_Path_Types = row.get('ETL Path Types', "")
            this_ETL_Dataset_Type = row.get('ETL Dataset Type', "")
            this_ETL_Dataset_Name = row.get('ETL Dataset Name', "")
            this_ETL_Element_Type = row.get('ETL Element Type', "")
            this_ETL_Element_Name = row.get('ETL Element Name', "")

            extra_fields = create_extra_fields(row)

            resource_ref_id_base = get_reference_prefix(this_ETL_Resource_Name)
            base_path = ""
            base_type = ""
            base_id = ""
            generated_base = False

            source_dataset_match = df_full_export_Datasets[
                df_full_export_Datasets[dataset_hierarchical_column].str.startswith(f"{this_Source_Resource}/") &
                df_full_export_Datasets[dataset_name_column].str.contains('^'+this_Source_Dataset_regex+'$', na=False, regex=True, flags=re.IGNORECASE)
            ]

            global final_element_dataframe
            final_element_dataframe = None

            for index, source in source_dataset_match.iterrows():
                ds_hp = source[dataset_hierarchical_column]
                ds_id = source[dataset_refid_column]
                ds_name = source[dataset_name_column]

                updated_regex = this_Target_Dataset_regex.replace('{name}',ds_name).replace('{s_dataset}',ds_name)
                target_dataset_match = df_full_export_Datasets[
                    df_full_export_Datasets[dataset_hierarchical_column].str.startswith(f"{this_Target_Resource}/", na=False) &
                    df_full_export_Datasets[dataset_name_column].str.contains('^'+updated_regex+'$', na=False, regex=True, flags=re.IGNORECASE)
                ]

                for index, target in target_dataset_match.iterrows():
                    dt_hp = target[dataset_hierarchical_column]
                    dt_id = target[dataset_refid_column]
                    dt_name = target[dataset_name_column]
                    
                    score = difflib.SequenceMatcher(None, ds_name, dt_name).ratio()
                    if score >= float(this_Dataset_Match_Score):

                        if len(this_ETL_Path) > 1 and len(this_ETL_Path_Types) > 1:
                            replacement_text = {'{name}': ds_name, '{s_dataset}': ds_name, '{t_dataset}': dt_name }
                            base_path, base_type, base_id = generate_resource_path(this_ETL_Resource_Name, this_ETL_Path, this_ETL_Path_Types, dataset_name=ds_name, extra_fields=extra_fields, replacement_text=replacement_text)
                            etl_dataset_name = this_ETL_Dataset_Name.replace('{name}',ds_name).replace('{s_dataset}',ds_name).replace('{t_dataset}',dt_name)
                            etl_dataset_path, etl_dataset_type, etl_dataset_id = generate_additional_class(this_ETL_Resource_Name,base_path, base_type, base_id, this_ETL_Dataset_Type, {"name": etl_dataset_name}, dataset_name=ds_name, extra_fields=extra_fields, replacement_text=replacement_text )
                            etl_ref_id = resource_ref_id_base+"://"+etl_dataset_id+"~"+etl_dataset_type

                            dataset_lineage = {
                                            'src Reference ID': ds_id, 
                                            'src HierarchicalPath': ds_hp, 
                                            'src Name': ds_name,
                                            'tgt Reference ID': etl_ref_id, 
                                            'tgt HierarchicalPath': etl_dataset_path, 
                                            'tgt Name': etl_dataset_name,
                                            'Association': 'core.DataSetDataFlow',
                                            'Match Score': score
                                            }
                            df = pandas.DataFrame(dataset_lineage, index=[0])
                            final_dataframe = append_or_create(final_dataframe,df)

                            dataset_lineage = {
                                            'src Reference ID': etl_ref_id, 
                                            'src HierarchicalPath': etl_dataset_path, 
                                            'src Name': etl_dataset_name,
                                            'tgt Reference ID': dt_id, 
                                            'tgt HierarchicalPath': dt_hp, 
                                            'tgt Name': dt_name,
                                            'Association': 'core.DataSetDataFlow',
                                            'Match Score': score
                                            }
                            df = pandas.DataFrame(dataset_lineage, index=[0])
                            final_dataframe = append_or_create(final_dataframe,df)

                        else:
                            dataset_lineage = {
                                            'src Reference ID': ds_id, 
                                            'src HierarchicalPath': ds_hp, 
                                            'src Name': ds_name,
                                            'tgt Reference ID': dt_id, 
                                            'tgt HierarchicalPath': dt_hp, 
                                            'tgt Name': dt_name,
                                            'Association': 'core.DataSetDataFlow',
                                            'Match Score': score
                                            }
                            df = pandas.DataFrame(dataset_lineage, index=[0])
                            final_dataframe = append_or_create(final_dataframe,df)

                source_element_match = df_full_export_Elements[
                    df_full_export_Elements[element_hierarchical_column].str.startswith(f"{ds_hp}/", na=False) &
                    df_full_export_Elements[element_name_column].str.contains('^'+this_Source_Element_regex+'$', na=False, regex=True, flags=re.IGNORECASE)
                ]


                for index, s_element in source_element_match.iterrows():
                    es_hp = s_element[element_hierarchical_column]
                    es_id = s_element[element_refid_column]
                    es_name = s_element[element_name_column]

                    for index, target in target_dataset_match.iterrows():
                        dt_hp = target[dataset_hierarchical_column]
                        dt_id = target[dataset_refid_column]
                        dt_name = target[dataset_name_column]                        
                        target_dataset_score = difflib.SequenceMatcher(None, ds_name, dt_name).ratio()
                        if target_dataset_score >= float(this_Dataset_Match_Score):


                            updated_regex =  this_Target_Element_regex.replace('{name}',es_name).replace('{s_dataset}',ds_name).replace('{t_dataset}',dt_name).replace('{s_element}',es_name)
                            target_element_match = df_full_export_Elements[
                                df_full_export_Elements[element_hierarchical_column].str.startswith(f"{target[element_hierarchical_column]}/", na=False) &
                                df_full_export_Elements[element_name_column].str.contains('^'+updated_regex+'$', na=False, regex=True, flags=re.IGNORECASE)
                            ] 
                            for index, target in target_element_match.iterrows():
                                et_hp = target[element_hierarchical_column]
                                et_id = target[element_refid_column]
                                et_name = target[element_name_column]

                                score = difflib.SequenceMatcher(None, es_name, et_name).ratio()
                                if score >= float(this_Element_Match_Score):
                                    if len(base_type) > 1 and len(base_id) > 1:
                                        replacement_text = {'{name}': es_name, '{s_dataset}': ds_name, '{t_dataset}': dt_name, '{s_element}': es_name, '{t_element}': et_name}
                                        etl_element_name = this_ETL_Element_Name.replace('{name}',es_name).replace('{s_dataset}',ds_name).replace('{t_dataset}',dt_name).replace('{s_element}',es_name).replace('{t_element}',et_name)
                                        etl_element_path, etl_element_type, etl_element_id = generate_additional_class(this_ETL_Resource_Name,etl_dataset_path, etl_dataset_type, etl_dataset_id, this_ETL_Element_Type, {"name": etl_element_name}, extra_fields=extra_fields, element_name=es_name, replacement_text=replacement_text)
                                        etl_element_ref_id = resource_ref_id_base+"://"+etl_element_id+"~"+etl_element_type

                                        element_lineage = {
                                                        'src Reference ID': es_id, 
                                                        'src HierarchicalPath': es_hp, 
                                                        'src Name': es_name,
                                                        'tgt Reference ID': etl_element_ref_id, 
                                                        'tgt HierarchicalPath': etl_element_path, 
                                                        'tgt Name': etl_element_name,
                                                        'Association': 'core.DirectionalDataFlow',
                                                        'Match Score': score
                                                        }
                                        df = pandas.DataFrame(element_lineage, index=[0])   
                                        final_dataframe = append_or_create(final_dataframe,df)  

                                        element_lineage = {
                                                        'src Reference ID': etl_element_ref_id, 
                                                        'src HierarchicalPath': etl_element_path, 
                                                        'src Name': etl_element_name,
                                                        'tgt Reference ID': et_id, 
                                                        'tgt HierarchicalPath': et_hp, 
                                                        'tgt Name': et_name,
                                                        'Association': 'core.DirectionalDataFlow',
                                                        'Match Score': score
                                                        }
                                        df = pandas.DataFrame(element_lineage, index=[0])   
                                        final_dataframe = append_or_create(final_dataframe,df)                                          
                                    else:
                                        element_lineage = {
                                                        'src Reference ID': es_id, 
                                                        'src HierarchicalPath': es_hp, 
                                                        'src Name': es_name,
                                                        'tgt Reference ID': et_id, 
                                                        'tgt HierarchicalPath': et_hp, 
                                                        'tgt Name': et_name,
                                                        'Association': 'core.DirectionalDataFlow',
                                                        'Match Score': score
                                                        }
                                        df = pandas.DataFrame(element_lineage, index=[0])   
                                        final_dataframe = append_or_create(final_dataframe,df)                                         


    created_resources = []
    if resource_classes is not None and resource_classes:
        for resource_name, resource_dict in resource_classes.items():
            this_resource_name, this_zip_file = write_resource_to_zip(resource_name, resource_dict)
            this_item = {"resource_name": this_resource_name, "file_location": this_zip_file}
            created_resources.append(this_item)

    if final_dataframe is None or final_dataframe.empty:
        print("No Matches")
    else:
        for index, row in final_dataframe.iterrows():
            print(f"{row['src HierarchicalPath']} -> {row['tgt HierarchicalPath']} ({row['Match Score']})")
        final_dataframe.drop_duplicates(inplace=True)
        ## write_csv(final_dataframe)
        this_resource_name, this_zip_file = write_links_zip(final_dataframe, resource_name=default_lineage_resource_name)
        this_item = {"resource_name": this_resource_name, "file_location": this_zip_file}
        created_resources.append(this_item)        

    if use_api:
        global default_resource_type
        for r in created_resources:
            this_name = r['resource_name']
            this_path = r['file_location']            
            print(f'INFO: Preparing to create/update "{this_name}" using "{this_path}"')

        if pause_before_loading:
            programPause = input("Press the <ENTER> key create resources...")

        ## First Create all the resources        
        for r in created_resources:
            this_name = r['resource_name']
            this_path = r['file_location']
            create_custom_resource(name=this_name, type=default_resource_type, zipfile_path=this_path)

        ## Now run the scan for all the resources
        for r in created_resources:
            this_name = r['resource_name']
            this_path = r['file_location']
            run_metadata_scan(name=this_name)
        

    



def upload_file_for_resource(origin="", name="", zipfile_path=""):
    getCredentials()
    login()
    # Extract the filename from the file path
    filename = os.path.basename(zipfile_path)

    # Open the file in binary mode
    with open(zipfile_path, 'rb') as f:
        # Define a custom boundary for the multipart form
        boundary = '----WebKitFormBoundaryFenQla42OgEFacoK'

        # Create a MultipartEncoder with the custom boundary
        encoder = MultipartEncoder(
            fields={
                'file': (filename, f, 'application/x-zip-compressed')
            },
            boundary=boundary
        )

        this_header = headers_bearer.copy()
        this_header['Content-Type'] = 'multipart/form-data; boundary=----WebKitFormBoundaryFenQla42OgEFacoK'
        Result = requests.post(cdgc_url+"/ccgf-metadata-staging/api/v1/staging/files?serviceFunction=catalog-source-staging-producer", headers=this_header, data=encoder)
        return filename, Result.text

def modify_custom_resource_file_details(id="", name="", zipfile_path=""):
    getCredentials()
    login()

    this_name = name
    if len(this_name) < 1:
        dataframe_result = df_full_export_Resources.loc[df_full_export_Resources['Origin'] == id, 'Name']
        if not dataframe_result.empty:
            this_name = dataframe_result.iloc[0]     

    new_filename, response_text = upload_file_for_resource(zipfile_path=zipfile_path)
    file_upload_response = json.loads(response_text)
    new_file_location = file_upload_response['filePath']


    this_id = id
    if len(this_id) < 1:
        dataframe_result = df_full_export_Resources.loc[df_full_export_Resources['Name'] == name, 'Origin']
        if not dataframe_result.empty:
            this_id = dataframe_result.iloc[0]    

    # Construct the GET URL
    get_url = f"{cdgc_url}/ccgf-catalog-source-management/api/v1/datasources/{this_id}"

    this_header = headers_bearer.copy()    
    
    # Make the GET request to fetch current details
    response = requests.get(get_url, headers=this_header)
    
    if not response.status_code in [200, 201, 202, 204]:
        print(f"ERROR: Failed to get data: {response.status_code}, {response.text}")
        return

    # Parse the JSON response
    datasource = response.json()
        
    # Locate and modify the "File Details" in "typeOptions"
    for option_group in datasource.get("typeOptions", {}).get("configurationProperties", []):
        if option_group.get("optionGroupName") == "Custom OptionGroup":
            for config_option in option_group.get("configOptions", []):
                if config_option.get("key") == "File Details":
                    # Modify the file details to new values
                    config_option["values"] = [new_filename, new_file_location]
    
    # Construct the PUT URL (or POST if creating a new record)
    put_url = get_url
    datasource_string = json.dumps(datasource, indent=4)

    # Make the PUT request to update the details
    put_response = requests.put(put_url, headers=this_header, data=datasource_string)
    
    if put_response.status_code in [200, 201, 202, 204]:
        print(f"INFO: Successfully updated resource \"{this_name}\"")
    else:
        print(f"ERROR: Failed to update resource \"{this_name}\": {put_response.status_code}, {put_response.text}")

def create_custom_resource(name="", type=default_resource_type, zipfile_path=""):
    global df_full_export_Resources, df_full_export_Datasets, df_full_export_Elements
    getCredentials()
    login()

    this_zipfile_path = zipfile_path
    ## If you don't specify a file path, then I'll create a dummy file
    if len(this_zipfile_path) < 2:
        this_zipfile_path = directory_to_write_resource_files+"/dummy.zip"
        ## Only if the dummy file doesn't exist already
        if not os.path.exists(this_zipfile_path):
            if not os.path.exists(directory_to_write_resource_files):
                os.makedirs(directory_to_write_resource_files)             
            with zipfile.ZipFile(this_zipfile_path, 'w') as dummy_zip:
                # Create dummy file content
                file_name = f'dummy_file.txt'
                file_content = f'This is the content of {file_name}'
                
                # Write content to the zip file
                dummy_zip.writestr(file_name, file_content) 

    this_existing_id = ""
    dataframe_result = df_full_export_Resources.loc[df_full_export_Resources['Name'] == name, 'Origin']
    if not dataframe_result.empty:
        this_existing_id = dataframe_result.iloc[0]

    if len(this_existing_id) > 1:
        modify_custom_resource_file_details(id=this_existing_id, zipfile_path=this_zipfile_path)
    else:
        new_filename, response_text = upload_file_for_resource(zipfile_path=this_zipfile_path)
        file_upload_response = json.loads(response_text)
        new_file_location = file_upload_response['filePath']

        this_header = headers_bearer.copy()
        
        ## payload = '''{"$id":"ds-1fl","name":"'''+name+'''","type":"'''+type+'''","custom":true,"typeOptions":{"configurationProperties":[{"optionGroupName":"Custom OptionGroup","configOptions":[{"key":"Metadata Source Type","values":["CSV Files"]},{"key":"Source Type","values":["Upload"]},{"key":"File Details","values":["'''+new_filename+'''","'''+new_file_location+'''"]}]}]},"capabilities":[{"capabilityName":"Metadata Extraction","configurationProperties":[{"optionGroupName":"Metadata Extraction OptionGroup","configOptions":[{"key":"Execution Mode","values":["Online"]},{"key":"Metadata Change Option","values":["Delete"]}]}]}],"globalConfigOptions":[{"globalConfigurationName":"Configuration","configurationProperties":[{"optionGroupName":"Metadata and Data Filters","configOptions":[{"key":"Filter Options","values":["No Filters"]}]}]},{"globalConfigurationName":"Execution","configurationProperties":[{"optionGroupName":"Runtime Settings","configOptions":[{"key":"Runtime Environment","values":[]}]},{"optionGroupName":"Metadata Sync Settings","configOptions":[{"key":"Mode of Execution","values":["Full Sync"]}]},{"optionGroupName":"Metadata Schedule Settings","configOptions":[{"key":"Schedules","values":[],"additionalMetadata":{}}]}]}],"typeCapabilities":[{"capabilityName":"Metadata Extraction","configurationProperties":[{"optionGroupName":"Scanner Configuration Options","configOptions":[]},{"optionGroupName":"Additional settings","configOptions":[]}]}]}'''
        ## Updated payload for Nov 2024 release
        payload = '''{"name":"'''+name+'''","type":"'''+type+'''","custom":true,"typeOptions":{"configurationProperties":[{"optionGroupName":"Custom OptionGroup","configOptions":[{"key":"Metadata Source Type","values":["CSV Files"]},{"key":"Source Type","values":["Upload"]},{"key":"File Details","values":["'''+new_filename+'''","'''+new_file_location+'''"]}]}]},"capabilities":[{"capabilityName":"Metadata Extraction","configurationProperties":[{"optionGroupName":"Metadata Extraction OptionGroup","configOptions":[{"key":"Execution Mode","values":["Online"]},{"key":"Type of scan","values":["Full"]},{"key":"Metadata Change Option","values":["Delete"]}]}]}],"globalConfigOptions":[{"globalConfigurationName":"Configuration","configurationProperties":[{"optionGroupName":"Metadata and Data Filters","configOptions":[{"key":"Filter Options","values":["No Filters"]}]}]},{"globalConfigurationName":"Execution","configurationProperties":[{"optionGroupName":"Runtime Settings","configOptions":[{"key":"Runtime Environment","values":[]}]},{"optionGroupName":"Metadata Sync Settings","configOptions":[{"key":"Mode of Execution","values":["Full Sync"]}]},{"optionGroupName":"Metadata Schedule Settings","configOptions":[{"key":"Schedules","values":[],"additionalMetadata":{}}]}]}],"typeCapabilities":[{"capabilityName":"Metadata Extraction","configurationProperties":[{"optionGroupName":"Scanner Configuration Options","configOptions":[]},{"optionGroupName":"Additional settings","configOptions":[]}]}]}'''
        post_url = f"{cdgc_url}/ccgf-catalog-source-management/api/v1/datasources"
        response = requests.post(post_url, headers=this_header, data=payload)

        if response.status_code in [200, 201, 202, 204]:
            print(f"INFO: Successfully created resource \"{name}\"")
        else:
            print(f"ERROR: Failed to create resource \"{name}\": {response.status_code}, {response.text}")
            finish_up()

        resource_file_name = ""
        for search in searches:
            if search.get('search_name') == 'All Resources':
                resource_file_name = search.get('save_filename')
        resource_file_path = extracts_folder+'/'+resource_file_name

        assets_file_name = ""
        for search in searches:
            if search.get('search_name') == 'Assets in a Resource':
                assets_file_name = search.get('save_filename')
        assets_file_path = extracts_folder+'/'+assets_file_name


        getObjectsFromApi()

        
        df_full_export_Resources = pandas.DataFrame()
        df_full_export_Datasets = pandas.DataFrame()
        df_full_export_Elements = pandas.DataFrame()
        df_full_export_Resources, df_full_export_Datasets, df_full_export_Elements = read_json_files_into_dataframes(assets_file_path, resource_file_path)

def get_custom_resource_type():
    getCredentials()
    login()

    this_datatype = ""
    this_header = headers_bearer.copy()
    get_url = f"{cdgc_url}/ccgf-catalog-source-management/api/v1/datasourceTypes?custom=true&offset=0&limit=10000"
    get_response = requests.get(get_url, headers=this_header)

    if get_response.status_code in [200, 201, 202, 204]:
        json_response = get_response.json()
        datasource_types = json_response.get("datasourceTypes", [])
        if datasource_types:  # Check if the array exists and is not empty
            this_datatype = datasource_types[0].get("name", "")  # Return the 'name' of the first item

    if len(this_datatype) < 1:
        
        ## There are no custom datatypes. We'll need to create one.
        post_url = f"{cdgc_url}/ccgf-catalog-source-management/api/v1/datasourceTypes"
        payload = '{"id":"","name":"Custom Datatype","description":"","category":"Custom"}'
        post_response = requests.post(get_url, headers=this_header, data=payload)
        
        if post_response.status_code in [200, 201, 202, 204]:
            ## Let's check again.
            get_url = f"{cdgc_url}/ccgf-catalog-source-management/api/v1/datasourceTypes?custom=true&offset=0&limit=10000"
            get_response = requests.get(get_url, headers=this_header)
            

            if get_response.status_code in [200, 201, 202, 204]:
                json_response = get_response.json()
                datasource_types = json_response.get("datasourceTypes", [])
                if datasource_types:  # Check if the array exists and is not empty
                    this_datatype = datasource_types[0].get("name", "")  # Return the 'name' of the first item  
        else:
            print(f"ERROR: Unable to create custom Datatype: {post_response.text}")          
            finish_up()

    if len(this_datatype) < 1:
        print(f"ERROR: Cannot discover a custom datatype or create one. ")          
        finish_up()
    
    return this_datatype

def print_message_loop(message, state=None, is_first_message=False, is_final_message=False):
    """
    Prints messages, appending dots for repeated messages, or resets for new messages.
    """
    if state is None:
        state = {"last_message": None, "repeat_counter": 0}

    if message == state["last_message"]:
        if state["repeat_counter"] < 30:
            state["repeat_counter"] += 1
            print('.', end='', flush=True)
        else:
            state["repeat_counter"] = 1
            print(f"\n{message}", end='', flush=True)
    else:
        if not is_first_message:
            print()  # Print a new line for a new message
        state["repeat_counter"] = 0
        print(message, end='', flush=True)

    state["last_message"] = message

    if is_final_message:
        print()  # Print a final newline


def monitor_job(result_response):
    """
    Monitors a job by polling its status every 10 seconds until completion or failure.
    """
    try:
        result_response_json = result_response.json()
        job_id = result_response_json.get('jobId')
        tracking_uri = result_response_json.get('trackingURI')
        job_status = result_response_json.get('status')

        state = {"last_message": None, "repeat_counter": 0}
        job_loop = True

        while job_loop:
            time.sleep(10)
            get_url = f"{idmc_url}{tracking_uri}"
            this_header = headers_bearer.copy()
            response = requests.get(get_url, headers=this_header)

            if response.status_code in [200, 201, 202, 204]:
                result_json = response.json()
                job_status = result_json.get('status')

                # Check terminal states
                if any(term in job_status for term in ["COMPLETED", "ERROR", "CANCELLED", "FAILED"]):
                    print_message_loop(f"    {job_status}", state=state, is_final_message=True)
                    job_loop = False
                else:
                    print_message_loop(f"    {job_status}", state=state)
            else:
                print(f"\nERROR: with tracking the scan: {response.text}")
                job_loop = False  # Exit the loop on failure
    except Exception as e:
        print(f"\nERROR: with tracking the scan: {e}")

def run_metadata_scan(id="", name=""):
    getCredentials()
    login()

    this_id = id

    global df_full_export_Resources
    if len(this_id) < 1:
        ## print(f"DEBUGSCOTT")
        ## print(df_full_export_Resources)
        dataframe_result = df_full_export_Resources.loc[df_full_export_Resources['Name'] == name, 'Origin']
        if not dataframe_result.empty:
            this_id = dataframe_result.iloc[0]  

    post_url = f"{idmc_url}/data360/executable/v1/catalogsource/{this_id}"
    this_header = headers_bearer.copy()
    payload = '{"capabilityNames": ["Metadata Extraction"]}'
    post_response = requests.post(post_url, headers=this_header, data=payload)
    

    if post_response.status_code in [200, 201, 202, 204]:
        print(f"INFO: Successfully started scan job for \"{name}\"")
        monitor_job(post_response)
    else:
        print(f"ERROR: Unable to start scan job for \"{name}\": {post_response.text}")
        finish_up()

def check_and_create_dummy_resources():
    global default_resource_type
    with open(config_file_path) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            this_ETL_Resource_Name = row.get('ETL Resource Name', "")
            if len(this_ETL_Resource_Name) > 1:
                final_ETL_Resource_Name = this_ETL_Resource_Name.split("/")[0]
                dataframe_result = df_full_export_Resources.loc[df_full_export_Resources['Name'] == final_ETL_Resource_Name, 'Origin']
                if dataframe_result.empty:
                    ## It looks like this resource doesn't exist. Let's create a custom resource, with a dummy zip file.
                    ## This will ensure that it has an ID, and we'll be able to build lineage using it.
                    create_custom_resource(name=final_ETL_Resource_Name, type=default_resource_type)



def main():
    
    global default_resource_type, df_full_export_Resources, df_full_export_Datasets, df_full_export_Elements, default_config_file, config_file_path

    parse_parameters()

    if len(config_file) > 2:
       default_config_file = config_file
       config_file_path = script_location+"/"+default_config_file

    if not os.path.isfile(config_file_path):
        default_config_file = select_recent_csv(script_location)
        config_file_path = default_config_file


    if use_api:
        print(f"INFO: Downloading initial data")
        set_lineage_resource_name(filepath=config_file_path, set_name=True)
        if len(default_resource_type) < 1:
            default_resource_type = get_custom_resource_type()
        getObjectsFromApi()

        resources_file = next( (extracts_folder + "/" + s['save_filename'] for s in searches if s['search_name'] == 'All Resources'),  None)
        assets_file = next( (extracts_folder + "/" + s['save_filename'] for s in searches if s['search_name'] == 'Assets in a Resource'),  None)

        df_full_export_Resources, df_full_export_Datasets, df_full_export_Elements = read_json_files_into_dataframes(assets_file, resources_file)
        check_and_create_dummy_resources()
    else:
        export_file = find_latest_xlsx(directory_with_assets_export)

        df_full_export_Elements = None
        df_full_export_Datasets = None
        df_full_export_Resources = None
        try:
            _, file_extension = os.path.splitext(export_file)
            if file_extension == '.zip':
                df_full_export_Elements = extract_dataframe_from_zip(export_file, technical_data_element_sheet)
                df_full_export_Datasets = extract_dataframe_from_zip(export_file, technical_data_set_sheet)
                df_full_export_Resources = extract_dataframe_from_zip(export_file, catalog_source_sheet)

            elif file_extension == '.xlsx':
                df_full_export_Elements = pandas.read_excel(export_file, sheet_name=technical_data_element_sheet)
                df_full_export_Datasets = pandas.read_excel(export_file, sheet_name=technical_data_set_sheet)
                df_full_export_Resources = pandas.read_excel(export_file, sheet_name=catalog_source_sheet)
        except Exception as e:
            print(f"Error reading the Asset Export Excel or Zip file")
            print(f"   Please perform a search for \"resources\" in CDGC, and export including children.")
            print(f"   Place the resulting xlsx file or zip file in: {directory_with_assets_export}")
            finish_up()

        required_columns_datasets = [ dataset_name_column, dataset_refid_column, dataset_hierarchical_column]
        if not all(column in df_full_export_Datasets.columns for column in required_columns_datasets):
            print(f"The required columns {required_columns_datasets} are not present in {export_file}| Try running a search for \"Resources\" in CDGC, and exporting, including children")
            finish_up()

        required_columns_elements = [element_parent_column, element_name_column, element_refid_column, element_hierarchical_column]
        if not all(column in df_full_export_Elements.columns for column in required_columns_elements):
            print(f"The required columns {required_columns_elements} are not present in {export_file} | Try running a search for \"Resources\" in CDGC, and exporting, including children")
            finish_up()

    load_metamodels()
    
    ##### Testing create_custom_resource(name="Testing Resource1", type="Custom Source")
    ##### Testing modify_custom_resource_file_details(id='aaa63390-dad9-3238-ae4a-6ff3f2349dc9', zipfile_path='./resources/Mass Ingestion_20241115111349.zip')

    
    readConfigAndStart(config_file_path)
    finish_up()    

if __name__ == "__main__":
    main()

