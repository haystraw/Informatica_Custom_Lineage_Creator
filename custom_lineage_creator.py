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
warnings.simplefilter("ignore")

'''
pip install pandas datetime openpyxl
'''

version = 20241104
pause_when_done = True

error_quietly = False

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

default_config_file = "config.csv"
if len(sys.argv) > 1:
    default_config_file = sys.argv[1]

script_location = os.path.dirname(os.path.abspath(__file__))
config_file = script_location+"/"+default_config_file
directory_with_assets_export = script_location+""
directory_to_write_links_file = script_location+"/links"
directory_with_templates = script_location+"/templates"
directory_to_write_resource_files = script_location+"/resources"





timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')


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

def add_df_to_resource_classes(resource_name, class_name, new_row):
    global resource_classes

    # Create a DataFrame from the new_row
    new_df = pandas.DataFrame([new_row])
    
    # Use setdefault to initialize the resource_classes if it doesn't exist
    resource_classes.setdefault(resource_name, {})
    
    # Check if the class_name DataFrame exists; if not, initialize it
    if class_name not in resource_classes[resource_name]:
        resource_classes[resource_name][class_name] = pandas.DataFrame()  # Initialize as empty DataFrame

    # Concatenate the existing DataFrame with the new row
    resource_classes[resource_name][class_name] = pandas.concat(
        [resource_classes[resource_name][class_name], new_df],
        ignore_index=True
    )

def generate_additional_class(resource_name, parent_path, parent_type, parent_id, current_type, attribute_dict, dataset_name="XXX", extra_fields={}):
    global resource_links
    ## Fatching the name from the provided 
    this_etl_name = attribute_dict["name"].replace('{name}',dataset_name)
    this_etl_path = parent_path+"/"+this_etl_name
    this_etl_id_work = parent_id+"_"+this_etl_name
    this_etl_id = re.sub(r'[^a-zA-Z0-9]', '_', this_etl_id_work).lower()
    this_association = find_association(parent_type, current_type)
    new_assocation_row = {"Source": parent_id, "Target": this_etl_id , "Association": this_association}
    ## new_assocation_df = pandas.concat([resource_links, pandas.DataFrame([new_assocation_row])], ignore_index=True) 
    ## resource_links = new_assocation_df
    add_df_to_resource_classes(resource_name, "links", new_assocation_row)

    df = load_template(resource_name, current_type)
    new_row = {"core.name": this_etl_name, "core.externalId": this_etl_id}


    add_df_to_resource_classes(resource_name, current_type, new_row)
    ## test new_df = pandas.concat([df, pandas.DataFrame([new_row])], ignore_index=True)       
    ## test resource_classes[resource_name][current_type]  = new_df

    return this_etl_path, current_type, this_etl_id


def generate_resource_path(resource_name,etl_path_string, etl_types_string, dataset_name="XXX"):
    global resource_links

    formatted_etl_path_string = etl_path_string.replace('{name}',dataset_name)
    etl_names = formatted_etl_path_string.split('/')
    etl_types = etl_types_string.split('/')

    last_type = ""
    last_id = ""
    for index, value in enumerate(etl_names):
        this_etl_name = value
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
            add_df_to_resource_classes(resource_name, "links", new_assocation_row)
        else:
            this_etl_id_work = this_etl_name
            this_etl_id = re.sub(r'[^a-zA-Z0-9]', '_', this_etl_id_work).lower()
            new_assocation_row = {"Source": "$resource", "Target": this_etl_id , "Association": "core.ResourceParentChild"}
            add_df_to_resource_classes(resource_name, "links", new_assocation_row)
      

        df = load_template(resource_name,this_etl_type)
        new_row = {"core.name": value, "core.externalId": this_etl_id}
        add_df_to_resource_classes(resource_name, this_etl_type, new_row)

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

            if (is_class_or_superclass(association_from, from_class, class_hierarchy) and
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
    exit


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

def write_csv(df):
    # Get the current timestamp
    file_name = directory_to_write_links_file+'/links_'+timestamp+'.csv'
    # Open the CSV file in append mode
    with open(file_name, mode='a', newline='') as file:
        writer = csv.writer(file)
        
        # Write header if the file is empty or doesn't exist
        if file.tell() == 0:
            writer.writerow(['Source','Target','Association'])
        
        # Write each name with the current timestamp
        print(f"Writing to {file_name}")
        for index, row in final_dataframe.iterrows():
            writer.writerow([row['src Reference ID'], row['tgt Reference ID'], row['Association']])

def write_links_zip(df):
    # Get the current timestamp
    zip_file_name = os.path.join(directory_to_write_links_file, f'lineage_{timestamp}.zip')
    
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

    except Exception as e:
        print(f"An error occurred while writing to the zip file: {e}")


def write_resource_to_zip(resource_name, df_dict):
    # Create a zip file and write each DataFrame to separate CSV files

    zip_file_path = directory_to_write_resource_files+"/"+resource_name+"_"+timestamp+".zip"
    try:
        with zipfile.ZipFile(zip_file_path, mode='w') as zf:
            for name, df in df_dict.items():
                with io.StringIO() as csv_buffer:
                    # Write the DataFrame to the buffer
                    df_cleaned = df.loc[:, ~df.columns.str.contains('^Unnamed')]  # Remove any "Unnamed" columns if they exist
                    df_cleaned.to_csv(csv_buffer, index=False, header=True)
                    csv_buffer.seek(0)  # Reset the buffer to the beginning
                    
                    # Write the buffer content to the zip file as {name}.csv
                    zf.writestr(f"{name}.csv", csv_buffer.getvalue())
        
        print(f"Successfully written {resource_name} to {zip_file_path}")

    except Exception as e:
        print(f"An error occurred while writing to the zip file: {e}")

def write_dataframe_to_csv(df, file_name):
    df_cleaned = df.loc[:, ~df.columns.str.contains('^Unnamed')]  # Remove any "Unnamed" columns if they exist
    df_cleaned.to_csv(file_name, index=False) 


def find_latest_xlsx():
    xlsx_files = glob.glob("*.xlsx")
    
    if not xlsx_files:
        print("No XLSX files found.")
        return None
    
    latest_file = max(xlsx_files, key=os.path.getmtime)
    return latest_file

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
    filtered_dict = {col: row[col] for col in row.index if col not in excluded_headers}

    return filtered_dict


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
            this_ETL_Resource_Name = row['ETL Resource Name']
            this_ETL_Path = row['ETL Path']	
            this_ETL_Path_Types = row['ETL Path Types']	
            this_ETL_Dataset_Type = row['ETL Dataset Type']	
            this_ETL_Dataset_Name = row['ETL Dataset Name']	
            this_ETL_Element_Type = row['ETL Element Type']
            this_ETL_Element_Name = row['ETL Element Name']

            ## extra_fields = create_extra_fields(row)

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

                target_dataset_match = df_full_export_Datasets[
                    df_full_export_Datasets[dataset_hierarchical_column].str.startswith(f"{this_Target_Resource}/", na=False) &
                    df_full_export_Datasets[dataset_name_column].str.contains('^'+this_Target_Dataset_regex.replace('{name}',ds_name)+'$', na=False, regex=True, flags=re.IGNORECASE)
                ]

                for index, target in target_dataset_match.iterrows():
                    dt_hp = target[dataset_hierarchical_column]
                    dt_id = target[dataset_refid_column]
                    dt_name = target[dataset_name_column]
                    
                    score = difflib.SequenceMatcher(None, ds_name, dt_name).ratio()
                    if score >= float(this_Dataset_Match_Score):

                        if len(this_ETL_Path) > 1 and len(this_ETL_Path_Types) > 1:
                            if not generated_base:
                                base_path, base_type, base_id = generate_resource_path(this_ETL_Resource_Name, this_ETL_Path, this_ETL_Path_Types, dataset_name=ds_name)
                                generated_base = True

                            etl_dataset_name = this_ETL_Dataset_Name.replace('{name}',ds_name)
                            etl_dataset_path, etl_dataset_type, etl_dataset_id = generate_additional_class(this_ETL_Resource_Name,base_path, base_type, base_id, this_ETL_Dataset_Type, {"name": etl_dataset_name}, dataset_name=ds_name)
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


                            target_element_match = df_full_export_Elements[
                                df_full_export_Elements[element_hierarchical_column].str.startswith(f"{target[element_hierarchical_column]}/", na=False) &
                                df_full_export_Elements[element_name_column].str.contains('^'+this_Target_Element_regex.replace('{name}',es_name)+'$', na=False, regex=True, flags=re.IGNORECASE)
                            ] 
                            for index, target in target_element_match.iterrows():
                                et_hp = target[element_hierarchical_column]
                                et_id = target[element_refid_column]
                                et_name = target[element_name_column]

                                score = difflib.SequenceMatcher(None, es_name, et_name).ratio()
                                if score >= float(this_Element_Match_Score):
                                    if len(base_type) > 1 and len(base_id) > 1:
                                        etl_element_name = this_ETL_Element_Name.replace('{name}',es_name)
                                        etl_element_path, etl_element_type, etl_element_id = generate_additional_class(this_ETL_Resource_Name,etl_dataset_path, etl_dataset_type, etl_dataset_id, this_ETL_Element_Type, {"name": etl_element_name})
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


    if final_dataframe is None or final_dataframe.empty:
        print("No Matches")
    else:
        for index, row in final_dataframe.iterrows():
            print(f"{row['src HierarchicalPath']} -> {row['tgt HierarchicalPath']} ({row['Match Score']})")

        write_csv(final_dataframe)
        write_links_zip(final_dataframe)

    if resource_classes is not None and resource_classes:
        for resource_name, resource_dict in resource_classes.items():
            write_resource_to_zip(resource_name, resource_dict)

load_metamodels()


readConfigAndStart(config_file)
finish_up()


'''
df = load_template('com.infa.odin.models.IICS.V2.Project')
new_row = {"core.name": "Test Name", "core.externalId": "test_name"}
df = pandas.concat([df, pandas.DataFrame([new_row])], ignore_index=True)

for key, df in resource_classes.items():
    print(df)
'''

'''
load_metamodels()
print(f"Done loading...")

print(find_association("com.infa.odin.models.IICS.V2.Project", "com.infa.odin.models.IICS.V2.Folder"))
print(find_association("com.infa.odin.models.IICS.V2.Folder", "com.infa.odin.models.IICS.V2.MappingTask"))
print(find_association("com.infa.odin.models.IICS.V2.MappingTask", "com.infa.odin.models.IICS.V2.MappingTaskInstance"))
print(find_association("com.infa.odin.models.IICS.V2.MappingTaskInstance", "com.infa.odin.models.IICS.V2.Calculation"))

'''

'''
base_type, base_id = generate_resource_path("Production/Replication/oracle_to_sqlserver", "com.infa.odin.models.IICS.V2.Project/com.infa.odin.models.IICS.V2.Folder/com.infa.odin.models.IICS.V2.MappingTask")
dataset_type, dataset_id = generate_additional_class(base_type, base_id, "com.infa.odin.models.IICS.V2.MappingTaskInstance", {"name": "oracle_to_sqlserver (1)"})
generate_additional_class(dataset_type, dataset_id, "com.infa.odin.models.IICS.V2.Calculation", {"name": "element 1"})
generate_additional_class(dataset_type, dataset_id, "com.infa.odin.models.IICS.V2.Calculation", {"name": "element 2"})
generate_additional_class(dataset_type, dataset_id, "com.infa.odin.models.IICS.V2.Calculation", {"name": "element 3"})

for key, df in resource_classes.items():
    write_dataframe_to_csv(df, directory_to_write_resource_files+"/"+key+".csv")
    print(df)

print(resource_links)    
write_dataframe_to_csv(resource_links, directory_to_write_resource_files+"/links.csv")

'''




