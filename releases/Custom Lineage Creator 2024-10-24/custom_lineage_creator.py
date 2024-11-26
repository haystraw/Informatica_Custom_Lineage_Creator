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
warnings.simplefilter("ignore")

version = 20241024
pause_when_done = True

technical_data_element_sheet = "Technical Data element"
element_name_column = "Name"
element_parent_column = "Parent: Technical Data Set"
element_refid_column = "Reference ID"
element_hierarchical_column = "HierarchicalPath"


technical_data_set_sheet = "Technical Data Set"
dataset_name_column = "Name"
dataset_refid_column = "Reference ID"
dataset_hierarchical_column = "HierarchicalPath"

script_location = os.path.dirname(os.path.abspath(__file__))
config_file = script_location+"/config.csv"
directory_with_assets_export = script_location+""
directory_to_write_links_file = script_location+"/links"








def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        # Call the default hook for keyboard interrupts
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    # Here you can log the exception or handle it as you want
    ## print(f"An error occurred: {exc_value}")
    
sys.excepthook = handle_exception    


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
        print("No 'Technical Data Set' sheets found in any files.")
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

global df_full_export_Elements
global df_full_export_Datasets
try:
    _, file_extension = os.path.splitext(export_file)
    if file_extension == '.zip':
        df_full_export_Elements = extract_dataframe_from_zip(export_file, technical_data_element_sheet)
        df_full_export_Datasets = extract_dataframe_from_zip(export_file, technical_data_set_sheet)
    elif file_extension == '.xlsx':
        df_full_export_Elements = pandas.read_excel(export_file, sheet_name=technical_data_element_sheet)
        df_full_export_Datasets = pandas.read_excel(export_file, sheet_name=technical_data_set_sheet)
except Exception as e:
    print(f"Error reading the Asset Export Excel or Zip file")
    print(f"   Please perform a search for \"resources\" in CDGC, and export.")
    print(f"   Place the resulting xlsx file or zip file in: {directory_with_assets_export}")
    finish_up()

required_columns_datasets = [ dataset_name_column, dataset_refid_column, dataset_hierarchical_column]
if not all(column in df_full_export_Datasets.columns for column in required_columns_datasets):
    print(f"The required columns {required_columns_datasets} are not present in {export_file}")
    finish_up()

required_columns_elements = [element_parent_column, element_name_column, element_refid_column, element_hierarchical_column]
if not all(column in df_full_export_Elements.columns for column in required_columns_elements):
    print(f"The required columns {required_columns_elements} are not present in {export_file}")
    finish_up()

def write_csv(df):
    # Get the current timestamp
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
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
                                    dataset_lineage = {
                                                    'src Reference ID': es_id, 
                                                    'src HierarchicalPath': es_hp, 
                                                    'src Name': es_name,
                                                    'tgt Reference ID': et_id, 
                                                    'tgt HierarchicalPath': et_hp, 
                                                    'tgt Name': et_name,
                                                    'Association': 'core.DirectionalDataFlow',
                                                    'Match Score': score
                                                    }
                                    df = pandas.DataFrame(dataset_lineage, index=[0])   
                                    final_dataframe = append_or_create(final_dataframe,df)                     


    if final_dataframe is None or final_dataframe.empty:
        print("No Matches")
    else:
        for index, row in final_dataframe.iterrows():
            print(f"{row['src HierarchicalPath']} -> {row['tgt HierarchicalPath']} ({row['Match Score']})")

        write_csv(final_dataframe)



readConfigAndStart(config_file)
finish_up()





