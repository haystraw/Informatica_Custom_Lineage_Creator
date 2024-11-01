import pandas as pd
import argparse

def find_reference_id(excel_file, parent_value, name_value):
    """
    Read an Excel file, search in the 'Technical Data element' tab, and find the 'Reference ID' 
    based on 'Parent: Technical Data Set' and 'Name' values.

    :param excel_file: Path to the Excel file.
    :param parent_value: Value to match in the 'Parent: Technical Data Set' column.
    :param name_value: Value to match in the 'Name' column.
    """
    # Read the specific sheet 'Technical Data element'
    try:
        df = pd.read_excel(excel_file, sheet_name="Technical Data element")
    except Exception as e:
        print(f"Error reading the Excel file or sheet: {e}")
        return
    
    # Ensure the required columns are present
    required_columns = ["Parent: Technical Data Set", "Name", "Reference ID"]
    if not all(column in df.columns for column in required_columns):
        print(f"The required columns {required_columns} are not present in the sheet.")
        return
    
    # Filter rows based on the provided Parent and Name values
    matched_row = df[(df["Parent: Technical Data Set"] == parent_value) & (df["Name"] == name_value)]
    
    # Check if there are any matches and print the 'Reference ID'
    if not matched_row.empty:
        reference_id = matched_row["Reference ID"].values[0]
        print(f"Reference ID for '{parent_value}' and '{name_value}': {reference_id}")
    else:
        print(f"No match found for Parent: '{parent_value}' and Name: '{name_value}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find Reference ID based on Parent and Name from the 'Technical Data element' sheet in an Excel file.")
    parser.add_argument("excel_file", help="Path to the Excel file.")
    parser.add_argument("parent_value", help="Value for 'Parent: Technical Data Set' to match.")
    parser.add_argument("name_value", help="Value for 'Name' to match.")
    
    args = parser.parse_args()
    
    # Call the function with the provided arguments
    find_reference_id(args.excel_file, args.parent_value, args.name_value)
