import requests
import json
import utils
import os 
import pandas as pd 
import zipfile
from ipyaggrid import Grid

SEMTUI_URI = ""


def load_local_data(file_path_or_link, file_type='auto', load_as='DataFrame'):
    """
    Load data from a local file path or a link. Supports loading CSV and JSON files,
    and ZIP archives containing multiple CSV or JSON files.

    Parameters:
        file_path_or_link (str): Path to the file or the link to load data from.
        file_type (str, optional): Type of file ('csv', 'json', or 'zip'). If 'auto', the function will infer the type from the file extension. Default is 'auto'.
        load_as (str, optional): Load data as a single DataFrame ('DataFrame') or a list of DataFrames ('List'). Default is 'DataFrame'.

    Returns:
        DataFrame or List[DataFrame]: Loaded data.
    """
    data = None  # Initialize 'data' to ensure it's defined

    if file_type == 'auto':
        if file_path_or_link.endswith('.csv'):
            file_type = 'csv'
        elif file_path_or_link.endswith('.json'):
            file_type = 'json'
        elif file_path_or_link.endswith('.zip'):
            file_type = 'zip'
        else:
            raise ValueError("Unsupported file type. Please specify file_type as 'csv', 'json', or 'zip'.")

    if file_type == 'csv':
        data = pd.read_csv(file_path_or_link)
    elif file_type == 'json':
        with open(file_path_or_link, 'r') as file:
            data_json = json.load(file)

        if isinstance(data_json, dict):  # Single JSON object
            data = pd.DataFrame([data_json])
        elif isinstance(data_json, list):  # Array of JSON objects
            data = pd.DataFrame(data_json)
        else:
            raise ValueError("JSON file format not recognized.")
    elif file_type == 'zip' and load_as == 'List':
        data = []  # Initialize as an empty list for ZIP case
        with zipfile.ZipFile(file_path_or_link, 'r') as z:
            data_files = [f for f in z.namelist() if f.endswith('.csv') or f.endswith('.json')]
            for data_file in data_files:
                if data_file.endswith('.csv'):
                    # Specify the correct separator for tab-separated values
                    # Adjust the quoting as necessary for your data
                    try:
                        df = pd.read_csv(z.open(data_file), sep='\t', on_bad_lines='skip')
                    except pd.errors.ParserError as e:
                        print(f"Error parsing {data_file}: {e}")
                        continue  # Skip this file and continue with the next
                    data.append(df)
                elif data_file.endswith('.json'):
                    # Your existing JSON handling logic here
                    json_data = json.loads(z.open(data_file).read().decode('utf-8'))
                    if isinstance(json_data, dict):  # Single JSON object
                        data.append(pd.DataFrame([json_data]))
                    elif isinstance(json_data, list):  # Array of JSON objects
                        data.append(pd.DataFrame(json_data))
    return data

# Example usage:
# Load data from a ZIP file containing multiple CSV and JSON files as a list of DataFrames
# dfs = load_local_data("path_to_zip_file.zip", file_type='zip', load_as='List')

# Load data from a CSV file as a DataFrame
# df = load_local_data("path_to_csv_file.csv", file_type='csv')

# Load data from a JSON file as a DataFrame
# df = load_local_data("path_to_json_file.json", file_type='json')

# Load data from a zip file containing CSVs as a list of DataFrames
# dfs = load_local_data("path_to_zip_file.zip", file_type='csv', load_as='List')


def display_csv_in_grid(csv_file_path):
    """
    Displays the contents of a CSV file in an interactive grid using ipyaggrid.

    Parameters:
        csv_file_path (str): The path to the CSV file to be displayed.
    """
    # Load the CSV file into a Pandas DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Define grid options for displaying the DataFrame
    grid_options = {
        'columnDefs': [{'field': c} for c in df.columns],
        'enableSorting': True,
        'enableFilter': True,
        'enableColResize': True,
        'enableRangeSelection': True,
    }
    
    # Create and display the grid
    grid = Grid(grid_data=df,
                grid_options=grid_options,
                quick_filter=True,
                show_toggle_edit=True,
                export_mode="buttons",
                theme='ag-theme-balham')
    
    return grid

# Example usage
# Assuming you have a path to a CSV file
# csv_file_path = "path/to/your/file.csv"
# display_csv_in_grid(csv_file_path)


def addDataset(datasetInput, datasetName, inputType='file'):
    """
    Adds a new dataset to the T-Rich backend. Supports uploading a dataset by providing
    a zip file directly or via a URL to the zip file.
    
    :param datasetInput: Path to the zip file or a URL to the zip file of the dataset.
    :param datasetName: Name to be assigned to the dataset.
    :param inputType: 'file' for local zip files, 'url' for URLs to zip files.
    :return: String indicating the result of the operation.
    """
    data = {'name': datasetName}
    headers = {'Authorization': 'Bearer <your_access_token>'}  # Update with your actual JWT token

    if inputType == 'file':
        if not os.path.exists(datasetInput) or not datasetInput.endswith('.zip'):
            return "Zip file not found or invalid file type. Please check the path and ensure it is a .zip file."
        
        with open(datasetInput, 'rb') as file:
            files = {'file': (os.path.basename(datasetInput), file, 'application/zip')}
            response = requests.post(SEMTUI_URI + 'dataset/upload', headers=headers, files=files, data=data)
    elif inputType == 'url':
        if not datasetInput.lower().endswith('.zip'):
            return "Invalid URL. Please ensure the URL points to a .zip file."
        
        data['url'] = datasetInput
        response = requests.post(SEMTUI_URI + 'dataset/from_url', headers=headers, data=data)
    else:
        return "Invalid input type specified. Use 'file' for local zip files or 'url' for URLs to zip files."

    if response.status_code in [200, 201]:
        return "Dataset creation successful."
    else:
        return f"Dataset creation failed. Status code: {response.status_code}, Message: {response.text}"


def checkDataset(zipFilePath):
    """
    Loads and displays information about the tables in a dataset zip file.
    
    :param zipFilePath: Path to the dataset zip file.
    :return: None
    """
    if not os.path.exists(zipFilePath) or not zipFilePath.endswith('.zip'):
        print("Zip file not found or invalid file type. Please check the path and ensure it is a .zip file.")
        return
    
    try:
        with zipfile.ZipFile(zipFilePath, 'r') as zip_ref:
            table_files = [file.filename for file in zip_ref.filelist if file.filename.endswith('.csv') or file.filename.endswith('.json')]
            
            if table_files:
                print("Tables in the zip file:")
                for table in table_files:
                    print(table)
            else:
                print("No tables found in the zip file.")
    except zipfile.BadZipFile:
        print("Failed to read the zip file. Please ensure it's a valid zip archive.")

# Assuming `tables` is a dictionary of DataFrames loaded from your CSV files
# and `schema` is your schema object loaded from JSON

def load_tables(zip_file_path):
    """Load tables from a zip file into Pandas DataFrames with error handling, supporting tab-separated files."""
    tables = {}
    with zipfile.ZipFile(zip_file_path, 'r') as z:
        for text_file in z.infolist():
            if text_file.filename.endswith('.csv'):
                try:
                    # Use sep='\t' to specify that the separator is a tab character
                    df = pd.read_csv(z.open(text_file.filename), sep='\t', error_bad_lines=False, warn_bad_lines=True)
                    tables[text_file.filename] = df
                except pd.errors.EmptyDataError:
                    print(f"Warning: {text_file.filename} is empty and will be skipped.")
                except Exception as e:
                    print(f"Error loading {text_file.filename}: {e}")
    return tables



def merge_tables(tables):
    """
    Merge tables based on PK-FK relationships. This function assumes a simple case where FKs in one table
    match PKs in another. More complex relationships will require additional logic.
    """
    # Example: For simplification, assuming 'id' as PK and '<table_name>_id' as FK
    merged_table = None
    for name, table in tables.items():
        if merged_table is None:
            merged_table = table
        else:
            fk_name = f'{name[:-4].lower()}_id'  # Assuming table name in FK is formatted as <tablename>_id
            if fk_name in merged_table.columns:
                merged_table = pd.merge(merged_table, table, left_on=fk_name, right_on='id', how='left')
            elif 'id' in table.columns:
                merged_table = pd.merge(merged_table, table, left_on='id', right_on=fk_name, how='left')
    
    return merged_table

def automate_merging(zip_file_path):
    """Automates the process of loading and merging tables from a zip file."""
    if not os.path.exists(zip_file_path) or not zip_file_path.endswith('.zip'):
        print("Zip file not found or invalid file type. Please check the path and ensure it is a .zip file.")
        return

    tables = load_tables(zip_file_path)
    merged_table = merge_tables(tables)
    return merged_table

# Example usage
#zip_file_path = 'path/to/your/dataset.zip'
#merged_table = automate_merging(zip_file_path)
#print(merged_table)


def merge_tables_based_on_schema(tables, schema):
    merged_table = None
    for table_name, table_info in schema["tables"].items():
        if table_name in tables:
            if merged_table is None:
                merged_table = tables[table_name]
            else:
                for fk, related_table_name in table_info["relationships"].items():
                    if related_table_name in tables:
                        pk = schema["tables"][related_table_name]["primaryKey"]
                        merged_table = pd.merge(merged_table, tables[related_table_name], 
                                                left_on=fk, right_on=pk, 
                                                how='left', suffixes=('', '_'+related_table_name))
    return merged_table

# Load schema
#with open('schema.json', 'r') as f:
#    schema = json.load(f)

# Perform merging
#merged_table = merge_tables_based_on_schema(tables, schema)
#print(merged_table)

def getDatasetsList():
    """
    Show the datasets available in the backend.

    :return: JSON object containing dataset information. If there's an error fetching or processing the data,
             an empty JSON object is returned.
    """
    try:
        response = requests.get(SEMTUI_URI+'/dataset/')
        response.raise_for_status()  # Check if the request was successful
        
        # Directly return the JSON data from the response
        # This assumes the backend always returns a JSON response
        return response.json()  # Returns a Python dictionary that represents the JSON data
    except requests.RequestException as e:
        print(f"Request failed: {e}")
    except json.JSONDecodeError as e:  # Catch JSON decoding errors
        print(f"Error processing the datasets list: {e}")
    
    # Return an empty JSON object as a fallback in case of errors
    return {}

import pandas as pd
import json
import requests  # This is placeholder for actual Groq client usage

import pandas as pd
import requests
import json
import os
from groq import Groq

import pandas as pd
import requests
import json
import os
from groq import Groq
import pandas as pd
import requests
import json
import os
from groq import Groq

from pandasai import SmartDataframe


def get_transformation_recommendations_from_df(df_csv, api_key):
    try:
        # Extract the feature names from the DataFrame
        feature_names = df_csv.columns.tolist()
        print(f"Feature Names:\n{feature_names}\n")

        # Format the feature names as a JSON object
        feature_json = json.dumps({feature: 'float' for feature in feature_names})

        client = Groq(
            api_key=api_key,
        )

        stream = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": "you are a helpful assistant. Your job is to return recommendations for data transformations that involve external available on the internet or external APIs."
                },
                {
                    "role": "user",
                    "content": f"{feature_json}",
                }
            ],
            model="mixtral-8x7b-32768",
            temperature=0.0,
            max_tokens=1024,
            top_p=1,
            stop=None,
            stream=True,
        )
        # Print the incremental deltas returned by the LLM.
        for chunk in stream:
            print(chunk.choices[0].delta.content, end="")


    except requests.exceptions.RequestException as e:
        print("Error making the request:", e)
    except json.JSONDecodeError as e:
        print("Error decoding JSON response:", e)
    except Exception as e:
        print("An error occurred:", e)

# Example usage:
#my_api_key = os.getenv("GROQ_API_KEY")
#df_csv = pd.read_csv('your_dataset.csv')
#recommendations = get_transformation_recommendations_from_df(df_csv, my_api_key)
#print(recommendations)


import os
from langchain_groq import ChatGroq
#from smart_dataframe_module import SmartDataFrame  # Import or define SmartDataFrame

def df_chat(df_csv, question, api_key=None):
    """
    Given a DataFrame and a question, use a ChatGroq instance to process the question
    and return the answer based on the data in the DataFrame.
    
    Parameters:
        df_csv (DataFrame): The DataFrame to analyze.
        question (str): The question to ask about the data in the DataFrame.
        api_key (str, optional): The API key for the Groq service. If not provided,
                                 it tries to fetch from the environment variable "GROQ_API_KEY".
    
    Returns:
        str: The answer to the question based on the DataFrame data.
    """
    # If the API key is not provided as an argument, try to get it from the environment variable
    if api_key is None:
        api_key = os.getenv("GROQ_API_KEY")
        if api_key is None:
            return "API key is not provided. Please provide an API key."

    try:
        # Initialize the ChatGroq instance with the model name and API key
        llm = ChatGroq(
            model_name="mixtral-8x7b-32768", 
            api_key=api_key
        )

        # Initialize the SmartDataFrame with the DataFrame and configuration for the LLM
        df = SmartDataFrame(df_csv, config={"llm": llm})

        # Use the chat method of SmartDataFrame to ask the question and get the answer
        answer = df.chat(question)
        return answer

    except requests.exceptions.RequestException as e:
        return f"Request error: {e}"
    except Exception as e:
        return f"An error occurred: {e}"

# Example usage:
# Assuming 'df_csv' is your DataFrame and you want to ask a question
# answer = df_chat(df_csv, 'What are the top 5 countries?')
# print(f"Answer:\n{answer}")


def convert_to_iso8601_pandas(df: pd.DataFrame, date_col_name: str) -> pd.DataFrame:
    """
    Convert date column in a DataFrame to ISO 8601 format using pandas.

    :param df: The DataFrame containing the date information.
    :param date_col_name: The name of the column with date strings to be converted.
    :return: A DataFrame with the date column converted to ISO 8601 format.
    """
    # Check if the date column exists in the DataFrame
    if date_col_name not in df.columns:
        raise ValueError(f"The specified column '{date_col_name}' does not exist in the DataFrame.")
    
    try:
        # Convert each date in the column to ISO 8601 format
        df[date_col_name] = pd.to_datetime(df[date_col_name]).dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    except Exception as e:
        # Instead of printing, it's usually better to handle exceptions in a way that the calling code can manage
        raise Exception(f"An error occurred while converting dates: {e}")
    
    return df


def get_geocoded_data(df: pd.DataFrame, address_columns: list, api_key: str) -> pd.DataFrame:
    """
    Retrieves geocoded data for a given DataFrame using the HERE Geocoding API.

    :param df: The DataFrame containing address data to geocode.
    :param address_columns: List of column names in df to use for constructing the address query.
    :param api_key: The API key for the HERE Geocoding service.
    :return: A DataFrame with original data and added geocoded information.
    """
    base_url = "https://geocode.search.hereapi.com/v1/geocode"
    resolved_addresses = []

    for index, row in df.iterrows():
        # Construct the address query using the specified columns
        address_parts = [str(row[col]) for col in address_columns if col in df.columns]
        address_query = ', '.join(address_parts)
        params = {
            'q': address_query,
            'apiKey': api_key
        }

        # Make the GET request to the HERE Geocoding API
        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            
            if items:
                # Extract resolved address and coordinates
                resolved_address = items[0]['address']['label']
                lat = items[0]['position']['lat']
                lng = items[0]['position']['lng']
                
                # Append the resolved data
                resolved_addresses.append({
                    'Original Address': address_query,
                    'Resolved Address': resolved_address,
                    'Latitude': lat,
                    'Longitude': lng
                })
            else:
                # Handle cases where no items were found
                resolved_addresses.append({
                    'Original Address': address_query,
                    'Resolved Address': 'Not found',
                    'Latitude': None,
                    'Longitude': None
                })
        else:
            # Handle API response errors
            print(f"Failed to geocode address: {address_query}. Status code: {response.status_code}")
            resolved_addresses.append({
                'Original Address': address_query,
                'Resolved Address': 'Error',
                'Latitude': 'Error',
                'Longitude': 'Error'
            })

    # Create a DataFrame from the resolved addresses
    geocoded_df = pd.DataFrame(resolved_addresses)

    # Merge the original DataFrame with the geocoded data
    merged_df = pd.merge(df, geocoded_df, left_on=address_columns[0], right_on='Original Address', how='left')

    # Drop the duplicate 'Original Address' column resulting from the merge
    merged_df.drop('Original Address', axis=1, inplace=True)

    return merged_df

# Example usage:
# Assuming your original DataFrame is named 'df' and you have an API key
#api_key = 'YOUR_ACTUAL_API_KEY'
#address_columns = ['Point of Interest', 'Place', 'Adm1', 'Country']
# Get the merged DataFrame with geocoded information
#merged_geocoded_df = get_geocoded_data(df, address_columns, api_key)
#print(merged_geocoded_df)



def getDataset(idDataset):
    """
    Show general information about the dataset

    :idDataset: the dataset's ID in the backend
    :return: dataframe containing general information about the dataset
    """
    response = requests.get(SEMTUI_URI + 'dataset/' + str(idDataset))
    return utils.cleanDatasetsData(response.text)


def getDatasetTables(idDataset):
    """
    Show a list of tables contained in the dataset

    :idDataset: the dataset's ID in the backend
    :return: dataframe containing the list of tables and their respective information
    """
    response = requests.get(SEMTUI_URI + 'dataset/' + str(idDataset) + '/table')
    return utils.cleanDatasetsTables(response.text)

def getTable(idDataset, idTable):
    """
    Retrieve a table from the backend in two different formats:
        - raw: the table in JSON format
        - parsed: the table in dataframe format

    :idDataset: the dataset's ID in the backend
    :idTable: the ID of the table to retrieve
    :return: the table in the two described formats
    """
    response = requests.get(SEMTUI_URI + 'dataset/' + str(idDataset)+'/table/'+str(idTable))
    return {'raw': json.loads(response.text)}

def getExtendersList():
    """
    Provides a list of available extenders with their main information

    :return: a dataframe containing extenders and their information
    """
    response = getExtenderData()
    return utils.cleanServiceList(response)

def getReconciliatorsList():
    """
    Provides a list of available reconciliators with their main information

    :return: a dataframe containing reconciliators and their information
    """
    response = getReconciliatorData()
    return utils.cleanServiceList(response)

def addTable(idDataset, filePath, tableName):
    """
    Allows adding a table to a dataset in the backend

    :idDataset: the dataset's ID in the backend
    :filePath: .csv file to be loaded as a table
    :tableName: the name the table will have in the dataset
    :return: loading status
    """
    url = SEMTUI_URI + 'dataset/' + str(idDataset) + '/table'
    files = {'file': open(filePath, 'rb')}
    response = requests.post(url, files=files, data={'name': tableName})
    return response.status_code

def reconcile(table, columnName, idReconciliator):
    """
    Reconciles a column with the chosen reconciliator

    :table: the table with the column to reconcile 
    :columnName: the name of the column to reconcile 
    :idReconciliator: ID of the reconciliator to use 
    :return: table with reconciled column
    """
    table = table['raw']
    reconciliatorResponse = getReconciliatorData()
    # creating the request
    url = SEMTUI_URI + '/reconciliators' + str(utils.getReconciliator(idReconciliator, reconciliatorResponse)['relativeUrl'])
    payload = utils.createReconciliationPayload(table, columnName, idReconciliator)
    response = requests.post(url, json=payload)
    response = json.loads(response.text)
    # inserting data into the table
    metadata = utils.createCellMetadataNameField(response, idReconciliator, reconciliatorResponse)
    table = utils.updateMetadataCells(table, metadata)
    table = utils.updateMetadataColumn(table, columnName, idReconciliator, metadata, reconciliatorResponse)
    table = utils.updateMetadataTable(table)
    return {'raw': table}

def updateTable(table):
    """
    Allows updating the table in the backend by inserting the table with the new information

    :table: the table to be updated within the backend
    :return: update status
    """
    table = table['raw']
    url = SEMTUI_URI + 'dataset/' + str(table["table"]["id"])+'/table/'+str(table["table"]["idDataset"])
    payload = utils.createUpdatePayload(table)
    response = requests.put(url, json=payload)
    return response.text

def extendColumn(table, reconciliatedColumnName, idExtender, properties, newColumnsName):
    """
    Allows extending specified properties present in the Knowledge Graph as a new column

    :table: the table containing the data
    :reconciliatedColumnName: the column containing the ID in the KG
    :idExtender: the extender to use for extension
    :properties: the properties to extend in the table
    :newColumnsName: the name of the new column to add
    :return: the extended table
    """
    reconciliatorResponse = getReconciliatorData()
    table = table["raw"]
    url = SEMTUI_URI + "extenders/" + \
        str(utils.getExtender(idExtender, getExtenderData())['relativeUrl'])
    payload = utils.createExensionPayload(table, reconciliatedColumnName, idExtender, properties)
    response = requests.post(url, json=payload)
    table = utils.addExtendedColumns(table, json.loads(response.text), newColumnsName, reconciliatorResponse)
    return {'raw': table}

def getExtenderData():
    """
    Retrieves extender data from the backend

    :return: data of extension services in JSON format
    """
    response = requests.get(SEMTUI_URI + '/extenders/list')
    return json.loads(response.text)

def getReconciliatorData():
    """
    Retrieves reconciliator data from the backend

    :return: data of reconciliator services in JSON format
    """
    response = requests.get(SEMTUI_URI + '/reconciliators/list')
    return json.loads(response.text)

