import pandas as pd
import requests
import yaml


def load_data(path):
    # Assuming the function reads a CSV file into a pandas DataFrame
    return pd.read_csv(path)

def clean_data(data, method):
    # Implement the logic for the 'drop_missing' method or other methods
    if method == "drop_missing":
        return data.dropna()
    else:
        # Other cleaning methods
        pass

def get_geocoded_data(df, api_key):
    base_url = "https://geocode.search.hereapi.com/v1/geocode"
    resolved_addresses = []

    for index, row in df.iterrows():
        # Concatenate the address components
        address_query = f"{row['Point of Interest']}, {row['Place']}, {row['Adm1']}, {row['Country']}"
        params = {
            'q': address_query,
            'apiKey': api_key
        }

        # Construct the full URL for debugging purposes
        request_url = f"{base_url}?q={address_query}&apiKey={api_key}"
        #print(f"Request URL: {request_url}")

        # Make the GET request to the geocoding API
        response = requests.get(base_url, params=params)
        #print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            #print(f"Response Data: {data}")  # Print the entire response data for debugging
            items = data.get('items', [])
            
            if items:
                resolved_address = items[0]['address']['label']
                lat = items[0]['position']['lat']
                lng = items[0]['position']['lng']
                
                # Append the resolved data including the correct postal code and coordinates
                resolved_addresses.append({
                    'Original Address': address_query,
                    'Resolved Address': resolved_address,
                    'Latitude': lat,
                    'Longitude': lng
                })
            else:
                # In case no items were found
                resolved_addresses.append({
                    'Original Address': address_query,
                    'Resolved Address': None,
                    'Latitude': None,
                    'Longitude': None
                })
        else:
            print(f"Failed to geocode address: {address_query}. Status code: {response.status_code}")
            resolved_addresses.append({
                'Original Address': address_query,
                'Resolved Address': 'Error',
                'Latitude': 'Error',
                'Longitude': 'Error'
            })

    # Convert the resolved addresses to a DataFrame
    return pd.DataFrame(resolved_addresses)


def enrich_data(data, source, api_key):
    # Fetch additional data from an external API and merge it with the existing data
    response = requests.get(source)
    external_data = response.json()
    
    # Call the function to get geocoded data
    geocoded_data = get_geocoded_data(data, api_key)
    
    # Merge the geocoded data with the existing data
    #enriched_data = pd.merge(data, geocoded_data, on='Original Address', how='left')
    # Merge the original DataFrame with the geocoded DataFrame based on their indices
    enriched_data = pd.concat([data, geocoded_data], axis=1)
    
    return enriched_data

def save_data(data, path):
    # Assuming the function writes the pandas DataFrame to a CSV file
    data.to_csv(path)


# Paste all the provided functions here

# Define the path to your data file and API key
#path = 'C:\\Users\\user\\Documents\\BICOCCA\SEMNOTE--\\SemTpy\\Museums.csv'
path = 'C:\\Users\\user\\Documents\\BICOCCA\\SEMNOTE--\\SemTpy\\Museums.csv'
api_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

# Load data
df = load_data(path)
df = df.head()

# Define step functions mapping
step_functions = {
    'load_data': load_data,
    'clean_data': clean_data,
    'enrich_data': enrich_data,
    'save_data': save_data
}


# Read pipeline configuration from YAML file
with open('pipeline.yaml', 'r') as file:
    pipeline_config = yaml.safe_load(file)

# Dictionary to store the output of each step
data_store = {}

# Iterate through each step in the pipeline
for step in pipeline_config['pipeline']:
    step_name = step['step']
    parameters = step['parameters']
    
    # Fetch the input data if an 'input' is specified
    input_data = data_store.get(step.get('input')) if 'input' in step else None
    
    # Call the corresponding function
    if input_data is not None:
        # Pass input_data if it's available
        result = step_functions[step_name](input_data, **parameters)
    else:
        # Pass only parameters (excluding input_data) to the function
        result = step_functions[step_name](**parameters)
    
    # Store the output for use in subsequent steps
    if 'output' in step:
        data_store[step['output']] = result
