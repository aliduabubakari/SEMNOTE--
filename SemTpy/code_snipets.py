def addDataset(datasetInput, datasetName, inputType='file'):
    """
    Create a new dataset in the T-Rich backend. This function supports adding a dataset
    by directly uploading a zip file containing the dataset's tables (and optionally, metadata),
    or by providing a URL to a zip file of the dataset.

    Instructions for zip file:
    - The zip file should contain all tables of the dataset, with each table saved as a separate file.
    - Supported table formats include CSV and JSON.
    - If including metadata, it should be in a JSON or TXT file within the archive.
    
    :param datasetInput: Path to the zip file or a URL to the zip file of the dataset.
    :param datasetName: Name to be assigned to the dataset.
    :param inputType: Type of the dataset input ('file' for local files, 'url' for URLs to zip files).
    :return: The result of the creation operation, indicating success or failure.
    """
    data = {'name': datasetName}
    
    if inputType == 'file':
        if not os.path.exists(datasetInput) or not datasetInput.endswith('.zip'):
            return "Zip file not found or invalid file type. Please check the path and ensure it is a .zip file."
        
        files = {'file': (os.path.basename(datasetInput), open(datasetInput, 'rb'), 'application/zip')}
        response = requests.post(SEMTUI_URI + 'dataset/upload', files=files, data=data)
    elif inputType == 'url':
        if not datasetInput.endswith('.zip'):
            return "Invalid URL. Please ensure the URL points to a .zip file."
        
        data['url'] = datasetInput
        response = requests.post(SEMTUI_URI + 'dataset/from_url', data=data)
    else:
        return "Invalid input type specified. Use 'file' for local zip files or 'url' for URLs to zip files."

    # Check the response status code
    if response.status_code in [200, 201]:
        return "Dataset creation successful."
    else:
        return f"Dataset creation failed. Status code: {response.status_code}, Message: {response.text}"



import aiohttp
import asyncio
import pandas as pd

async def fetch_geocoded_data(session, base_url, address_query, api_key):
    params = {
        'q': address_query,
        'apiKey': api_key
    }
    async with session.get(base_url, params=params) as response:
        if response.status == 200:
            data = await response.json()
            items = data.get('items', [])
            if items:
                resolved_address = items[0]['address']['label']
                lat = items[0]['position']['lat']
                lng = items[0]['position']['lng']
                return {
                    'Original Address': address_query,
                    'Resolved Address': resolved_address,
                    'Latitude': lat,
                    'Longitude': lng
                }
            else:
                return {
                    'Original Address': address_query,
                    'Resolved Address': 'Not found',
                    'Latitude': None,
                    'Longitude': None
                }
        else:
            print(f"Failed to geocode address: {address_query}. Status code: {response.status}")
            return {
                'Original Address': address_query,
                'Resolved Address': 'Error',
                'Latitude': 'Error',
                'Longitude': 'Error'
            }

async def get_geocoded_data(df: pd.DataFrame, address_columns: list, api_key: str) -> pd.DataFrame:
    base_url = "https://geocode.search.hereapi.com/v1/geocode"
    resolved_addresses = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for index, row in df.iterrows():
            address_parts = [str(row[col]) for col in address_columns if col in df.columns]
            address_query = ', '.join(address_parts)
            task = asyncio.create_task(fetch_geocoded_data(session, base_url, address_query, api_key))
            tasks.append(task)

        results = await asyncio.gather(*tasks)
        resolved_addresses.extend(results)

    geocoded_df = pd.DataFrame(resolved_addresses)
    merged_df = pd.merge(df, geocoded_df, left_on=address_columns[0], right_on='Original Address', how='left')
    merged_df.drop('Original Address', axis=1, inplace=True)

    return merged_df

# Example usage:
geocoded_df = pd.DataFrame({
    'Address': [
        'John F. Kennedy Presidential Library and Museum, Columbia Point, Massachusetts, United States',
        'Petrie Museum of Egyptian Archaeology, London, England, United Kingdom',
        # ... more rows ...
    ]
})

address_columns = ['Address']
api_key = 'YOUR_HERE_API_KEY'

geocoded_result = asyncio.run(get_geocoded_data(geocoded_df, address_columns, api_key))
print(geocoded_result)
