import requests
import pandas as pd

def get_airtable_data():
    auth_token = ''
    base_id = ''
    table_name = ''

    # Specify the filter formula (can differ based on you use case)
    filter_formula = "{BAL_Qty} != 0"

    # Initialize an empty list to store records
    all_records = []

    # Keep track of the offset for pagination
    offset = None

    # Fetch records using pagination
    while True:
        # Construct the API URL with pagination and filter formula
        url = f'https://api.airtable.com/v0/{base_id}/{table_name}?filterByFormula={filter_formula}'
        if offset:
            url += f'&offset={offset}'
        
        # Make a GET request to the Airtable API
        headers = {
            'Authorization': f'Bearer {auth_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            
            # Extract records from the response
            records = data.get('records', [])
            
            # Append records to the list
            all_records.extend(records)
            
            # Check if there are more records for pagination
            if 'offset' in data:
                offset = data['offset']
            else:
                break
        else:
            print(f"Failed to fetch records. Status code: {response.status_code}")
            break

    # Create a list to store field values
    fields_data = []

    # Iterate through records and extract field values
    for record in all_records:
        fields_data.append(record.get('fields', {}))

    # Convert the records to a DataFrame
    df = pd.DataFrame(fields_data)

    # Select only the 'Trans#' column
    df = df[['Trans#']]
    df = df.drop_duplicates()
    df = df.dropna()
    
    return df

def send_urlattachment_to_airtable(data):
                
    auth_token = ''
    base_id = ''
    table_name = ''
    
    # Group rows by 'Trans#' and collect attachments
    grouped_data = data.groupby('Trans#')
  
    for trans_num, group in grouped_data:
        attachment_objects = []  # Initialize an empty list for attachments
        for _, row in group.iterrows():
            attachment_objects.append({
                "url": row["public_Url"],
                "filename": row["Filename"]
            })
        
        airtable_data = {
            "performUpsert": {
                "fieldsToMergeOn": [
                    "fldrA2wrkE99UxA02"
                ]
            },
            "records": [
                {
                    "fields": {
                        "fldrA2wrkE99UxA02": trans_num,
                        "fldo7LUf4YtnSlGFp": attachment_objects       
                    }
                },
            ]
        }
        update_url = f'https://api.airtable.com/v0/{base_id}/{table_name}'
        headers = {
            'Authorization': f'Bearer {auth_token}',
            'Content-Type': 'application/json',
        }
        response = requests.patch(update_url, json=airtable_data, headers=headers)

        if response.status_code == 200:
            print(f'Successfully updated Diaspark PO#:{trans_num}')
        else:
            error_message = response.json().get('error', {}).get('message', '')
            if 'INVALID_MULTIPLE_CHOICE_OPTIONS' in error_message:
                print(f'Error: Invalid select option in Airtable field')
            else:
                print(f'Error updating Airtable record: {error_message}')