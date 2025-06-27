# This is a simple example for how to work with the fivetran_connector_sdk module.
# Note: This code is currently configured to hit the getSheet endpoint and get data from 1 pre-defined sheet in Smartsheets that does not need pagination
# You will need to provide your own smartsheet credentials for this to work --> api_key variable in configuration.json together with the smartsheet sheet id
# getSheet endpoint: https://smartsheet.redoc.ly/tag/sheets#operation/getSheet
# Add Additional code in the update function to handle multiple sheets and/or pagination through sheets
# Can also add code to extract from other endpoints as needed
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from calendar import c
from fivetran_connector_sdk import Connector, Logging as log, Operations as op # For supporting Connector operations like Update() and Schema()
import requests
import json
from datetime import datetime, timezone, timedelta
import pytz

# This creates the connector object that will use the update function defined in this connector.py file.

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.

def format_table_name(sheet_name):
    return sheet_name.lower().replace(" ", "_")

def schema(configuration: dict):
    api_token = configuration.get('smartsheet_api_token')
    sheet_ids_raw = configuration.get("smartsheet_sheet_ids", "")
    sheet_ids = [s.strip() for s in sheet_ids_raw.split(",") if s.strip()]
    tables = []
    for sheet_id in sheet_ids:
        try:
            api_url = f"https://api.smartsheet.com/2.0/sheets/{sheet_id}"
            response = requests.get(api_url, headers={'Authorization': f'Bearer {api_token}'})
            response.raise_for_status()
            data = response.json()
            sheet_name = data.get("name", f"smartsheet_{sheet_id}")
            table_name = format_table_name(sheet_name)
        except Exception:
            table_name = f"smartsheet_{sheet_id}"
        tables.append({
            "table": table_name,
            "primary_key": ["id"]
        })
    return tables

def update(configuration: dict, state: dict):
    api_token = configuration.get('smartsheet_api_token')
    sheet_ids_raw = configuration.get("smartsheet_sheet_ids", "")
    sheet_ids = [s.strip() for s in sheet_ids_raw.split(",") if s.strip()]
    sync_cursor = state.get('sync_cursor')
    # sync_cursor = None
    # 7-day window for full sync to detect deletes
    if sync_cursor:
        try:
            last_sync = datetime.fromisoformat(sync_cursor)
            now = datetime.now(last_sync.tzinfo)  # Use the same timezone as sync_cursor
            if (now - last_sync).days >= 7:
                log.info("Forcing full sync due to age of sync_cursor.")
                sync_cursor = None
        except Exception as e:
            log.info(f"Could not parse sync_cursor: {e}. Forcing full sync.")
            sync_cursor = None

    # Use Pacific time and subtract 2 minutes to avoid future/near-future timestamps
    pacific = pytz.timezone('US/Pacific')
    sync_start = datetime.now(pacific).replace(microsecond=0).isoformat()

    if not api_token or not sheet_ids:
        log.severe("Missing API token or sheet IDs in configuration.")
        raise ValueError("API token and sheet IDs are required.")

    all_state_ids = state.get("all_ids", {})

    for sheet_id in sheet_ids:
        log.info(f"Processing Sheet ID: {sheet_id}")

        # Build the API URL
        if sync_cursor:
            api_url = f"https://api.smartsheet.com/2.0/sheets/{sheet_id}?rowsModifiedSince={sync_cursor}"
            log.info(f"Attempting incremental sync from cursor: {sync_cursor}")
        else:
            api_url = f"https://api.smartsheet.com/2.0/sheets/{sheet_id}"
            log.info("Performing full sync (no cursor)")

        log.info(f"Fetching data from: {api_url}")

        try:
            response = requests.get(api_url, headers={'Authorization': f'Bearer {api_token}'})
            response.raise_for_status()
            data = response.json()
            log.info("Sheet fetch successful")
            sheet_name = data.get("name", f"smartsheet_{sheet_id}")
            table_name = format_table_name(sheet_name)
        except Exception as e:
            log.severe(f"Failed to fetch sheet {sheet_id}: {e}")
            continue

        column_mapping = {col['id']: col['title'] for col in data.get('columns', [])}
        previous_ids = set(all_state_ids.get(sheet_id, []))
        new_ids = set()
        processed_rows = 0

        for row in data.get('rows', []):
            try:
                row_id = row.get('id')
                new_ids.add(row_id)
                row_data = {
                    'id': row_id,
                    'row_number': row.get('rowNumber'),
                    'expanded': row.get('expanded'),
                    'created_at': row.get('createdAt'),
                    'modified_at': row.get('modifiedAt')
                }
                for cell in row.get('cells', []):
                    column_name = column_mapping.get(cell.get('columnId'))
                    if column_name:
                        row_data[column_name] = cell.get('value')

                yield op.upsert(table_name, row_data)
                processed_rows += 1
            except Exception as e:
                log.severe(f"Error processing row {row.get('id', 'unknown')} in sheet {sheet_id}: {e}")
                continue

        current_ids = new_ids
        print(current_ids)
        log.info(f"Processed {processed_rows} rows for sheet {sheet_id}")
        log.info(f"Current IDs (merged if incremental): {current_ids}")
        log.info(f"Current IDs: {current_ids}")

        if sync_cursor is None:
            # Full sync: perform delete detection
            deleted_ids = previous_ids - current_ids
            for deleted_id in deleted_ids:
                yield op.delete(table_name, {"id": deleted_id})
        # else: skip delete detection

        # Update state per sheet
        all_state_ids[sheet_id] = list(current_ids)

    # Final checkpoint
    yield op.checkpoint({
        "sync_cursor": sync_start,
        "all_ids": all_state_ids
    })


connector = Connector(schema=schema, update=update)




# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open('configuration.json', 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration=configuration)