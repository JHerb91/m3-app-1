import datetime
import time
import anvil.server  # Make sure to import anvil.server
import anvil.http
import json

# Define the UTC offset for Eastern Time
EASTERN_TIME_OFFSET = datetime.timedelta(hours=-5)  # Eastern Time is UTC-5

# Global variables to store updates
updates_queue = []
last_check_time = datetime.datetime.utcnow() + EASTERN_TIME_OFFSET

def monitor_feature_layer():
    global updates_queue, last_check_time  # Preserve updates_queue and last_check_time across runs

    print(f"Last check time (Eastern Time): {last_check_time}")

    # Query URL for fetching features
    query_url = f"https://services.arcgis.com/rD2ylXRs80UroD90/arcgis/rest/services/Project_Tracker_View_Layer/FeatureServer/0/query?where=1=1&outFields=*&f=json"

    try:
        # Send a GET request to the ArcGIS Feature Server
        response = anvil.http.request(query_url, method="GET")

        # Decode response
        raw_data = json.loads(response.get_bytes().decode("utf-8"))
        print("Query successful. Processing features...")

        updates_found = False  # Flag to track whether any updates were found

        # Check if features are present in the response
        if "features" in raw_data and raw_data["features"]:
            for feature in raw_data["features"]:
                attributes = feature["attributes"]

                # Extract EditDate and convert it to Eastern Time
                edit_date = attributes.get("EditDate", None)
                if edit_date:
                    # Convert edit_date (UTC timestamp) to datetime in UTC
                    readable_edit_date = datetime.datetime.utcfromtimestamp(edit_date / 1000)
                    # Convert to Eastern Time
                    readable_edit_date_et = readable_edit_date + EASTERN_TIME_OFFSET

                    # Check if the update is within the desired time window
                    tolerance_time = last_check_time - datetime.timedelta(minutes=1)

                    if readable_edit_date_et > tolerance_time:
                        updates_found = True
                        print(f"Update detected at {readable_edit_date_et}!")

                        # Prepare payload for the update to be added later
                        payload = {
                            "edit_date": readable_edit_date_et.strftime('%Y-%m-%d %H:%M:%S'),
                            "globalid": attributes.get("globalid"),
                            "job_number": attributes.get("job_number"),
                            "job_name": attributes.get("job_name")
                        }

                        # Add the update to the updates queue
                        updates_queue.append(payload)

        else:
            print("No features found in query response.")

        # Update last check time (set to Eastern Time)
        last_check_time = datetime.datetime.utcnow() + EASTERN_TIME_OFFSET

        # If updates are found, trigger the process to handle them
        if updates_found:
            # Now call the background task to save the updates to the table
            anvil.server.call('save_updates_to_table_task')

    except Exception as e:
        print(f"Error during monitoring: {e}")

def save_updates_to_table():
    """
    This function saves the updates in the updates_queue to the Anvil table (processed_updates).
    This function runs locally, avoiding recursive calls.
    """
    global updates_queue  # Ensure that updates_queue is treated as global

    try:
        # Check if there are updates to save
        if updates_queue:
            print(f"Saving {len(updates_queue)} updates to table...")

            # Loop through each update and save to the Anvil table
            for update in updates_queue:
                # Call the Anvil server function to add the update to the 'processed_updates' table
                anvil.server.call('add_to_processed_updates_table', update)

            # Clear the queue after saving the updates
            updates_queue = []

            print("Updates saved to table.")

    except Exception as e:
        print(f"Error while saving updates to table: {e}")

# Create a background task to save updates to table asynchronously
@anvil.server.background_task
def save_updates_to_table_task():
    save_updates_to_table()

# Continuous monitoring loop (every 60 seconds)
while True:
    monitor_feature_layer()
    print("Waiting 60 seconds before the next check...")
    time.sleep(60)
