import json
import datetime
import time
import anvil.http

# Hosted feature layer URL
feature_layer_url = "https://services.arcgis.com/rD2ylXRs80UroD90/arcgis/rest/services/Project_Tracker_View_Layer/FeatureServer/0"

# Webhook URL
make_webhook_url = "https://hook.us2.make.com/ij23k1z88l9ie4pmnv421ylnpljzaz8b"

# File to persist processed updates
processed_updates_file = "processed_updates.json"

# Track last check time
last_check_time = datetime.datetime.utcnow()

# Load previously processed updates from file, if available
def load_processed_updates():
    try:
        with open(processed_updates_file, "r") as file:
            print("Loaded processed updates from file.")
            return json.load(file)
    except FileNotFoundError:
        print("No previous processed updates found.")
        return {}  # Return empty if the file doesn't exist

# Save processed updates to file
def save_processed_updates():
    with open(processed_updates_file, "w") as file:
        print(f"Saving processed updates to file: {processed_updates}")
        json.dump(processed_updates, file)

# Load existing processed updates
processed_updates = load_processed_updates()

def monitor_feature_layer():
    global last_check_time  # Preserve timestamp across runs

    try:
        print(f"Last check time (UTC): {last_check_time}")

        # Query URL for fetching features
        query_url = f"{feature_layer_url}/query?where=1=1&outFields=*&f=json"

        print(f"Sending request to {query_url}")
        response = anvil.http.request(query_url, method="GET")

        # Decode response
        raw_data = json.loads(response.get_bytes().decode("utf-8"))
        print(f"Query successful. Processing features...")

        # Check if features are present
        if "features" in raw_data and raw_data["features"]:
            updates_found = False  # Track updates

            for feature in raw_data["features"]:
                attributes = feature["attributes"]
                globalid = attributes.get("globalid", None)

                if not globalid:
                    continue  # Skip records without globalid

                # Extract EditDate and compare with last processed EditDate
                edit_date = attributes.get("EditDate", None)
                if edit_date:
                    # Convert EditDate to a proper datetime object
                    readable_edit_date = datetime.datetime.utcfromtimestamp(edit_date / 1000)

                    print(f"Checking record: globalid={globalid}, EditDate (raw): {edit_date}, EditDate (readable): {readable_edit_date}")

                    # Log current state of processed_updates before comparison
                    print(f"Current processed_updates: {processed_updates}")

                    # Check if this record has been processed before (first-time check)
                    if globalid not in processed_updates:
                        updates_found = True
                        print(f"First time processing record with globalid {globalid}.")

                        # Prepare payload for webhook
                        payload = {
                            "edit_date": readable_edit_date.strftime('%Y-%m-%d %H:%M:%S'),
                            "attributes": attributes
                        }

                        # Log updated record details
                        print(f"Sending updated record to webhook:\n{json.dumps(payload, indent=2)}")

                        # Send payload to webhook
                        webhook_response = anvil.http.request(
                            make_webhook_url,
                            method="POST",
                            json=payload
                        )

                        # Debug webhook response
                        print(f"Webhook Response Status: {webhook_response.status}")
                        print(f"Webhook Response Body: {webhook_response.get_bytes().decode('utf-8')}")

                        # Store the initial EditDate for this record
                        processed_updates[globalid] = readable_edit_date.isoformat()
                        print(f"Processed_updates after storing: {processed_updates}")

                    else:
                        # If the record has been processed before, compare timestamps
                        last_processed_timestamp_str = processed_updates[globalid]
                        last_processed_timestamp = datetime.datetime.fromisoformat(last_processed_timestamp_str)

                        print(f"Last processed timestamp for globalid={globalid}: {last_processed_timestamp} (stored timestamp)")

                        # Calculate the difference between the current timestamp and the last processed timestamp
                        time_diff = (readable_edit_date - last_processed_timestamp).total_seconds()

                        print(f"Timestamp difference: {time_diff} seconds")

                        # If there's a significant difference, process the update
                        if time_diff > 1:  # Consider update if difference is more than 1 second
                            updates_found = True
                            print(f"Update detected at {readable_edit_date} for globalid {globalid}!")

                            # Prepare payload for webhook
                            payload = {
                                "edit_date": readable_edit_date.strftime('%Y-%m-%d %H:%M:%S'),
                                "attributes": attributes
                            }

                            # Log updated record details
                            print(f"Sending updated record to webhook:\n{json.dumps(payload, indent=2)}")

                            # Send payload to webhook
                            webhook_response = anvil.http.request(
                                make_webhook_url,
                                method="POST",
                                json=payload
                            )

                            # Debug webhook response
                            print(f"Webhook Response Status: {webhook_response.status}")
                            print(f"Webhook Response Body: {webhook_response.get_bytes().decode('utf-8')}")

                            # Update the last processed EditDate for this globalid
                            processed_updates[globalid] = readable_edit_date.isoformat()
                            print(f"Processed_updates after updating: {processed_updates}")

            # Print only if no updates were detected
            if not updates_found:
                print("No updates found.")

        else:
            print("No features found in query response.")

        # Update last check time
        last_check_time = datetime.datetime.utcnow()

        # Save the processed updates to file
        save_processed_updates()

    except Exception as e:
        print(f"Error during monitoring: {e}")
        # Ensure the loop doesn't stop due to an error
        pass

# Continuous monitoring loop (with countdown every 60 seconds)
while True:
    try:
        monitor_feature_layer()
    except Exception as e:
        print(f"Error during monitoring loop: {e}")
    
    # Countdown before the next check
    countdown = 60
    while countdown > 0:
        print(f"Waiting {countdown} seconds before the next check...", end="\r")
        time.sleep(1)
        countdown -= 1

    print()  # Newline after countdown
