import datetime
import time
import anvil.http
import json
import anvil.server

# Define the UTC offset for Eastern Time
EASTERN_TIME_OFFSET = datetime.timedelta(hours=-5)  # Eastern Time is UTC-5

# Track last check time and if processing is already in progress
last_check_time = datetime.datetime.utcnow() + EASTERN_TIME_OFFSET  # Adjust last check time to Eastern Time
is_processing = False  # Flag to prevent multiple calls

def monitor_feature_layer():
    global last_check_time  # Preserve timestamp across runs
    global is_processing     # Prevent repeated processing

    if is_processing:
        return  # Skip if processing is already in progress

    try:
        is_processing = True  # Set flag to true when processing starts
        print(f"Last check time (Eastern Time): {last_check_time}")

        # Query URL for fetching features
        query_url = f"https://services.arcgis.com/rD2ylXRs80UroD90/arcgis/rest/services/Project_Tracker_View_Layer/FeatureServer/0/query?where=1=1&outFields=*&f=json"
        
        print(f"Sending request to {query_url}")
        response = anvil.http.request(query_url, method="GET")

        # Decode response
        raw_data = json.loads(response.get_bytes().decode("utf-8"))
        print("Query successful. Processing features...")

        # Check if features are present
        if "features" in raw_data and raw_data["features"]:
            updates_found = False  # Track updates

            for feature in raw_data["features"]:
                attributes = feature["attributes"]

                # Extract EditDate and convert it to Eastern Time
                edit_date = attributes.get("EditDate", None)
                if edit_date:
                    # Convert edit_date (UTC timestamp) to datetime in UTC
                    readable_edit_date = datetime.datetime.utcfromtimestamp(edit_date / 1000)
                    # Convert to Eastern Time
                    readable_edit_date_et = readable_edit_date + EASTERN_TIME_OFFSET

                    # 1-minute buffer for time comparison
                    tolerance_time = last_check_time - datetime.timedelta(minutes=1)

                    # Process only updated records
                    if readable_edit_date_et > tolerance_time:
                        updates_found = True
                        print(f"Update detected at {readable_edit_date_et}!")

                        # Prepare payload for webhook
                        payload = {
                            "edit_date": readable_edit_date_et.strftime('%Y-%m-%d %H:%M:%S'),
                            "globalid": attributes.get("globalid"),
                            "job_number": attributes.get("job_number"),
                            "job_name": attributes.get("job_name")
                        }

                        # Convert the payload to a string for JSON pass-through
                        payload_string = json.dumps(payload)

                        # Send payload to webhook as a string
                        webhook_response = anvil.http.request(
                            "https://hook.us2.make.com/m7lwew8alckuusm2dnn3d7tkvqnum2j3",  # Replace with your webhook URL
                            method="POST",
                            data=payload_string,  # Send data as a string, as per Make's pass-through configuration
                            headers={"Content-Type": "application/json"}
                        )

                        # Debug the response from the webhook
                        response_body = webhook_response.get_bytes().decode("utf-8")
                        print(f"Webhook Response Body: {response_body}")

                        # Check if the response is successful by checking the content
                        if "error" in response_body.lower():
                            print(f"Webhook error: {response_body}")
                        else:
                            print("Webhook response successfully received")

                        # Now call the Anvil server-side function to add the record to the table
                        try:
                            result = anvil.server.call('add_record_to_processed_updates',
                                                      attributes.get("globalid"),
                                                      attributes.get("job_number"),
                                                      attributes.get("job_name"),
                                                      readable_edit_date_et)
                            print(f"Record added: {result}")
                        except Exception as e:
                            print(f"Error adding record to processed_updates: {e}")

        else:
            print("No features found in query response.")

        # Update last check time (set to Eastern Time)
        last_check_time = datetime.datetime.utcnow() + EASTERN_TIME_OFFSET  # Adjust to Eastern Time

    except Exception as e:
        print(f"Error during monitoring: {e}")

    finally:
        is_processing = False  # Reset the flag to allow future processing

# Continuous monitoring loop (every 60 seconds)
while True:
    monitor_feature_layer()
    print("Waiting 60 seconds before the next check...")
    time.sleep(60)
