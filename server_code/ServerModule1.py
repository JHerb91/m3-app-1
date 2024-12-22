import datetime
import time
import anvil.http
import json

# Hosted feature layer URL
feature_layer_url = "https://services.arcgis.com/rD2ylXRs80UroD90/arcgis/rest/services/Project_Tracker_View_Layer/FeatureServer/0"

# Webhook URL
make_webhook_url = "https://hook.us2.make.com/ij23k1z88l9ie4pmnv421ylnpljzaz8b"

# Track last check time
last_check_time = datetime.datetime.utcnow()

def monitor_feature_layer():
    global last_check_time  # Preserve timestamp across runs

    try:
        print(f"Last check time (UTC): {last_check_time}")

        # Query URL for fetching features
        query_url = f"{feature_layer_url}/query?where=1=1&outFields=*&f=json"

        print(f"Sending request to {query_url}")
        response = anvil.http.request(
            query_url,
            method="GET"
        )

        # Decode response
        raw_data = json.loads(response.get_bytes().decode("utf-8"))
        print(f"Query successful. Processing features...")

        # Check if features are present
        if "features" in raw_data and raw_data["features"]:
            updates_found = False  # Track updates

            for feature in raw_data["features"]:
                attributes = feature["attributes"]

                # Extract EditDate and compare with last check time
                edit_date = attributes.get("EditDate", None)
                if edit_date:
                    readable_edit_date = datetime.datetime.utcfromtimestamp(edit_date / 1000)

                    # 1-minute buffer for time comparison
                    tolerance_time = last_check_time - datetime.timedelta(minutes=1)

                    # Process only updated records
                    if readable_edit_date > tolerance_time:
                        updates_found = True
                        print(f"Update detected at {readable_edit_date}!")

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

                        # Handle webhook errors
                        if webhook_response.status != 200:
                            print(f"Webhook error. Status code: {webhook_response.status}")

            # Print only if no updates were detected
            if not updates_found:
                print("No updates found.")

        else:
            print("No features found in query response.")

        # Update last check time
        last_check_time = datetime.datetime.utcnow()

    except Exception as e:
        print(f"Error during monitoring: {e}")

# Continuous monitoring loop (every 60 seconds)
while True:
    monitor_feature_layer()
    print("Waiting 60 seconds before the next check...")
    time.sleep(60)
