import datetime
import time
import anvil.http
import json
import anvil.tables
from anvil.tables import app_tables
from datetime import timezone
from zoneinfo import ZoneInfo
from urllib.parse import quote

# Hosted feature layer URL
feature_layer_url = "https://services.arcgis.com/rD2ylXRs80UroD90/arcgis/rest/services/Project_Tracker_View_Layer/FeatureServer/0"

# Webhook URL
make_webhook_url = "https://hook.us2.make.com/m7lwew8alckuusm2dnn3d7tkvqnum2j3"

# Set up Eastern timezone
eastern_tz = ZoneInfo("America/New_York")

def sync_to_processed_updates(feature):
    """Sync a feature to the processed_updates table without triggering webhook"""
    try:
        attributes = feature["attributes"]
        job_number = attributes.get("job_number")
        edit_date = attributes.get("EditDate")
        precon_timestamp = attributes.get("precon_timestamp")
        
        if edit_date and job_number:
            readable_edit_date = datetime.datetime.fromtimestamp(edit_date / 1000, eastern_tz)
            
            # Search for existing record
            existing_records = [r for r in app_tables.processed_updates.search() if r['job_number'] == job_number]
            
            if not existing_records:
                # Add new record
                app_tables.processed_updates.add_row(
                    job_number=job_number,
                    last_processed_edit_date=datetime.datetime.now(eastern_tz),
                    edit_date=readable_edit_date,
                    precon_timestamp=precon_timestamp
                )
                print(f"Synced new record for job number: {job_number}")
            
    except Exception as e:
        print(f"Error syncing record: {e}")
        print(f"Full error details: {str(e)}")

def send_webhook_notification(job_name, readable_edit_date, attributes):
    """Send notification to webhook"""
    payload = {
        "edit_date": readable_edit_date.strftime('%Y-%m-%d %H:%M:%S %Z'),
        "job_name": job_name,
        "attributes": {
            "job_name": job_name,
            "job_number": attributes.get("job_number"),
            "precon_timestamp": attributes.get("precon_timestamp"),
            "edit_date": attributes.get("EditDate")
        }
    }
    
    try:
        json_payload = json.dumps(payload)
        webhook_response = anvil.http.request(
            make_webhook_url,
            method="POST",
            data=json_payload,
            headers={'Content-Type': 'application/json'}
        )
        print(f"Successfully sent webhook for job: {job_name}")
        return True
    except Exception as webhook_error:
        print(f"Error sending webhook: {webhook_error}")
        return False

def check_for_updates(feature, existing_record):
    """Check if a feature has been updated compared to our stored record"""
    if not existing_record:
        print("No existing record found - skipping update check")
        return False
        
    attributes = feature["attributes"]
    new_precon = attributes.get("precon_timestamp")
    job_number = attributes.get("job_number")
    job_name = attributes.get("job_name", "No Job Name")
    
    try:
        stored_precon = existing_record['precon_timestamp']
        
        print(f"\nDetailed comparison for {job_name} (#{job_number}):")
        print(f"  Stored precon_timestamp: {stored_precon} ({type(stored_precon)})")
        print(f"  New precon_timestamp: {new_precon} ({type(new_precon)})")
        
        # Convert both to strings for comparison, but only if they exist
        if new_precon is not None and stored_precon is not None:
            stored_str = str(int(stored_precon))  # Convert to int first to remove any decimal places
            new_str = str(int(new_precon))
            
            is_different = stored_str != new_str
            if is_different:
                print(f"  ✓ Update detected - timestamps are different")
                print(f"    Old: {stored_str}")
                print(f"    New: {new_str}")
            else:
                print(f"  × No update - timestamps match")
            return is_different
        else:
            print(f"  × Skipping - missing timestamp values")
            if new_precon is None:
                print("    New precon_timestamp is None")
            if stored_precon is None:
                print("    Stored precon_timestamp is None")
            return False
    except Exception as e:
        print(f"Error checking for updates: {e}")
        print(f"  × Skipping due to error")
    
    return False

def monitor_feature_layer():
    try:
        # Query all records
        where_clause = quote("1=1")
        query_url = f"{feature_layer_url}/query?where={where_clause}&outFields=*&f=json"
        
        response = anvil.http.request(
            query_url,
            method="GET",
            json=True
        )

        if "features" in response and response["features"]:
            updates_found = False
            
            print(f"\nProcessing {len(response['features'])} features...")
            
            # First, sync any new records without sending webhooks
            for feature in response["features"]:
                sync_to_processed_updates(feature)
            
            print("\nChecking for updates...")
            # Then check for actual updates
            for feature in response["features"]:
                attributes = feature["attributes"]
                job_number = attributes.get("job_number")
                job_name = attributes.get("job_name", "No Job Name")
                edit_date = attributes.get("EditDate")
                precon_timestamp = attributes.get("precon_timestamp")
                
                if not job_number:
                    print(f"Skipping record with no job number")
                    continue
                
                # Search for existing record
                existing_records = [r for r in app_tables.processed_updates.search() if r['job_number'] == job_number]
                existing_record = existing_records[0] if existing_records else None
                
                needs_update = check_for_updates(feature, existing_record)
                
                # Only proceed if we have an existing record and there's an actual update
                if existing_record and needs_update:
                    updates_found = True
                    readable_edit_date = datetime.datetime.fromtimestamp(edit_date / 1000, eastern_tz)
                    print(f"\n→ Sending webhook for job: {job_name} (#{job_number})")
                    
                    if send_webhook_notification(job_name, readable_edit_date, attributes):
                        # Update our record only if webhook was successful
                        existing_record.update(
                            last_processed_edit_date=datetime.datetime.now(eastern_tz),
                            edit_date=readable_edit_date,
                            precon_timestamp=precon_timestamp
                        )
                        print(f"✓ Successfully updated record for {job_name} (#{job_number})")
            
            if not updates_found:
                print("\nNo new updates found.")
        else:
            print("No features found in query response.")

    except Exception as e:
        print(f"Error during monitoring: {e}")
        print(f"Full error details: {str(e)}")

# Continuous monitoring loop
while True:
    monitor_feature_layer()
    print("\nWaiting 60 seconds before the next check...")
    time.sleep(60)