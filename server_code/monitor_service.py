import anvil.tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import datetime
from datetime import timezone
from zoneinfo import ZoneInfo
import json
from urllib.parse import quote
import anvil.http

# Constants
FEATURE_LAYER_URL = "https://services.arcgis.com/rD2ylXRs80UroD90/arcgis/rest/services/Project_Tracker_View_Layer/FeatureServer/0"
MAKE_WEBHOOK_URL = "https://hook.us2.make.com/77sbapae8w8ih3ymbcrc26ft9af1vi0s"
EASTERN_TZ = ZoneInfo("America/New_York")

def sync_to_processed_updates(feature):
    """Sync a feature to the processed_updates table without triggering webhook"""
    try:
        attributes = feature["attributes"]
        job_number = attributes.get("job_number")
        edit_date = attributes.get("EditDate")
        precon_timestamp = attributes.get("precon_timestamp")
        
        if edit_date and job_number:
            readable_edit_date = datetime.datetime.fromtimestamp(edit_date / 1000, EASTERN_TZ)
            
            # Search for existing record
            existing_records = [r for r in app_tables.processed_updates.search() if r['job_number'] == job_number]
            
            if not existing_records:
                # Add new record
                app_tables.processed_updates.add_row(
                    job_number=job_number,
                    last_processed_edit_date=datetime.datetime.now(EASTERN_TZ),
                    edit_date=readable_edit_date,
                    precon_timestamp=precon_timestamp
                )
                print(f"Synced new record for job number: {job_number}")
            
    except Exception as e:
        print(f"Error syncing record: {e}")
        print(f"Full error details: {str(e)}")

def send_webhook_notification(job_name, readable_edit_date, attributes):
    """Send notification to webhook"""
    # Convert precon_timestamp to readable date
    precon_timestamp = attributes.get("precon_timestamp")
    precon_date = datetime.datetime.fromtimestamp(precon_timestamp / 1000, EASTERN_TZ).strftime('%Y-%m-%d %H:%M:%S %Z') if precon_timestamp else None
    
    # Create data structure that Make can parse
    webhook_data = {
        "job_name": job_name,
        "job_number": attributes.get("job_number"),
        "precon_date": precon_date,
        "edit_date": readable_edit_date.strftime('%Y-%m-%d %H:%M:%S %Z'),
        "notification_time": datetime.datetime.now(EASTERN_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
    }
    
    try:
        # Convert to string and send as text
        json_str = json.dumps(webhook_data)
        webhook_response = anvil.http.request(
            MAKE_WEBHOOK_URL,
            method="POST",
            data=json_str,
            headers={
                'Content-Type': 'text/plain'
            }
        )
        print(f"Successfully sent webhook for job: {job_name}")
        print(f"Payload sent: {json_str}")
        return True
    except Exception as webhook_error:
        print(f"Error sending webhook: {webhook_error}")
        print(f"Attempted payload: {json_str}")
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
        print(f"  Stored in Anvil table:")
        print(f"    precon_timestamp: {stored_precon}")
        print(f"    As date: {datetime.datetime.fromtimestamp(stored_precon / 1000, EASTERN_TZ)}")
        print(f"  Coming from ArcGIS:")
        print(f"    precon_timestamp: {new_precon}")
        print(f"    As date: {datetime.datetime.fromtimestamp(new_precon / 1000, EASTERN_TZ)}")
        
        # Compare with just the stored timestamp
        if new_precon is not None and stored_precon is not None:
            stored_int = int(stored_precon)
            new_int = int(new_precon)
            
            # Only trigger update if new timestamp is more recent
            if new_int > stored_int:
                print(f"  ✓ Update detected - newer timestamp found")
                print(f"    From: {datetime.datetime.fromtimestamp(stored_int / 1000, EASTERN_TZ)}")
                print(f"    To: {datetime.datetime.fromtimestamp(new_int / 1000, EASTERN_TZ)}")
                print(f"    Raw values - Stored: {stored_int}, New: {new_int}")
                return True
            else:
                print(f"  × No update - new timestamp is not more recent")
                print(f"    Current: {datetime.datetime.fromtimestamp(stored_int / 1000, EASTERN_TZ)}")
                print(f"    Received: {datetime.datetime.fromtimestamp(new_int / 1000, EASTERN_TZ)}")
                return False
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
        print(f"  Full error details: {str(e)}")
        return False

def monitor_feature_layer():
    try:
        # Query all records
        where_clause = quote("1=1")
        query_url = f"{FEATURE_LAYER_URL}/query?where={where_clause}&outFields=*&f=json"
        
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
                    readable_edit_date = datetime.datetime.fromtimestamp(edit_date / 1000, EASTERN_TZ)
                    print(f"\n→ Sending webhook for job: {job_name} (#{job_number})")
                    
                    if send_webhook_notification(job_name, readable_edit_date, attributes):
                        # Update our record only if webhook was successful
                        existing_record.update(
                            last_processed_edit_date=datetime.datetime.now(EASTERN_TZ),
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

@anvil.server.background_task
def run_single_check():
    """Run a single check of the feature layer"""
    print(f"\n=== Running check at {datetime.datetime.now(EASTERN_TZ)} ===")
    monitor_feature_layer()
    return True

@anvil.server.callable
def start_monitoring():
    """Start the monitoring background task"""
    try:
        # Check if already running by looking at the status table
        status_rows = list(app_tables.monitoring_status.search())
        
        # If there's an existing status row and it shows running
        if status_rows and status_rows[0]['is_running']:
            print("Monitoring service is already running")
            return True, "Monitoring service is already running"
        
        # Clear any old status rows
        app_tables.monitoring_status.delete_all_rows()
        
        # Create new status row before starting task
        app_tables.monitoring_status.add_row(
            is_running=True,
            started_at=datetime.datetime.now(EASTERN_TZ)
        )
        
        # Run the first check immediately
        run_single_check.call_as_background_task()
        
        # Schedule recurring checks
        @anvil.server.background_task
        def schedule_checks():
            while True:
                try:
                    anvil.server.sleep(60)
                    run_single_check.call_as_background_task()
                except Exception as e:
                    print(f"Error in scheduler: {e}")
                    anvil.server.sleep(5)
        
        # Start the scheduler
        print("Starting monitoring scheduler...")
        schedule_checks.call_as_background_task()
        print("Monitoring service started")
        
        return True, "Monitoring service started successfully"
    except Exception as e:
        print(f"Error starting monitoring service: {e}")
        print(f"Full error details: {str(e)}")
        return False, f"Error starting monitoring service: {e}"

@anvil.server.callable
def get_monitoring_status():
    """Check if the monitoring service is running"""
    try:
        # Check the status table
        status_rows = list(app_tables.monitoring_status.search())
        if status_rows and status_rows[0]['is_running']:
            print("Monitoring service is running")
            return True
        else:
            print("No monitoring service found")
            return False
    except Exception as e:
        print(f"Error checking monitoring status: {e}")
        return False