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
import time
import sys

# Constants
FEATURE_LAYER_URL = "https://services.arcgis.com/rD2ylXRs80UroD90/arcgis/rest/services/Project_Tracker_View_Layer/FeatureServer/0"
MAKE_WEBHOOK_URL = "https://hook.us2.make.com/77sbapae8w8ih3ymbcrc26ft9af1vi0s"
EASTERN_TZ = ZoneInfo("America/New_York")

# Add this near the top with your other constants
_IS_STARTING = False

# Constants remain the same...

@anvil.server.callable
def startup():
    """Function that runs when the app starts"""
    # ... startup function remains the same ...

@anvil.server.callable
def cleanup_old_logs(keep_hours=24):
    """Clean up old logs, keeping only the most recent hours specified"""
    # ... cleanup_old_logs function remains the same ...

def server_log(message, log_type="INFO"):
    """Helper function to write to server logs table"""
    try:
        # Clean up old logs if we have too many rows
        if len(app_tables.server_logs.search()) > 1000:  # Adjust this number as needed
            cleanup_old_logs()
        
        # Add to table
        app_tables.server_logs.add_row(
            timestamp=datetime.datetime.now(EASTERN_TZ),
            message=str(message),
            type=log_type
        )
        
        # Try multiple output methods
        print(f"[{log_type}] {message}", flush=True)
        print(f"[{log_type}] {message}", file=sys.stderr, flush=True)
        
        # Force Python's stdout to flush
        sys.stdout.flush()
        sys.stderr.flush()
        
    except Exception as e:
        print(f"Logging error: {e}", flush=True)

# ... rest of the file remains the same ...

@anvil.server.callable
def simple_test():
    """Simple test to verify server output"""
    server_log("=== Starting Simple Test ===")
    server_log("If you see this in the Server Console, printing works!")
    server_log("Testing background task...")
    
    @anvil.server.background_task
    def inner_test():
        server_log("Background task started")
        server_log("Waiting 2 seconds...")
        time.sleep(2)
        server_log("Background task complete")
        return True
    
    # Launch the background task
    inner_test()
    return "Test started"

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
                server_log(f"Synced new record for job number: {job_number}")
            
    except Exception as e:
        server_log(f"Error syncing record: {e}", "ERROR")
        server_log(f"Full error details: {str(e)}", "ERROR")

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
        server_log(f"Successfully sent webhook for job: {job_name}")
        server_log(f"Payload sent: {json_str}")
        return True
    except Exception as webhook_error:
        server_log(f"Error sending webhook: {webhook_error}", "ERROR")
        server_log(f"Attempted payload: {json_str}", "ERROR")
        return False

def check_for_updates(feature, existing_record):
    """Check if a feature has been updated compared to our stored record"""
    if not existing_record:
        server_log("No existing record found - skipping update check")
        return False
        
    attributes = feature["attributes"]
    new_precon = attributes.get("precon_timestamp")
    job_number = attributes.get("job_number")
    job_name = attributes.get("job_name", "No Job Name")
    
    try:
        stored_precon = existing_record['precon_timestamp']
        
        server_log(f"\nDetailed comparison for {job_name} (#{job_number}):")
        server_log(f"  Stored in Anvil table:")
        server_log(f"    precon_timestamp: {stored_precon}")
        server_log(f"    As date: {datetime.datetime.fromtimestamp(stored_precon / 1000, EASTERN_TZ)}")
        server_log(f"  Coming from ArcGIS:")
        server_log(f"    precon_timestamp: {new_precon}")
        server_log(f"    As date: {datetime.datetime.fromtimestamp(new_precon / 1000, EASTERN_TZ)}")
        
        # Compare with just the stored timestamp
        if new_precon is not None and stored_precon is not None:
            stored_int = int(stored_precon)
            new_int = int(new_precon)
            
            # Only trigger update if new timestamp is more recent
            if new_int > stored_int:
                server_log(f"  ✓ Update detected - newer timestamp found")
                server_log(f"    From: {datetime.datetime.fromtimestamp(stored_int / 1000, EASTERN_TZ)}")
                server_log(f"    To: {datetime.datetime.fromtimestamp(new_int / 1000, EASTERN_TZ)}")
                server_log(f"    Raw values - Stored: {stored_int}, New: {new_int}")
                return True
            else:
                server_log(f"  × No update - new timestamp is not more recent")
                server_log(f"    Current: {datetime.datetime.fromtimestamp(stored_int / 1000, EASTERN_TZ)}")
                server_log(f"    Received: {datetime.datetime.fromtimestamp(new_int / 1000, EASTERN_TZ)}")
                return False
        else:
            server_log(f"  × Skipping - missing timestamp values")
            if new_precon is None:
                server_log("    New precon_timestamp is None")
            if stored_precon is None:
                server_log("    Stored precon_timestamp is None")
            return False
            
    except Exception as e:
        server_log(f"Error checking for updates: {e}", "ERROR")
        server_log(f"  × Skipping due to error", "ERROR")
        server_log(f"  Full error details: {str(e)}", "ERROR")
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
        
        # Log the response for debugging
        server_log(f"Response from ArcGIS: {response}")

        if "features" in response and response["features"]:
            updates_found = False
            
            server_log(f"\nProcessing {len(response['features'])} features...")
            
            # First, sync any new records without sending webhooks
            for feature in response["features"]:
                sync_to_processed_updates(feature)
            
            server_log("\nChecking for updates...")
            # Then check for actual updates
            for feature in response["features"]:
                attributes = feature["attributes"]
                job_number = attributes.get("job_number")
                job_name = attributes.get("job_name", "No Job Name")
                edit_date = attributes.get("EditDate")
                precon_timestamp = attributes.get("precon_timestamp")
                
                if not job_number:
                    server_log(f"Skipping record with no job number")
                    continue
                
                # Search for existing record
                existing_records = [r for r in app_tables.processed_updates.search() if r['job_number'] == job_number]
                existing_record = existing_records[0] if existing_records else None
                
                needs_update = check_for_updates(feature, existing_record)
                
                # Log before sending webhook
                server_log(f"Preparing to send webhook for job: {job_name} (#{job_number})")
                
                # Only proceed if we have an existing record and there's an actual update
                if existing_record and needs_update:
                    updates_found = True
                    readable_edit_date = datetime.datetime.fromtimestamp(edit_date / 1000, EASTERN_TZ)
                    server_log(f"\n→ Sending webhook for job: {job_name} (#{job_number})")
                    
                    if send_webhook_notification(job_name, readable_edit_date, attributes):
                        # Update our record only if webhook was successful
                        existing_record.update(
                            last_processed_edit_date=datetime.datetime.now(EASTERN_TZ),
                            edit_date=readable_edit_date,
                            precon_timestamp=precon_timestamp
                        )
                        server_log(f"✓ Successfully updated record for {job_name} (#{job_number})")
            
            if not updates_found:
                server_log("\nNo new updates found.")
        else:
            server_log("No features found in query response.")

    except Exception as e:
        server_log(f"Error during monitoring: {e}", "ERROR")
        server_log(f"Full error details: {str(e)}", "ERROR")

@anvil.server.background_task
def monitor_loop():
    """Background task to continuously monitor the feature layer"""
    server_log("Starting monitoring service...")
    server_log(f"Start time: {datetime.datetime.now(EASTERN_TZ)}")
    
    # Set a flag in the app tables to track the running state
    try:
        app_tables.monitoring_status.delete_all_rows()
        app_tables.monitoring_status.add_row(
            is_running=True,
            started_at=datetime.datetime.now(EASTERN_TZ)
        )
        server_log("Set monitoring status to running")
    except Exception as e:
        server_log(f"Error setting monitoring status: {e}", "ERROR")
        return
    
    # Main monitoring loop
    loop_count = 0
    while True:
        try:
            loop_count += 1
            server_log(f"\n=== Monitor Loop #{loop_count} ===")
            server_log(f"Current time: {datetime.datetime.now(EASTERN_TZ)}")
            
            monitor_feature_layer()
            
            server_log("\nWaiting 60 seconds before next check...")
            server_log(f"Next check at: {datetime.datetime.now(EASTERN_TZ) + datetime.timedelta(seconds=60)}")
            time.sleep(60)
            
        except Exception as e:
            server_log(f"Error in monitor loop: {e}", "ERROR")
            time.sleep(5)

@anvil.server.callable
def start_monitoring():
    """Start the monitoring background task"""
    try:
        # Check if already running by looking at the status table
        status_rows = list(app_tables.monitoring_status.search())
        
        # If there's an existing status row and it shows running
        if status_rows and status_rows[0]['is_running']:
            server_log("Monitoring service is already running")
            return True, "Monitoring service is already running"
        
        # Clear any old status rows and start fresh
        app_tables.monitoring_status.delete_all_rows()
        
        server_log("Starting monitoring service...")
        anvil.server.launch_background_task('monitor_loop')
        server_log("Monitoring service started")
        
        return True, "Monitoring service started successfully"
    except Exception as e:
        server_log(f"Error starting monitoring service: {e}", "ERROR")
        server_log(f"Full error details: {str(e)}", "ERROR")
        return False, f"Error starting monitoring service: {e}"

@anvil.server.callable
def get_monitoring_status():
    """Check if the monitoring service is running"""
    try:
        # Check the status table
        status_rows = list(app_tables.monitoring_status.search())
        if status_rows and status_rows[0]['is_running']:
            server_log("Monitoring service is running")
            return True
        else:
            server_log("No monitoring service found")
            return False
    except Exception as e:
        server_log(f"Error checking monitoring status: {e}", "ERROR")
        return False