import anvil.server
import anvil.tables
from anvil.tables import app_tables
import time

# Function to retrieve the last processed edit date from the database
def get_last_processed_edit_date_from_database():
    try:
        # Assuming 'processed_updates' is your table
        record = app_tables.processed_updates.get()  # Get the last record or adjust as needed
        if record:
            return record['last_processed_edit_date']
        else:
            return None  # No record found
    except Exception as e:
        print(f"Error retrieving last processed edit date: {e}")
        return None


# Function to save updates to the table
def save_updates_to_database(updates):
    try:
        for update in updates:
            # Assuming 'processed_updates' table and relevant fields
            app_tables.processed_updates.add_row(
                globalid=update.get('globalid'),
                last_processed_edit_date=update.get('last_processed_edit_date'),
                edit_date=update.get('edit_date')
            )
        print(f"Saved {len(updates)} updates to the database.")
    except Exception as e:
        print(f"Error saving updates to table: {e}")


# Function to fetch feature data (use your own logic here to query the external service)
def fetch_feature_data(last_processed_edit_date):
    try:
        # Here, you would implement the logic to fetch data, for example:
        # query the external service using last_processed_edit_date
        print("Fetching feature data...")

        # Sample data (replace with actual logic)
        updates = [
            {
                'globalid': '123',
                'last_processed_edit_date': time.time(),
                'edit_date': time.time()
            },
            {
                'globalid': '456',
                'last_processed_edit_date': time.time(),
                'edit_date': time.time()
            }
        ]
        print(f"Found {len(updates)} updates.")
        return updates
    except Exception as e:
        print(f"Error during feature data fetch: {e}")
        return []


# Function to monitor updates (to be triggered manually)
def monitor_updates():
    # Step 1: Get the last processed edit date
    last_processed_edit_date = get_last_processed_edit_date_from_database()
    print(f"Last processed edit date: {last_processed_edit_date}")

    # Step 2: Fetch feature data based on the last processed edit date
    updates = fetch_feature_data(last_processed_edit_date)

    # Step 3: Save the updates to the database
    if updates:
        print(f"Saving {len(updates)} updates to table...")
        save_updates_to_database(updates)
    else:
        print("No updates to save.")


# Start monitoring updates when a button is clicked
@anvil.server.callable
def start_monitoring():
    monitor_updates()
    return "Monitoring completed, updates have been processed!"
