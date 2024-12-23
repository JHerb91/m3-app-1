from ._anvil_designer import Form1Template
from anvil import *
import anvil.server  # Add this import

class Form1(Form1Template):
    def __init__(self, **properties):
        self.init_components(**properties)
        # Create the monitoring control components directly
        self.create_monitoring_control()

    def create_monitoring_control(self):
        # Create a column panel
        panel = ColumnPanel()
        
        # Create the buttons and label
        start_button = Button(text="Start Monitoring", enabled=True)
        status_label = Label(text="Status: Click Refresh to check", background='#ffffff')
        refresh_button = Button(text="Refresh Status")
        
        # Add click event handlers
        start_button.set_event_handler('click', self.start_button_click)
        refresh_button.set_event_handler('click', self.refresh_button_click)
        
        # Add components to panel
        panel.add_component(start_button)
        panel.add_component(status_label)
        panel.add_component(refresh_button)
        
        # Store references
        self.start_button = start_button
        self.status_label = status_label
        
        # Add panel to main form
        self.column_panel_1.add_component(panel)

    def refresh_status(self):
        with anvil.server.no_loading_indicator:
            try:
                is_running = anvil.server.call('get_monitoring_status')
                self.status_label.text = "Status: Running" if is_running else "Status: Not Running"
                self.status_label.background = '#a0ffa0' if is_running else '#ffa0a0'
                self.start_button.enabled = not is_running
            except Exception as e:
                print(f"Error checking status: {e}")
                self.status_label.text = "Status: Error checking"
                self.status_label.background = '#ffff00'
                self.start_button.enabled = True

    def start_button_click(self, **event_args):
        self.status_label.text = "Starting monitoring..."
        try:
            success, message = anvil.server.call('start_monitoring')
            if success:
                alert("Monitoring service started successfully")
            else:
                alert(f"Error: {message}")
            self.refresh_status()
        except Exception as e:
            print(f"Error starting monitoring: {e}")
            alert("Error starting monitoring service")
            self.refresh_status()

    def refresh_button_click(self, **event_args):
        self.status_label.text = "Checking status..."
        self.refresh_status()