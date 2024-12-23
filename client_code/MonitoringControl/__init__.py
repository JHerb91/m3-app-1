from ._anvil_designer import MonitoringControlTemplate
from anvil import *
import anvil.server

class MonitoringControl(MonitoringControlTemplate):
    def __init__(self, **properties):
        self.init_components(**properties)
        # Don't call refresh_status in init to avoid timeout
        self.start_button.enabled = True
        self.status_label.text = "Status: Click Refresh to check"
        self.status_label.background = '#ffffff'

    def refresh_status(self):
        try:
            is_running = anvil.server.call('get_monitoring_status')
            self.status_label.text = "Status: Running" if is_running else "Status: Not Running"
            self.status_label.background = '#a0ffa0' if is_running else '#ffa0a0'
            self.start_button.enabled = not is_running
        except:
            self.status_label.text = "Status: Error checking"
            self.status_label.background = '#ffff00'
            self.start_button.enabled = True

    def start_button_click(self, **event_args):
        try:
            success, message = anvil.server.call('start_monitoring')
            if success:
                alert("Monitoring service started successfully")
            else:
                alert(f"Error: {message}")
            self.refresh_status()
        except:
            alert("Error starting monitoring service")
            self.refresh_status()

    def refresh_button_click(self, **event_args):
        self.refresh_status()