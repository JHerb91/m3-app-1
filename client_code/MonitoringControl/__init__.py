from ._anvil_designer import MonitoringControlTemplate
from anvil import *
import anvil.server

class MonitoringControl(MonitoringControlTemplate):
    def __init__(self, **properties):
        self.init_components(**properties)
        self.refresh_status()

    def refresh_status(self):
        is_running = anvil.server.call('get_monitoring_status')
        self.status_label.text = "Status: Running" if is_running else "Status: Not Running"
        self.status_label.background = '#a0ffa0' if is_running else '#ffa0a0'
        self.start_button.enabled = not is_running

    def start_button_click(self, **event_args):
        success, message = anvil.server.call('start_monitoring')
        if success:
            alert("Monitoring service started successfully")
        else:
            alert(f"Error: {message}")
        self.refresh_status()

    def refresh_button_click(self, **event_args):
        self.refresh_status()