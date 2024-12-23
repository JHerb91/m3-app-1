from ._anvil_designer import Form1Template
from anvil import *

class Form1(Form1Template):
    def __init__(self, **properties):
        self.init_components(**properties)
        # Import MonitoringControl here to avoid circular imports
        from .MonitoringControl import MonitoringControl
        # Add the MonitoringControl to the content_panel
        self.content_panel.add_component(MonitoringControl())