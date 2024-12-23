from ._anvil_designer import Form1Template
from anvil import *
from .MonitoringControl import MonitoringControl

class Form1(Form1Template):
    def __init__(self, **properties):
        self.init_components(**properties)
        # Add the MonitoringControl to the content_panel
        self.content_panel.add_component(MonitoringControl())