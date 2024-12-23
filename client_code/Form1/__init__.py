from ._anvil_designer import Form1Template
from anvil import *

class Form1(Form1Template):
    def __init__(self, **properties):
        self.init_components(**properties)
        # Get the form class
        monitoring_form = app.get_form('MonitoringControl')
        # Add it to the content panel
        self.content_panel.add_component(monitoring_form())