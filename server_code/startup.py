import anvil.server
@anvil.server.background_task
ef _ensure_monitoring_running():
   """Background task to ensure monitoring stays running"""
   anvil.server.call('startup')
# Start the monitoring when server starts
ensure_monitoring_running()