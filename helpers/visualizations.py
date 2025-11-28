# In app.py - Update the imports section
try:
    from helpers.data_cleaning import DataCleaner
    from helpers.metrics import LeadMetrics
    from helpers.visualizations import create_dashboards
except ImportError as e:
    print(f"Import error: {e}")
    print("Some helper files might be missing. Please check the helpers folder.")
    
    # Create dummy classes for testing
    class LeadMetrics:
        def __init__(self, *args, **kwargs):
            pass
        def calculate_all_metrics(self):
            return pd.DataFrame(), pd.DataFrame()
        def save_metrics(self, *args, **kwargs):
            pass
    
    def create_dashboards(*args, **kwargs):
        print("Visualizations module not available")