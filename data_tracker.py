import pandas as pd
import numpy as np
import json

class DataTracker(nbextension.Extension):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.log_file = open("data_tracker.log", "w")

    def on_change(self, change):
        if change["type"] == "cell_update":
            cell_content = change["content"]
            df = pd.read_json(cell_content)
            self.log_file.write(df.to_json())

def load_jupyter_extension(nb):
    nb.register_extension(DataTracker())

