import pandas as pd
from transformations import LoggedDataFrameTransformer
import visualizations

# Create an instance of the LoggedDataFrameTransformer
transformer = LoggedDataFrameTransformer('transform_logs.db')
transformer.log_dataframe_methods()

# Create a LoggedDataFrame
data = {
    'Name': ['Alice', 'Bob', 'Charlie', 'David'],
    'Age': [25, 30, 35, 40],
    'City': ['New York', 'Chicago', 'San Francisco', 'Boston'],
    'Salary': [50000, 60000, 70000, 80000]
}
df = transformer.LoggedDataFrame(data, transformer=transformer)

# Perform transformations
df = df.drop(['City'], axis=1)

# Get the log history
log_history = df.get_log_history()

# Save the log history to a JSON file
transformer.save_log_history(log_history, 'log_history.json')
print("Log history JSON saved as 'log_history.json'. Please download it.")

# Visualize the transformation logs
visualizations.visualize_transformations(transformer.conn)
visualizations.visualize_interactive_data_lineage(transformer.conn)