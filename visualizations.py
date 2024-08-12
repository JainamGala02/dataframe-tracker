import pandas as pd
import json
import matplotlib.pyplot as plt
import seaborn as sns
from graphviz import Digraph
from IPython.display import Image
from pyvis.network import Network
import IPython


def visualize_transformations(conn):
    query = "SELECT * FROM transform_logs"
    df_logs = pd.read_sql_query(query, conn)

    df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp'])

    plt.figure(figsize=(12, 8))

    plt.subplot(2, 1, 1)
    sns.lineplot(x='timestamp', y='transform_name', data=df_logs, marker='o')
    plt.title('Timeline of Transformations')
    plt.xlabel('Timestamp')
    plt.ylabel('Transform Name')
    plt.grid(True)

    plt.subplot(2, 1, 2)
    sns.countplot(y='transform_name', data=df_logs, order=df_logs['transform_name'].value_counts().index)
    plt.title('Count of Each Transformation Type')
    plt.xlabel('Count')
    plt.ylabel('Transform Name')
    plt.grid(axis='x')

    plt.tight_layout()
    plt.show()

def visualize_interactive_data_lineage(conn):
    with open('log_history.json', 'r') as f:
        log_history = json.load(f)

    dot = Digraph()
    dot.attr(rankdir='TB')  # Top to bottom layout

    net = Network(height='800px', width='100%', directed=True, notebook=True, cdn_resources='in_line')

    dot.node('0', 'Raw Data')
    net.add_node(0, label='Raw Data', title='Raw Data', size=20, color='#66c2a5')

    for i, entry in enumerate(log_history):
        transform_name = entry['transform_name']
        timestamp = entry['timestamp']
        transform_id = i + 1  # Node IDs start from 1
        parent_id = i if i > 0 else 0  # Parent node ID

        df_hash = entry['df_hash']
        df_args = entry['args']
        df_kwargs = entry['kwargs']

        node_label = f"Transformation Name: {transform_name}\nTimestamp: {timestamp}\nHash: {df_hash}\nArguments: {df_args}\nKeyword_Arguments: {df_kwargs}"
        dot.node(str(transform_id), node_label)
        net.add_node(transform_id, label=f"({transform_id}) {transform_name}", title=node_label, size=20, color='#ff9900')

        edge_label = f"{transform_name}\n{timestamp}"
        dot.edge(str(parent_id), str(transform_id), label=edge_label)
        net.add_edge(parent_id, transform_id)

    dot.render('lineage', format='png', cleanup=True)

    IPython.display.HTML(filename='lineage.html')