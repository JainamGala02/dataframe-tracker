# -*- coding: utf-8 -*-
import pandas as pd
from functools import wraps
import sqlite3
import datetime
import pickle
import json
import hashlib

class LoggedDataFrameTransformer:
    def __init__(self, database_path):
        self.conn = sqlite3.connect(database_path)
        self.c = self.conn.cursor()
        self._create_table()
        self._is_logging = False

    def _create_table(self):
        self.c.execute('''CREATE TABLE IF NOT EXISTS transform_logs
                          (id INTEGER PRIMARY KEY AUTOINCREMENT, transform_name TEXT, timestamp TIMESTAMP, df_hash TEXT, args BLOB, kwargs BLOB, df_after BLOB)''')

    class LoggedDataFrame(pd.DataFrame):
        _metadata = ['_log_history']

        def __init__(self, data=None, *args, **kwargs):
            self.transformer = kwargs.pop('transformer', None)
            super().__init__(data, *args, **kwargs)
            self._log_history = []

        def get_log_history(self):
            return self._log_history

        @property
        def _constructor(self):
            return LoggedDataFrameTransformer.LoggedDataFrame

        def __getattr__(self, name):
            if name in self._internal_names_set:
                return object.__getattribute__(self, name)
            else:
                return object.__getattribute__(self, name)

    def log_transform(self, func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                if self.transformer and not getattr(self.transformer, '_is_logging', False):
                    self.transformer._is_logging = True
                    transform_name = func.__name__
                    df_hash = hashlib.sha256(pickle.dumps(self)).hexdigest()

                    try:
                        result = func(self, *args, **kwargs)
                    except Exception as e:
                        self.transformer._is_logging = False
                        raise e

                    if isinstance(result, pd.DataFrame):
                        result = self.transformer.LoggedDataFrame(result, transformer=self.transformer)

                    df_after = pickle.dumps(result)

                    self.transformer.c.execute("INSERT INTO transform_logs (transform_name, timestamp, df_hash, args, kwargs, df_after) VALUES (?, ?, ?, ?, ?, ?)",
                                                (transform_name, datetime.datetime.now(), df_hash, pickle.dumps(args), pickle.dumps(kwargs), df_after))
                    self.transformer.conn.commit()

                    result._log_history.append({
                        'transform_name': transform_name,
                        'timestamp': datetime.datetime.now().isoformat(),
                        'df_hash': df_hash,
                        'args': args,
                        'kwargs': kwargs
                    })
                    self.transformer._is_logging = False
                else:
                    result = func(self, *args, **kwargs)
                return result
            return wrapper

    def log_dataframe_methods(self):
        methods_to_log = ['drop', 'assign', 'sort_values', 'rename', 'cut']
        for method_name in methods_to_log:
            method = getattr(pd.DataFrame, method_name, getattr(pd, method_name, None))
            if method:
                setattr(self.LoggedDataFrame, method_name, self.log_transform(method))

    def convert_to_serializable(self, obj):
        if isinstance(obj, pd.Series):
            return obj.to_dict()
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict(orient='records')
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        else:
            return str(obj)

    def serialize_log_history(self, log_history):
        serializable_log_history = []
        for entry in log_history:
            serializable_entry = {key: self.convert_to_serializable(value) for key, value in entry.items()}
            serializable_log_history.append(serializable_entry)

        return json.dumps(serializable_log_history, indent=4)

    def save_log_history(self, log_history, file_path):
        log_history_json = self.serialize_log_history(log_history)
        with open(file_path, 'w') as f:
            f.write(log_history_json)

    def close_connection(self):
        self.conn.close()