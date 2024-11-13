import pandas as pd
from src.utils.utils import downcast_cols


def load_data(data_paths: dict):

    data = {
        table_name: pd.read_csv(table_path) for table_name, table_path in data_paths.items()
    }
    return data

def prepare_data(data: dict, config: dict):

    # optimize events dataset
    # convert timestamp to Unix format and event type to a category
    data['events']['timestamp'] = pd.to_datetime(data['events']['timestamp'], unit='ms')
    data['events']['event'] = data['events']['event'].astype('category')
    # sort events by visitor and timestamp and define sessions based on 30min duration
    data['events'] = data['events'].sort_values(by=['visitorid', 'timestamp'])
    data['events']['session_gap'] = data['events'].groupby('visitorid')['timestamp'].diff().dt.total_seconds() > config['session_duration']
    data['events']['session_id'] = data['events'].groupby('visitorid')['session_gap'].cumsum()
    data['events'].drop(columns=['session_gap'], inplace=True)
    # downcast integers to the smallest possible type
    data['events']['transactionid'] = data['events']['transactionid'].fillna(value=-1).astype('int64')
    data['events'] = downcast_cols(dataframe = data['events'])

    return data