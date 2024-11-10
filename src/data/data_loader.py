import pandas as pd

def load_data(data_paths: dict):

    data = {
        table_name: pd.read_csv(table_path) for table_name, table_path in data_paths.items()
    }
    return data

def prepare_data(data: dict):

    # optimize events dataset
    # convert timestamp to Unix format and event type to a category
    data['events']['timestamp'] = pd.to_datetime(data['events']['timestamp'], unit='ms')
    data['events']['event'] = data['events']['event'].astype('category')
    # downcast integers to the smallest possible type
    data['events']['transactionid'] = data['events']['transactionid'].fillna(value=-1).astype('int64')
    for col in ['visitorid', 'itemid', 'transactionid']:
        data['events'][col] = pd.to_numeric(data['events'][col], downcast='integer')

    return data