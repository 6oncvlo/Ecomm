import pandas as pd
from src.utils.utils import downcast_cols

def visitor_features(data: dict):
    
    # count event types at visitorid level
    output = (
        data['events'].groupby(['visitorid', 'event']).size()
        .unstack(fill_value=0)
        .rename(columns={'view': 'num_views', 'addtocart': 'num_a2cart', 'transaction': 'num_itemsbought'})
        .astype('int32')
    )
    # add a column for total records per visitorid
    output['records'] = output.sum(axis=1).astype('int32')
    # count unique transactions and merge
    output['num_txs'] = (
        data['events'].loc[data['events']['transactionid'] != -1]
        .groupby('visitorid')['transactionid'].nunique()
        .reindex(output.index, fill_value=0).astype('int32')
    )


    # calculate time-based features for each event type
    # filter events where the count of each event type is greater than 1 for each visitorid
    valid_visitor_ids = output.loc[output[['num_views', 'num_a2cart', 'num_txs']].gt(1).any(axis=1)].index
    time_stats = data['events'][data['events']['visitorid'].isin(valid_visitor_ids)].copy()
    # calculate time differences for each event type
    time_stats['time_diff_seconds'] = time_stats.groupby(by=['visitorid', 'event'])['timestamp'].diff().dt.total_seconds()
    time_stats.dropna(inplace=True)
    # aggregate time statistics for each event type
    time_stats = (
        time_stats.groupby(['visitorid', 'event'])['time_diff_seconds'].agg(['min', 'mean', 'max', 'std'])
        .fillna(-1).round(1).unstack('event')
    )
    # set column naming
    time_stats.columns = [f"{stat}_{event_type}_delta" for stat in ['min', 'mean', 'max', 'std'] for event_type in ['addtocart', 'transaction', 'view']]
    # merge the time stats into the visitorid features
    output = output.join(time_stats, how='left').fillna(-1)

    """
    # aggregate per session
    session_features = (
        data['events'].loc[(data['events']['visitorid'].isin(output.loc[output['records'] > 2].index))].copy()
        .groupby(['visitorid', 'session_id', 'event']).size().unstack(fill_value=0)
        .groupby('visitorid').agg({
            'view': ['min', 'mean', 'max'],
            'addtocart': ['min', 'mean', 'max'],
            'transaction': ['min', 'mean', 'max']
            }).round(1)
            )
    # set column naming
    session_features.columns = ["_".join(col) for col in session_features.columns]
    # merge the time stats into the visitorid features
    output = output.join(session_features, how='left').fillna(-1)
    """

    # downcast integers for efficient storage
    output = downcast_cols(dataframe = output)
    
    return output