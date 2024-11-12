import pandas as pd
from src.utils.utils import downcast_cols

def visitor_features(data: dict):
    """Generate visitor-level features."""

    # Step 1: Count event types (view, add to cart, transaction) per visitor
    output = (
        data['events']
        .groupby(['visitorid', 'event'], observed=True)
        .size()
        .unstack(fill_value=0)
        .rename(columns={'view': 'num_views', 'addtocart': 'num_a2cart', 'transaction': 'num_itemsbought'})
        .astype('int32')
    )
    
    # Step 2: Add a total records column per visitor
    output['records'] = output.sum(axis=1).astype('int32')

    # Step 3: Count unique transactions per visitor
    output['num_txs'] = (
        data['events']
        .loc[data['events']['transactionid'] != -1]
        .groupby('visitorid', observed=True)['transactionid']
        .nunique()
        .reindex(output.index, fill_value=0)
        .astype('int32')
    )

    # Step 4: Calculate time-based statistics (min, mean, max, std) for each consecutive event type
    valid_visitor_ids = output.loc[output[['num_views', 'num_a2cart', 'num_txs']].gt(1).any(axis=1)].index
    time_stats = data['events'].loc[data['events']['visitorid'].isin(valid_visitor_ids)].copy()
    
    time_stats['time_diff_seconds'] = (
        time_stats.groupby(['visitorid', 'event'], observed=True)['timestamp']
        .diff()
        .dt.total_seconds()
    )
    time_stats.dropna(inplace=True)

    # Aggregate time statistics for each event type
    time_stats = (
        time_stats.groupby(['visitorid', 'event'], observed=True)['time_diff_seconds']
        .agg(['min', 'mean', 'max', 'std'])
        .fillna(-1)
        .round(1)
        .unstack('event')
    )

    # Rename columns for time-based statistics
    time_stats.columns = [
        f"{stat}_{event_type}_delta" 
        for stat in ['min', 'mean', 'max', 'std'] 
        for event_type in ['addtocart', 'transaction', 'view']
    ]

    # Merge time statistics into the main output DataFrame
    output = output.join(time_stats, how='left').fillna(-1)

    # Step 5: Extract hourly and daily features
    data['events']['hour_range'] = pd.cut(
        data['events']['timestamp'].dt.hour,
        bins=[0, 6, 12, 18, 24],
        labels=['0006h', '0612h', '1218h', '1824h'],
        right=False
    )
    data['events']['day_of_week'] = data['events']['timestamp'].dt.day_name().str.lower()

    # Create pivot tables for hourly and daily activity, adding prefixes to column names
    hourly_activity = (
        data['events']
        .pivot_table(index='visitorid', columns='hour_range', aggfunc='size', fill_value=0, observed=True)
        .add_prefix('numevents_')
    )
    
    daily_activity = (
        data['events']
        .pivot_table(index='visitorid', columns='day_of_week', aggfunc='size', fill_value=0, observed=True)
        .add_prefix('numevents_')
    )

    # Merge hourly and daily activity features into the main output DataFrame
    output = output.join(hourly_activity, how='left').fillna(0)
    output = output.join(daily_activity, how='left').fillna(0)

    # Step 6: Downcast integer columns for efficient storage
    output = downcast_cols(output)

    return output


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