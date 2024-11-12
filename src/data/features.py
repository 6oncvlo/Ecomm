import pandas as pd
import numpy as np
from src.utils.utils import downcast_cols

def visitor_features(data: dict):
    """Generate visitor-level features based on event data."""
    
    # Step 1: Count event types (view, add to cart, transaction) per visitor
    event_counts = (
        data['events']
        .groupby(['visitorid', 'event'], observed=True)
        .size()
        .unstack(fill_value=0)
        .rename(columns={
            'view': 'num_views', 
            'addtocart': 'num_add_to_cart', 
            'transaction': 'num_items_bought'
        })
        .astype('int32')
    )
    
    # Step 2: Calculate total records and unique transactions per visitor
    event_counts['total_events'] = event_counts.sum(axis=1).astype('int32')
    event_counts['num_transactions'] = (
        data['events']
        .loc[data['events']['transactionid'] != -1]
        .groupby('visitorid', observed=True)['transactionid']
        .nunique()
        .reindex(event_counts.index, fill_value=0)
        .astype('int32')
    )

    # Step 3: Calculate time-based statistics (min, mean, max, std) for consecutive events
    valid_visitors = event_counts.loc[event_counts[['num_views', 'num_add_to_cart', 'num_transactions']].gt(1).any(axis=1)].index
    time_data = data['events'][data['events']['visitorid'].isin(valid_visitors)].copy()
    
    time_data['time_diff_seconds'] = (
        time_data.groupby(['visitorid', 'event'], observed=True)['timestamp']
        .diff()
        .dt.total_seconds()
    ).dropna()
    
    time_stats = (
        time_data.groupby(['visitorid', 'event'], observed=True)['time_diff_seconds']
        .agg(['min', 'mean', 'max', 'std'])
        .unstack('event')
        .fillna(-1)
        .round(1)
    )
    time_stats.columns = [f"{stat}_{event}_delta" for stat in ['min', 'mean', 'max', 'std'] for event in ['addtocart', 'transaction', 'view']]

    # Merge time stats into main output
    output = event_counts.join(time_stats, how='left').fillna(-1)

    # Step 4: Generate hourly and daily activity features
    data['events']['hour_range'] = pd.cut(
        data['events']['timestamp'].dt.hour, 
        bins=[0, 6, 12, 18, 24], 
        labels=['0006h', '0612h', '1218h', '1824h'], 
        right=False
    )
    data['events']['day_of_week'] = data['events']['timestamp'].dt.day_name().str.lower()

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

    # Merge hourly and daily activity into the main output
    output = output.join([hourly_activity, daily_activity], how='left').fillna(0)

    # Step 5: Calculate burst activity (views per minute) and repetitive behavior
    data['events']['minute'] = data['events']['timestamp'].dt.floor('T')
    views_per_min = (
        data['events'][data['events']['event'] == 'view']
        .groupby(['visitorid', 'minute'])
        .size()
        .reset_index(name='views_per_minute')
    )
    max_views_per_min = views_per_min.groupby('visitorid')['views_per_minute'].max()
    output['max_views_per_minute'] = max_views_per_min
    output['burst_activity_flag'] = (max_views_per_min > 10).astype(int)

    # Calculate repetitive behavior by identifying consecutive views on the same item
    data['events']['next_itemid'] = data['events'].groupby('visitorid')['itemid'].shift(-1)
    data['events']['is_repetitive'] = (
        (data['events']['event'] == 'view') & 
        (data['events']['itemid'] == data['events']['next_itemid'])
    ).astype(int)
    repetitive_counts = data['events'].groupby('visitorid')['is_repetitive'].sum()
    output['repetitive_action_count'] = repetitive_counts
    output['cyclic_behavior_flag'] = (repetitive_counts > 5).astype(int)

    # Step 6: Event sequence analysis (e.g., view → add to cart → transaction)
    data['events']['next_event'] = data['events'].groupby('visitorid')['event'].shift(-1)
    data['events']['second_next_event'] = data['events'].groupby('visitorid')['event'].shift(-2)
    data['events']['sequence_flag'] = (
        (data['events']['event'] == 'view') & 
        (data['events']['next_event'] == 'addtocart') & 
        (data['events']['second_next_event'] == 'transaction')
    ).astype(int)
    output['view_add_to_cart_transaction_count'] = data['events'].groupby('visitorid')['sequence_flag'].sum()

    # Step 7: Downcast columns to reduce memory usage
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