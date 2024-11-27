import pandas as pd
import numpy as np
from src.utils.utils import downcast_cols

def visitor_features(data: dict, config: dict, drop_bouncers: bool = False):
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
    
    # Step 3: Drop Bouncers - users that only viewed a single page once and didn't add to cart neither purchase
    if drop_bouncers:
        event_counts = event_counts.loc[
            ~(
                (event_counts.loc[:,'num_views']<=1)
                & (event_counts.loc[:,'num_add_to_cart']==0)
                & (event_counts.loc[:,'num_transactions']==0)
                )
                ,:]        

    # Step 4: Calculate time-based statistics (min, mean, max, std) for consecutive events
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

    # Step 5: Generate hourly and daily activity features
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

    # Step 6: Calculate burst activity (views per minute) and repetitive behavior
    data['events']['minute'] = data['events']['timestamp'].dt.floor('min')
    views_per_min = (
        data['events'][data['events']['event'] == 'view']
        .groupby(['visitorid', 'minute'])
        .size()
        .reset_index(name='views_per_minute')
    )
    max_views_per_min = views_per_min.groupby('visitorid')['views_per_minute'].max()
    max_views_per_min.name = 'max_views_per_min'

    # Merge burst activity into the main output
    output = output.join(max_views_per_min, how='left').fillna(-1)
    output['burst_activity_flag'] = (output['max_views_per_min'] > config['visitor_features']['thold_max_views_per_minute']).astype(int).fillna(-1)

    # Calculate repetitive behavior by identifying consecutive views on the same item
    data['events']['next_itemid'] = data['events'].groupby('visitorid')['itemid'].shift(-1)
    data['events']['is_repetitive'] = (
        (data['events']['event'] == 'view') & 
        (data['events']['itemid'] == data['events']['next_itemid'])
    ).astype(int)
    repetitive_counts = data['events'].groupby('visitorid')['is_repetitive'].sum()
    repetitive_counts.name = 'repetitive_action_count'

    # Merge repetitive behaviour into the main output
    output = output.join(repetitive_counts, how='left')
    output['cyclic_behavior_flag'] = (output['repetitive_action_count'] > config['visitor_features']['thold_repetitive_action']).astype(int)

    # Step 7: Event sequence analysis (e.g., view → add to cart → transaction)
    data['events']['next_event'] = data['events'].groupby('visitorid')['event'].shift(-1)
    data['events']['second_next_event'] = data['events'].groupby('visitorid')['event'].shift(-2)
    data['events']['sequence_flag'] = (
        (data['events']['event'] == 'view') & 
        (data['events']['next_event'] == 'addtocart') & 
        (data['events']['second_next_event'] == 'transaction')
    ).astype(int)
    view_add_to_cart_transaction_count = data['events'].groupby('visitorid')['sequence_flag'].sum()
    view_add_to_cart_transaction_count.name = 'view_add_to_cart_transaction_count'

    # Merge sequence steps into the main output
    output = output.join(view_add_to_cart_transaction_count, how='left')

    # Step 8: Compute metrics at the session level
    session_features = (
        data['events'].loc[
            (data['events']['visitorid'].isin(output.loc[output['total_events'] > 2].index))
            ].copy()
            .groupby(['visitorid', 'session_id', 'event'], observed=True).size().unstack(fill_value=0)
            .groupby('visitorid').agg({
                'view': ['min', 'mean', 'max'],
                'addtocart': ['min', 'mean', 'max'],
                'transaction': ['min', 'mean', 'max']
                }).round(1)
                )
    session_features.columns = ["_".join(col) for col in session_features.columns]
    
    # Merge the time stats into the main output
    # output = output.join(session_features, how='left').fillna(-1)

    # Step 9: Downcast columns to reduce memory usage
    output = downcast_cols(output)

    return output