import pandas as pd

def predictions_correction(dataframe: pd.DataFrame):

    # anomalous points with positive number of transactions should be non-anomalous
    dataframe.loc[
        (dataframe['anomaly_label']==1)
        & (dataframe['num_transactions']>0)
        , 'anomaly_label'] = 1
    
    return dataframe