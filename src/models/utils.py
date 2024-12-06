import pandas as pd
import mlflow
import os

def predictions_correction(dataframe: pd.DataFrame):

    # anomalous points with positive number of transactions should be non-anomalous
    dataframe.loc[
        (dataframe['anomaly_label']==1)
        & (dataframe['num_transactions']>0)
        , 'anomaly_label'] = 1
    
    return dataframe

def log_artifact(artifact_path: str, df: pd.DataFrame =None, df_name: str = None, image_name : str = None):
    """Save dataframe or image, log it as an artifact, and then delete the file."""
    if df is not None:
        df.to_csv(f'{df_name}.csv')
        mlflow.log_artifact(f'{df_name}.csv', artifact_path=artifact_path)
        os.remove(f'{df_name}.csv')
    elif image_name is not None:
        mlflow.log_artifact(f'{image_name}.png', artifact_path=artifact_path)
        os.remove(f'{image_name}.png')