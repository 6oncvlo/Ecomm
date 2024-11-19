import pandas as pd
import numpy as np

def normalization(dataframe: pd.DataFrame, method: str = 'z_score'):

    if method=='z_score':
        dataframe = (dataframe - dataframe.mean())/dataframe.std()
    elif method=='min_max':
        dataframe = (dataframe-dataframe.min())/(dataframe.max()-dataframe.min())
    
    return dataframe