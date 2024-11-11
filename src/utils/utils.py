import pandas as pd


def downcast_cols(dataframe: pd.DataFrame):
    """ downcast floats & integers to the smallest possible type """

    fcols = dataframe.select_dtypes('float').columns
    icols = dataframe.select_dtypes('integer').columns

    dataframe[fcols] = dataframe[fcols].apply(pd.to_numeric, downcast='float')
    dataframe[icols] = dataframe[icols].apply(pd.to_numeric, downcast='integer')
        
    return dataframe