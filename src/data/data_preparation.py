import pandas as pd
from src.data.utils import analyze_dataframe, correlated_features

def feature_selection(dataframe: pd.DataFrame, min_percentage_threshold: float = 95.0, correlated_count_threshold: float = None):

    # minimum analysis and summary stats excluding the minimum
    df_analysis = analyze_dataframe(df=dataframe)
    # select features to keep based on the share of the minimum value
    keep_features = df_analysis[df_analysis['min_percentage'] < min_percentage_threshold]['feature'].to_list()

    # get number of correlations per feature
    df_analysis = correlated_features(dataframe = dataframe[keep_features], corr_threshold = 0.8)
    # set correlated_count_threshold to half of the total number of features
    if correlated_count_threshold == None:
        correlated_count_threshold = df_analysis.shape[0]//2
    # select features to keep based on the total number of features correlated with
    keep_features = df_analysis[df_analysis['correlated_count'] < correlated_count_threshold]['feature'].to_list()
    
    return keep_features