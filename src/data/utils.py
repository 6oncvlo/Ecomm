import pandas as pd
import numpy as np

def downcast_cols(dataframe: pd.DataFrame):
    """Downcast floats & integers to the smallest possible type."""

    fcols = dataframe.select_dtypes('float').columns
    icols = dataframe.select_dtypes('integer').columns

    dataframe[fcols] = dataframe[fcols].apply(pd.to_numeric, downcast='float')
    dataframe[icols] = dataframe[icols].apply(pd.to_numeric, downcast='integer')
        
    return dataframe

def normalization(dataframe: pd.DataFrame, method: str = 'z_score'):

    if method=='z_score':
        dataframe = (dataframe - dataframe.mean())/dataframe.std()
    elif method=='min_max':
        dataframe = (dataframe-dataframe.min())/(dataframe.max()-dataframe.min())
    
    return dataframe

def analyze_dataframe(df: pd.DataFrame):
    result = []
    for col in df.columns:
        # Step 1: Count occurrences and percentage of the minimum value
        min_value = df[col].min()
        min_count = (df[col] == min_value).sum()
        min_percentage = ((min_count / len(df))*100).round(2)

        # Step 2: Filter out rows with the minimum value
        filtered_data = df[df[col] != min_value][col]

        # Calculate summary statistics
        summary_stats = {
            "min": filtered_data.min(),
            "q1": filtered_data.quantile(0.25),
            "median": filtered_data.median(),
            "mean": filtered_data.mean(),
            "q3": filtered_data.quantile(0.75),
            "max": filtered_data.max(),
            "std_dev": filtered_data.std(),
            "variance": filtered_data.var()
        }

        # Append results to the list
        result.append({
            "feature": col,
            "min_value": min_value,
            "min_count": min_count,
            "min_percentage": min_percentage,
            **summary_stats
        })

    # Create a concise dataframe for analysis
    return pd.DataFrame(result).sort_values(by='min_count').reset_index(drop=True)

def correlated_features(dataframe: pd.DataFrame, corr_threshold: float = 0.8):

    # Create a boolean mask for correlations above the threshold
    correlation_matrix = dataframe.corr()
    mask = (correlation_matrix.abs() > corr_threshold) & (correlation_matrix != 1.0)

    # Count the number of correlations above the threshold for each feature
    correlation_counts = mask.sum(axis=1)

    # Create the result table
    return pd.DataFrame({
        'feature': correlation_counts.index,
        'correlated_count': correlation_counts.values
        }).sort_values(by='correlated_count', ascending=False).reset_index(drop=True)