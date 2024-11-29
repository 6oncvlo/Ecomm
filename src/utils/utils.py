import pandas as pd
from sklearn.model_selection import StratifiedShuffleSplit
import faiss
import numpy as np

def sampling(sampling_method: str, n_splits: int, sample_size: float, dataframe: pd.DataFrame):
    """Perform stratified or random sampling. Last column is label for stratified sampling."""
    
    if sampling_method == 'stratified_sampling':
        # Perform stratified sampling
        splitter = StratifiedShuffleSplit(n_splits=n_splits, test_size=sample_size, random_state=42)
        datasets = [
            dataframe.iloc[test_index,:-1]
            for _, test_index in splitter.split(X=dataframe.iloc[:,:-1], y=dataframe.iloc[:,-1])
        ]
    else:
        # Perform random sampling
        datasets = [dataframe.iloc[:,:-1].sample(frac=sample_size, random_state=42 + i) for i in range(n_splits)]
    return datasets

def compute_k_distances(data, n_neighbors):
    """Compute the k-distance graph data for the given dataset."""
    
    index = faiss.IndexFlatL2(data.shape[1])
    index.add(data)
    distances, _ = index.search(data, n_neighbors)
    return np.sort(distances[:, -1])