import pandas as pd
import matplotlib.pyplot as plt
from src.utils.utils import sampling, compute_k_distances

def kde_group(dataframe: pd.DataFrame, measure: str, column_group: str, xlabel: str):

    # Create a figure with 1 row and 3 columns for the subplots
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))  # Adjust the size as needed

    # First plot: KDE of the entire measure
    dataframe[measure].plot.kde(ax=axes[0])
    axes[0].set_title(f'KDE of {xlabel} | ALL')
    axes[0].set_xlabel(xlabel)
    axes[0].set_ylabel('Density')

    for val, i in zip(dataframe[column_group].unique(), [1,2]):
        # Plot per value: KDE for the val subset
        dataframe[dataframe[column_group] == val][measure].plot.kde(ax=axes[i])
        axes[i].set_title(f'KDE of {xlabel} | {val}')
        axes[i].set_xlabel(xlabel)
        axes[i].set_ylabel('Density')

    # Adjust layout to make sure everything fits well and show the plot
    plt.tight_layout()
    plt.show()

def k_distance(sampling_method: str, sample_size: float, dataframe: pd.DataFrame):

    n_neighbors = 2*(len(dataframe.columns)-1)
    zoom_last_n_points = 20
    
    # Perform stratified or random sampling.
    datasets = sampling(
        sampling_method = sampling_method
        , n_splits = 6
        , sample_size = sample_size
        , dataframe = dataframe
        )

    # Plot k-distance graphs for all datasets
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    axes = axes.flatten()
    for i, (data, ax) in enumerate(zip(datasets, axes)):
        # compute k-distance for each dataset
        distances = compute_k_distances(data=data.astype('float32'), n_neighbors=n_neighbors)

        # Plot distances
        ax.plot(range(len(distances)), distances, marker='o', linestyle='-')
        ax.set_xlabel('Points (sorted by distance)')
        ax.set_ylabel(f'{n_neighbors}-distance')
        ax.set_title(f'Dataset {i + 1}')

        # Set x-axis limits and ticks
        x_min = data.shape[0] - zoom_last_n_points
        x_max = data.shape[0] + 1
        x_mid = (x_min + x_max) // 2
        ax.set_xlim([x_min, x_max])
        ax.set_xticks([x_min, x_mid, x_max])
        ax.set_xticklabels([f'{x_min}', f'{x_mid}', f'{x_max}'])

    plt.tight_layout()
    plt.show()