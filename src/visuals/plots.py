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

def k_distance(dataframe: pd.DataFrame, zoom_last_n_points: int = 20):

    # n_neighbors/min_points based rule of thumb
    n_neighbors = 2*len(dataframe.columns)
    
    # Plot k-distance graph
    distances = compute_k_distances(data=dataframe.astype('float32'), n_neighbors=n_neighbors)
    plt.plot(range(len(distances)), distances, marker='o', linestyle='-')

    # Set x-axis limits and ticks
    x_min = dataframe.shape[0] - zoom_last_n_points
    x_max = dataframe.shape[0] + 1
    x_mid = (x_min + x_max) // 2

    plt.xlim([x_min, x_max])  # Correct method
    plt.xticks([x_min, x_mid, x_max], labels=[f'{x_min}', f'{x_mid}', f'{x_max}'])

    # Show the plot
    plt.xlabel("Points (sorted by distance)")
    plt.ylabel(f"{n_neighbors}-distance")
    plt.title("Plot Title")
    plt.show()