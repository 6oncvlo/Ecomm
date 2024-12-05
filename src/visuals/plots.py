import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from src.utils.utils import sampling, compute_k_distances

def kde_group(dataframe: pd.DataFrame, measure: str, column_group: str, xlabel: str, save_path: str = None):

    # Set a seaborn style for better aesthetics and create a single figure for all KDE plots
    sns.set(style="whitegrid")
    plt.figure(figsize=(10, 6))

    # Plot KDE for the entire measure with a shaded area
    sns.kdeplot(data=dataframe, x=measure, label='ALL', fill=True, color='blue', alpha=0.2, linewidth=2)

    # Use seaborn's kdeplot to plot KDE for each unique value in column_group
    unique_values = dataframe[column_group].unique()
    palette = sns.color_palette("Set2", len(unique_values))
    for i, val in enumerate(unique_values):
        sns.kdeplot(
            data=dataframe[dataframe[column_group] == val],
            x=measure, label=str(val), fill=True, color=palette[i], alpha=0.4, linewidth=2
            )

    # Add titles, labels, and legend to differentiate the curves
    plt.title(f'KDE of {xlabel}')
    plt.xlabel(xlabel)
    plt.ylabel('Density')
    plt.legend(title=column_group)
    plt.tight_layout()
    if save_path != None:
        plt.savefig(f'{save_path}.png')
    plt.close()

def k_distance(data: np.array, zoom_last_n_points: int = 20):

    # n_neighbors/min_points based rule of thumb
    n_neighbors = 2*data.shape[1]
    
    # Plot k-distance graph
    distances = compute_k_distances(data=data.astype('float32'), n_neighbors=n_neighbors)
    plt.plot(range(len(distances)), distances, marker='o', linestyle='-')

    # Set x-axis limits and ticks
    x_min = data.shape[0] - zoom_last_n_points
    x_max = data.shape[0] + 1
    x_mid = (x_min + x_max) // 2

    plt.xlim([x_min, x_max])  # Correct method
    plt.xticks([x_min, x_mid, x_max], labels=[f'{x_min}', f'{x_mid}', f'{x_max}'])

    # Show the plot
    plt.xlabel("Points (sorted by distance)")
    plt.ylabel(f"{n_neighbors}-distance")
    plt.title("k-Distance Plot for Estimating DBSCAN Epsilon")
    plt.show()