import matplotlib.pyplot as plt
import pandas as pd

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