import numpy as np
import pandas as pd
import shap

class ModelExplainability:
    def __init__(self, model, data: pd.DataFrame):
        self.model = model
        self.data = data

    def feature_importance(self):
        # Initialize array to store feature contributions
        feature_importances = np.zeros(self.data.shape[1])
        # Access 'estimators_' safely
        estimators = self.model.get_model_attribute("estimators_")

        for tree in estimators:
            tree_features = tree.tree_.feature
            # Count how many times each feature is used across all splits in this tree
            for feature in range(self.data.shape[1]):
                # Sum path lengths for nodes where this feature is used to split
                feature_importances[feature] += np.sum(tree_features == feature)

        # Normalize to get relative feature contributions
        feature_importances /= feature_importances.sum()
        feature_contributions = pd.Series(feature_importances, index=self.data.columns)
        return pd.DataFrame(feature_contributions, columns=['weight']).sort_values(by='weight', ascending=False)