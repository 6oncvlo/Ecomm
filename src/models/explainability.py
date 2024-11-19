import numpy as np
import pandas as pd
import shap

class ModelExplainability:
    def __init__(self, model, data: pd.DataFrame):
        self.model = model
        self.data = data
        self.shap_values = self.ShapValues(self)

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
        return pd.DataFrame(
            list(zip(self.data.columns, feature_importances)), columns=['feature','weight']
            ).set_index(keys='feature').sort_values(by=['weight'], ascending=False)
    
    class ShapValues:
        def __init__(self, parent):
            self.parent = parent
            self.explainer = None  # Initialize explainer as None
            self._shap_values = None  # Private attribute for SHAP values
    
        @property
        def shap_values(self):
            # Compute SHAP values only when accessed
            if self._shap_values is None:
                self.explainer = shap.TreeExplainer(self.parent.model.model)
                self._shap_values = self.explainer.shap_values(self.parent.data)
            return self._shap_values
    
        def plot(self, method: str, ind=None, interaction_index=None, instance_index=None):
            # Ensure SHAP values are computed before plotting
            _ = self.shap_values
            
            if method == 'global':
                # Global summary plot to view the overall feature importance
                shap.summary_plot(
                    shap_values = self.shap_values
                    , features = self.parent.data
                    , max_display = 20
                    , plot_type = "bar"
                    , plot_size = (10,10)
                    )
            elif method == 'dependence':
                # Use a SHAP dependence plot for specific features
                shap.dependence_plot(
                    ind = ind
                    , shap_values = self.shap_values
                    , features = self.parent.data
                    , interaction_index = interaction_index
                    )
            elif method == 'instance':
                # For a specific instance, view SHAP values and plot them
                shap.force_plot(
                    base_value  = self.explainer.expected_value
                    , shap_values = self.shap_values[instance_index]
                    , features = self.parent.data.iloc[instance_index]
                    )
                
        def importance_values(self):

            shapX = pd.DataFrame(self.shap_values, columns = self.parent.data.columns)
            vals = np.abs(shapX.values).mean(0)
            return pd.DataFrame(
                list(zip(self.parent.data.columns, vals)), columns=['feature','weight']
                ).set_index(keys='feature').sort_values(by=['weight'], ascending=False)