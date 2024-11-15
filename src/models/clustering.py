from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN
from sklearn.metrics import silhouette_score

# Dictionary of supported algorithms
algos = {
    'isolation_forest': IsolationForest,
    'dbscan': DBSCAN
}

class AnomalyDetection:
    def __init__(self, method="isolation_forest", **kwargs):
        """
        Initializes the AnomalyDetection model.

        Parameters:
            method (str): The algorithm to use ('isolation_forest' or 'dbscan').
            kwargs: Parameters for the specified model.
        """
        self.method = method.lower()
        
        # Validate the method and initialize the model
        if self.method in algos:
            self.model = algos[self.method](**kwargs)
        else:
            raise ValueError(f"Invalid method '{self.method}'. Choose from {list(algos.keys())}.")

    def fit(self, X):
        """Fits the model to the data."""
        self.model.fit(X)
        return self
    
    def predict(self, X):
        """
        Returns anomaly scores or cluster labels based on the method.

        For Isolation Forest, returns -1 for anomalies and 1 for normal points.
        For DBSCAN, returns cluster labels (-1 for noise points).
        """
        return self.model.fit_predict(X)


    def scoring(self, X):
        """
        Returns anomaly or similarity scores based on the method.
        
        For Isolation Forest, returns anomaly scores (the lower, the more anomalous).
        For DBSCAN, calculates silhouette score for cluster quality (only for valid clusters).
        """
        if self.method == "isolation_forest":
            return self.model.decision_function(X)
        elif self.method == "dbscan":
            labels = self.model.fit_predict(X)
            # Silhouette score is only defined if there are more than 1 cluster and no noise-only labels
            if len(set(labels)) > 1 and len(set(labels)) < len(X):
                return silhouette_score(X, labels)
            else:
                raise ValueError("Silhouette score not defined for the current DBSCAN clustering.")

    def get_model_attribute(self, attr_name):
        """Get an attribute from the underlying model if it exists."""
        if hasattr(self.model, attr_name):
            return getattr(self.model, attr_name)
        else:
            raise AttributeError(f"The attribute '{attr_name}' does not exist for the current model.")