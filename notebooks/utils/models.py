"""Model training and evaluation helpers."""
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

def build_cluster_pipeline(n_clusters: int = 3) -> Pipeline:
    return Pipeline([
        ("scaler", StandardScaler()),
        ("model", KMeans(n_clusters=n_clusters, n_init="auto")),
    ])
