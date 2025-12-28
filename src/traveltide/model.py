import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans, DBSCAN
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score

class SegmentationPipeline:
    def __init__(self, n_clusters=5, use_pca=True, pca_components=2):
        self.n_clusters = n_clusters
        self.use_pca = use_pca
        self.pca_components = pca_components
        self.scaler = StandardScaler()
        self.pca = PCA(n_components=pca_components)
        self.kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        self.dbscan = DBSCAN(eps=0.5, min_samples=5)
        self.feature_cols = [
            'avg_clicks', 'total_flights', 'cancellation_rate',
            'avg_flight_fare', 'avg_hotel_price', 'nights', 'checked_bags'
        ]

    def fit_predict(self, df: pd.DataFrame, method='kmeans') -> pd.DataFrame:
        """
        Runs the full pipeline: Scaling -> PCA (optional) -> Clustering.
        Method can be 'kmeans' or 'dbscan'.
        """
        data = df[self.feature_cols].copy()

        # 1. Scale
        X_scaled = self.scaler.fit_transform(data)

        # 2. PCA (Dimensionality Reduction)
        if self.use_pca:
            print(f"   🧠 Applying PCA (reducing to {self.pca_components} components)...")
            X_processed = self.pca.fit_transform(X_scaled)
            explained_variance = np.sum(self.pca.explained_variance_ratio_)
            print(f"      Explained Variance: {explained_variance:.2%}")
        else:
            X_processed = X_scaled

        # 3. Clustering
        if method == 'kmeans':
            print(f"   🤖 Running K-Means (k={self.n_clusters})...")
            labels = self.kmeans.fit_predict(X_processed)
        elif method == 'dbscan':
            print(f"   🤖 Running DBSCAN...")
            labels = self.dbscan.fit_predict(X_processed)
        else:
            raise ValueError("Unknown method")

        # 4. Evaluation (Silhouette Score)
        # Only calculate if we have more than 1 cluster and less than N-1 (noise)
        unique_labels = len(set(labels)) - (1 if -1 in labels else 0)
        if unique_labels > 1:
            score = silhouette_score(X_processed, labels)
            print(f"   ✨ Silhouette Score: {score:.3f}")

        df_result = df.copy()
        df_result['cluster_id'] = labels

        # PCA coords for plotting later
        if self.use_pca:
            df_result['pca_x'] = X_processed[:, 0]
            df_result['pca_y'] = X_processed[:, 1]

        return df_result

    def get_cluster_stats(self, df_result: pd.DataFrame):
        return df_result.groupby('cluster_id')[self.feature_cols].mean()