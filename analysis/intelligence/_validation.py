"""Validation helpers for ML artefacts."""

import numpy as np


def bootstrap_ari_stability(X_std, n_clusters, n_bootstrap=5, sample_frac=0.85, random_state=42):
    """Estimate cluster stability by re-running K-Means on bootstrap resamples
    and computing the Adjusted Rand Index against the full-fit labels."""
    from sklearn.cluster import KMeans
    from sklearn.metrics import adjusted_rand_score

    full = KMeans(n_clusters=n_clusters, n_init=10, random_state=random_state).fit(X_std)
    full_labels = full.labels_
    n = len(X_std)
    rng = np.random.default_rng(random_state)
    aris = []
    for _ in range(n_bootstrap):
        idx = rng.choice(n, size=int(n * sample_frac), replace=False)
        sample = X_std[idx]
        km = KMeans(n_clusters=n_clusters, n_init=5, random_state=random_state).fit(sample)
        # Map bootstrap labels back via indices
        sampled_full = full_labels[idx]
        aris.append(adjusted_rand_score(sampled_full, km.labels_))
    return float(np.mean(aris)) if aris else 0.0
