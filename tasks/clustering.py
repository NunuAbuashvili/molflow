"""
Clusters molecules using K-Means on Morgan fingerprints.

Morgan fingerprints encode the local chemical environment around each atom
up to a given radius, producing a binary bit vector that represents the
molecule's structural features. K-Means then groups molecules by the
similarity of these vectors.
"""
import logging

import numpy as np
from rdkit import Chem
from rdkit.Chem.rdFingerprintGenerator import GetMorganGenerator
from sklearn.cluster import KMeans


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


MORGAN_RADIUS = 2
MORGAN_FP_SIZE = 2048


def cluster_molecules(
    smiles_list: list[str],
    num_clusters: int = 5,
    random_state: int = 42,
) -> list[dict]:
    """
    Cluster molecules using K-Means on Morgan (ECFP4) fingerprints.

    Each valid molecule is converted to a Morgan fingerprint (a binary
    bit vector encoding its structural features), then K-Means groups
    them into `num_clusters` clusters based on fingerprint similarity.

    Invalid SMILES are skipped with a warning. If fewer valid molecules
    remain than `num_clusters`, the cluster count is reduced automatically.

    Args:
        smiles_list:  List of SMILES strings to cluster.
        num_clusters: Desired number of K-Means clusters (default: 5).
        random_state: Random seed for reproducibility (default: 42).

    Returns:
        results: List of dicts, one per valid molecule, each containing:
                 'smiles' and 'cluster' (integer cluster ID, 0-indexed).

    Raises:
        ValueError: If `smiles_list` is empty, or if `num_clusters` < 1.
    """
    if not smiles_list:
        raise ValueError("Smiles list is empty — nothing to calculate.")
    if num_clusters < 1:
        raise ValueError(f"Number of clusters must be >= 1, got {num_clusters}.")

    logger.info(
        "Clustering %d molecules into %d clusters using Morgan fingerprints "
        "(radius=%d, fpSize=%d).",
        len(smiles_list), num_clusters, MORGAN_RADIUS, MORGAN_FP_SIZE,
    )

    morgan_gen = GetMorganGenerator(radius=MORGAN_RADIUS, fpSize=MORGAN_FP_SIZE)

    valid_smiles: list[str] = []
    fingerprints: list[np.ndarray] = []
    failed = 0

    for smi in smiles_list:
        mol = Chem.MolFromSmiles(smi)

        if mol is None:
            logger.warning("Invalid SMILES (could not parse): '%s' — skipping.", smi)
            failed += 1
            continue

        try:
            fp = morgan_gen.GetFingerprintAsNumPy(mol)
            valid_smiles.append(smi)
            fingerprints.append(fp)
        except Exception as e:
            logger.error("Failed to generate fingerprint for '%s': %s", smi, e)
            failed += 1

    if not valid_smiles:
        raise ValueError("No valid molecules remain after parsing — cannot cluster.")

    # Guard: can't have more clusters than molecules
    actual_clusters = min(num_clusters, len(valid_smiles))
    if actual_clusters < num_clusters:
        logger.warning(
            "Requested %d clusters but only %d valid molecules — "
            "reducing to %d clusters.",
            num_clusters, len(valid_smiles), actual_clusters,
        )

    # Run K-Means
    X = np.array(fingerprints)
    kmeans = KMeans(n_clusters=actual_clusters, random_state=random_state, n_init=10)
    labels = kmeans.fit_predict(X)

    results = [
        {"smiles": smi, "cluster": int(label)}
        for smi, label in zip(valid_smiles, labels)
    ]

    logger.info("Clustering complete.")
    logger.info("  Clustered : %d", len(results))
    logger.info("  Failed    : %d", failed)

    return results


if __name__ == "__main__":
    test_molecules = [
        "CC(=O)Nc1ccc(C(=O)O)cc1",
        "CC(=O)Nc1ccc(C(N)=O)cc1",
        "CC(=O)Nc1ccc(CN)cc1",
        "COC1CCNCC1",
        "COc1ccc(NC(C)=O)cc1",
        "COc1ccccc1",
        "NC(=O)C1CCNCC1",
        "NC(=O)c1ccccc1",
        "NCC1CCNCC1",
        "NCc1ccccc1",
        "O=C(O)C1CCNCC1",
        "O=C(O)c1ccccc1",
        "INVALID_SMILES",
    ]

    results = cluster_molecules(test_molecules, num_clusters=3)
    print(f"\n{'='*55}")
    print(f"{'SMILES':<40} {'Cluster':>7}")
    print(f"{'='*55}")
    for r in sorted(results, key=lambda x: x["cluster"]):
        print(f"{r['smiles']:<40} {r['cluster']:>7}")
