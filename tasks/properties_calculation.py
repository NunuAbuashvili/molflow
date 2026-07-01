"""
Calculates molecular descriptors for a list of SMILES strings using RDKit.
"""
import logging
from typing import Any

from rdkit import Chem
from rdkit.Chem import Descriptors, rdMolDescriptors


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


_FLOAT_DESCRIPTORS: dict[str, Any] = {
    "mol_weight": Descriptors.MolWt,    # average molecular weight
    "log_p": Descriptors.MolLogP,       # lipophilicity (octanol-water partition)
    "tpsa": Descriptors.TPSA,           # topological polar surface area
}

_INT_DESCRIPTORS: dict[str, Any] = {
    "hba": rdMolDescriptors.CalcNumHBA,  # hydrogen bond acceptors
    "hbd": rdMolDescriptors.CalcNumHBD,  # hydrogen bond donors
    "rotatable_bonds": rdMolDescriptors.CalcNumRotatableBonds,
    "aromatic_rings": rdMolDescriptors.CalcNumAromaticRings,
}


def _passes_lipinski(props: dict[str, Any]) -> bool:
    """
    Evaluate Lipinski's Rule of Five (Ro5) for drug-likeness.

    A molecule is considered drug-like if it satisfies all four conditions:
        - Molecular weight <= 500 Da
        - LogP <= 5 (not too lipophilic)
        - Hydrogen bond acceptors <= 10
        - Hydrogen bond donors <= 5

    Args:
        props: Dictionary of computed molecular properties.

    Returns:
        True if all Ro5 conditions are satisfied, False otherwise.
    """
    return (
        props["mol_weight"] <= 500
        and props["log_p"] <= 5
        and props["hba"] <= 10
        and props["hbd"] <= 5
    )


def calculate_properties(smiles_list: list[str]) -> list[dict]:
    """
    Calculate molecular descriptors for each SMILES string in the input list.

    For each valid molecule, computes:
        - mol_weight, log_p, tpsa (floats, rounded to 4dp)
        - hba, hbd, rotatable_bonds, aromatic_rings (integers)
        - lipinski_pass (bool - True if Lipinski's Ro5 is satisfied)

    Invalid SMILES (unparseable) are skipped with a warning and counted.

    Args:
        smiles_list: List of SMILES strings to process.

    Returns:
        - results: List of dicts, one per valid molecule.
                   Each dict contains 'smiles' + all computed descriptors.

    Raises:
        ValueError: If `smiles_list` is empty.
    """
    if not smiles_list:
        raise ValueError("Smiles list is empty — nothing to calculate.")

    logger.info("Calculating properties for %d molecules.", len(smiles_list))

    results: list[dict] = []
    failed = 0

    for smi in smiles_list:
        mol = Chem.MolFromSmiles(smi)

        if mol is None:
            logger.warning("Invalid SMILES (could not parse): '%s' — skipping.", smi)
            failed += 1
            continue

        try:
            props: dict[str, Any] = {"smiles": smi}

            for name, fn in _FLOAT_DESCRIPTORS.items():
                props[name] = round(fn(mol), 4)

            for name, fn in _INT_DESCRIPTORS.items():
                props[name] = fn(mol)

            props["lipinski_pass"] = _passes_lipinski(props)
            results.append(props)

        except Exception as e:
            logger.error("Failed to calculate properties for '%s': %s", smi, e)
            failed += 1

    logger.info("Properties calculation complete.")
    logger.info("  Calculated : %d", len(results))
    logger.info("  Failed     : %d", failed)

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

    results = calculate_properties(test_molecules)
