"""
Generates molecules by combining scaffold and R-group SMILES using RDKit.
Scaffolds and R-groups must each contain exactly one dummy atom (*)
representing the attachment point.
"""
import io
import csv
import logging
import itertools
from rdkit import Chem


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-3s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def validate_smiles(
    smiles_list: list[str],
    source_label: str
) -> list[Chem.Mol]:
    """
    Convert a list of SMILES strings to RDKit Mol objects, skipping invalid ones.

    Also enforces that each molecule has exactly one dummy atom (*), which is
    required for the attachment-point logic to work correctly.

    Args:
        smiles_list: Raw SMILES strings.
        source_label: Human-readable label used in log messages (e.g., "scaffold")

    Returns:
        List of valid RDKit Mol objects.
    """
    valid_mols = []
    invalid_count = 0

    for smi in smiles_list:
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            logger.warning(
                "[%s] Invalid SMILES (could not parse): '%s' — skipping.",
                source_label, smi,
            )
            invalid_count += 1
            continue

        dummy_atoms = [a for a in mol.GetAtoms() if a.GetAtomicNum() == 0]
        if len(dummy_atoms) == 0:
            logger.warning(
                "[%s] SMILES has no attachment point (*): '%s' — skipping.",
                source_label, smi,
            )
            invalid_count += 1
            continue

        if len(dummy_atoms) > 1:
            logger.warning(
                "[%s] SMILES has %d attachment points (*), expected 1: '%s' — skipping.",
                source_label, len(dummy_atoms), smi,
            )
            invalid_count += 1
            continue

        valid_mols.append(mol)

    logger.info(
        "[%s] Validated %d/%d SMILES (%d skipped).",
        source_label, len(valid_mols), len(smiles_list), invalid_count,
    )
    return valid_mols


def _label_dummy_atom(mol: Chem.Mol, label: int = 1) -> Chem.Mol:
    """
    Set the atom map number on the dummy atom (*) to `label`.

    molzip requires matching atom map numbers to know which * pairs
    with which *.
    Without this, it doesn't know how to connect scaffold to R-group.

    Args:
        mol: RDKit Mol with exactly one dummy atom.
        label: Integer atom map number to assign (default: 1).

    Returns:
        A new RDKit Mol with the labeled dummy atom.
    """
    rw_mol = Chem.RWMol(mol)
    for atom in rw_mol.GetAtoms():
        if atom.GetAtomicNum() == 0:
            atom.SetAtomMapNum(label)
            break

    return rw_mol.GetMol()


def _combine_scaffold_and_rgroup(
    scaffold: Chem.Mol,
    r_group: Chem.Mol
) -> str | None:
    """
    Combine one scaffold and one R-group into a single molecule SMILES.

    Steps:
        1. Label both dummy atoms with the same map number (:1).
        2. Merge into one disconnected Mol object with CombineMols.
        3. Use molzip to find the labeled * pair, delete them, and bond their neighbors.
        4. Sanitize and return the canonical SMILES.

    Args:
        scaffold: Validated scaffold Mol (one dummy atom).
        r_group: Validated R-group Mol (one dummy atom).

    Returns:
        Canonical SMILES string, or None if the combination failed.
    """
    try:
        labeled_scaffold = _label_dummy_atom(scaffold, label=1)
        labeled_rgroup = _label_dummy_atom(r_group, label=1)

        combined = Chem.CombineMols(labeled_scaffold, labeled_rgroup)
        product = Chem.molzip(combined)

        if product is None:
            return None

        Chem.SanitizeMol(product)
        return Chem.MolToSmiles(product)

    except Exception as e:
        scaffold_smi = Chem.MolToSmiles(scaffold)
        rgroup_smi = Chem.MolToSmiles(r_group)
        logger.error(
            "Failed to combine scaffold '%s' with R-group '%s': %s",
            scaffold_smi, rgroup_smi, e,
        )
        return None


def generate_molecules(
    scaffold_smiles: list[str],
    r_group_smiles: list[str],
) -> list[str]:
    """
    Generate all unique product molecules from scaffold x R-group combinations.

    Args:
        scaffold_smiles: Raw SMILES strings for scaffolds.
        r_group_smiles: Raw SMILE strings for R-groups.

    Returns:
        Sorted list of unique canonical product SMILES strings.

    Raises:
        ValueError: If no valid scaffolds or R-groups remain after validation.
    """
    scaffolds = validate_smiles(scaffold_smiles, source_label="scaffold")
    r_groups = validate_smiles(r_group_smiles, source_label="r-group")

    if not scaffolds:
        raise ValueError("No valid scaffold molecules found — cannot generate products.")
    if not r_groups:
        raise ValueError("No valid R-group molecules found — cannot generate products.")

    logger.info(
        "Generating molecules: %d scaffolds × %d R-groups = %d combinations.",
        len(scaffolds), len(r_groups), len(scaffolds) * len(r_groups),
    )

    generated = set()
    failed = 0

    for scaffold, r_group in itertools.product(scaffolds, r_groups):
        product_smi = _combine_scaffold_and_rgroup(scaffold, r_group)

        if product_smi is None:
            failed += 1
            continue

        generated.add(product_smi)

    duplicate_count = (len(scaffolds) * len(r_groups)) - failed - len(generated)

    logger.info("Generation complete.")
    logger.info("  Unique molecules : %d", len(generated))
    logger.info("  Duplicates removed: %d", duplicate_count)
    logger.info("  Failed combinations: %d", failed)

    return sorted(generated)
