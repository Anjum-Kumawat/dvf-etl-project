"""RETL0-11: implementation of the quality rules documented in
docs/governance/quality-rules.md (RETL0-93). Each function checks one rule
against the Silver DVF DataFrame and returns a QualityCheckResult.
run_quality_checks() runs all of them, prints a report, and raises
QualityCheckFailure if any HARD FAIL rule is violated. This must run BEFORE
the PostgreSQL write in run_dvf_silver.py, so a broken pipeline run never
overwrites a good Silver table with bad data.

Thresholds and category sets here are taken directly from
docs/governance/quality-rules.md, which documents why each one was chosen
(grounded in real measured data, not assumed).

RETL0-49 addendum: running Silver for departments 92 and 93 for the first
time (via Dagster's department-partitioned materialization) surfaced real
findings requiring changes here -- see quality-rules.md's "RETL0-49
Addendum" section for the full investigation and real measurements:

  - check_code_postal_format is a WARNING, not a HARD FAIL. A real,
    documented Paris-border postal-routing quirk (specific
    Boulogne-Billancourt/Issy-les-Moulineaux streets carry a Paris postal
    code despite being in department 92) makes the department-prefix
    match a real edge case, not a pipeline defect.
  - KNOWN_NATURE_MUTATION now includes "Expropriation", a real DVF
    category (confirmed against DVF's own published documentation) that
    departments 75 and 92 simply never happened to contain.
  - BAN and DPE join coverage are now WARNING, not HARD FAIL. Across three
    real departments (75, 92, 93), coverage kept dropping further each
    time (BAN: 99.8% -> 93.9% -> 89.1%; DPE: 94.9% -> 75.5% -> 62.6%),
    spread continuously across dozens of communes each time rather than
    concentrated in a few -- real regional variance in how completely an
    area has been BAN-geocoded or DPE-surveyed, not a pipeline defect. No
    HARD FAIL on either has ever actually caught a real bug. Filosofi and
    geo coverage remain HARD FAIL: both held at 100% across all three
    departments once the geo join's real commune-merger gap (department
    93's Pierrefitte-sur-Seine, merged into Saint-Denis on 2025-01-01) was
    fixed in dvf_geo_join.py, so a drop there still signals a real defect.
"""
from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

KNOWN_NATURE_MUTATION = {
    "Vente", "Echange", "Vente en l'état futur d'achèvement",
    "Adjudication", "Vente terrain à bâtir", "Expropriation",
}
KNOWN_TYPE_LOCAL = {
    "Appartement", "Maison", "Dépendance",
    "Local industriel. commercial ou assimilé",
}
BAN_LOW_CONFIDENCE_THRESHOLD = 0.5
LOW_VALUE_THRESHOLD = 1000
HIGH_VALUE_THRESHOLD = 10_000_000
BAN_COVERAGE_FLOOR = 0.90
DPE_COVERAGE_FLOOR = 0.70
FILOSOFI_COVERAGE_FLOOR = 1.0
GEO_COVERAGE_FLOOR = 1.0


class QualityCheckFailure(Exception):
    """Raised when one or more HARD FAIL quality rules are violated."""


@dataclass
class QualityCheckResult:
    rule_id: str
    description: str
    severity: str  # "HARD FAIL" or "WARNING"
    violation_count: int
    passed: bool


def check_nature_mutation_known(df: DataFrame) -> QualityCheckResult:
    count = df.filter(~F.col("nature_mutation").isin(list(KNOWN_NATURE_MUTATION))).count()
    return QualityCheckResult(
        "2.1", "nature_mutation must be a known category", "HARD FAIL", count, count == 0
    )


def check_type_local_known(df: DataFrame) -> QualityCheckResult:
    count = df.filter(
        F.col("type_local").isNotNull() & ~F.col("type_local").isin(list(KNOWN_TYPE_LOCAL))
    ).count()
    return QualityCheckResult(
        "2.2", "type_local must be a known category or null", "HARD FAIL", count, count == 0
    )


def check_code_postal_format(df: DataFrame, dept: str) -> QualityCheckResult:
    """WARNING, not HARD FAIL (RETL0-49) -- a real Paris-border postal
    quirk means a department-prefix mismatch is a legitimate edge case,
    not necessarily a broken department filter. See quality-rules.md."""
    pattern = f"^{dept}[0-9]{{3}}$"
    count = df.filter(~F.col("code_postal").rlike(pattern)).count()
    return QualityCheckResult(
        "2.3", "code_postal should match dept-prefixed 5-digit format", "WARNING", count, True
    )


def check_date_mutation_range(df: DataFrame, year: int) -> QualityCheckResult:
    count = df.filter(
        (F.col("date_mutation") < F.lit(f"{year}-01-01"))
        | (F.col("date_mutation") > F.lit(f"{year}-12-31"))
    ).count()
    return QualityCheckResult(
        "2.4", f"date_mutation must fall within {year}", "HARD FAIL", count, count == 0
    )


def check_valeur_fonciere_positive(df: DataFrame) -> QualityCheckResult:
    count = df.filter(F.col("valeur_fonciere").isNotNull() & (F.col("valeur_fonciere") <= 0)).count()
    return QualityCheckResult(
        "3.1", "valeur_fonciere must be positive when present", "HARD FAIL", count, count == 0
    )


def check_no_exact_duplicates(df: DataFrame) -> QualityCheckResult:
    total = df.count()
    distinct = df.dropDuplicates().count()
    count = total - distinct
    return QualityCheckResult("4.1", "no exact full-row duplicates", "HARD FAIL", count, count == 0)


def check_valeur_fonciere_completeness(df: DataFrame) -> QualityCheckResult:
    count = df.filter(F.col("valeur_fonciere").isNull()).count()
    return QualityCheckResult("1.1", "valeur_fonciere should be present", "WARNING", count, True)


def check_built_property_missing_surface(df: DataFrame) -> QualityCheckResult:
    count = df.filter(
        F.col("type_local").isin(["Appartement", "Maison"])
        & (F.col("surface_reelle_bati").isNull() | (F.col("surface_reelle_bati") <= 0))
    ).count()
    return QualityCheckResult(
        "1.2", "built properties (Appartement/Maison) should have a positive surface",
        "WARNING", count, True,
    )


def check_low_value_outliers(df: DataFrame) -> QualityCheckResult:
    count = df.filter(F.col("valeur_fonciere") < LOW_VALUE_THRESHOLD).count()
    return QualityCheckResult(
        "3.2", f"valeur_fonciere below {LOW_VALUE_THRESHOLD}", "WARNING", count, True
    )


def check_high_value_outliers(df: DataFrame) -> QualityCheckResult:
    count = df.filter(F.col("valeur_fonciere") > HIGH_VALUE_THRESHOLD).count()
    return QualityCheckResult(
        "3.3", f"valeur_fonciere above {HIGH_VALUE_THRESHOLD}", "WARNING", count, True
    )


def check_ban_low_confidence(df: DataFrame) -> QualityCheckResult:
    count = df.filter(
        F.col("ban_result_score").isNotNull()
        & (F.col("ban_result_score") < BAN_LOW_CONFIDENCE_THRESHOLD)
    ).count()
    return QualityCheckResult(
        "3.4", f"BAN geocoding confidence below {BAN_LOW_CONFIDENCE_THRESHOLD}",
        "WARNING", count, True,
    )


def check_join_coverage(
    df: DataFrame, column: str, floor: float, label: str, severity: str = "HARD FAIL"
) -> QualityCheckResult:
    """severity defaults to HARD FAIL (Filosofi, geo -- both real 100%
    invariants). Pass severity="WARNING" for a source whose coverage is
    real regional variance rather than a broken/working signal (RETL0-49:
    BAN, DPE) -- the row is still flagged and the floor still describes
    when it's worth a look, but it never blocks the pipeline."""
    total = df.count()
    matched = df.filter(F.col(column).isNotNull()).count()
    rate = matched / total if total else 0.0
    met_floor = rate >= floor
    passed = True if severity == "WARNING" else met_floor
    return QualityCheckResult(
        f"5-{label}",
        f"{label} join coverage >= {floor:.0%} (actual: {rate:.1%})",
        severity,
        0 if met_floor else (total - matched),
        passed,
    )


def run_quality_checks(df: DataFrame, year: int, dept: str) -> List[QualityCheckResult]:
    """Run every rule from docs/governance/quality-rules.md and print a
    report. Raises QualityCheckFailure if any HARD FAIL rule is violated --
    call this BEFORE the PostgreSQL write."""
    results = [
        check_nature_mutation_known(df),
        check_type_local_known(df),
        check_code_postal_format(df, dept),
        check_date_mutation_range(df, year),
        check_valeur_fonciere_positive(df),
        check_no_exact_duplicates(df),
        check_valeur_fonciere_completeness(df),
        check_built_property_missing_surface(df),
        check_low_value_outliers(df),
        check_high_value_outliers(df),
        check_ban_low_confidence(df),
        check_join_coverage(df, "ban_result_status", BAN_COVERAGE_FLOOR, "BAN", severity="WARNING"),
        check_join_coverage(df, "dpe_etiquette_dpe", DPE_COVERAGE_FLOOR, "DPE", severity="WARNING"),
        check_join_coverage(df, "filosofi_revenu_median", FILOSOFI_COVERAGE_FLOOR, "Filosofi"),
        check_join_coverage(df, "geo_commune_nom", GEO_COVERAGE_FLOOR, "geo"),
    ]

    print("\n=== Quality check report ===")
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"[{status}] {r.rule_id} ({r.severity}): {r.description} -- {r.violation_count} violation(s)")
    print("============================\n")

    hard_failures = [r for r in results if r.severity == "HARD FAIL" and not r.passed]
    if hard_failures:
        names = ", ".join(f"{r.rule_id} ({r.description})" for r in hard_failures)
        raise QualityCheckFailure(f"{len(hard_failures)} HARD FAIL rule(s) violated: {names}")

    return results