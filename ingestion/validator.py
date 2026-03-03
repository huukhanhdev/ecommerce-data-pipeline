"""
validator.py
────────────
Data quality checks on DataFrames before loading to staging.
Logs warnings and raises errors for critical issues.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


@dataclass
class ValidationResult:
    table: str
    passed: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    row_count: int = 0
    rejected_count: int = 0


class DataValidator:
    """
    Validates a DataFrame against a set of rules and returns a ValidationResult.
    Drops invalid rows (soft rejection) rather than failing the whole pipeline.
    """

    def __init__(self, table: str, df: pd.DataFrame):
        self.table = table
        self.df = df.copy()
        self.result = ValidationResult(table=table, passed=True, row_count=len(df))

    # ── Rule methods ──────────────────────────────────────────────────────

    def not_null(self, columns: List[str]) -> "DataValidator":
        for col in columns:
            if col not in self.df.columns:
                self.result.errors.append(f"Column '{col}' not found")
                self.result.passed = False
                continue
            null_mask = self.df[col].isna()
            n_nulls = null_mask.sum()
            if n_nulls > 0:
                self.result.warnings.append(f"'{col}' has {n_nulls} nulls — dropping those rows")
                self.df = self.df[~null_mask]
                self.result.rejected_count += n_nulls
        return self

    def unique(self, column: str) -> "DataValidator":
        dupes = self.df.duplicated(subset=[column], keep="first")
        n_dupes = dupes.sum()
        if n_dupes > 0:
            self.result.warnings.append(f"'{column}' has {n_dupes} duplicates — keeping first")
            self.df = self.df[~dupes]
            self.result.rejected_count += n_dupes
        return self

    def value_in(self, column: str, valid_values: List) -> "DataValidator":
        if column not in self.df.columns:
            return self
        mask = ~self.df[column].isin(valid_values)
        n_invalid = mask.sum()
        if n_invalid > 0:
            self.result.warnings.append(
                f"'{column}' has {n_invalid} invalid values — dropping those rows"
            )
            self.df = self.df[~mask]
            self.result.rejected_count += n_invalid
        return self

    def positive(self, column: str) -> "DataValidator":
        if column not in self.df.columns:
            return self
        mask = self.df[column] <= 0
        n_bad = mask.sum()
        if n_bad > 0:
            self.result.warnings.append(
                f"'{column}' has {n_bad} non-positive values — dropping those rows"
            )
            self.df = self.df[~mask]
            self.result.rejected_count += n_bad
        return self

    def build(self) -> tuple["pd.DataFrame", ValidationResult]:
        """Return cleaned DataFrame and validation result."""
        self.result.row_count = len(self.df)
        return self.df, self.result


class PipelineValidator:
    """
    Applies table-specific validation rules to all extracted DataFrames.
    Returns cleaned DataFrames ready for staging load.
    """

    VALID_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
    VALID_PAYMENTS = ["cod", "credit_card", "e_wallet", "bank_transfer"]
    VALID_GENDERS  = ["male", "female", "other"]

    def validate_all(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        cleaned = {}
        all_passed = True

        for table, df in data.items():
            validator = DataValidator(table, df)

            if table == "users":
                validator.not_null(["user_id", "email"]).unique("user_id")

            elif table == "sellers":
                validator.not_null(["seller_id", "email"]).unique("seller_id")

            elif table == "products":
                validator.not_null(["product_id", "seller_id", "base_price"]) \
                         .positive("base_price") \
                         .unique("product_id")

            elif table == "orders":
                validator.not_null(["order_id", "user_id", "status"]) \
                         .value_in("status", self.VALID_STATUSES) \
                         .value_in("payment_method", self.VALID_PAYMENTS) \
                         .unique("order_id")

            elif table == "order_items":
                validator.not_null(["item_id", "order_id", "product_id", "quantity", "unit_price"]) \
                         .positive("quantity") \
                         .positive("unit_price")

            elif table == "categories":
                validator.not_null(["category_id", "name"]).unique("category_id")

            clean_df, result = validator.build()
            cleaned[table] = clean_df

            # Log results
            status = "✅ PASS" if result.passed else "❌ FAIL"
            logger.info(
                f"{status} [{table}]: "
                f"{result.row_count:,} rows kept, "
                f"{result.rejected_count:,} rejected"
            )
            for w in result.warnings:
                logger.warning(f"  ⚠️  {w}")
            for e in result.errors:
                logger.error(f"  ❌  {e}")
                all_passed = False

        if not all_passed:
            raise ValueError("Critical validation errors found — check logs above")

        return cleaned
