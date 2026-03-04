"""Data validation module"""

from pyspark.sql.functions import (
    count, col, lit, when, isnull, min as spark_min, max as spark_max,
    percentile_approx, avg
)
from utils.logger import get_logger

logger = get_logger(__name__)

class DataValidator:
    """Handle data quality checks"""

    def __init__(self):
        self.validation_results = {}

    def profile_data(self, df):
        """
        Perform comprehensive data profiling
        
        Args:
            df: PySpark DataFrame to profile
            
        Returns:
            Dictionary containing profiling statistics
        """
        logger.info("=" * 80)
        logger.info("DATA PROFILING".center(80))
        logger.info("=" * 80)
        
        profile_report = {}
        total_rows = df.count()
        total_cols = len(df.columns)
        
        # 1. Basic Dataset Information
        logger.info("\n1. BASIC DATASET INFORMATION")
        logger.info("-" * 80)
        logger.info(f"   Total Rows: {total_rows:,}")
        logger.info(f"   Total Columns: {total_cols}")
        logger.info(f"\n   Column Names and Types:")
        
        profile_report['row_count'] = total_rows
        profile_report['column_count'] = total_cols
        profile_report['columns'] = {}
        
        for idx, (col_name, col_type) in enumerate(df.dtypes, 1):
            logger.info(f"   {idx:2d}. {col_name:30s} -> {col_type:15s}")
            profile_report['columns'][col_name] = col_type
        
        # 2. Null/Missing Values Analysis
        logger.info("\n2. NULL/MISSING VALUES ANALYSIS")
        logger.info("-" * 80)
        
        null_stats = df.select(
            [count(when(isnull(col(c)), 1)).alias(c) for c in df.columns]
        ).collect()[0]
        
        null_columns = {}
        logger.info(f"   {'Column Name':35s} | {'Null Count':>12s} | {'Null %':>8s}")
        logger.info(f"   {'-'*35}-+-{'-'*12}-+-{'-'*8}")
        
        for col_name in df.columns:
            null_count = null_stats[col_name]
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            if null_count > 0:
                null_columns[col_name] = {"count": null_count, "percentage": null_pct}
            logger.info(f"   {col_name:35s} | {null_count:>12,d} | {null_pct:>7.2f}%")
        
        profile_report['null_analysis'] = null_columns
        
        # 3. Categorical Column Analysis
        logger.info("\n3. CATEGORICAL COLUMNS ANALYSIS")
        logger.info("-" * 80)
        
        categorical_cols = {}
        for col_name, col_type in df.dtypes:
            if col_type == "string":
                distinct_count = df.select(col(col_name)).distinct().count()
                categorical_cols[col_name] = distinct_count
                logger.info(f"   {col_name}: {distinct_count:,} distinct values")
        
        profile_report['categorical_columns'] = categorical_cols
        
        # 4. Numeric Column Statistics (resale_price and floor_area_sqm)
        logger.info("\n4. NUMERIC COLUMNS STATISTICS (resale_price, floor_area_sqm)")
        logger.info("-" * 80)
        
        numeric_stats = {}
        
        # Calculate stats for specific numeric columns
        try:
            stats_data = df.select(
                spark_min(col("resale_price").cast("double")).alias("resale_price_min"),
                spark_max(col("resale_price").cast("double")).alias("resale_price_max"),
                percentile_approx(col("resale_price").cast("double"), 0.5).alias("resale_price_median"),
                avg(col("resale_price").cast("double")).alias("resale_price_mean"),
                spark_min(col("floor_area_sqm").cast("double")).alias("floor_area_min"),
                spark_max(col("floor_area_sqm").cast("double")).alias("floor_area_max"),
                percentile_approx(col("floor_area_sqm").cast("double"), 0.5).alias("floor_area_median"),
                avg(col("floor_area_sqm").cast("double")).alias("floor_area_mean")
            ).collect()[0]
            
            numeric_stats["resale_price"] = {
                "min": stats_data['resale_price_min'],
                "max": stats_data['resale_price_max'],
                "median": stats_data['resale_price_median'],
                "mean": round(stats_data['resale_price_mean'], 2) if stats_data['resale_price_mean'] else None
            }
            
            numeric_stats["floor_area_sqm"] = {
                "min": stats_data['floor_area_min'],
                "max": stats_data['floor_area_max'],
                "median": stats_data['floor_area_median'],
                "mean": round(stats_data['floor_area_mean'], 2) if stats_data['floor_area_mean'] else None
            }
            
            logger.info(f"\n   RESALE PRICE:")
            logger.info(f"     Min:    ${numeric_stats['resale_price']['min']:>15,.0f}")
            logger.info(f"     Max:    ${numeric_stats['resale_price']['max']:>15,.0f}")
            logger.info(f"     Median: ${numeric_stats['resale_price']['median']:>15,.0f}")
            logger.info(f"     Mean:   ${numeric_stats['resale_price']['mean']:>15,.2f}")
            
            logger.info(f"\n   FLOOR AREA (sqm):")
            logger.info(f"     Min:    {numeric_stats['floor_area_sqm']['min']:>15,.2f}")
            logger.info(f"     Max:    {numeric_stats['floor_area_sqm']['max']:>15,.2f}")
            logger.info(f"     Median: {numeric_stats['floor_area_sqm']['median']:>15,.2f}")
            logger.info(f"     Mean:   {numeric_stats['floor_area_sqm']['mean']:>15,.2f}")
            
        except Exception as e:
            logger.warning(f"   Could not compute numeric statistics: {e}")
        
        profile_report['numeric_statistics'] = numeric_stats
        
        logger.info("\n" + "=" * 80)
        return profile_report

    def check_null_values(self, df, critical_columns):
        """
        Validate no critical columns have nulls
        
        Args:
            df: PySpark DataFrame
            critical_columns: List of column names to check
            
        Returns:
            Boolean indicating if check passed
        """
        logger.info("Checking for null values...")
        
        # Filter to existing columns only
        existing_cols = [c for c in critical_columns if c in df.columns]
        
        if not existing_cols:
            logger.warning("No critical columns found in data")
            return True
        
        null_counts = df.select(
            [count(when(col(c).isNull(), 1)).alias(c) for c in existing_cols]
        ).collect()[0]
        
        self.validation_results['null_check'] = dict(null_counts.asDict())
        
        has_nulls = any(null_counts)
        if has_nulls:
            logger.warning(f"Null values found: {self.validation_results['null_check']}")
            return False
        
        logger.info("✓ No null values in critical columns")
        return True

    def check_row_count(self, df, min_rows=0):
        """
        Validate minimum row count
        
        Args:
            df: PySpark DataFrame
            min_rows: Minimum expected row count
            
        Returns:
            Boolean indicating if check passed
        """
        logger.info("Checking row count...")
        row_count = df.count()
        self.validation_results['row_count'] = row_count
        
        if row_count < min_rows:
            logger.error(f"Insufficient rows: {row_count} < {min_rows}")
            return False
        
        logger.info(f"✓ Row count valid: {row_count}")
        return True

    def validate(self, df, critical_columns, min_rows=100):
        """
        Execute all validations
        
        Args:
            df: PySpark DataFrame
            critical_columns: List of critical columns to check
            min_rows: Minimum expected row count
            
        Returns:
            Tuple of (is_valid: bool, validation_results: dict)
        """
        logger.info("=" * 50)
        logger.info("Starting Data Validation")
        logger.info("=" * 50)
        
        checks = [
            self.check_row_count(df, min_rows),
            self.check_null_values(df, critical_columns),
        ]

        master_fields_pass, master_fields_report = validate_master_fields(df)
        self.validation_results['master_fields_check'] = master_fields_pass
        self.validation_results['master_fields_report'] = master_fields_report
        checks.append(master_fields_pass)
        
        is_valid = all(checks)
        
        if is_valid:
            logger.info("✓ All validations passed!")
        else:
            logger.error("✗ Validation failed!")
        
        return is_valid, self.validation_results


def validate_master_fields(df, total=None, freq_pct=0.001, min_count=10, start_date="2012-01", end_date="2016-12"):
    """
    Validate Date (month), Town, Flat Type, Flat Model, storey_range based on dataset stats.
    
    Performs comprehensive validation of critical master fields against statistical properties
    and predefined patterns.
    
    Args:
        df: PySpark DataFrame to validate
        total: Total row count (calculated if not provided)
        freq_pct: Frequency percentage for threshold calculation (default: 0.001 = 0.1%)
        min_count: Minimum count threshold (default: 10)
        start_date: Start date for date range validation (YYYY-MM format, default: "2012-01")
        end_date: End date for date range validation (YYYY-MM format, default: "2016-12")
    
    Returns:
        Tuple of (is_valid: bool, report_dict: dict) where report_dict contains per-field validation results
    
    Example:
        is_valid, report = validate_master_fields(df, total=55391)
        if is_valid:
            print("All fields passed validation")
        for field, stats in report.items():
            print(f"{field}: {stats['pass']}")
    """
    from pyspark.sql.functions import to_date
    from utils.helpers import cat_stats
    import builtins
    
    report = {}
    total_rows = total if total is not None else df.count()
    # Use Python's built-in max/min, not PySpark's
    freq_threshold = builtins.max(min_count, int(total_rows * freq_pct))

    # 1) Date: month within configured start/end, no/low nulls
    min_month = df.select(spark_min("month")).collect()[0][0]
    max_month = df.select(spark_max("month")).collect()[0][0]
    null_months = df.filter(col("month").isNull()).count()
    out_of_range = df.filter((col("month") < to_date(lit(start_date), "yyyy-MM")) | (col("month") > to_date(lit(end_date), "yyyy-MM"))).count()
    report["month"] = {
        "min_observed": str(min_month),
        "max_observed": str(max_month),
        "null_count": null_months,
        "pct_null": round(null_months / total_rows * 100, 2) if total_rows > 0 else 0,
        "out_of_expected_range_count": out_of_range,
        "expected_range": f"{start_date} to {end_date}",
        "pass": (null_months == 0) and (out_of_range == 0)
    }

    # 2) Town
    report["town"] = cat_stats(df, "town", total_rows, freq_threshold)

    # 3) Flat Type
    report["flat_type"] = cat_stats(df, "flat_type", total_rows, freq_threshold)

    # 4) Flat Model
    report["flat_model"] = cat_stats(df, "flat_model", total_rows, freq_threshold)

    # 5) storey_range: additionally check formatting pattern "NN TO NN"
    storey_stats = cat_stats(df, "storey_range", total_rows, freq_threshold)
    pattern_match_count = df.filter(col("storey_range").rlike(r"^\d{2}\s+TO\s+\d{2}$")).count()
    storey_stats.update({
        "pattern_match_count": pattern_match_count,
        "pattern_match_pct": round(pattern_match_count / total_rows * 100, 2) if total_rows > 0 else 0,
        "pattern_pass": pattern_match_count / total_rows >= 0.95 if total_rows > 0 else False  # expect >=95% to match pattern
    })
    # tighten pass condition for storey_range: low nulls AND pattern pass
    storey_stats["pass"] = storey_stats["pass"] and storey_stats["pattern_pass"]
    report["storey_range"] = storey_stats

    # Overall pass: all individual passes true
    overall_pass = all(v["pass"] for v in report.values())
    return overall_pass, report

