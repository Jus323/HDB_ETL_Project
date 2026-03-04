"""Data transformation module"""

from pyspark.sql.functions import (
    col, to_date, trim, upper, add_months, current_date, 
    floor, datediff, make_date, lit, row_number, desc,
    regexp_replace, lpad, substring, date_format, concat,
    avg as spark_avg, sha2, percentile_approx
)
from pyspark.sql.window import Window
from utils.logger import get_logger

logger = get_logger(__name__)


class DataTransformer:
    """Handle data transformations"""

    def __init__(self):
        pass

    def cast_datatypes(self, df, schema_map=None):
        """
        Cast columns to specified data types
        
        Args:
            df: PySpark DataFrame
            schema_map: Dictionary mapping column names to target types
                       If None, uses predefined HDB schema
        
        Returns:
            DataFrame with casted columns
        """
        logger.info("Casting data types...")
        
        # Default schema for HDB resale prices
        if schema_map is None:
            schema_map = {
                "month": "date",
                "town": "string",
                "flat_type": "string",
                "block": "string",
                "street_name": "string",
                "storey_range": "string",
                "floor_area_sqm": "float",
                "flat_model": "string",
                "lease_commence_date": "integer",
                "resale_price": "float"
            }
        
        df_casted = df
        for col_name, target_type in schema_map.items():
            if col_name in df.columns:
                if target_type == "date":
                    df_casted = df_casted.withColumn(col_name, to_date(col(col_name), "yyyy-MM"))
                else:
                    df_casted = df_casted.withColumn(col_name, col(col_name).cast(target_type))
        
        logger.info("✓ Data types casted")
        return df_casted

    def deduplicate_by_full_rows(self, df):
        """Remove full-row duplicates"""
        logger.info("Deduplicating by full rows...")

        rows_before = df.count()

        # Full-row deduplication (all columns)
        df_dedup = df.dropDuplicates()

        rows_after = df_dedup.count()
        duplicates_removed = rows_before - rows_after

        logger.info(f"✓ Deduplicated data: {rows_before:,} → {rows_after:,} rows")
        logger.info(f"  Full-row duplicates removed: {duplicates_removed:,}")

        return df_dedup

    def standardize_data(self, df):
        """Standardize column values"""
        logger.info("Standardizing data...")
        
        # Identify text columns and remove leading/trailing whitespace, convert to uppercase
        df_standardized = df
        for col_name in df.columns:
            if df.select(col_name).dtypes[0][1] == "string":
                df_standardized = df_standardized.withColumn(
                    col_name, 
                    upper(trim(col(col_name)))
                )
        
        logger.info("✓ Data standardized")
        return df_standardized

    def filter_by_date_range(self, df, date_column, start_date, end_date):
        """
        Filter DataFrame by date range
        
        Args:
            df: Input PySpark DataFrame
            date_column: Name of the date column to filter on
            start_date: Start date string (e.g., "2012-01")
            end_date: End date string (e.g., "2016-12")
        
        Returns:
            DataFrame containing only rows within the date range
        
        Example:
            filtered_df = transformer.filter_by_date_range(
                df,
                date_column="month",
                start_date="2012-01",
                end_date="2016-12"
            )
        """
        logger.info(f"Filtering by date range: {start_date} to {end_date}")
        
        total_records = df.count()
        filter_condition = (col(date_column) >= start_date) & (col(date_column) <= end_date)
        
        filtered_df = df.filter(filter_condition)
        passed_count = filtered_df.count()
        failed_count = total_records - passed_count
        
        logger.info(f"✓ Date range filtering completed:")
        logger.info(f"  Total records: {total_records:,}")
        logger.info(f"  Passed filter: {passed_count:,}")
        logger.info(f"  Failed filter: {failed_count:,}")
        
        return filtered_df

    def compute_remaining_lease(self, df):
        """
        Compute remaining lease years and months
        
        Assumes lease is 99 years starting from lease_commence_date (Jan 1 of that year).
        
        Args:
            df: PySpark DataFrame with lease_commence_date column
            
        Returns:
            DataFrame with added lease calculation columns
        """
        logger.info("Computing remaining lease duration...")
        
        if "lease_commence_date" not in df.columns:
            logger.warning("lease_commence_date column not found, skipping lease calculation")
            return df
        
        lease_expiry_expr = add_months(
            make_date(col("lease_commence_date"), lit(1), lit(1)),
            99 * 12,
        )
        remaining_total_months_expr = floor(
            datediff(lease_expiry_expr, current_date()) / (365 / 12)
        )

        df_lease = (df
            # Compute final lease fields without persisting intermediate columns
            .withColumn("remaining_lease_years", floor(remaining_total_months_expr / 12))
            .withColumn("remaining_lease_months", remaining_total_months_expr % 12)
        )
        
        logger.info("✓ Lease duration computed")
        return df_lease

    def remove_resale_price_outliers(self, df, iqr_multiplier=3.0):
        """
        Remove resale price outliers using grouped IQR.

        Heuristic
        ---------
        Records are grouped by (town, flat_type, floor_area_sqm, flat_model,
        lease_commence_date) so each group represents transactions for the
        *same kind of flat in the same location and vintage*.  Within every
        group the first quartile (Q1) and third quartile (Q3) of resale_price
        are computed via ``percentile_approx``.  A record is considered an
        outlier when its resale_price falls outside:

            [Q1 - iqr_multiplier * IQR ,  Q3 + iqr_multiplier * IQR]

        where IQR = Q3 - Q1.

        Assumptions
        -----------
        * Prices within each property-profile group are approximately
          symmetrically distributed around the median, making IQR a robust
          measure of spread.
        * Groups with IQR = 0 (all identical prices) are excluded because
          no meaningful spread exists.
        * An ``iqr_multiplier`` of 3.0 (default) is deliberately lenient,
          flagging only extreme deviations while tolerating natural market
          variation.  Lower values (e.g. 1.5) would flag more records.
        * ``percentile_approx`` with accuracy 100 provides sufficiently
          precise quantile estimates for this dataset size.

        Args:
            df: PySpark DataFrame with at least ``resale_price`` and the
                grouping columns.
            iqr_multiplier: Scaling factor applied to the IQR (default 3.0).

        Returns:
            DataFrame with outlier rows removed.
        """
        group_columns = [
            "town",
            "flat_type",
            "floor_area_sqm",
            "flat_model",
            "lease_commence_date",
        ]
        missing_columns = [c for c in group_columns if c not in df.columns]
        if missing_columns:
            logger.warning(
                f"Required grouping columns not found, skipping outlier removal: {missing_columns}"
            )
            return df

        # Compute grouped quartiles
        grouped_stats = df.groupBy(*group_columns).agg(
            percentile_approx(col("resale_price"), 0.25, 100).alias("q1"),
            percentile_approx(col("resale_price"), 0.75, 100).alias("q3"),
        )
        grouped_stats = (
            grouped_stats
            .withColumn("iqr", col("q3") - col("q1"))
            .withColumn("lower_bound", col("q1") - (lit(iqr_multiplier) * col("iqr")))
            .withColumn("upper_bound", col("q3") + (lit(iqr_multiplier) * col("iqr")))
        )

        df_with_bounds = df.join(
            grouped_stats.select(*group_columns, "q1", "q3", "iqr", "lower_bound", "upper_bound"),
            on=group_columns,
            how="left",
        )

        total_before = df.count()

        # Keep non-outlier rows
        filtered_df = df_with_bounds.filter(
            # 1) Keep rows with missing resale_price (cannot classify as inlier/outlier)
            col("resale_price").isNull()
            # 2) Keep rows where quartiles could not be computed for the group
            | col("q1").isNull()
            | col("q3").isNull()
            # 3) Keep rows from groups with zero/invalid spread (IQR <= 0)
            | (col("iqr") <= 0)
            # 4) Keep rows whose resale_price falls within the IQR bounds
            | (
                (col("resale_price") >= col("lower_bound"))
                & (col("resale_price") <= col("upper_bound"))
            )
        ).drop("q1", "q3", "iqr", "lower_bound", "upper_bound")

        total_after = filtered_df.count()
        logger.info("✓ Outlier removal complete:")
        logger.info(f"  Grouping columns: {group_columns}")
        logger.info(f"  IQR multiplier: {iqr_multiplier}")
        logger.info(f"  Rows removed: {total_before - total_after:,}")

        return filtered_df

    def deduplicate_by_composite_key(self, df, key_columns=None):
        """
        Deduplicate rows based on composite key, keeping highest resale_price
        
        Args:
            df: PySpark DataFrame
            key_columns: List of columns forming the composite key
                        If None, uses all columns except resale_price
        
        Returns:
            Deduplicated DataFrame
        """
        logger.info("Deduplicating by composite key...")
        
        if key_columns is None:
            key_columns = [c for c in df.columns if c != "resale_price"]
        
        total_before = df.count()
        
        # Create window function: partition by composite key, order by resale_price descending
        window_spec = Window.partitionBy(*key_columns).orderBy(desc("resale_price"))
        
        # Add row number - 1 means highest price, 2+ means duplicates with lower prices
        df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
        
        # Keep only the highest price (row_num == 1)
        deduplicated_df = df_with_row_num.filter(col("row_num") == 1).drop("row_num")

        total_kept = deduplicated_df.count()
        
        logger.info(f"✓ Deduplication complete:")
        logger.info(f"  Original Records: {total_before:,}")
        logger.info(f"  Deduplicated Records (Kept): {total_kept:,}")
        logger.info(f"  Records Removed: {total_before - total_kept:,}")
        
        return deduplicated_df

    def create_resale_identifier(self, df):
        """
        Create 'Resale Identifier' column using business rules.

        Format:
        - 'S'
        - first 3 digits from block (non-digits removed, left-padded with zeros)
        - first 2 digits of avg resale_price grouped by year-month, town, flat_type
        - month as 2 digits
        - first character of town
        """
        logger.info("Creating Resale Identifier...")

        required_columns = ["block", "month", "town", "flat_type", "resale_price"]
        missing_columns = [c for c in required_columns if c not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns for Resale Identifier: {missing_columns}")

        avg_window = Window.partitionBy(
            date_format(col("month"), "yyyy-MM"),
            col("town"),
            col("flat_type")
        )
        #remove all characters that are not digits (0-9) from the "block" column
        block_digits = regexp_replace(col("block"), "[^0-9]", "")
        """
        extracts the first three characters of the block_digits string and pads them with leading 
        zeros to ensure the result is exactly 3 characters long. If block_digits is shorter than 3 characters, it 
        adds '0' to the left. If it is 3 or more, it takes the first three. 
        """
        block_3_digits = lpad(substring(block_digits, 1, 3), 3, "0")

        avg_price_digits = lpad(
            substring(floor(spark_avg(col("resale_price")).over(avg_window)).cast("long").cast("string"), 1, 2),
            2,
            "0"
        )

        month_digits = date_format(col("month"), "MM")
        town_initial = substring(col("town"), 1, 1)

        df_with_identifier = df.withColumn(
            "Resale Identifier",
            concat(
                lit("S"),
                block_3_digits,
                avg_price_digits,
                month_digits,
                town_initial
            )
        )

        logger.info("✓ Resale Identifier created")
        return df_with_identifier

    def hash_resale_identifier(self, df):
        """
        Hash 'Resale Identifier' with SHA-256 and validate uniqueness is preserved.
        """
        logger.info("Hashing Resale Identifier with SHA-256...")

        if "Resale Identifier" not in df.columns:
            raise ValueError("'Resale Identifier' column not found")

        unique_before = df.select(col("Resale Identifier")).distinct().count()

        hashed_df = df.withColumn("Resale Identifier", sha2(col("Resale Identifier"), 256))

        unique_after = hashed_df.select(col("Resale Identifier")).distinct().count()
        if unique_before != unique_after:
            raise ValueError(
                "Hash uniqueness check failed for 'Resale Identifier': "
                f"before={unique_before}, after={unique_after}"
            )

        logger.info("✓ Resale Identifier hashed with SHA-256")
        return hashed_df