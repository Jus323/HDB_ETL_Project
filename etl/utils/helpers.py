"""Utility helper functions"""


def cat_stats(df, field, total_rows, freq_threshold):
    """
    Calculate categorical statistics for a field
    
    Analyzes a categorical field in a DataFrame and returns distribution stats,
    distinct counts, null percentages, and top values.
    
    Args:
        df: Input PySpark DataFrame
        field: Field/column name to analyze
        total_rows: Total count of rows in DataFrame
        freq_threshold: Minimum frequency threshold for value inclusion
    
    Returns:
        Dictionary containing:
        - distinct_count: Number of distinct values
        - top_values: List of (value, count) tuples for top 10 values
        - null_count: Count of null values
        - pct_null: Percentage of null values
        - freq_threshold_count: Frequency threshold used
        - pass: Boolean indicating if field passes validation (<=1% nulls)
    
    Example:
        stats = cat_stats(df, "town", 55391, 100)
        print(stats["distinct_count"])  # Number of unique towns
        print(stats["pct_null"])        # Percentage of nulls
    """
    from pyspark.sql.functions import col, count as spark_count
    
    vc = df.groupBy(field).count().orderBy(col("count").desc())
    distinct = vc.count()
    top = vc.limit(10).collect()
    nulls = df.filter(col(field).isNull()).count()
    pct_null = round(nulls / total_rows * 100, 2) if total_rows > 0 else 0
    pass_flag = (pct_null <= 1.0)  # allow up to 1% nulls by default
    
    return {
        "distinct_count": distinct,
        "top_values": [(r[0], r[1]) for r in top],
        "null_count": nulls,
        "pct_null": pct_null,
        "freq_threshold_count": freq_threshold,
        "pass": pass_flag
    }
