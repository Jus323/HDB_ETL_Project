"""Data extraction module"""

import os
import shutil
from pathlib import Path
from pyspark.sql.functions import col
from utils.logger import get_logger

logger = get_logger(__name__)


class DataExtractor:
    """Handle data extraction from various sources"""

    def __init__(self, spark):
        self.spark = spark

    def extract_csv(self, file_path, header=True, infer_schema=True):
        """
        Extract a single CSV file into a DataFrame.

        Args:
            file_path: Path to CSV file
            header: Whether CSV contains header row
            infer_schema: Whether to infer column types

        Returns:
            PySpark DataFrame
        """
        logger.info(f"Reading CSV: {file_path}")
        return (
            self.spark.read
            .option("header", header)
            .option("inferSchema", infer_schema)
            .csv(file_path)
        )

    def extract_and_combine(self, config, loader=None):
        """
        Extract all CSV files from source directory and combine into single dataset
        
        This method:
        1. Discovers all CSV files in config.source_path
        2. Loads each file as a DataFrame
        3. Saves raw extracted data to output/raw/
        4. Combines all files into a single DataFrame
        5. Selects only critical columns
        6. Returns combined DataFrame and extraction statistics
        
        Args:
            config: ETLConfig instance with source_path and critical_columns
            loader: Optional DataLoader instance to save raw files
        
        Returns:
            Tuple of (combined_df: DataFrame, extraction_stats: list)
            - combined_df: Single combined DataFrame with all records and critical columns
            - extraction_stats: List of dicts with file extraction details
        
        Example:
            combined_df, stats = extractor.extract_and_combine(config, loader)
            for stat in stats:
                print(f"{stat['file']}: {stat['rows']:,} rows")
        """
        logger.info("="*80)
        logger.info("EXTRACTION: Discovering and loading CSV files")
        logger.info("="*80)
        
        source_dir = config.source_path
        combined_df = None
        extraction_stats = []
        
        # Discover CSV files
        if not os.path.exists(source_dir):
            logger.error(f"Source directory not found: {source_dir}")
            return None, []
        
        csv_files = sorted([f for f in os.listdir(source_dir) if f.endswith('.csv')])
        logger.info(f"Found {len(csv_files)} CSV files in {source_dir}")
        
        if not csv_files:
            logger.warning("No CSV files found in source directory")
            return None, []
        
        # Create output directory for raw files
        raw_output_dir = "output/raw"
        os.makedirs(raw_output_dir, exist_ok=True)
        
        # Extract and combine each file
        for idx, file in enumerate(csv_files, 1):
            try:
                file_path = os.path.join(source_dir, file)
                logger.info(f"\n  [{idx}/{len(csv_files)}] Extracting: {file}")
                
                # Extract single file
                raw_df = self.extract_csv(file_path)
                row_count = raw_df.count()
                col_count = len(raw_df.columns)
                
                extraction_stats.append({
                    "file": file,
                    "rows": row_count,
                    "columns": col_count,
                    "status": "✓ Extracted"
                })
                
                logger.info(f"    Records: {row_count:,} | Columns: {col_count}")
                
                # Save raw data if loader provided
                if loader:
                    output_filename = f"{Path(file).stem}_raw"
                    temp_path = os.path.join(raw_output_dir, output_filename)
                    loader.load_csv(raw_df, temp_path, mode="overwrite")
                    
                    # Rename part file
                    part_files = [f for f in os.listdir(temp_path) if f.startswith("part-")]
                    if part_files:
                        part_file = part_files[0]
                        part_path = os.path.join(temp_path, part_file)
                        final_path = os.path.join(raw_output_dir, f"{output_filename}.csv")
                        shutil.move(part_path, final_path)
                        shutil.rmtree(temp_path)
                        logger.info(f"    Saved raw data to {final_path}")
                
                # Select only critical columns
                available_cols = [c for c in config.critical_columns if c in raw_df.columns]
                raw_df_filtered = raw_df.select([col(c) for c in available_cols])
                
                # Combine with previous dataframes
                if combined_df is None:
                    combined_df = raw_df_filtered
                else:
                    combined_df = combined_df.unionByName(raw_df_filtered)
                
                logger.info(f"    ✓ Combined successfully")
                
            except Exception as e:
                logger.error(f"Error extracting {file}: {e}")
                extraction_stats[-1]["status"] = f"✗ Failed: {str(e)[:50]}"
        
        # Log summary
        if combined_df is not None:
            total_rows = combined_df.count()
            total_cols = len(combined_df.columns)
            logger.info(f"\n{'='*80}")
            logger.info(f"EXTRACTION COMPLETE")
            logger.info(f"  Total Combined Records: {total_rows:,}")
            logger.info(f"  Total Columns: {total_cols}")
            logger.info(f"  Columns: {combined_df.columns}")
            logger.info(f"{'='*80}\n")
            return combined_df, extraction_stats
        else:
            logger.error("No data extracted - all files failed")
            return None, extraction_stats