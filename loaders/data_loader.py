"""Data loading module"""

import os
from utils.logger import get_logger

logger = get_logger(__name__)


class DataLoader:
    """Handle data loading to destinations"""

    def __init__(self, spark):
        self.spark = spark

    def _rename_single_output_file(self, path, extension, output_file_name):
        """
        Rename Spark's generated part file to a deterministic filename.

        Args:
            path: Output directory where Spark wrote files
            extension: File extension to match (e.g., ".csv", ".parquet")
            output_file_name: Desired filename (with or without extension)
        """
        if not output_file_name:
            return

        target_name = output_file_name
        if not target_name.lower().endswith(extension):
            target_name = f"{target_name}{extension}"

        part_files = [
            file_name for file_name in os.listdir(path)
            if file_name.startswith("part-") and file_name.lower().endswith(extension)
        ]

        if not part_files:
            logger.warning(f"No Spark part file found in {path} to rename")
            return

        source_path = os.path.join(path, part_files[0])
        target_path = os.path.join(path, target_name)

        if os.path.exists(target_path):
            os.remove(target_path)

        os.replace(source_path, target_path)
        logger.info(f"Renamed output file to {target_name}")

    def load_csv(self, df, path, mode="overwrite", output_file_name=None):
        """
        Write DataFrame to CSV
        
        Args:
            df: PySpark DataFrame
            path: Destination path
            mode: Write mode (overwrite, append, ignore, error)
            output_file_name: Optional explicit filename for the CSV output
        """
        logger.info(f"Loading CSV to {path}")
        os.makedirs(path, exist_ok=True)
        df.coalesce(1).write.mode(mode).option("header", True).csv(path)
        self._rename_single_output_file(path, ".csv", output_file_name)
        logger.info("✓ CSV load complete")

    def load_parquet(self, df, path, mode="overwrite", output_file_name=None):
        """
        Write DataFrame to Parquet
        
        Args:
            df: PySpark DataFrame
            path: Destination path
            mode: Write mode (overwrite, append, ignore, error)
            output_file_name: Optional explicit filename for the Parquet output
        """
        logger.info(f"Loading Parquet to {path}")
        os.makedirs(path, exist_ok=True)
        df.coalesce(1).write.mode(mode).parquet(path)
        self._rename_single_output_file(path, ".parquet", output_file_name)
        logger.info("✓ Parquet load complete")

    def load(self, df, path, format="csv", mode="overwrite", output_file_name=None):
        """
        Generic load method based on format
        
        Args:
            df: PySpark DataFrame
            path: Destination path
            format: Output format (csv, parquet)
            mode: Write mode (overwrite, append, ignore, error)
            output_file_name: Optional explicit output filename
        """
        logger.info(f"Loading data to {path} in {format} format")
        
        if format.lower() == "csv":
            self.load_csv(df, path, mode, output_file_name=output_file_name)
        elif format.lower() == "parquet":
            self.load_parquet(df, path, mode, output_file_name=output_file_name)
        else:
            logger.error(f"Unsupported format: {format}")
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"✓ Data load complete: {df.count():,} records")