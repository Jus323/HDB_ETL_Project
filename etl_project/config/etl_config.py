"""Configuration management for ETL pipeline"""


class ETLConfig:
    """Centralized configuration management"""

    def __init__(self):
        # Data paths
        self.source_path = "../ResaleFlatPrices/"
        self.destination_path = "output/processed_data"

        # Data formats
        self.source_format = "csv"
        self.target_format = "csv"

        # Pipeline parameters
        self.min_rows = 100

        # Date filter parameters
        self.start_month = "2012-01"
        self.end_month = "2016-12"

        # Critical columns for validation
        self.critical_columns = [
            "month", "town", "flat_type", "block", "street_name",
            "storey_range", "floor_area_sqm", "flat_model",
            "lease_commence_date", "resale_price"
        ]

