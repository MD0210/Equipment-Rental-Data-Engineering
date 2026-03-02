# equipment_rental/exception/exception.py

class EquipmentRentalException(Exception):
    """
    Base exception class for Equipment Rental pipeline.
    All custom exceptions should inherit from this.
    """
    def __init__(self, message: str, errors=None):
        super().__init__(message)
        self.errors = errors


class BronzeIngestionException(EquipmentRentalException):
    """Exception raised during Bronze layer ingestion."""
    pass


class SilverValidationException(EquipmentRentalException):
    """Exception raised during Silver layer validation."""
    pass


class SilverTransformationException(EquipmentRentalException):
    """Exception raised during Silver layer transformation."""
    pass


class QuarantineProcessingException(EquipmentRentalException):
    """Exception raised during quarantine handling in Silver layer."""
    pass


class GoldAggregationException(EquipmentRentalException):
    """Exception raised during Gold layer aggregation."""
    pass


class PipelineManagerException(EquipmentRentalException):
    """Exception raised during Pipeline Manager operations (DB, batch, task)."""
    pass