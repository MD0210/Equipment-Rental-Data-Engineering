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

class SLAAlertException(Exception):
    """
    Exception raised when a pipeline stage breaches SLA
    or when sending an SLA alert email fails.
    """
    def __init__(self, stage: str, pipeline_run_id: str, duration: float, max_allowed: float, message: str = None):
        self.stage = stage
        self.pipeline_run_id = pipeline_run_id
        self.duration = duration
        self.max_allowed = max_allowed
        self.message = message or f"SLA breached for stage '{stage}' | duration: {duration:.2f}s, allowed: {max_allowed:.2f}s"
        super().__init__(self.message)

class PipelineEmailException(Exception):
    """
    Exception raised when sending pipeline alert emails fails.
    """
    def __init__(self, stage: str, pipeline_run_id: str, original_error: Exception):
        self.stage = stage
        self.pipeline_run_id = pipeline_run_id
        self.original_error = original_error
        self.message = f"Failed to send email for stage '{stage}' | pipeline_run_id={pipeline_run_id} | error={str(original_error)}"
        super().__init__(self.message)