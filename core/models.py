# pyre-strict

from dataclasses import dataclass, field
from typing import Any, Dict, List, Protocol, runtime_checkable


@runtime_checkable
class SessionNode(Protocol):
    """Protocol for nodes representing pipeline instances.

    Each node represents one pipeline execution instance with metadata
    and status tracking. Implementations must provide these attributes.
    """

    capture_date: str
    capture_time: str
    participant_id: str
    metadata: Dict[str, Any]


@dataclass
class StandardSessionNode:
    """Concrete implementation of SessionNode with proper mutable defaults.

    This is the standard node type for most use cases. It correctly handles
    initialization of mutable defaults (metadata dict and errors list) to
    avoid the common Python pitfall of shared mutable default arguments.

    Attributes:
        capture_date: Date of capture in YYYYMMDD format.
        capture_time: Time of capture in HHMM format.
        participant_id: Participant identifier (e.g., 'ABC123').
        metadata: Extensible dictionary for provider-enriched data.
        errors: List of error records from providers.
    """

    id: str
    capture_date: str  # YYYYMMDD
    capture_time: str  # HHMM
    participant_id: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    errors: List[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate capture_date and capture_time formats."""
        # Validate capture_date: must be YYYYMMDD (8 digits)
        if not (len(self.capture_date) == 8 and self.capture_date.isdigit()):
            raise ValueError(
                f"capture_date must be in YYYYMMDD format, got: '{self.capture_date}'"
            )
        month = int(self.capture_date[4:6])
        day = int(self.capture_date[6:8])
        if not (1 <= month <= 12):
            raise ValueError(
                f"capture_date month must be 01-12, got: '{self.capture_date[4:6]}'"
            )
        if not (1 <= day <= 31):
            raise ValueError(
                f"capture_date day must be 01-31, got: '{self.capture_date[6:8]}'"
            )

        # Validate capture_time: must be HHMM where HH < 24 and MM < 60
        if not (len(self.capture_time) == 4 and self.capture_time.isdigit()):
            raise ValueError(
                f"capture_time must be in HHMM format, got: '{self.capture_time}'"
            )
        hour = int(self.capture_time[:2])
        minute = int(self.capture_time[2:4])
        if not (0 <= hour <= 23):
            raise ValueError(
                f"capture_time hour must be 00-23, got: '{self.capture_time[:2]}'"
            )
        if not (0 <= minute <= 59):
            raise ValueError(
                f"capture_time minute must be 00-59, got: '{self.capture_time[2:4]}'"
            )

    def __hash__(self) -> int:
        return hash(f"{self.participant_id}_{self.capture_date}_{self.capture_time}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StandardSessionNode):
            return False
        return (
            self.participant_id == other.participant_id
            and self.capture_date == other.capture_date
            and self.capture_time == other.capture_time
        )

    @property
    def study_key(self) -> str:
        """Generate unique study key for this node."""
        return f"study_{self.participant_id}_{self.capture_date}"


@dataclass
class RuleResult:
    """Result of evaluating a rule against a node.

    Attributes:
        passed: Whether the rule check passed.
        name: Name of the rule that produced this result.
        message: Optional message explaining the result (usually for failures).
        metadata: Additional context/data from the rule evaluation.
    """

    passed: bool
    name: str
    message: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HiveCaptureRecord:
    """Data record from ghs_stages_by_system_detailed Hive table.

    Contains capture pipeline status information for a participant session.
    Used by HiveCaptureProvider to enrich nodes with capture stage data.

    Attributes:
        participant_id: Participant identifier (e.g., 'ABC123').
        capture_date: Date of capture in YYYYMMDD format.
        scheduling_stage: Scheduling status from Hive.
        readiness_stage: Participant readiness status.
        capture_stage_lighticon_decoder: Lighticon decoder capture status.
        capture_stage_iphone: iPhone capture status.
        capture_stage_decoder: Decoder capture status.
        ghsdoctor_stage_decoder: GHS Doctor stage status.
        dcpp: DCPP processing status.
    """

    participant_id: str
    capture_date: str
    scheduling_stage: str
    readiness_stage: str
    capture_stage_lighticon_decoder: str
    capture_stage_iphone: str
    capture_stage_decoder: str
    ghsdoctor_stage_decoder: str
    dcpp: str
