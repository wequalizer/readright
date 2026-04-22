"""Apple Health export XML parser."""

from __future__ import annotations

import defusedxml.ElementTree as ET
from collections import defaultdict
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Most commonly requested Apple Health record types and their human labels.
# Full list is much longer; uncommon types fall through as-is.
_TYPE_LABELS = {
    "HKQuantityTypeIdentifierStepCount": "Steps",
    "HKQuantityTypeIdentifierDistanceWalkingRunning": "Walking/Running Distance",
    "HKQuantityTypeIdentifierDistanceCycling": "Cycling Distance",
    "HKQuantityTypeIdentifierFlightsClimbed": "Flights Climbed",
    "HKQuantityTypeIdentifierActiveEnergyBurned": "Active Energy Burned",
    "HKQuantityTypeIdentifierBasalEnergyBurned": "Basal Energy Burned",
    "HKQuantityTypeIdentifierHeartRate": "Heart Rate",
    "HKQuantityTypeIdentifierRestingHeartRate": "Resting Heart Rate",
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": "Heart Rate Variability (SDNN)",
    "HKQuantityTypeIdentifierOxygenSaturation": "Blood Oxygen Saturation",
    "HKQuantityTypeIdentifierBloodPressureSystolic": "Blood Pressure (Systolic)",
    "HKQuantityTypeIdentifierBloodPressureDiastolic": "Blood Pressure (Diastolic)",
    "HKQuantityTypeIdentifierBodyMass": "Body Mass",
    "HKQuantityTypeIdentifierBodyMassIndex": "Body Mass Index",
    "HKQuantityTypeIdentifierBodyFatPercentage": "Body Fat Percentage",
    "HKQuantityTypeIdentifierLeanBodyMass": "Lean Body Mass",
    "HKQuantityTypeIdentifierHeight": "Height",
    "HKQuantityTypeIdentifierBloodGlucose": "Blood Glucose",
    "HKQuantityTypeIdentifierDietaryEnergyConsumed": "Dietary Energy Consumed",
    "HKQuantityTypeIdentifierDietaryProtein": "Dietary Protein",
    "HKQuantityTypeIdentifierDietaryCarbohydrates": "Dietary Carbohydrates",
    "HKQuantityTypeIdentifierDietaryFatTotal": "Dietary Fat",
    "HKQuantityTypeIdentifierDietaryWater": "Dietary Water",
    "HKQuantityTypeIdentifierSleepAnalysis": "Sleep Analysis",
    "HKQuantityTypeIdentifierMindfulSession": "Mindful Session",
    "HKQuantityTypeIdentifierVO2Max": "VO2 Max",
    "HKCategoryTypeIdentifierSleepAnalysis": "Sleep Analysis",
    "HKCategoryTypeIdentifierMindfulSession": "Mindful Session",
    "HKQuantityTypeIdentifierStandTime": "Stand Time",
    "HKQuantityTypeIdentifierExerciseTime": "Exercise Time",
    "HKQuantityTypeIdentifierAppleExerciseTime": "Apple Exercise Time",
    "HKQuantityTypeIdentifierAppleStandHour": "Apple Stand Hour",
}

_DATE_FMT = "%Y-%m-%d %H:%M:%S %z"


def _parse_date(raw: str) -> str:
    """Parse Apple Health datetime string to ISO 8601."""
    try:
        return datetime.strptime(raw, _DATE_FMT).isoformat()
    except (ValueError, TypeError):
        return raw or ""


def _short_type(full_type: str) -> str:
    """Strip the HKQuantityTypeIdentifier / HKCategoryTypeIdentifier prefix."""
    for prefix in ("HKQuantityTypeIdentifier", "HKCategoryTypeIdentifier", "HKDataType"):
        if full_type.startswith(prefix):
            return full_type[len(prefix):]
    return full_type


class AppleHealthParser(BaseParser):
    """Parser for Apple Health export.xml.

    Apple Health exports a single export.xml file containing:
    - <HealthData> root element
    - <Me> element with personal attributes (date-of-birth, sex, blood-type, etc.)
    - Thousands of <Record> elements with health measurements
    - <Workout> elements for workout sessions
    - <ActivitySummary> elements for daily ring data

    Each <Record> has:
        type        — HKQuantityTypeIdentifier* or HKCategoryTypeIdentifier*
        sourceName  — device/app that recorded it
        sourceVersion — app version
        unit        — measurement unit (count, km, kcal, bpm, %, etc.)
        creationDate — when the record was created
        startDate   — measurement start
        endDate     — measurement end
        value       — the measured value

    This parser normalizes all Record elements into a flat list.
    Records from different types are mixed; consumers should filter by 'type'.
    """

    def source_type(self) -> str:
        return "apple_health_xml"

    def source_label(self) -> str:
        return "Apple Health Export XML"

    def detect(self, content: bytes, filename: str) -> float:
        name_lower = filename.lower()

        # Apple Health always exports as export.xml
        if name_lower == "export.xml":
            if b"<HealthData" in content[:4096]:
                return 0.98
            if b"HKQuantityTypeIdentifier" in content[:8192]:
                return 0.90

        # Also accept .xml files with Apple Health markers
        if name_lower.endswith(".xml"):
            if b"<HealthData" in content[:4096]:
                return 0.95
            if b"HKQuantityTypeIdentifier" in content[:8192]:
                return 0.80

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="type", dtype="string", description="Full Apple Health type identifier",
                                examples=["HKQuantityTypeIdentifierStepCount", "HKQuantityTypeIdentifierHeartRate"]),
                FieldAnnotation(name="type_label", dtype="string",
                                description="Human-readable label for the type",
                                examples=["Steps", "Heart Rate", "Body Mass"]),
                FieldAnnotation(name="type_short", dtype="string",
                                description="Type identifier with HK prefix stripped",
                                examples=["StepCount", "HeartRate", "BodyMass"]),
                FieldAnnotation(name="source_name", dtype="string",
                                description="Device or app that recorded this measurement",
                                examples=["iPhone", "Apple Watch", "MyFitnessPal"]),
                FieldAnnotation(name="source_version", dtype="string",
                                description="Version of the recording source", nullable=True),
                FieldAnnotation(name="unit", dtype="string",
                                description="Unit of the value",
                                examples=["count", "km", "kcal", "bpm", "%", "kg", "mg/dL"]),
                FieldAnnotation(name="start_date", dtype="date",
                                description="Measurement start timestamp (ISO 8601)", format="ISO8601"),
                FieldAnnotation(name="end_date", dtype="date",
                                description="Measurement end timestamp (ISO 8601)", format="ISO8601"),
                FieldAnnotation(name="creation_date", dtype="date",
                                description="Record creation timestamp (ISO 8601)", format="ISO8601", nullable=True),
                FieldAnnotation(name="value", dtype="string",
                                description="Measured value as string (cast to decimal for numeric types)"),
                FieldAnnotation(name="device", dtype="string",
                                description="Device hardware identifier", nullable=True),
            ],
            conventions=[
                "All record types are mixed in one list — filter by 'type' or 'type_label' to get specific metrics.",
                "Dates are normalized from Apple's 'YYYY-MM-DD HH:MM:SS +ZZZZ' to ISO 8601.",
                "Value is always a string; cast to float for arithmetic. Some category types have non-numeric values.",
                "A single day may have hundreds of step count records (one per walking segment) — aggregate with sum.",
                "Heart rate records are individual beats — use mean/median for averages.",
                "Sleep analysis 'value' is 0=In Bed, 1=Asleep, 2=Awake (category type, not quantity).",
                "Export may contain millions of records for active users — consider filtering by type or date range.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        try:
            # ElementTree handles encoding declaration in the XML itself
            root = ET.fromstring(content)
        except ET.ParseError as e:
            return ParseResult(success=False, error=f"XML parse error: {e}")

        if root.tag != "HealthData":
            return ParseResult(success=False, error=f"Root element is '{root.tag}', expected 'HealthData'")

        rows = []
        warnings = []
        skipped = 0

        for record in root.iter("Record"):
            try:
                row = self._parse_record(record)
                rows.append(row)
            except Exception as e:
                skipped += 1
                if skipped <= 5:
                    warnings.append(f"Skipped record: {e}")

        if skipped > 5:
            warnings.append(f"... {skipped - 5} more records skipped")

        if not rows:
            return ParseResult(success=False, error="No Record elements found in HealthData XML")

        # Summarize what types are present
        type_counts: dict[str, int] = defaultdict(int)
        for row in rows:
            type_counts[row["type_label"]] += 1
        summary = ", ".join(f"{label}: {count}" for label, count in
                            sorted(type_counts.items(), key=lambda x: -x[1])[:10])
        warnings.insert(0, f"Record types found (top 10): {summary}")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _parse_record(self, elem: ET.Element) -> dict:
        full_type = elem.get("type", "")
        return {
            "type": full_type,
            "type_label": _TYPE_LABELS.get(full_type, _short_type(full_type)),
            "type_short": _short_type(full_type),
            "source_name": elem.get("sourceName", ""),
            "source_version": elem.get("sourceVersion") or None,
            "unit": elem.get("unit", ""),
            "start_date": _parse_date(elem.get("startDate", "")),
            "end_date": _parse_date(elem.get("endDate", "")),
            "creation_date": _parse_date(elem.get("creationDate", "")) or None,
            "value": elem.get("value", ""),
            "device": elem.get("device") or None,
        }


registry.register(AppleHealthParser())
