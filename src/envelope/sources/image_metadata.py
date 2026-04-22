"""Image metadata parser — EXIF, IPTC, XMP from JPEG, TIFF, PNG.

Extracts camera settings, GPS coordinates, timestamps, and document properties.
Uses exifread for comprehensive EXIF support (pure Python, no deps).

Dependency: exifread
    Install: pip install exifread
"""

from __future__ import annotations

import io
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


# Image magic bytes
_JPEG_MAGIC = (b"\xff\xd8\xff",)
_TIFF_MAGIC = (b"II\x2a\x00", b"MM\x00\x2a")  # Little/big endian
_PNG_MAGIC = (b"\x89PNG",)

_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".tif", ".tiff", ".png", ".heic", ".heif"}


def _dms_to_decimal(values, ref: str) -> float | None:
    """Convert EXIF GPS DMS (degrees, minutes, seconds) to decimal degrees."""
    try:
        if not values or len(values) < 3:
            return None
        d = float(values[0])
        m = float(values[1])
        s = float(values[2])
        decimal = d + m / 60 + s / 3600
        if ref in ("S", "W"):
            decimal = -decimal
        return round(decimal, 6)
    except (ValueError, TypeError, ZeroDivisionError):
        return None


def _exif_value(tags: dict, key: str) -> str | None:
    """Safely extract an EXIF tag value as string."""
    tag = tags.get(key)
    if tag is None:
        return None
    val = str(tag).strip()
    return val if val else None


class ImageMetadataParser(BaseParser):
    """Extracts EXIF metadata from image files.

    Returns a single row per image with all available metadata:
    camera, lens, settings, GPS, timestamps, and document properties.

    Useful for photographers, forensics, privacy auditing, and cataloging.
    """

    def source_type(self) -> str:
        return "image_metadata"

    def source_label(self) -> str:
        return "Image EXIF Metadata"

    def detect(self, content: bytes, filename: str) -> float:
        try:
            import exifread  # noqa: F401
        except ImportError:
            return 0.0

        name_lower = filename.lower()
        for ext in _IMAGE_EXTENSIONS:
            if name_lower.endswith(ext):
                return 0.85

        # Magic bytes
        head = content[:8]
        for magic in _JPEG_MAGIC + _TIFF_MAGIC + _PNG_MAGIC:
            if head[:len(magic)] == magic:
                return 0.80

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="camera_make", dtype="string", description="Camera manufacturer", nullable=True),
                FieldAnnotation(name="camera_model", dtype="string", description="Camera model", nullable=True),
                FieldAnnotation(name="lens_model", dtype="string", description="Lens model", nullable=True),
                FieldAnnotation(name="date_taken", dtype="date", description="When the photo was taken", format="ISO8601", nullable=True),
                FieldAnnotation(name="date_digitized", dtype="date", description="When the photo was digitized", format="ISO8601", nullable=True),
                FieldAnnotation(name="image_width", dtype="integer", description="Image width in pixels", nullable=True),
                FieldAnnotation(name="image_height", dtype="integer", description="Image height in pixels", nullable=True),
                FieldAnnotation(name="orientation", dtype="string", description="Image orientation (1-8 EXIF rotation)", nullable=True),
                FieldAnnotation(name="iso", dtype="integer", description="ISO sensitivity", nullable=True),
                FieldAnnotation(name="aperture", dtype="string", description="Aperture (f-number)", nullable=True),
                FieldAnnotation(name="shutter_speed", dtype="string", description="Shutter speed", nullable=True),
                FieldAnnotation(name="focal_length", dtype="string", description="Focal length in mm", nullable=True),
                FieldAnnotation(name="flash", dtype="string", description="Flash status", nullable=True),
                FieldAnnotation(name="exposure_mode", dtype="string", description="Exposure mode (auto, manual, etc.)", nullable=True),
                FieldAnnotation(name="white_balance", dtype="string", description="White balance mode", nullable=True),
                FieldAnnotation(name="gps_latitude", dtype="decimal", description="GPS latitude in decimal degrees", nullable=True),
                FieldAnnotation(name="gps_longitude", dtype="decimal", description="GPS longitude in decimal degrees", nullable=True),
                FieldAnnotation(name="gps_altitude", dtype="decimal", description="GPS altitude in meters", nullable=True, unit="m"),
                FieldAnnotation(name="software", dtype="string", description="Software used to process the image", nullable=True),
                FieldAnnotation(name="copyright", dtype="string", description="Copyright notice", nullable=True),
                FieldAnnotation(name="artist", dtype="string", description="Artist/photographer name", nullable=True),
                FieldAnnotation(name="file_size_bytes", dtype="integer", description="File size in bytes"),
            ],
            conventions=[
                "Returns a single row per image with all available EXIF metadata.",
                "Fields are null when not present in the image's EXIF data.",
                "GPS coordinates are decimal degrees (positive = N/E, negative = S/W).",
                "date_taken is the original capture time. date_digitized may differ for scanned photos.",
                "orientation is the EXIF rotation flag (1=normal, 6=rotated 90° CW, etc.).",
                "aperture is displayed as the f-number string (e.g. 'f/2.8').",
                "Images without EXIF data (screenshots, web images, stripped photos) return minimal info.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        try:
            import exifread
        except ImportError:
            return ParseResult(
                success=False,
                error="exifread is not installed. Install it with: pip install exifread",
            )

        warnings: list[str] = []

        try:
            tags = exifread.process_file(io.BytesIO(content), details=False)
        except Exception as e:
            return ParseResult(success=False, error=f"Failed to read EXIF data: {e}")

        # Parse EXIF date format: "YYYY:MM:DD HH:MM:SS"
        def parse_exif_date(key: str) -> str | None:
            val = _exif_value(tags, key)
            if not val:
                return None
            try:
                # EXIF dates use colons: 2024:01:15 14:30:00
                val = val.replace(":", "-", 2)  # Fix date part only
                from datetime import datetime
                dt = datetime.fromisoformat(val)
                return dt.isoformat()
            except (ValueError, TypeError):
                return val

        # GPS extraction
        gps_lat = None
        gps_lon = None
        gps_alt = None

        lat_tag = tags.get("GPS GPSLatitude")
        lat_ref = _exif_value(tags, "GPS GPSLatitudeRef") or "N"
        lon_tag = tags.get("GPS GPSLongitude")
        lon_ref = _exif_value(tags, "GPS GPSLongitudeRef") or "E"

        if lat_tag and lon_tag:
            try:
                gps_lat = _dms_to_decimal(lat_tag.values, lat_ref)
                gps_lon = _dms_to_decimal(lon_tag.values, lon_ref)
            except Exception:
                pass

        alt_tag = tags.get("GPS GPSAltitude")
        if alt_tag:
            try:
                gps_alt = round(float(alt_tag.values[0]), 1)
                alt_ref = tags.get("GPS GPSAltitudeRef")
                if alt_ref and str(alt_ref) == "1":
                    gps_alt = -gps_alt
            except Exception:
                pass

        # Safe integer extraction
        def safe_int(key: str) -> int | None:
            val = _exif_value(tags, key)
            if not val:
                return None
            try:
                return int(float(val))
            except (ValueError, TypeError):
                return None

        row = {
            "camera_make": _exif_value(tags, "Image Make"),
            "camera_model": _exif_value(tags, "Image Model"),
            "lens_model": _exif_value(tags, "EXIF LensModel") or _exif_value(tags, "EXIF LensInfo"),
            "date_taken": parse_exif_date("EXIF DateTimeOriginal") or parse_exif_date("Image DateTime"),
            "date_digitized": parse_exif_date("EXIF DateTimeDigitized"),
            "image_width": safe_int("EXIF ExifImageWidth") or safe_int("Image ImageWidth"),
            "image_height": safe_int("EXIF ExifImageLength") or safe_int("Image ImageLength"),
            "orientation": _exif_value(tags, "Image Orientation"),
            "iso": safe_int("EXIF ISOSpeedRatings"),
            "aperture": _exif_value(tags, "EXIF FNumber") or _exif_value(tags, "EXIF ApertureValue"),
            "shutter_speed": _exif_value(tags, "EXIF ExposureTime") or _exif_value(tags, "EXIF ShutterSpeedValue"),
            "focal_length": _exif_value(tags, "EXIF FocalLength"),
            "flash": _exif_value(tags, "EXIF Flash"),
            "exposure_mode": _exif_value(tags, "EXIF ExposureMode") or _exif_value(tags, "EXIF ExposureProgram"),
            "white_balance": _exif_value(tags, "EXIF WhiteBalance"),
            "gps_latitude": gps_lat,
            "gps_longitude": gps_lon,
            "gps_altitude": gps_alt,
            "software": _exif_value(tags, "Image Software"),
            "copyright": _exif_value(tags, "Image Copyright"),
            "artist": _exif_value(tags, "Image Artist"),
            "file_size_bytes": len(content),
        }

        # Warn if no meaningful EXIF
        meaningful = ["camera_make", "camera_model", "date_taken", "gps_latitude", "iso"]
        if not any(row.get(f) for f in meaningful):
            warnings.append("No meaningful EXIF data found. Image may have been stripped of metadata.")

        if gps_lat is not None:
            warnings.append("GPS coordinates present — this image contains location data.")

        schema = SchemaAnnotation(
            source_type=self.source_type(),
            source_label=f"Image EXIF: {filename}" if filename else self.source_label(),
            fields=self.schema().fields,
            conventions=list(self.schema().conventions),
        )

        envelope = ContextEnvelope(
            schema=schema,
            data=[row],
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)


registry.register(ImageMetadataParser())
