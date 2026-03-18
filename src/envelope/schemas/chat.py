"""Chat message schemas."""

from datetime import datetime

from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    """Normalized chat message — works for WhatsApp, Telegram, etc."""

    timestamp: datetime = Field(description="Message timestamp")
    sender: str = Field(description="Sender name or phone number")
    text: str = Field(default="", description="Message text content")
    is_media: bool = Field(default=False, description="Whether this is a media message")
    media_type: str = Field(default="", description="Type of media: image, video, audio, document, sticker")
    is_system: bool = Field(default=False, description="System message (group created, user joined, etc.)")
