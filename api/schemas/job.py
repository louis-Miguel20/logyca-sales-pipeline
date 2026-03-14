from enum import Enum
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field

class JobStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class JobResponse(BaseModel):
    id: str = Field(alias="job_id")
    status: JobStatus
    file_name: str
    created_at: Optional[datetime] = None
    error_message: Optional[str] = None
    
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

class UploadResponse(BaseModel):
    job_id: str
    message: str
    status: str
