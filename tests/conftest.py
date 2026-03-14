import pytest
from unittest.mock import AsyncMock, MagicMock
from decimal import Decimal
from datetime import datetime

@pytest.fixture
def mock_blob_service():
    service = AsyncMock()
    service.upload_file = AsyncMock(return_value="test_blob.csv")
    
    # Mock para download_file_stream que debe ser un generador asíncrono
    async def mock_stream(blob_name):
        # Simulamos chunks de bytes
        yield b"date,product_id,quantity,price\n"
        yield b"2026-01-01,1001,2,10.50\n"
        yield b"2026-01-02,1002,1,5.20\n"
    
    service.download_file_stream = mock_stream
    return service

@pytest.fixture
def mock_queue_service():
    service = AsyncMock()
    service.send_message = AsyncMock(return_value=None)
    service.receive_messages = AsyncMock(return_value=[])
    service.delete_message = AsyncMock(return_value=None)
    service.decode_message = MagicMock(return_value={"job_id": "test-uuid", "blob_name": "test.csv"})
    return service

@pytest.fixture
def mock_job_repo():
    repo = AsyncMock()
    repo.create_job = AsyncMock(return_value="550e8400-e29b-41d4-a716-446655440000")
    repo.get_job = AsyncMock(return_value={
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "file_name": "test.csv",
        "status": "PENDING",
        "created_at": datetime.now(),
        "error_message": None
    })
    repo.update_status = AsyncMock(return_value=None)
    return repo

@pytest.fixture
def mock_sales_repo():
    repo = AsyncMock()
    repo.bulk_insert_sales = AsyncMock(return_value=5)
    repo.upsert_daily_summary = AsyncMock(return_value=None)
    return repo

@pytest.fixture
def sample_csv_content():
    return b"date,product_id,quantity,price\n2026-01-01,1001,2,10.50\n2026-01-02,1002,1,5.20\n"
