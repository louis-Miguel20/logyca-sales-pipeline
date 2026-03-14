import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import UploadFile
from services.upload_service import UploadService

class TestUploadService:
    
    @pytest.fixture
    def service(self, mock_blob_service, mock_queue_service, mock_job_repo):
        # Mockear el logger igual que en csv_processor
        with patch("services.upload_service.logger") as mock_logger:
            mock_logger.bind.return_value = mock_logger
            mock_logger.info = AsyncMock()
            mock_logger.error = AsyncMock()
            
            service = UploadService()
            service.blob_service = mock_blob_service
            service.queue_service = mock_queue_service
            service.job_repo = mock_job_repo
            yield service

    async def test_process_upload_success(self, service, sample_csv_content):
        # Arrange
        file = UploadFile(filename="test.csv", file=MagicMock())
        file.read = AsyncMock(return_value=sample_csv_content)

        # Act
        job_id = await service.process_upload(file)

        # Assert
        assert job_id == "550e8400-e29b-41d4-a716-446655440000"
        service.job_repo.create_job.assert_called_once_with("test.csv")
        service.blob_service.upload_file.assert_called_once()
        service.queue_service.send_message.assert_called_once()

    async def test_process_upload_blob_fails(self, service, sample_csv_content):
        # Arrange
        file = UploadFile(filename="test.csv", file=MagicMock())
        file.read = AsyncMock(return_value=sample_csv_content)
        service.blob_service.upload_file.side_effect = Exception("Blob Error")

        # Act & Assert
        with pytest.raises(Exception, match="Blob Error"):
            await service.process_upload(file)
        
        # Verificar que se intentó crear el job pero falló la subida
        service.job_repo.create_job.assert_called_once()
        service.queue_service.send_message.assert_not_called()
