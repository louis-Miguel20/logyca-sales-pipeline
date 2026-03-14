import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from repositories.job_repository import JobRepository

class TestJobRepository:
    
    @pytest.fixture
    def mock_pool(self):
        pool = MagicMock() # IMPORTANTE: MagicMock, no AsyncMock, para que acquire() no sea awaitable
        conn = AsyncMock()
        
        # Configurar el contexto asíncrono
        cm = MagicMock()
        cm.__aenter__.return_value = conn
        cm.__aexit__.return_value = None
        
        pool.acquire.return_value = cm
        
        return pool, conn

    async def test_create_job_returns_uuid(self, mock_pool):
        # Arrange
        pool, conn = mock_pool
        conn.fetchval = AsyncMock(return_value="550e8400-e29b-41d4-a716-446655440000")
        
        repo = JobRepository()
        with patch('repositories.job_repository.get_pool', new=AsyncMock(return_value=pool)):
            # Act
            job_id = await repo.create_job("test.csv")
            
            # Assert
            assert job_id == "550e8400-e29b-41d4-a716-446655440000"
            conn.fetchval.assert_called_once()

    async def test_get_job_not_found(self, mock_pool):
        # Arrange
        pool, conn = mock_pool
        conn.fetchrow = AsyncMock(return_value=None)
        
        repo = JobRepository()
        with patch('repositories.job_repository.get_pool', new=AsyncMock(return_value=pool)):
            # Act
            result = await repo.get_job("non-existent-id")
            
            # Assert
            assert result is None

    async def test_update_status_completed(self, mock_pool):
        # Arrange
        pool, conn = mock_pool
        repo = JobRepository()
        
        # Mockear logger para evitar error con await logger.info
        with patch("repositories.job_repository.logger") as mock_logger:
            mock_logger.info = AsyncMock()
            
            with patch('repositories.job_repository.get_pool', new=AsyncMock(return_value=pool)):
                # Act
                await repo.update_status("job-123", "COMPLETED")
                
                # Assert
                conn.execute.assert_called_once()
                args = conn.execute.call_args[0]
                assert "UPDATE jobs" in args[0]
                assert args[1] == "job-123"
                assert args[2] == "COMPLETED"
                mock_logger.info.assert_called_once()
