import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock, patch, call
from sqlalchemy import text
from utils.db_client import DatabaseClient


@pytest.fixture
def db_client():
    """Fixture to create a DatabaseClient instance with mocked engine"""
    with patch('utils.db_client.create_engine') as mock_create_engine:
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        client = DatabaseClient("postgresql://user:pass@localhost:5432/testdb")
        # Force engine creation
        _ = client.engine
        
        yield client, mock_engine


class TestDatabaseClient:
    
    def test_engine_lazy_initialization(self):
        """Test that engine is created lazily"""
        with patch('utils.db_client.create_engine') as mock_create_engine:
            client = DatabaseClient("postgresql://user:pass@localhost:5432/testdb")
            
            # Engine should not be created yet
            mock_create_engine.assert_not_called()
            
            # Access engine property
            _ = client.engine
            
            # Now engine should be created
            mock_create_engine.assert_called_once_with("postgresql://user:pass@localhost:5432/testdb")
    
    def test_engine_caching(self, db_client):
        """Test that engine is cached after first creation"""
        client, mock_engine = db_client
        
        # Access engine multiple times
        engine1 = client.engine
        engine2 = client.engine
        
        # Should return same instance
        assert engine1 is engine2
    
    def test_read_sql_success(self, db_client):
        """Test read_sql method with successful query"""
        client, mock_engine = db_client
        
        # Create mock DataFrame
        expected_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        
        # Mock connection context manager
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)
        
        with patch('utils.db_client.pd.read_sql_query', return_value=expected_df) as mock_read_sql_query:
            result = client.read_sql("SELECT * FROM test_table")
            
            # Verify pd.read_sql_query was called with text-wrapped query and connection
            mock_read_sql_query.assert_called_once()
            call_args = mock_read_sql_query.call_args
            assert str(call_args[0][0]) == "SELECT * FROM test_table"  # text() object
            assert call_args[0][1] == mock_connection
            
            # Verify result
            pd.testing.assert_frame_equal(result, expected_df)
    
    def test_read_sql_with_kwargs(self, db_client):
        """Test read_sql method with additional kwargs"""
        client, mock_engine = db_client
        
        expected_df = pd.DataFrame({'col1': [1, 2]})
        
        # Mock connection context manager
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)
        
        with patch('utils.db_client.pd.read_sql_query', return_value=expected_df) as mock_read_sql_query:
            result = client.read_sql(
                "SELECT * FROM test_table WHERE id = :id",
                params={'id': 123},
                chunksize=100
            )
            
            # Verify pd.read_sql_query was called with text-wrapped query and connection
            mock_read_sql_query.assert_called_once()
            call_args = mock_read_sql_query.call_args
            assert str(call_args[0][0]) == "SELECT * FROM test_table WHERE id = :id"
            assert call_args[0][1] == mock_connection
            assert call_args[1]['params'] == {'id': 123}
            assert call_args[1]['chunksize'] == 100
    
    def test_write_df_success(self, db_client):
        """Test write_df method with successful write"""
        client, mock_engine = db_client
        
        # Create test DataFrame
        test_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        
        # Mock connection context manager (begin for transactions)
        mock_connection = MagicMock()
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
        
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            client.write_df(test_df, "test_table")
            
            # Verify to_sql was called with correct arguments including method='multi'
            mock_to_sql.assert_called_once_with(
                "test_table",
                con=mock_connection,
                if_exists="append",
                index=False,
                method='multi'
            )
    
    def test_write_df_with_custom_params(self, db_client):
        """Test write_df with custom parameters"""
        client, mock_engine = db_client
        
        test_df = pd.DataFrame({'col1': [1, 2]})
        
        mock_connection = MagicMock()
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=False)
        
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            client.write_df(
                test_df,
                "test_table",
                if_exists="replace",
                index=True,
                schema="public"
            )
            
            mock_to_sql.assert_called_once_with(
                "test_table",
                con=mock_connection,
                if_exists="replace",
                index=True,
                method='multi',
                schema="public"
            )
    
    def test_execute_query_success(self, db_client):
        """Test execute_query method"""
        client, mock_engine = db_client
        
        # Mock connection and transaction
        mock_connection = MagicMock()
        mock_transaction = MagicMock()
        mock_connection.begin.return_value.__enter__ = Mock(return_value=mock_transaction)
        mock_connection.begin.return_value.__exit__ = Mock(return_value=False)
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)
        
        query = "INSERT INTO test_table VALUES (1, 'test')"
        client.execute_query(query)
        
        # Verify execute was called
        mock_connection.execute.assert_called_once()
        # Verify the text() wrapper was used
        call_args = mock_connection.execute.call_args[0][0]
        assert str(call_args) == query
    
    def test_fetch_one_success(self, db_client):
        """Test fetch_one method"""
        client, mock_engine = db_client
        
        # Mock connection and result
        mock_connection = MagicMock()
        mock_result = MagicMock()
        expected_row = (1, 'test', 123)
        mock_result.fetchone.return_value = expected_row
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)
        
        query = "SELECT * FROM test_table WHERE id = 1"
        result = client.fetch_one(query)
        
        assert result == expected_row
        mock_connection.execute.assert_called_once()
        mock_result.fetchone.assert_called_once()
    
    def test_fetch_one_no_results(self, db_client):
        """Test fetch_one when no results found"""
        client, mock_engine = db_client
        
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)
        
        result = client.fetch_one("SELECT * FROM test_table WHERE id = 999")
        
        assert result is None
    
    def test_fetch_all_success(self, db_client):
        """Test fetch_all method"""
        client, mock_engine = db_client
        
        mock_connection = MagicMock()
        mock_result = MagicMock()
        expected_rows = [(1, 'test1'), (2, 'test2'), (3, 'test3')]
        mock_result.fetchall.return_value = expected_rows
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)
        
        query = "SELECT * FROM test_table"
        result = client.fetch_all(query)
        
        assert result == expected_rows
        mock_connection.execute.assert_called_once()
        mock_result.fetchall.assert_called_once()
    
    def test_fetch_all_empty_results(self, db_client):
        """Test fetch_all with no results"""
        client, mock_engine = db_client
        
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)
        
        result = client.fetch_all("SELECT * FROM empty_table")
        
        assert result == []
    
    def test_execute_scalar_success(self, db_client):
        """Test execute_scalar method"""
        client, mock_engine = db_client
        
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = 42
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)
        
        query = "SELECT COUNT(*) FROM test_table"
        result = client.execute_scalar(query)
        
        assert result == 42
        mock_connection.execute.assert_called_once()
        mock_result.scalar.assert_called_once()
    
    def test_execute_scalar_boolean(self, db_client):
        """Test execute_scalar with boolean result"""
        client, mock_engine = db_client
        
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = True
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)
        
        query = "SELECT EXISTS (SELECT * FROM test_table WHERE id = 1)"
        result = client.execute_scalar(query)
        
        assert result is True
    
    def test_close_disposes_engine(self, db_client):
        """Test that close() disposes the engine"""
        client, mock_engine = db_client
        
        client.close()
        
        mock_engine.dispose.assert_called_once()
        assert client._engine is None
    
    def test_close_without_engine(self):
        """Test close() when engine was never created"""
        client = DatabaseClient("postgresql://user:pass@localhost:5432/testdb")
        
        # Should not raise error
        client.close()
        assert client._engine is None
    
    def test_del_calls_close(self, db_client):
        """Test that __del__ calls close()"""
        client, mock_engine = db_client
        
        with patch.object(client, 'close') as mock_close:
            client.__del__()
            mock_close.assert_called_once()
    
    def test_connection_error_handling(self, db_client):
        """Test error handling when connection fails"""
        client, mock_engine = db_client
        
        mock_engine.connect.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception, match="Connection failed"):
            client.fetch_one("SELECT 1")
    
    def test_query_execution_error_handling(self, db_client):
        """Test error handling when query execution fails"""
        client, mock_engine = db_client
        
        mock_connection = MagicMock()
        mock_connection.execute.side_effect = Exception("Query error")
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)
        
        with pytest.raises(Exception, match="Query error"):
            client.execute_query("INVALID SQL")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

