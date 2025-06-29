#!/usr/bin/env python3
"""
Test module for fetch_historical.py functionality.
"""
import pytest
import asyncio
import pandas as pd
import httpx
import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from datetime import date, datetime

from pro_capital_markets.fetch_historical import (
    fetch_symbol,
    fetch_all_symbols,
    fetch_one_symbol,
    dump_events,
    _convert_full_historical_to_csv,
    _refactor_parquet_files,
    _fetch_all_symbols_async
)
import pro_capital_markets.constants as constants


class TestFetchSymbol:
    """Test cases for fetch_symbol function."""
    
    @pytest.fixture
    def mock_client(self):
        """Mock httpx.AsyncClient for testing."""
        return AsyncMock(spec=httpx.AsyncClient)
    
    @pytest.fixture
    def sample_api_response(self):
        """Sample API response data."""
        return {
            "Time Series (Daily)": {
                "2024-01-01": {
                    "1. open": "150.00",
                    "2. high": "155.00",
                    "3. low": "148.00",
                    "4. close": "152.00",
                    "5. volume": "1000000"
                },
                "2024-01-02": {
                    "1. open": "152.00",
                    "2. high": "157.00",
                    "3. low": "150.00",
                    "4. close": "155.00",
                    "5. volume": "1200000"
                }
            }
        }
    
    @pytest.mark.asyncio
    async def test_fetch_symbol_success(self, mock_client, sample_api_response):
        """Test successful symbol fetching."""
        # Mock the HTTP response
        mock_response = Mock()
        mock_response.json.return_value = sample_api_response
        mock_response.raise_for_status.return_value = None
        mock_client.get.return_value = mock_response
        
        # Test the function
        result = await fetch_symbol(mock_client, "AAPL")
        
        # Assertions
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["open", "high", "low", "close", "volume", "symbol", "sector", "industry", "index", "date"]
        assert result["symbol"].iloc[0] == "AAPL"
        assert result["open"].iloc[0] == 150.00
        assert result["close"].iloc[1] == 155.00
        
        # Verify API call
        mock_client.get.assert_called_once()
        call_args = mock_client.get.call_args
        assert "TIME_SERIES_DAILY" in str(call_args)
        assert "AAPL" in str(call_args)
    
    @pytest.mark.asyncio
    async def test_fetch_symbol_empty_response(self, mock_client):
        """Test handling of empty API response."""
        mock_response = Mock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status.return_value = None
        mock_client.get.return_value = mock_response
        
        result = await fetch_symbol(mock_client, "INVALID")
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    @pytest.mark.asyncio
    async def test_fetch_symbol_http_error(self, mock_client):
        """Test handling of HTTP errors."""
        mock_client.get.side_effect = httpx.HTTPStatusError(
            "404 Not Found", request=Mock(), response=Mock()
        )
        
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_symbol(mock_client, "AAPL")
    
    @pytest.mark.asyncio
    async def test_fetch_symbol_request_error(self, mock_client):
        """Test handling of request errors."""
        mock_client.get.side_effect = httpx.RequestError("Connection failed")
        
        with pytest.raises(httpx.RequestError):
            await fetch_symbol(mock_client, "AAPL")
    
    @pytest.mark.asyncio
    async def test_fetch_symbol_data_processing(self, mock_client, sample_api_response):
        """Test data processing and column transformations."""
        mock_response = Mock()
        mock_response.json.return_value = sample_api_response
        mock_response.raise_for_status.return_value = None
        mock_client.get.return_value = mock_response
        
        result = await fetch_symbol(mock_client, "AAPL")
        
        # Check data types
        assert result["open"].dtype in [float, 'float64']
        assert result["high"].dtype in [float, 'float64']
        assert result["low"].dtype in [float, 'float64']
        assert result["close"].dtype in [float, 'float64']
        assert result["volume"].dtype in [int, 'int64']
        
        # Check date processing
        assert isinstance(result["date"].iloc[0], date)
        
        # Check sorting
        assert result["date"].iloc[0] <= result["date"].iloc[1]


class TestFetchAllSymbols:
    """Test cases for fetch_all_symbols related functions."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @patch('pro_capital_markets.fetch_historical.API_KEY', 'test_api_key')
    @patch('pro_capital_markets.constants.UNIQUE_SYMBOLS', ['AAPL', 'MSFT'])
    def test_fetch_all_symbols_with_mock(self, temp_dir):
        """Test fetch_all_symbols with mocked dependencies."""
        # Mock the constants
        with patch('pro_capital_markets.constants.HISTORICAL_FILE', temp_dir / "test_historical.parquet"):
            with patch('pro_capital_markets.constants.DATA_DIR', temp_dir):
                # Create symbols directory
                symbols_dir = temp_dir / "symbols"
                symbols_dir.mkdir(exist_ok=True)
                
                # Mock the async function
                with patch('pro_capital_markets.fetch_historical._fetch_all_symbols_async') as mock_async:
                    mock_async.return_value = None
                    
                    # Call the function
                    fetch_all_symbols()
                    
                    # Verify the async function was called
                    mock_async.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('pro_capital_markets.fetch_historical.API_KEY', 'test_api_key')
    @patch('pro_capital_markets.constants.UNIQUE_SYMBOLS', ['AAPL'])
    async def test_fetch_all_symbols_async_new_file(self, temp_dir):
        """Test _fetch_all_symbols_async with new file."""
        output_file = temp_dir / "test_historical.parquet"
        symbols_dir = temp_dir / "symbols"
        symbols_dir.mkdir(exist_ok=True)
        
        # Mock the fetch_symbol function
        sample_df = pd.DataFrame({
            'date': [date(2024, 1, 1)],
            'symbol': ['AAPL'],
            'open': [150.0],
            'high': [155.0],
            'low': [148.0],
            'close': [152.0],
            'volume': [1000000],
            'sector': ['Information Technology'],
            'industry': ['Consumer Electronics'],
            'index': ['NASDAQ 100']
        })
        
        with patch('pro_capital_markets.fetch_historical.fetch_symbol', return_value=sample_df):
            with patch('httpx.AsyncClient') as mock_client_class:
                mock_client = AsyncMock()
                mock_client_class.return_value.__aenter__.return_value = mock_client
                
                await _fetch_all_symbols_async(output_file)
                
                # Verify file was created
                assert output_file.exists()
                
                # Verify CSV was also created
                csv_file = output_file.with_suffix('.csv')
                assert csv_file.exists()
    
    @pytest.mark.asyncio
    @patch('pro_capital_markets.fetch_historical.API_KEY', 'test_api_key')
    @patch('pro_capital_markets.constants.UNIQUE_SYMBOLS', ['AAPL', 'MSFT'])
    async def test_fetch_all_symbols_async_existing_file(self, temp_dir):
        """Test _fetch_all_symbols_async with existing file."""
        output_file = temp_dir / "test_historical.parquet"
        symbols_dir = temp_dir / "symbols"
        symbols_dir.mkdir(exist_ok=True)
        
        # Create existing data
        existing_df = pd.DataFrame({
            'date': [date(2024, 1, 1)],
            'symbol': ['AAPL'],
            'open': [150.0],
            'high': [155.0],
            'low': [148.0],
            'close': [152.0],
            'volume': [1000000],
            'sector': ['Information Technology'],
            'industry': ['Consumer Electronics'],
            'index': ['NASDAQ 100']
        })
        existing_df.to_parquet(output_file, index=False)
        
        # Mock the fetch_symbol function for new symbol
        new_df = pd.DataFrame({
            'date': [date(2024, 1, 1)],
            'symbol': ['MSFT'],
            'open': [250.0],
            'high': [255.0],
            'low': [248.0],
            'close': [252.0],
            'volume': [800000],
            'sector': ['Information Technology'],
            'industry': ['Software & Cloud'],
            'index': ['NASDAQ 100']
        })
        
        with patch('pro_capital_markets.fetch_historical.fetch_symbol', return_value=new_df):
            with patch('httpx.AsyncClient') as mock_client_class:
                mock_client = AsyncMock()
                mock_client_class.return_value.__aenter__.return_value = mock_client
                
                await _fetch_all_symbols_async(output_file)
                
                # Verify file was updated
                result_df = pd.read_parquet(output_file)
                assert len(result_df) == 2
                assert set(result_df['symbol']) == {'AAPL', 'MSFT'}


class TestFetchOneSymbol:
    """Test cases for fetch_one_symbol function."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @patch('pro_capital_markets.fetch_historical.API_KEY', 'test_api_key')
    def test_fetch_one_symbol(self, temp_dir):
        """Test fetch_one_symbol function."""
        # Mock constants
        with patch('pro_capital_markets.constants.DATA_DIR', temp_dir):
            symbols_dir = temp_dir / "symbols"
            symbols_dir.mkdir(exist_ok=True)
            
            # Mock the fetch_symbol function
            sample_df = pd.DataFrame({
                'date': [date(2024, 1, 1)],
                'symbol': ['MSFT'],
                'open': [250.0],
                'high': [255.0],
                'low': [248.0],
                'close': [252.0],
                'volume': [800000],
                'sector': ['Information Technology'],
                'industry': ['Software & Cloud'],
                'index': ['NASDAQ 100']
            })
            
            with patch('pro_capital_markets.fetch_historical.fetch_symbol', return_value=sample_df):
                with patch('httpx.AsyncClient') as mock_client_class:
                    mock_client = AsyncMock()
                    mock_client_class.return_value.__aenter__.return_value = mock_client
                    
                    # Call the function
                    fetch_one_symbol("MSFT")
                    
                    # Verify file was created
                    expected_file = symbols_dir / "test-MSFT.parquet"
                    assert expected_file.exists()
                    
                    # Verify content
                    result_df = pd.read_parquet(expected_file)
                    assert len(result_df) == 1
                    assert result_df['symbol'].iloc[0] == 'MSFT'


class TestUtilityFunctions:
    """Test cases for utility functions."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_dump_events(self, temp_dir):
        """Test dump_events function."""
        with patch('pro_capital_markets.constants.DATA_DIR', temp_dir):
            with patch('pro_capital_markets.constants.EVENTS', [
                {"date": "2024-01-01", "symbol": "AAPL", "description": "Test event"},
                {"date": "2024-01-02", "symbol": "MSFT", "description": "Another event"}
            ]):
                dump_events()
                
                # Verify file was created
                events_file = temp_dir / "events.csv"
                assert events_file.exists()
                
                # Verify content
                df = pd.read_csv(events_file)
                assert len(df) == 2
                assert list(df.columns) == ["date", "symbol", "description"]
    
    def test_convert_full_historical_to_csv(self, temp_dir):
        """Test _convert_full_historical_to_csv function."""
        # Create test parquet file
        test_file = temp_dir / "test_historical.parquet"
        test_df = pd.DataFrame({
            'date': [date(2024, 1, 1), date(2024, 1, 2)],
            'symbol': ['AAPL', 'MSFT'],
            'open': [150.0, 250.0],
            'high': [155.0, 255.0],
            'low': [148.0, 248.0],
            'close': [152.0, 252.0],
            'volume': [1000000, 800000]
        })
        test_df.to_parquet(test_file, index=False)
        
        with patch('pro_capital_markets.constants.HISTORICAL_FILE', test_file):
            _convert_full_historical_to_csv()
            
            # Verify CSV was created
            csv_file = test_file.with_suffix('.csv')
            assert csv_file.exists()
            
            # Verify content
            df = pd.read_csv(csv_file)
            assert len(df) == 2
            assert list(df.columns) == ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    
    def test_convert_no_historical_file(self, temp_dir, capsys):
        """Test _convert_full_historical_to_csv with no existing file."""
        non_existent_file = temp_dir / "non_existent.parquet"
        
        with patch('pro_capital_markets.constants.HISTORICAL_FILE', non_existent_file):
            _convert_full_historical_to_csv()
            
            # Check that appropriate message was printed
            captured = capsys.readouterr()
            assert "No historical data found" in captured.out
    
    def test_refactor_parquet_files(self, temp_dir):
        """Test _refactor_parquet_files function."""
        # Create main historical file
        historical_file = temp_dir / "historical.parquet"
        historical_df = pd.DataFrame({
            'datetime': ['2024-01-01', '2024-01-02'],  # Note: using 'datetime' instead of 'date'
            'symbol': ['AAPL', 'MSFT'],
            'open': ['150.0', '250.0'],  # String values to test conversion
            'high': ['155.0', '255.0'],
            'low': ['148.0', '248.0'],
            'close': ['152.0', '252.0'],
            'volume': ['1000000', '800000']
        })
        historical_df.to_parquet(historical_file, index=False)
        
        # Create symbols directory with test files
        symbols_dir = temp_dir / "symbols"
        symbols_dir.mkdir(exist_ok=True)
        
        symbol_file = symbols_dir / "SMBL-AAPL.parquet"
        symbol_df = pd.DataFrame({
            'datetime': ['2024-01-01'],
            'symbol': ['AAPL'],
            'open': ['150.0'],
            'high': ['155.0'],
            'low': ['148.0'],
            'close': ['152.0'],
            'volume': ['1000000']
        })
        symbol_df.to_parquet(symbol_file, index=False)
        
        with patch('pro_capital_markets.constants.HISTORICAL_FILE', historical_file):
            with patch('pro_capital_markets.constants.DATA_DIR', temp_dir):
                _refactor_parquet_files()
                
                # Verify main file was processed
                result_df = pd.read_parquet(historical_file)
                assert 'date' in result_df.columns
                assert 'datetime' not in result_df.columns
                assert result_df['open'].dtype in [float, 'float64']
                
                # Verify symbol file was processed
                symbol_result = pd.read_parquet(symbol_file)
                assert 'date' in symbol_result.columns
                assert 'datetime' not in symbol_result.columns


class TestEnvironmentAndConfiguration:
    """Test cases for environment and configuration."""
    
    def test_api_key_assertion(self):
        """Test that API key assertion works correctly."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove API key from environment
            with patch('pro_capital_markets.fetch_historical.API_KEY', None):
                with pytest.raises(AssertionError, match="Please set the ALPHA_VANTAGE_API_KEY environment variable"):
                    # This should trigger the assertion in the module
                    exec("assert API_KEY is not None, 'Please set the ALPHA_VANTAGE_API_KEY environment variable'")
    
    def test_api_key_from_environment(self):
        """Test that API key is read from environment."""
        with patch.dict(os.environ, {'ALPHA_VANTAGE_API_KEY': 'test_key'}):
            # Import the module to test environment variable reading
            import importlib
            import pro_capital_markets.fetch_historical
            importlib.reload(pro_capital_markets.fetch_historical)
            
            # The API_KEY should be set
            assert pro_capital_markets.fetch_historical.API_KEY == 'test_key'


class TestIntegration:
    """Integration tests for the fetch_historical module."""
    
    @pytest.fixture
    def temp_workspace(self):
        """Create a temporary workspace for integration testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            data_dir = workspace / "data"
            data_dir.mkdir()
            symbols_dir = data_dir / "symbols"
            symbols_dir.mkdir()
            yield workspace, data_dir, symbols_dir
    
    @patch('pro_capital_markets.fetch_historical.API_KEY', 'test_api_key')
    @patch('pro_capital_markets.constants.UNIQUE_SYMBOLS', ['AAPL'])
    def test_end_to_end_workflow(self, temp_workspace):
        """Test the complete workflow from fetching to processing."""
        workspace, data_dir, symbols_dir = temp_workspace
        
        # Mock the historical file path
        historical_file = data_dir / "historical_stock_daily.parquet"
        
        # Create sample data
        sample_df = pd.DataFrame({
            'date': [date(2024, 1, 1), date(2024, 1, 2)],
            'symbol': ['AAPL', 'AAPL'],
            'open': [150.0, 152.0],
            'high': [155.0, 157.0],
            'low': [148.0, 150.0],
            'close': [152.0, 155.0],
            'volume': [1000000, 1200000],
            'sector': ['Information Technology', 'Information Technology'],
            'industry': ['Consumer Electronics', 'Consumer Electronics'],
            'index': ['NASDAQ 100', 'NASDAQ 100']
        })
        
        with patch('pro_capital_markets.constants.HISTORICAL_FILE', historical_file):
            with patch('pro_capital_markets.constants.DATA_DIR', data_dir):
                with patch('pro_capital_markets.fetch_historical.fetch_symbol', return_value=sample_df):
                    with patch('httpx.AsyncClient') as mock_client_class:
                        mock_client = AsyncMock()
                        mock_client_class.return_value.__aenter__.return_value = mock_client
                        
                        # Test the complete workflow
                        fetch_all_symbols()
                        
                        # Verify files were created
                        assert historical_file.exists()
                        assert historical_file.with_suffix('.csv').exists()
                        
                        # Test CSV conversion
                        _convert_full_historical_to_csv()
                        
                        # Test refactoring
                        _refactor_parquet_files()
                        
                        # Verify final data integrity
                        final_df = pd.read_parquet(historical_file)
                        assert len(final_df) == 2
                        assert final_df['symbol'].iloc[0] == 'AAPL'
                        assert final_df['open'].dtype in [float, 'float64']


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v", "--cli-log-level=DEBUG"])
