"""Testes para o pipeline principal do CurrencyExchangePipeline.
"""

import pytest
from unittest.mock import patch, MagicMock

from src.pipeline import CurrencyExchangePipeline


class TestCurrencyExchangePipeline:
    """Testes para a classe CurrencyExchangePipeline."""
    
    @patch('src.pipeline.Config')
    @patch('src.pipeline.setup_logging')
    @patch('src.pipeline.ExchangeRateIngester')
    @patch('src.pipeline.ExchangeRateTransformer')
    @patch('src.pipeline.ExchangeRateLoader')
    @patch('src.pipeline.LLMAnalyzer')
    def test_pipeline_initialization(self, mock_llm, mock_loader, mock_transformer, 
                                   mock_ingester, mock_logging, mock_config):
        """Testa inicialização do pipeline."""
        # Setup mocks
        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger
        
        # Inicializa pipeline
        pipeline = CurrencyExchangePipeline()
        
        # Verifica se componentes foram inicializados
        assert pipeline.config is not None
        assert pipeline.ingester is not None
        assert pipeline.transformer is not None
        assert pipeline.loader is not None
        assert pipeline.llm_analyzer is not None
    
    @patch('src.pipeline.Config')
    def test_validate_setup_success(self, mock_config):
        """Testa validação bem-sucedida do setup."""
        mock_config_instance = MagicMock()
        mock_config_instance.validate_api_keys.return_value = None
        mock_config.return_value = mock_config_instance
        
        with patch('src.pipeline.setup_logging'), \
             patch('src.pipeline.ExchangeRateIngester'), \
             patch('src.pipeline.ExchangeRateTransformer'), \
             patch('src.pipeline.ExchangeRateLoader'), \
             patch('src.pipeline.LLMAnalyzer'):
            
            pipeline = CurrencyExchangePipeline()
            result = pipeline.validate_setup()
            
            assert result is True
            mock_config_instance.validate_api_keys.assert_called_once()
    
    @patch('src.pipeline.Config')
    def test_validate_setup_failure(self, mock_config):
        """Testa falha na validação do setup."""
        mock_config_instance = MagicMock()
        mock_config_instance.validate_api_keys.side_effect = ValueError("API key missing")
        mock_config.return_value = mock_config_instance
        
        with patch('src.pipeline.setup_logging'), \
             patch('src.pipeline.ExchangeRateIngester'), \
             patch('src.pipeline.ExchangeRateTransformer'), \
             patch('src.pipeline.ExchangeRateLoader'), \
             patch('src.pipeline.LLMAnalyzer'):
            
            pipeline = CurrencyExchangePipeline()
            result = pipeline.validate_setup()
            
            assert result is False
    
    @patch('src.pipeline.Config')
    def test_run_daily_pipeline_success(self, mock_config):
        """Testa execução bem-sucedida do pipeline diário."""
        # Setup mocks de configuração
        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance

        # Cria mocks dos componentes do pipeline
        mock_ingester = MagicMock()
        mock_transformer = MagicMock()
        mock_loader = MagicMock()
        mock_llm = MagicMock()

        # Define retornos simulados
        mock_ingester.ingest_daily_rates.return_value = "bronze_file.parquet"
        mock_transformer.transform_daily_data.return_value = "silver_file.parquet"
        mock_loader.load_daily_data.return_value = "gold_file.parquet"
        mock_llm.analyze_daily_data.return_value = {"insight": "test"}

        # Execução dentro do contexto de patches
        with patch('src.pipeline.setup_logging'), \
             patch('src.pipeline.ExchangeRateIngester', return_value=mock_ingester), \
             patch('src.pipeline.ExchangeRateTransformer', return_value=mock_transformer), \
             patch('src.pipeline.ExchangeRateLoader', return_value=mock_loader), \
             patch('src.pipeline.LLMAnalyzer', return_value=mock_llm):

            pipeline = CurrencyExchangePipeline()
            result = pipeline.run_daily_pipeline("2024-01-15")

            # Asserções de chamadas
            mock_ingester.ingest_daily_rates.assert_called_once_with("2024-01-15")
            mock_transformer.transform_daily_data.assert_called_once_with("bronze_file.parquet", "2024-01-15")
            mock_loader.load_daily_data.assert_called_once_with("silver_file.parquet", "2024-01-15")
            mock_llm.analyze_daily_data.assert_called_once_with("gold_file.parquet", "2024-01-15")

            # Asserções de resultado
            assert 'error' not in result
            assert result['bronze_file'] == "bronze_file.parquet"
            assert result['silver_file'] == "silver_file.parquet"
            assert result['gold_file'] == "gold_file.parquet"
            assert result['llm_analysis'] == {"insight": "test"}
            assert 'execution_time' in result


if __name__ == "__main__":
    pytest.main([__file__])
