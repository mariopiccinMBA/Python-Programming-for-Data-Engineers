"""
Testes para o módulo de configuração.
"""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, mock_open

import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.config import Config


class TestConfig:
    """Testes para a classe Config."""
    
    def test_load_default_config(self):
        """Testa carregamento da configuração padrão."""
        config = Config()
        
        assert config.exchange_rate_config is not None
        assert config.llm_config is not None
        assert config.data_paths is not None
        assert config.logging_config is not None
        
    def test_load_custom_config(self):
        """Testa carregamento de configuração personalizada."""
        custom_config = {
            'exchange_rate_api': {
                'base_url': 'https://custom-api.com',
                'timeout': 60
            },
            'llm': {
                'model': 'gpt-4',
                'temperature': 0.5
            },
            'data_paths': {
                'bronze': './custom_data/bronze',
                'silver': './custom_data/silver'
            },
            'logging': {
                'level': 'DEBUG'
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(custom_config, f)
            temp_path = f.name
        
        try:
            config = Config(temp_path)
            
            assert config.exchange_rate_config['base_url'] == 'https://custom-api.com'
            assert config.exchange_rate_config['timeout'] == 60
            assert config.llm_config['model'] == 'gpt-4'
            assert config.llm_config['temperature'] == 0.5
            assert config.data_paths['bronze'] == './custom_data/bronze'
            assert config.logging_config['level'] == 'DEBUG'
            
        finally:
            Path(temp_path).unlink()
    
    @patch.dict('os.environ', {'EXCHANGE_RATE_API_KEY': 'test_key', 'OPENAI_API_KEY': 'openai_key'})
    def test_validate_api_keys_success(self):
        """Testa validação bem-sucedida das chaves de API."""
        config = Config()
        
        # Não deve gerar exceção
        config.validate_api_keys()
    
    @patch.dict('os.environ', {}, clear=True)
    def test_validate_api_keys_missing(self):
        """Testa validação com chaves de API ausentes."""
        config = Config()
        
        with pytest.raises(ValueError, match="EXCHANGE_RATE_API_KEY não encontrada"):
            config.validate_api_keys()
    
    def test_merge_configs(self):
        """Testa merge de configurações."""
        default = {
            'a': 1,
            'b': {'c': 2, 'd': 3},
            'e': [1, 2, 3]
        }
        
        custom = {
            'b': {'c': 20},
            'f': 4
        }
        
        config = Config()
        result = config._merge_configs(default, custom)
        
        expected = {
            'a': 1,
            'b': {'c': 20, 'd': 3},
            'e': [1, 2, 3],
            'f': 4
        }
        
        assert result == expected
    
    def test_create_directories(self):
        """Testa criação de diretórios."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_data = {
                'data_paths': {
                    'bronze': f'{temp_dir}/bronze',
                    'silver': f'{temp_dir}/silver',
                    'gold': f'{temp_dir}/gold',
                    'logs': f'{temp_dir}/logs'
                }
            }
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                yaml.dump(config_data, f)
                temp_config_path = f.name
            
            try:
                config = Config(temp_config_path)
                
                # Verifica se os diretórios foram criados
                assert Path(config.data_paths['bronze']).exists()
                assert Path(config.data_paths['silver']).exists()
                assert Path(config.data_paths['gold']).exists()
                assert Path(config.data_paths['logs']).exists()
                
            finally:
                Path(temp_config_path).unlink()


if __name__ == "__main__":
    pytest.main([__file__])
