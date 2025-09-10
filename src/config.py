"""
Módulo de configuração do pipeline de cotações cambiais.
"""

import os
import yaml
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, List, Any
from copy import deepcopy

# Carrega variáveis de ambiente
load_dotenv()

class Config:
    """Classe para gerenciar configurações do projeto."""
    
    def __init__(self, config_path: str = None):
        """
        Inicializa a configuração.
        
        Args:
            config_path: Caminho para o arquivo de configuração YAML
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "config.yaml"

        self.config_path = Path(config_path)
        self._load_config()
        self._normalize_config_keys()
        self._load_env_vars()
        self._ensure_data_directories()
    
    def _load_config(self):
        """Carrega configurações do arquivo YAML."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                self._config = yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de configuração não encontrado: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Erro ao carregar configuração YAML: {e}")

    def _normalize_config_keys(self):
        """Normaliza variações de nomes de chaves para manter compatibilidade com testes/versões antigas."""
        # Suporte a 'exchange_rate_api' -> 'api'
        if 'exchange_rate_api' in self._config and 'api' not in self._config:
            self._config['api'] = self._config['exchange_rate_api']
        # Suporte a data_paths.raw -> bronze se bronze ausente
        if 'data_paths' in self._config:
            dp = self._config['data_paths']
            if 'raw' in dp and 'bronze' not in dp:
                dp['bronze'] = dp['raw']
        # Garantir estrutura mínima
        self._config.setdefault('data_paths', {})
        for k in ['bronze', 'silver', 'gold', 'logs']:
            self._config['data_paths'].setdefault(k, f"data/{k}" if k != 'logs' else 'logs')

    def _ensure_data_directories(self):
        """Cria diretórios de dados se não existirem."""
        try:
            for path in self._config.get('data_paths', {}).values():
                Path(path).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise RuntimeError(f"Falha ao criar diretórios de dados: {e}")
    
    def _load_env_vars(self):
        """Carrega variáveis sensíveis do ambiente."""
        self.exchange_rate_api_key = os.getenv('EXCHANGE_RATE_API_KEY')
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        
        # Configurações de banco (opcionais)
        self.db_host = os.getenv('DB_HOST', self._config.get('database', {}).get('host'))
        self.db_port = int(os.getenv('DB_PORT', self._config.get('database', {}).get('port', 5432)))
        self.db_name = os.getenv('DB_NAME', self._config.get('database', {}).get('database'))
        self.db_user = os.getenv('DB_USER', self._config.get('database', {}).get('user'))
        self.db_password = os.getenv('DB_PASSWORD', self._config.get('database', {}).get('password'))
    
    @property
    def api_base_url(self) -> str:
        """URL base da API de câmbio."""
        return self._config['api']['base_url']
    
    @property
    def api_timeout(self) -> int:
        """Timeout para requisições da API."""
        return self._config['api']['timeout']
    
    @property
    def base_currency(self) -> str:
        """Moeda base para cotações."""
        return self._config['currencies']['base']
    
    @property
    def target_currencies(self) -> List[str]:
        """Lista de moedas alvo."""
        return self._config['currencies']['targets']
    
    @property
    def data_paths(self) -> Dict[str, str]:
        """Caminhos dos diretórios de dados."""
        return self._config['data_paths']
    
    @property
    def llm_config(self) -> Dict[str, Any]:
        """Configurações do LLM."""
        return self._config['llm']
    
    @property
    def logging_config(self) -> Dict[str, Any]:
        """Configurações de logging."""
        return self._config['logging']
    
    @property
    def database_enabled(self) -> bool:
        """Se o banco de dados está habilitado."""
        return self._config.get('database', {}).get('enabled', False)
    
    def validate_api_keys(self) -> bool:
        """
        Valida se as chaves de API necessárias estão configuradas.
        
        Returns:
            bool: True se todas as chaves necessárias estão presentes
        """
        missing_messages = []
        if not self.exchange_rate_api_key:
            missing_messages.append('EXCHANGE_RATE_API_KEY não encontrada')
        if not self.openai_api_key:
            missing_messages.append('OPENAI_API_KEY não encontrada')
        if missing_messages:
            # Junta mensagens mantendo compatibilidade com testes que usam regex para EXCHANGE_RATE_API_KEY
            raise ValueError('; '.join(missing_messages))
        return True
    
    def get_full_api_url(self, endpoint: str) -> str:
        """
        Constrói URL completa da API.
        
        Args:
            endpoint: Endpoint da API
            
        Returns:
            str: URL completa
        """
        return f"{self.api_base_url}/{self.exchange_rate_api_key}/{endpoint}"

    # ---------------------- Métodos e propriedades adicionais para compatibilidade de testes ----------------------
    @property
    def exchange_rate_config(self) -> Dict[str, Any]:
        """Retorna bloco de configuração da API de câmbio (compatibilidade)."""
        return self._config.get('api', {})

    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Realiza merge superficial com merge profundo em dicionários aninhados.
        Listas são substituídas integralmente.
        """
        result = deepcopy(base)
        for k, v in override.items():
            if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                result[k] = self._merge_configs(result[k], v)
            else:
                result[k] = v
        return result
