"""
Módulo de ingestão de dados da API de cotações cambiais.
"""

import json
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import time

from .config import Config
from .logger import PipelineLogger


class ExchangeRateIngester:
    """Classe responsável pela ingestão de dados de cotações cambiais."""
    
    def __init__(self, config: Config, logger: PipelineLogger = None):
        """
        Inicializa o ingester.
        
        Args:
            config: Configuração do projeto
            logger: Logger estruturado
        """
        self.config = config
        self.logger = logger or PipelineLogger("ingest")
        self.session = requests.Session()
        self.session.timeout = config.api_timeout
    
    def fetch_exchange_rates(self, base_currency: str = None) -> Dict[str, Any]:
        """
        Busca cotações da API.
        
        Args:
            base_currency: Moeda base (padrão: USD)
            
        Returns:
            Dict contendo os dados da API
            
        Raises:
            requests.RequestException: Erro na requisição
            ValueError: Erro na validação dos dados
        """
        base_currency = base_currency or self.config.base_currency
        
        # Constrói URL da API
        endpoint = f"latest/{base_currency}"
        url = self.config.get_full_api_url(endpoint)
        
        self.logger.info("Iniciando busca de cotações", base_currency=base_currency)
        
        start_time = time.time()
        
        try:
            response = self.session.get(url)
            response_time = time.time() - start_time
            
            # Log da requisição
            self.logger.log_api_request(
                url=url,
                method="GET",
                status_code=response.status_code,
                response_time=response_time
            )
            
            response.raise_for_status()
            
            data = response.json()
            
            # Valida resposta da API
            self._validate_api_response(data)
            
            self.logger.info(
                "Cotações obtidas com sucesso",
                currencies_count=len(data.get('conversion_rates', {})),
                response_time_ms=response_time * 1000
            )
            
            return data
            
        except requests.RequestException as e:
            self.logger.error("Erro na requisição da API", error=e, url=url)
            raise
        except (json.JSONDecodeError, ValueError) as e:
            self.logger.error("Erro na validação dos dados da API", error=e)
            raise
    
    def _validate_api_response(self, data: Dict[str, Any]) -> None:
        """
        Valida a resposta da API.
        
        Args:
            data: Dados retornados pela API
            
        Raises:
            ValueError: Se os dados não estão no formato esperado
        """
        required_fields = ['result', 'base_code', 'conversion_rates']
        
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Campo obrigatório ausente na resposta da API: {field}")
        
        if data['result'] != 'success':
            raise ValueError(f"API retornou erro: {data.get('error-type', 'Unknown error')}")
        
        conversion_rates = data['conversion_rates']
        if not isinstance(conversion_rates, dict) or len(conversion_rates) == 0:
            raise ValueError("Nenhuma cotação encontrada na resposta da API")
        
        # Valida se as cotações são numéricas e positivas
        for currency, rate in conversion_rates.items():
            if not isinstance(rate, (int, float)) or rate <= 0:
                raise ValueError(f"Taxa inválida para {currency}: {rate}")
    
    def save_raw_data(self, data: Dict[str, Any], date_str: str = None) -> Path:
        """
        Salva dados brutos em JSON.
        
        Args:
            data: Dados a serem salvos
            date_str: String da data (formato YYYY-MM-DD)
            
        Returns:
            Path do arquivo salvo
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Cria diretório raw se não existir
        raw_dir = Path(self.config.data_paths['raw'])
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        # Nome do arquivo padronizado
        filename = f"exchange_rates_{date_str}.json"
        file_path = raw_dir / filename
        
        try:
            # Adiciona metadados
            enriched_data = {
                "metadata": {
                    "ingestion_timestamp": datetime.now().isoformat(),
                    "source": "exchangerate-api.com",
                    "base_currency": data.get('base_code'),
                    "currencies_count": len(data.get('conversion_rates', {}))
                },
                "raw_data": data
            }
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(enriched_data, f, indent=2, ensure_ascii=False)
            
            self.logger.log_data_processing(
                operation="save_raw_data",
                records_count=len(data.get('conversion_rates', {})),
                file_path=str(file_path)
            )
            
            return file_path
            
        except Exception as e:
            self.logger.error("Erro ao salvar dados brutos", error=e, file_path=str(file_path))
            raise
    
    def ingest_daily_rates(self, date_str: str = None) -> Path:
        """
        Executa o processo completo de ingestão diária.
        
        Args:
            date_str: String da data (formato YYYY-MM-DD)
            
        Returns:
            Path do arquivo de dados brutos salvo
        """
        start_time = time.time()
        
        try:
            self.logger.log_pipeline_stage("ingest", "started")
            
            # Busca dados da API
            data = self.fetch_exchange_rates()
            
            # Salva dados brutos
            file_path = self.save_raw_data(data, date_str)
            
            duration = time.time() - start_time
            self.logger.log_pipeline_stage(
                "ingest", 
                "completed", 
                duration=duration,
                file_path=str(file_path)
            )
            
            return file_path
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.log_pipeline_stage(
                "ingest", 
                "failed", 
                duration=duration,
                error=str(e)
            )
            raise
