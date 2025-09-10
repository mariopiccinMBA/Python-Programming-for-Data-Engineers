"""
Módulo de transformação e normalização de dados de cotações cambiais.
"""

import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import time

from .config import Config
from .logger import PipelineLogger


class ExchangeRateTransformer:
    """Classe responsável pela transformação de dados de cotações cambiais."""
    
    def __init__(self, config: Config, logger: PipelineLogger = None):
        """
        Inicializa o transformer.
        
        Args:
            config: Configuração do projeto
            logger: Logger estruturado
        """
        self.config = config
        self.logger = logger or PipelineLogger("transform")
    
    def load_raw_data(self, file_path: Path) -> Dict[str, Any]:
        """
        Carrega dados brutos do arquivo JSON.
        
        Args:
            file_path: Caminho para o arquivo de dados brutos
            
        Returns:
            Dict contendo os dados carregados
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.logger.info("Dados brutos carregados", file_path=str(file_path))
            return data
            
        except Exception as e:
            self.logger.error("Erro ao carregar dados brutos", error=e, file_path=str(file_path))
            raise
    
    def normalize_exchange_rates(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Normaliza os dados de cotações em formato tabular.
        
        Args:
            raw_data: Dados brutos da API
            
        Returns:
            DataFrame normalizado
        """
        try:
            # Extrai dados relevantes
            api_data = raw_data.get('raw_data', raw_data)
            metadata = raw_data.get('metadata', {})
            
            base_currency = api_data.get('base_code')
            conversion_rates = api_data.get('conversion_rates', {})
            last_update = api_data.get('time_last_update_utc')
            
            # Cria lista de registros normalizados
            records = []
            
            for target_currency, rate in conversion_rates.items():
                # Filtra apenas as moedas de interesse se especificadas
                if self.config.target_currencies and target_currency not in self.config.target_currencies:
                    continue
                
                record = {
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'timestamp': datetime.now().isoformat(),
                    'base_currency': base_currency,
                    'target_currency': target_currency,
                    'exchange_rate': float(rate),
                    'source': 'exchangerate-api.com',
                    'ingestion_timestamp': metadata.get('ingestion_timestamp'),
                    'api_last_update': last_update
                }
                records.append(record)
            
            # Cria DataFrame
            df = pd.DataFrame(records)
            
            self.logger.log_data_processing(
                operation="normalize_exchange_rates",
                records_count=len(df)
            )
            
            return df
            
        except Exception as e:
            self.logger.error("Erro na normalização dos dados", error=e)
            raise
    
    def validate_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Valida e limpa os dados transformados.
        
        Args:
            df: DataFrame a ser validado
            
        Returns:
            DataFrame validado e limpo
        """
        original_count = len(df)
        
        try:
            # Remove registros com taxas nulas ou negativas
            df = df.dropna(subset=['exchange_rate'])
            df = df[df['exchange_rate'] > 0]
            
            # Remove duplicatas
            df = df.drop_duplicates(subset=['base_currency', 'target_currency', 'date'])
            
            # Valida tipos de dados
            df['exchange_rate'] = pd.to_numeric(df['exchange_rate'], errors='coerce')
            df = df.dropna(subset=['exchange_rate'])
            
            # Adiciona validações de negócio
            # Taxa muito alta ou muito baixa pode indicar erro
            df = df[(df['exchange_rate'] >= 0.0001) & (df['exchange_rate'] <= 1000000)]
            
            cleaned_count = len(df)
            removed_count = original_count - cleaned_count
            
            if removed_count > 0:
                self.logger.warning(
                    "Registros removidos na validação",
                    original_count=original_count,
                    cleaned_count=cleaned_count,
                    removed_count=removed_count
                )
            
            self.logger.log_data_processing(
                operation="validate_data_quality",
                records_count=cleaned_count
            )
            
            return df
            
        except Exception as e:
            self.logger.error("Erro na validação dos dados", error=e)
            raise
    
    def add_calculated_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona campos calculados aos dados.
        
        Args:
            df: DataFrame a ser enriquecido
            
        Returns:
            DataFrame com campos adicionais
        """
        try:
            # Adiciona taxa inversa (para conversão reversa)
            df['inverse_rate'] = 1 / df['exchange_rate']
            
            # Adiciona categoria da moeda (desenvolvida, emergente, etc.)
            currency_categories = {
                'USD': 'major', 'EUR': 'major', 'GBP': 'major', 'JPY': 'major',
                'CHF': 'major', 'CAD': 'major', 'AUD': 'major',
                'BRL': 'emerging', 'MXN': 'emerging', 'ARS': 'emerging',
                'CNY': 'emerging'
            }
            
            df['currency_category'] = df['target_currency'].map(currency_categories).fillna('other')
            
            # Adiciona timestamp de processamento
            df['processing_timestamp'] = datetime.now().isoformat()
            
            self.logger.log_data_processing(
                operation="add_calculated_fields",
                records_count=len(df)
            )
            
            return df
            
        except Exception as e:
            self.logger.error("Erro ao adicionar campos calculados", error=e)
            raise
    
    def save_silver_data(self, df: pd.DataFrame, date_str: str = None) -> Path:
        """
        Salva dados transformados na camada silver.
        
        Args:
            df: DataFrame a ser salvo
            date_str: String da data (formato YYYY-MM-DD)
            
        Returns:
            Path do arquivo salvo
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Cria diretório silver se não existir
        silver_dir = Path(self.config.data_paths['silver'])
        silver_dir.mkdir(parents=True, exist_ok=True)
        
        # Nome do arquivo padronizado
        filename = f"exchange_rates_silver_{date_str}.parquet"
        file_path = silver_dir / filename
        
        try:
            # Salva em formato Parquet
            df.to_parquet(file_path, index=False, engine='pyarrow')
            
            self.logger.log_data_processing(
                operation="save_silver_data",
                records_count=len(df),
                file_path=str(file_path)
            )
            
            return file_path
            
        except Exception as e:
            self.logger.error("Erro ao salvar dados silver", error=e, file_path=str(file_path))
            raise
    
    def transform_daily_data(self, raw_file_path: Path, date_str: str = None) -> Path:
        """
        Executa o processo completo de transformação diária.
        
        Args:
            raw_file_path: Caminho para o arquivo de dados brutos
            date_str: String da data (formato YYYY-MM-DD)
            
        Returns:
            Path do arquivo de dados transformados salvo
        """
        start_time = time.time()
        
        try:
            self.logger.log_pipeline_stage("transform", "started")
            
            # Carrega dados brutos
            raw_data = self.load_raw_data(raw_file_path)
            
            # Normaliza dados
            df = self.normalize_exchange_rates(raw_data)
            
            # Valida qualidade dos dados
            df = self.validate_data_quality(df)
            
            # Adiciona campos calculados
            df = self.add_calculated_fields(df)
            
            # Salva dados transformados
            file_path = self.save_silver_data(df, date_str)
            
            duration = time.time() - start_time
            self.logger.log_pipeline_stage(
                "transform", 
                "completed", 
                duration=duration,
                file_path=str(file_path),
                records_processed=len(df)
            )
            
            return file_path
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.log_pipeline_stage(
                "transform", 
                "failed", 
                duration=duration,
                error=str(e)
            )
            raise
