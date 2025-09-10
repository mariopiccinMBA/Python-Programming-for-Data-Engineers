"""
Módulo de carga de dados para a camada gold (dados finais).
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import time
from sqlalchemy import create_engine, text
import sqlalchemy as sa

from .config import Config
from .logger import PipelineLogger


class ExchangeRateLoader:
    """Classe responsável pela carga de dados na camada gold."""
    
    def __init__(self, config: Config, logger: PipelineLogger = None):
        """
        Inicializa o loader.
        
        Args:
            config: Configuração do projeto
            logger: Logger estruturado
        """
        self.config = config
        self.logger = logger or PipelineLogger("load")
        self.db_engine = None
        
        if config.database_enabled:
            self._setup_database_connection()
    
    def _setup_database_connection(self):
        """Configura conexão com banco de dados."""
        try:
            connection_string = (
                f"postgresql://{self.config.db_user}:{self.config.db_password}@"
                f"{self.config.db_host}:{self.config.db_port}/{self.config.db_name}"
            )
            self.db_engine = create_engine(connection_string)
            self.logger.info("Conexão com banco de dados configurada")
        except Exception as e:
            self.logger.error("Erro ao configurar conexão com banco", error=e)
            raise
    
    def load_silver_data(self, file_path: Path) -> pd.DataFrame:
        """
        Carrega dados da camada silver.
        
        Args:
            file_path: Caminho para o arquivo silver
            
        Returns:
            DataFrame com dados silver
        """
        try:
            df = pd.read_parquet(file_path, engine='pyarrow')
            
            self.logger.info("Dados silver carregados", 
                           file_path=str(file_path), 
                           records_count=len(df))
            return df
            
        except Exception as e:
            self.logger.error("Erro ao carregar dados silver", error=e, file_path=str(file_path))
            raise
    
    def calculate_aggregations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula agregações e métricas para a camada gold.
        
        Args:
            df: DataFrame com dados silver
            
        Returns:
            DataFrame com dados agregados
        """
        try:
            # Converte timestamp para datetime se necessário
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Agrupa por moeda e calcula estatísticas
            gold_data = []
            
            for currency in df['target_currency'].unique():
                currency_data = df[df['target_currency'] == currency].copy()
                
                if len(currency_data) == 0:
                    continue
                
                # Calcula métricas básicas
                latest_rate = currency_data['exchange_rate'].iloc[-1]
                min_rate = currency_data['exchange_rate'].min()
                max_rate = currency_data['exchange_rate'].max()
                avg_rate = currency_data['exchange_rate'].mean()
                std_rate = currency_data['exchange_rate'].std()
                
                # Calcula volatilidade (coeficiente de variação)
                volatility = std_rate / avg_rate if avg_rate != 0 else 0
                
                record = {
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'base_currency': currency_data['base_currency'].iloc[0],
                    'target_currency': currency,
                    'latest_rate': latest_rate,
                    'min_rate': min_rate,
                    'max_rate': max_rate,
                    'avg_rate': avg_rate,
                    'std_rate': std_rate,
                    'volatility': volatility,
                    'currency_category': currency_data['currency_category'].iloc[0],
                    'processing_timestamp': datetime.now().isoformat(),
                    'records_count': len(currency_data)
                }
                
                gold_data.append(record)
            
            gold_df = pd.DataFrame(gold_data)
            
            self.logger.log_data_processing(
                operation="calculate_aggregations",
                records_count=len(gold_df)
            )
            
            return gold_df
            
        except Exception as e:
            self.logger.error("Erro ao calcular agregações", error=e)
            raise
    
    def save_gold_data(self, df: pd.DataFrame, date_str: str = None) -> Path:
        """
        Salva dados finais na camada gold.
        
        Args:
            df: DataFrame a ser salvo
            date_str: String da data (formato YYYY-MM-DD)
            
        Returns:
            Path do arquivo salvo
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Cria diretório gold se não existir
        gold_dir = Path(self.config.data_paths['gold'])
        gold_dir.mkdir(parents=True, exist_ok=True)
        
        # Nome do arquivo padronizado
        filename = f"exchange_rates_gold_{date_str}.parquet"
        file_path = gold_dir / filename
        
        try:
            # Salva em formato Parquet
            df.to_parquet(file_path, index=False, engine='pyarrow')
            
            self.logger.log_data_processing(
                operation="save_gold_data",
                records_count=len(df),
                file_path=str(file_path)
            )
            
            return file_path
            
        except Exception as e:
            self.logger.error("Erro ao salvar dados gold", error=e, file_path=str(file_path))
            raise
    
    def save_to_database(self, df: pd.DataFrame, table_name: str = "exchange_rates_gold"):
        """
        Salva dados no banco de dados (opcional).
        
        Args:
            df: DataFrame a ser salvo
            table_name: Nome da tabela
        """
        if not self.db_engine:
            self.logger.warning("Banco de dados não configurado, pulando salvamento")
            return
        
        try:
            # Salva dados na tabela
            df.to_sql(
                table_name,
                self.db_engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            self.logger.log_data_processing(
                operation="save_to_database",
                records_count=len(df),
                table_name=table_name
            )
            
        except Exception as e:
            self.logger.error("Erro ao salvar no banco de dados", error=e, table_name=table_name)
            raise
    
    def load_daily_data(self, silver_file_path: Path, date_str: str = None) -> Path:
        """
        Executa o processo completo de carga diária.
        
        Args:
            silver_file_path: Caminho para o arquivo de dados silver
            date_str: String da data (formato YYYY-MM-DD)
            
        Returns:
            Path do arquivo de dados gold salvo
        """
        start_time = time.time()
        
        try:
            self.logger.log_pipeline_stage("load", "started")
            
            # Carrega dados silver
            df = self.load_silver_data(silver_file_path)
            
            # Calcula agregações
            gold_df = self.calculate_aggregations(df)
            
            # Salva dados gold
            file_path = self.save_gold_data(gold_df, date_str)
            
            # Salva no banco de dados se habilitado
            if self.config.database_enabled:
                self.save_to_database(gold_df)
            
            duration = time.time() - start_time
            self.logger.log_pipeline_stage(
                "load", 
                "completed", 
                duration=duration,
                file_path=str(file_path),
                records_processed=len(gold_df)
            )
            
            return file_path
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.log_pipeline_stage(
                "load", 
                "failed", 
                duration=duration,
                error=str(e)
            )
            raise
