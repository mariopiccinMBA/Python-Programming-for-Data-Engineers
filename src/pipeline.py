"""
Módulo principal do pipeline de cotações cambiais.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import time

from .config import Config
from .logger import setup_logging, PipelineLogger
from .ingest import ExchangeRateIngester
from .transform import ExchangeRateTransformer
from .load import ExchangeRateLoader
from .llm_analyzer import LLMAnalyzer


class CurrencyExchangePipeline:
    """Pipeline principal para processamento de cotações cambiais."""
    
    def __init__(self, config_path: str = None):
        """
        Inicializa o pipeline.
        
        Args:
            config_path: Caminho para arquivo de configuração
        """
        # Carrega configuração
        self.config = Config(config_path)
        
        # Configura logging
        self.main_logger = setup_logging(
            log_level=self.config.logging_config['level'],
            log_dir=self.config.data_paths['logs']
        )
        self.logger = PipelineLogger("pipeline", self.main_logger)
        
        # Inicializa componentes
        self.ingester = ExchangeRateIngester(self.config, PipelineLogger("ingest", self.main_logger))
        self.transformer = ExchangeRateTransformer(self.config, PipelineLogger("transform", self.main_logger))
        self.loader = ExchangeRateLoader(self.config, PipelineLogger("load", self.main_logger))
        self.llm_analyzer = LLMAnalyzer(self.config, PipelineLogger("llm", self.main_logger))
    
    def validate_setup(self) -> bool:
        """
        Valida se o pipeline está configurado corretamente.
        
        Returns:
            bool: True se configuração é válida
        """
        try:
            self.logger.info("Validando configuração do pipeline")
            
            # Valida chaves de API
            self.config.validate_api_keys()
            
            self.logger.info("Pipeline configurado corretamente")
            return True
            
        except Exception as e:
            self.logger.error("Erro na validação da configuração", error=e)
            return False
    
    def run_daily_pipeline(self, date_str: str = None) -> Dict[str, Any]:
        """
        Executa o pipeline completo para um dia.
        
        Args:
            date_str: Data no formato YYYY-MM-DD (padrão: hoje)
            
        Returns:
            Dict com resultados da execução
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        start_time = time.time()
        results = {}
        
        try:
            self.logger.info(f"Iniciando pipeline diário para {date_str}")
            
            # 1. Ingestão
            self.logger.info("Etapa 1: Ingestão de dados")
            bronze_file = self.ingester.ingest_daily_rates(date_str)
            # Compat: expor tanto bronze_file quanto raw_file (legado)
            results['bronze_file'] = str(bronze_file)
            results['raw_file'] = str(bronze_file)
            
            # 2. Transformação
            self.logger.info("Etapa 2: Transformação de dados")
            silver_file = self.transformer.transform_daily_data(bronze_file, date_str)
            results['silver_file'] = str(silver_file)
            
            # 3. Carga
            self.logger.info("Etapa 3: Carga para camada gold")
            gold_file = self.loader.load_daily_data(silver_file, date_str)
            results['gold_file'] = str(gold_file)
            
            # 4. Análise LLM
            self.logger.info("Etapa 4: Análise com LLM")
            llm_results = self.llm_analyzer.analyze_daily_data(gold_file, date_str)
            results['llm_analysis'] = llm_results
            
            # Calcula tempo total
            execution_time = time.time() - start_time
            results['execution_time'] = execution_time
            
            self.logger.info(
                f"Pipeline concluído com sucesso para {date_str}",
                execution_time=execution_time,
                files_created=len([f for f in results.values() if isinstance(f, str) and f.endswith('.parquet')])
            )
            
            return results
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(
                f"Erro na execução do pipeline para {date_str}",
                error=e,
                execution_time=execution_time
            )
            results['error'] = str(e)
            results['execution_time'] = execution_time
            return results
    
    def run_historical_pipeline(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Executa o pipeline para um período histórico.
        
        Args:
            start_date: Data inicial no formato YYYY-MM-DD
            end_date: Data final no formato YYYY-MM-DD
            
        Returns:
            Dict com resultados da execução
        """
        from datetime import datetime, timedelta
        
        start_time = time.time()
        results = {'dates_processed': [], 'errors': []}
        
        try:
            self.logger.info(f"Iniciando pipeline histórico de {start_date} a {end_date}")
            
            # Converte strings para datetime
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            current_dt = start_dt
            while current_dt <= end_dt:
                date_str = current_dt.strftime("%Y-%m-%d")
                
                try:
                    daily_results = self.run_daily_pipeline(date_str)
                    if 'error' not in daily_results:
                        results['dates_processed'].append(date_str)
                    else:
                        results['errors'].append({
                            'date': date_str,
                            'error': daily_results['error']
                        })
                        
                except Exception as e:
                    results['errors'].append({
                        'date': date_str,
                        'error': str(e)
                    })
                
                current_dt += timedelta(days=1)
                
                # Pausa entre requests para não sobrecarregar APIs
                time.sleep(1)
            
            execution_time = time.time() - start_time
            results['total_execution_time'] = execution_time
            
            self.logger.info(
                f"Pipeline histórico concluído",
                dates_processed=len(results['dates_processed']),
                errors=len(results['errors']),
                execution_time=execution_time
            )
            
            return results
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(
                f"Erro na execução do pipeline histórico",
                error=e,
                execution_time=execution_time
            )
            results['fatal_error'] = str(e)
            results['total_execution_time'] = execution_time
            return results
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Retorna o status atual do pipeline.
        
        Returns:
            Dict com informações de status
        """
        try:
            status = {
                'pipeline_ready': self.validate_setup(),
                'config_loaded': self.config is not None,
                'components_initialized': all([
                    self.ingester is not None,
                    self.transformer is not None,
                    self.loader is not None,
                    self.llm_analyzer is not None
                ]),
                'data_paths': {
                    'bronze': str(Path(self.config.data_paths['bronze']).exists()),
                    'silver': str(Path(self.config.data_paths['silver']).exists()),
                    'gold': str(Path(self.config.data_paths['gold']).exists()),
                    'logs': str(Path(self.config.data_paths['logs']).exists())
                }
            }
            
            return status
            
        except Exception as e:
            return {
                'pipeline_ready': False,
                'error': str(e)
            }


if __name__ == "__main__":
    # Exemplo de uso
    pipeline = CurrencyExchangePipeline()
    
    if pipeline.validate_setup():
        # Executa pipeline para hoje
        results = pipeline.run_daily_pipeline()
        print("Resultados:", results)
    else:
        print("Pipeline não está configurado corretamente")
