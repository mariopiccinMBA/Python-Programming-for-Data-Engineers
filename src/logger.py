"""
Módulo de logging estruturado para o pipeline de cotações cambiais.
"""

import logging
import structlog
from pathlib import Path
from typing import Any, Dict
from datetime import datetime

def setup_logging(log_level: str = "INFO", log_dir: str = "logs") -> structlog.stdlib.BoundLogger:
    """
    Configura o sistema de logging estruturado.
    
    Args:
        log_level: Nível de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Diretório para salvar os logs
        
    Returns:
        Logger estruturado configurado
    """
    # Cria diretório de logs se não existir
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)
    
    # Configura o logging padrão do Python
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(message)s",
        handlers=[
            logging.FileHandler(
                log_path / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log",
                encoding='utf-8'
            ),
            logging.StreamHandler()
        ]
    )
    
    # Configura o structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.dev.ConsoleRenderer() if log_level == "DEBUG" else structlog.processors.JSONRenderer()
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    return structlog.get_logger()

class PipelineLogger:
    """Classe para logging específico do pipeline."""
    
    def __init__(self, component_name: str, logger: structlog.stdlib.BoundLogger = None):
        """
        Inicializa o logger do componente.
        
        Args:
            component_name: Nome do componente (ingest, transform, load, llm)
            logger: Logger estruturado (opcional)
        """
        self.component_name = component_name
        self.logger = logger or structlog.get_logger()
        self.logger = self.logger.bind(component=component_name)
    
    def info(self, message: str, **kwargs):
        """Log de informação."""
        self.logger.info(message, **kwargs)
    
    def error(self, message: str, error: Exception = None, **kwargs):
        """Log de erro."""
        if error:
            kwargs['error_type'] = type(error).__name__
            kwargs['error_message'] = str(error)
        self.logger.error(message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log de aviso."""
        self.logger.warning(message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log de debug."""
        self.logger.debug(message, **kwargs)
    
    def log_api_request(self, url: str, method: str = "GET", status_code: int = None, response_time: float = None):
        """Log específico para requisições de API."""
        self.logger.info(
            "API request completed",
            url=url,
            method=method,
            status_code=status_code,
            response_time_ms=response_time * 1000 if response_time else None
        )
    
    def log_data_processing(self, operation: str, records_count: int, file_path: str = None):
        """Log específico para processamento de dados."""
        self.logger.info(
            "Data processing completed",
            operation=operation,
            records_count=records_count,
            file_path=file_path
        )
    
    def log_llm_interaction(self, prompt_length: int, response_length: int, model: str, tokens_used: int = None):
        """Log específico para interações com LLM."""
        self.logger.info(
            "LLM interaction completed",
            model=model,
            prompt_length=prompt_length,
            response_length=response_length,
            tokens_used=tokens_used
        )
    
    def log_pipeline_stage(self, stage: str, status: str, duration: float = None, **kwargs):
        """Log específico para estágios do pipeline."""
        self.logger.info(
            f"Pipeline stage {status}",
            stage=stage,
            status=status,
            duration_seconds=duration,
            **kwargs
        )
