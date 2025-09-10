"""
Módulo de análise e geração de insights usando LLM (ChatGPT).
"""

import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import time
from openai import OpenAI

from .config import Config
from .logger import PipelineLogger


class LLMAnalyzer:
    """Classe responsável pela análise de dados usando LLM."""
    
    def __init__(self, config: Config, logger: PipelineLogger = None):
        """
        Inicializa o analisador LLM.
        
        Args:
            config: Configuração do projeto
            logger: Logger estruturado
        """
        self.config = config
        self.logger = logger or PipelineLogger("llm")
        
        # Inicializa cliente OpenAI
        self.client = OpenAI(api_key=config.openai_api_key)
    
    def load_gold_data(self, file_path: Path) -> pd.DataFrame:
        """
        Carrega dados da camada gold.
        
        Args:
            file_path: Caminho para o arquivo gold
            
        Returns:
            DataFrame com dados gold
        """
        try:
            df = pd.read_parquet(file_path, engine='pyarrow')
            
            self.logger.info("Dados gold carregados para análise LLM", 
                           file_path=str(file_path), 
                           records_count=len(df))
            return df
            
        except Exception as e:
            self.logger.error("Erro ao carregar dados gold", error=e, file_path=str(file_path))
            raise
    
    def prepare_data_summary(self, df: pd.DataFrame) -> str:
        """
        Prepara resumo dos dados para o LLM.
        
        Args:
            df: DataFrame com dados gold
            
        Returns:
            String com resumo formatado dos dados
        """
        try:
            # Ordena por volatilidade para destacar moedas mais voláteis
            df_sorted = df.sort_values('volatility', ascending=False)
            
            # Cria resumo estruturado
            summary_data = []
            
            for _, row in df_sorted.iterrows():
                currency_info = {
                    "moeda": row['target_currency'],
                    "categoria": row['currency_category'],
                    "taxa_atual": round(row['latest_rate'], 4),
                    "taxa_minima": round(row['min_rate'], 4),
                    "taxa_maxima": round(row['max_rate'], 4),
                    "taxa_media": round(row['avg_rate'], 4),
                    "volatilidade": round(row['volatility'] * 100, 2)  # Converte para percentual
                }
                summary_data.append(currency_info)
            
            # Formata como string legível
            summary_text = f"""
RELATÓRIO DE COTAÇÕES CAMBIAIS - {datetime.now().strftime('%d/%m/%Y')}

Moeda Base: {df.iloc[0]['base_currency']}
Total de Moedas Analisadas: {len(df)}

DETALHAMENTO POR MOEDA:
"""
            
            for currency in summary_data:
                summary_text += f"""
{currency['moeda']} ({currency['categoria'].upper()}):
- Taxa Atual: {currency['taxa_atual']}
- Variação: {currency['taxa_minima']} - {currency['taxa_maxima']}
- Taxa Média: {currency['taxa_media']}
- Volatilidade: {currency['volatilidade']}%
"""
            
            return summary_text.strip()
            
        except Exception as e:
            self.logger.error("Erro ao preparar resumo dos dados", error=e)
            raise
    
    def generate_business_insights(self, data_summary: str) -> str:
        """
        Gera insights de negócio usando LLM.
        
        Args:
            data_summary: Resumo dos dados formatado
            
        Returns:
            String com insights gerados pelo LLM
        """
        try:
            prompt = f"""
Você é um analista financeiro especializado em mercado de câmbio. Analise os dados de cotações cambiais abaixo e forneça insights executivos em linguagem simples para usuários de negócio.

{data_summary}

Por favor, forneça uma análise que inclua:

1. RESUMO EXECUTIVO: Um parágrafo resumindo a situação geral do mercado cambial hoje.

2. PRINCIPAIS DESTAQUES:
   - Moedas com maior volatilidade e o que isso significa
   - Moedas mais estáveis
   - Oportunidades ou riscos identificados

3. ANÁLISE POR CATEGORIA:
   - Moedas desenvolvidas (major): Como estão se comportando
   - Moedas emergentes: Tendências observadas

4. RECOMENDAÇÕES PRÁTICAS:
   - Para empresas que fazem importação/exportação
   - Para investidores
   - Pontos de atenção para os próximos dias

Use linguagem clara e evite jargões técnicos. Foque em insights acionáveis para tomada de decisão de negócio.
"""

            start_time = time.time()
            
            response = self.client.chat.completions.create(
                model=self.config.llm_config['model'],
                messages=[
                    {"role": "system", "content": "Você é um analista financeiro especializado em câmbio que fornece insights claros para executivos de negócio."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.config.llm_config['max_tokens'],
                temperature=self.config.llm_config['temperature']
            )
            
            response_time = time.time() - start_time
            insights = response.choices[0].message.content
            
            # Log da interação com LLM
            self.logger.log_llm_interaction(
                prompt_length=len(prompt),
                response_length=len(insights),
                model=self.config.llm_config['model'],
                tokens_used=response.usage.total_tokens if response.usage else None
            )
            
            self.logger.info("Insights de negócio gerados com sucesso", 
                           response_time_seconds=response_time)
            
            return insights
            
        except Exception as e:
            self.logger.error("Erro ao gerar insights com LLM", error=e)
            raise
    
    def generate_currency_explanation(self, df: pd.DataFrame, focus_currencies: List[str] = None) -> str:
        """
        Gera explicação específica sobre moedas selecionadas.
        
        Args:
            df: DataFrame com dados gold
            focus_currencies: Lista de moedas para focar (padrão: top 5 por volatilidade)
            
        Returns:
            String com explicação das moedas
        """
        try:
            if focus_currencies is None:
                # Seleciona top 5 moedas por volatilidade
                top_currencies = df.nlargest(5, 'volatility')['target_currency'].tolist()
            else:
                top_currencies = focus_currencies
            
            # Filtra dados das moedas selecionadas
            focus_data = df[df['target_currency'].isin(top_currencies)]
            
            # Prepara dados das moedas selecionadas
            currency_details = []
            for _, row in focus_data.iterrows():
                currency_details.append(f"""
{row['target_currency']}: Taxa atual de {row['latest_rate']:.4f}, volatilidade de {row['volatility']*100:.2f}%
""")
            
            prompt = f"""
Explique em termos simples como estão as seguintes moedas em relação ao {df.iloc[0]['base_currency']} hoje:

{''.join(currency_details)}

Para cada moeda, explique:
- Se a taxa atual está alta ou baixa
- O que a volatilidade significa na prática
- Impacto para quem precisa comprar/vender essa moeda

Use linguagem acessível, como se estivesse explicando para alguém que não é especialista em câmbio.
"""

            start_time = time.time()
            
            response = self.client.chat.completions.create(
                model=self.config.llm_config['model'],
                messages=[
                    {"role": "system", "content": "Você é um consultor financeiro que explica câmbio de forma simples e prática."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.config.llm_config['max_tokens'],
                temperature=self.config.llm_config['temperature']
            )
            
            response_time = time.time() - start_time
            explanation = response.choices[0].message.content
            
            # Log da interação
            self.logger.log_llm_interaction(
                prompt_length=len(prompt),
                response_length=len(explanation),
                model=self.config.llm_config['model'],
                tokens_used=response.usage.total_tokens if response.usage else None
            )
            
            return explanation
            
        except Exception as e:
            self.logger.error("Erro ao gerar explicação de moedas", error=e)
            raise
    
    def save_llm_insights(self, insights: str, explanation: str, date_str: str = None) -> Path:
        """
        Salva insights do LLM em arquivo.
        
        Args:
            insights: Insights de negócio gerados
            explanation: Explicação das moedas
            date_str: String da data (formato YYYY-MM-DD)
            
        Returns:
            Path do arquivo salvo
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Cria diretório gold se não existir
        gold_dir = Path(self.config.data_paths['gold'])
        gold_dir.mkdir(parents=True, exist_ok=True)
        
        # Estrutura o relatório final
        report = {
            "metadata": {
                "generation_date": datetime.now().isoformat(),
                "model_used": self.config.llm_config['model'],
                "report_type": "currency_analysis"
            },
            "business_insights": insights,
            "currency_explanation": explanation,
            "generation_timestamp": datetime.now().isoformat()
        }
        
        # Salva em JSON
        json_filename = f"llm_insights_{date_str}.json"
        json_path = gold_dir / json_filename
        
        # Salva em texto legível
        txt_filename = f"llm_report_{date_str}.txt"
        txt_path = gold_dir / txt_filename
        
        try:
            # Salva JSON estruturado
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            # Salva relatório em texto legível
            report_text = f"""
RELATÓRIO DE ANÁLISE CAMBIAL - {datetime.now().strftime('%d/%m/%Y %H:%M')}
{'='*60}

INSIGHTS DE NEGÓCIO:
{insights}

{'='*60}

EXPLICAÇÃO DAS MOEDAS:
{explanation}

{'='*60}
Relatório gerado automaticamente pelo Pipeline de Cotações Cambiais
Modelo LLM utilizado: {self.config.llm_config['model']}
"""
            
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(report_text)
            
            self.logger.log_data_processing(
                operation="save_llm_insights",
                records_count=2,  # JSON + TXT
                file_path=str(json_path)
            )
            
            return json_path
            
        except Exception as e:
            self.logger.error("Erro ao salvar insights do LLM", error=e)
            raise
    
    def analyze_daily_data(self, gold_file_path: Path, date_str: str = None) -> Dict[str, Path]:
        """
        Executa análise completa dos dados diários usando LLM.
        
        Args:
            gold_file_path: Caminho para o arquivo de dados gold
            date_str: String da data (formato YYYY-MM-DD)
            
        Returns:
            Dict com caminhos dos arquivos gerados
        """
        start_time = time.time()
        
        try:
            self.logger.log_pipeline_stage("llm_analysis", "started")
            
            # Carrega dados gold
            df = self.load_gold_data(gold_file_path)
            
            # Prepara resumo dos dados
            data_summary = self.prepare_data_summary(df)
            
            # Gera insights de negócio
            business_insights = self.generate_business_insights(data_summary)
            
            # Gera explicação das moedas
            currency_explanation = self.generate_currency_explanation(df)
            
            # Salva relatório final
            report_path = self.save_llm_insights(business_insights, currency_explanation, date_str)
            
            duration = time.time() - start_time
            self.logger.log_pipeline_stage(
                "llm_analysis", 
                "completed", 
                duration=duration,
                report_path=str(report_path)
            )
            
            return {
                "report_json": report_path,
                "report_txt": report_path.parent / f"llm_report_{date_str or datetime.now().strftime('%Y-%m-%d')}.txt"
            }
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.log_pipeline_stage(
                "llm_analysis", 
                "failed", 
                duration=duration,
                error=str(e)
            )
            raise
