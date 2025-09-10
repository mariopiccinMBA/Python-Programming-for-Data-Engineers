#!/usr/bin/env python3
"""
Ponto de entrada principal para o pipeline de cotações cambiais.
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Adiciona o diretório src ao path
sys.path.append(str(Path(__file__).parent / "src"))

from src.pipeline import CurrencyExchangePipeline


def main():
    """Função principal."""
    parser = argparse.ArgumentParser(
        description="Pipeline de Cotações Cambiais com Python + LLM",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python main.py --daily                    # Executa pipeline para hoje
  python main.py --daily --date 2024-01-15  # Executa para data específica
  python main.py --historical --start 2024-01-01 --end 2024-01-07  # Período histórico
  python main.py --status                   # Verifica status do pipeline
        """
    )
    
    # Argumentos principais
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--daily',
        action='store_true',
        help='Executa pipeline diário'
    )
    group.add_argument(
        '--historical',
        action='store_true',
        help='Executa pipeline histórico'
    )
    group.add_argument(
        '--status',
        action='store_true',
        help='Verifica status do pipeline'
    )
    
    # Argumentos opcionais
    parser.add_argument(
        '--date',
        type=str,
        help='Data específica no formato YYYY-MM-DD (padrão: hoje)'
    )
    parser.add_argument(
        '--start',
        type=str,
        help='Data inicial para pipeline histórico (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end',
        type=str,
        help='Data final para pipeline histórico (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--config',
        type=str,
        help='Caminho para arquivo de configuração personalizado'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Saída detalhada'
    )
    
    args = parser.parse_args()
    
    try:
        # Inicializa pipeline
        print("Inicializando pipeline de cotações cambiais...")
        pipeline = CurrencyExchangePipeline(config_path=args.config)
        
        if args.status:
            # Verifica status
            print("Verificando status do pipeline...")
            status = pipeline.get_pipeline_status()
            
            print("\n" + "="*50)
            print("STATUS DO PIPELINE")
            print("="*50)
            
            if status.get('pipeline_ready'):
                print("Pipeline pronto para execução")
            else:
                print("Pipeline não está pronto")
                if 'error' in status:
                    print(f"   Erro: {status['error']}")
            
            print(f"Configuração carregada: {'Sucesso' if status.get('config_loaded') else 'Falha'}")
            print(f"Componentes inicializados: {'Sucesso' if status.get('components_initialized') else 'Falha'}")
            
            if 'data_paths' in status:
                print("\nDiretórios de dados:")
                for path_type, exists in status['data_paths'].items():
                    print(f"   {path_type}: {'Sucesso' if exists == 'True' else 'Falha'}")
            
        elif args.daily:
            # Pipeline diário
            date_str = args.date if args.date else datetime.now().strftime("%Y-%m-%d")
            
            print(f"Executando pipeline diário para {date_str}...")
            results = pipeline.run_daily_pipeline(date_str)
            
            print("\n" + "="*50)
            print("RESULTADOS DO PIPELINE DIÁRIO")
            print("="*50)
            
            if 'error' in results:
                print(f"Erro na execução: {results['error']}")
                print(f"Tempo de execução: {results.get('execution_time', 0):.2f}s")
                return 1
            else:
                print(f"Pipeline executado com sucesso!")
                print(f"Tempo de execução: {results.get('execution_time', 0):.2f}s")
                
                if args.verbose:
                    print(f"\nArquivos gerados:")
                    for key, value in results.items():
                        if key.endswith('_file'):
                            print(f"   {key}: {value}")
                    
                    if 'llm_analysis' in results:
                        print(f"\nAnálise LLM:")
                        analysis = results['llm_analysis']
                        if isinstance(analysis, dict):
                            for key, value in analysis.items():
                                print(f"   {key}: {value}")
                        else:
                            print(f"   {analysis}")
        
        elif args.historical:
            # Pipeline histórico
            if not args.start or not args.end:
                print("Para pipeline histórico, especifique --start e --end")
                return 1
            
            print(f"Executando pipeline histórico de {args.start} a {args.end}...")
            results = pipeline.run_historical_pipeline(args.start, args.end)
            
            print("\n" + "="*50)
            print("RESULTADOS DO PIPELINE HISTÓRICO")
            print("="*50)
            
            if 'fatal_error' in results:
                print(f"Erro fatal: {results['fatal_error']}")
                print(f"Tempo total: {results.get('total_execution_time', 0):.2f}s")
                return 1
            else:
                dates_processed = len(results.get('dates_processed', []))
                errors = len(results.get('errors', []))
                
                print(f"Pipeline histórico concluído!")
                print(f"Datas processadas: {dates_processed}")
                print(f"Erros: {errors}")
                print(f"Tempo total: {results.get('total_execution_time', 0):.2f}s")
                
                if args.verbose and errors > 0:
                    print(f"\nDetalhes dos erros:")
                    for error in results.get('errors', []):
                        print(f"   {error['date']}: {error['error']}")
        
        print("\nExecução concluída!")
        return 0
        
    except KeyboardInterrupt:
        print("\nExecução interrompida pelo usuário")
        return 1
    except Exception as e:
        print(f"\nErro inesperado: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
