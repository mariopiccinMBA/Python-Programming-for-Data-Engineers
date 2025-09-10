import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px
import re
import re

# Título (corrigido colchete e acentuação)
st.title(":blue[Análise de Cotações Cambiais]")

BASE_DIR = Path(__file__).resolve().parents[1]  # volta para raiz do projeto
GOLD_DIR = BASE_DIR / "data" / "gold"

@st.cache_data(show_spinner=True)
def load_data(pasta: Path) -> pd.DataFrame:
	if not pasta.exists():
		st.warning(f"Pasta não encontrada: {pasta}")
		return pd.DataFrame()

	# Usa glob do pathlib (mais simples) / fallback glob para padrão string
	arquivos = sorted(pasta.glob("*.parquet"))
	if not arquivos:
		st.info("Nenhum arquivo parquet encontrado em data/gold.")
		return pd.DataFrame()

	dfs = []
	date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
	for f in arquivos:
		try:
			file_date_match = date_pattern.search(f.name)
			file_date = file_date_match.group(1) if file_date_match else None
			tmp = pd.read_parquet(f)
			# Força a coluna date a vir do nome do arquivo
			if file_date:
				try:
					tmp['date'] = pd.to_datetime(file_date)
				except Exception:
					tmp['date'] = file_date
			tmp["file_date_str"] = file_date
			tmp["source_file"] = f.name
			dfs.append(tmp)
		except Exception as e:
			st.error(f"Erro ao ler {f.name}: {e}")

	if not dfs:
		return pd.DataFrame()

	df = pd.concat(dfs, ignore_index=True, sort=False)
	return df

df = load_data(GOLD_DIR)

if df.empty:
	st.stop()

st.success(f"Registros carregados: {len(df):,}")

possible_date_cols = [c for c in df.columns if 'date' in c.lower() or 'data' in c.lower()]
date_col = possible_date_cols[0] if possible_date_cols else None
if date_col:
	try:
		df[date_col] = pd.to_datetime(df[date_col])
	except Exception:
		pass

###############################
# Simplificação solicitada
# - Apenas escolher moeda
# - Métrica fixa: latest_rate
# - Comparação: sempre última data (barra)
# - Mostrar insights LLM da última data (arquivo JSON)
###############################

required_metric = 'latest_rate'

if 'target_currency' not in df.columns or required_metric not in df.columns:
	st.error(f"Colunas necessárias ausentes: target_currency ou {required_metric}")
	st.stop()

# Seleção única ou múltipla de moedas (multiselect para permitir comparar algumas)
all_currencies = sorted(df['target_currency'].unique())
sel_currencies = st.multiselect(
	'Moedas:', all_currencies, default=all_currencies[:5] if len(all_currencies) > 5 else all_currencies
)
if not sel_currencies:
	st.info('Selecione ao menos uma moeda.'); st.stop()

df = df[df['target_currency'].isin(sel_currencies)]

# Última data
if date_col:
	last_date = df[date_col].max()
	last_df = df[df[date_col] == last_date]
else:
	last_df = df
	last_date = None

fig = px.bar(
	last_df.sort_values(required_metric, ascending=False),
	x='target_currency',
	y=required_metric,
	color='target_currency',
	title=f'latest_rate - comparação última data {last_date.date() if last_date else ""}'
)
st.plotly_chart(fig, use_container_width=True)

# Exibe insights LLM (arquivo json da última data disponível em data/gold)
gold_dir = GOLD_DIR
insight_file = None
if last_date:
	candidate = gold_dir / f"llm_insights_{last_date.date()}.json"
	if candidate.exists():
		insight_file = candidate
else:
	# fallback: pega o mais recente
	json_files = sorted(gold_dir.glob('llm_insights_*.json'))
	if json_files:
		insight_file = json_files[-1]

st.subheader('Insights LLM')
if insight_file:
	import json
	try:
		with open(insight_file, 'r', encoding='utf-8') as f:
			data_insight = json.load(f)
		st.caption(f"Fonte: {insight_file.name}")

		# Mostra sempre o resumo executivo completo (se existir)
		if 'business_insights' in data_insight:
			st.markdown(data_insight['business_insights'].split('**ANÁLISE POR CATEGORIA:**')[0])

		def filtrar_explicacao(texto: str, moedas: list[str]) -> str:
			"""Extrai somente blocos das moedas selecionadas do campo currency_explanation."""
			# Divide por dupla que começa com número + ponto ou linha em branco
			linhas = texto.splitlines()
			blocos = []
			bloco_atual = []
			for ln in linhas:
				if re.match(r'^\d+\.\s+\*\*', ln) and bloco_atual:
					blocos.append('\n'.join(bloco_atual))
					bloco_atual = [ln]
				else:
					bloco_atual.append(ln)
			if bloco_atual:
				blocos.append('\n'.join(bloco_atual))
			selecionados = []
			upper_set = {m.upper() for m in moedas}
			for b in blocos:
				m = re.search(r'\*\*(\w{3})\s', b)
				if m and m.group(1).upper() in upper_set:
					selecionados.append(b)
			return '\n\n'.join(selecionados)

		if 'currency_explanation' in data_insight:
			filtered = filtrar_explicacao(data_insight['currency_explanation'], sel_currencies)
			if filtered.strip():
				st.markdown(f"**Moedas selecionadas:** {', '.join(sel_currencies)}")
				st.markdown(filtered)
			else:
				st.info('Nenhum trecho específico encontrado para as moedas selecionadas.')
			with st.expander('Ver texto completo de explicação por moeda'):
				st.markdown(data_insight['currency_explanation'])
		meta = data_insight.get('metadata', {})
		if meta:
			st.json(meta)
	except Exception as e:
		st.error(f"Erro ao ler insights: {e}")
else:
	st.info('Nenhum arquivo de insight LLM encontrado para a data selecionada.')

with st.expander('Pré-visualização (última data filtrada)'):
	st.dataframe(last_df[['target_currency', required_metric, 'date']].head(100) if 'date' in last_df.columns else last_df.head(100))
