import os
import glob
import logging
import pandas as pd
import sys
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .etl_runner import process_ipca_data
from .etl_runner import generate_mock_sales_data
from .etl_runner import run_etl_pipeline

# Configura o sistema de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

def main():
    """
    Função principal para orquestrar a execução do pipeline de dados.
    """
    # Define as constantes e caminhos dos arquivos
    DATA_FOLDER = 'data/raw'
    OUTPUT_FOLDER = 'data/docs'
    OUTPUT_MOCK_FOLDER = 'data/docs'

    # Define a url para o arquivo zip com a planilha do IPCA
    IPCA_ZIP_FILE_URL = "https://ftp.ibge.gov.br/Precos_Indices_de_Precos_ao_Consumidor/IPCA/Serie_Historica/ipca_SerieHist.zip"
    
    # --- Passo 1: Verifica se o arquivo final já existe e retorna o caminho ---
    target_dir = os.path.join('data', 'raw')
    found_csv_files = glob.glob(os.path.join(target_dir, f"vendas_ficticias*.csv"))
    
    if found_csv_files:
        logger.info(f"✅ O arquivo de vendas '{found_csv_files[0]}' já existe.")
        return found_csv_files[0]
    
    # Se o arquivo não existe, inicia o processo de geração
    
    # 2. Obtém os dados de inflação do IBGE
    logger.info("--- Passo 2: Obtendo dados de inflação do IBGE ---")
    years_closed_interval = [2020, 2024]
    inflacao_file_name = 'inflacao_2020_2024.csv'

    try:
        process_ipca_data(IPCA_ZIP_FILE_URL, years_closed_interval, inflacao_file_name, OUTPUT_MOCK_FOLDER)
        logger.info("process_ipca_data() executado.")
    except Exception as e:
        logger.error(f"❌ Falha no passo de obtenção de dados de inflação: {e}")
        return None

    # 3. Executa o pipeline ETL para processar os arquivos Excel
    logger.info("\n--- Passo 3: Executando o pipeline ETL para dados de vendas ---")
    try:
        output_file_name = "dados.csv"
        run_etl_pipeline(DATA_FOLDER, output_file_name, OUTPUT_FOLDER)
        logger.info("run_etl_pipeline() executado.")
    except Exception as e:
        logger.error(f"❌ Falha no passo de ETL de vendas: {e}")
        return None

    # 4. Gera os dados de vendas fictícios com base na inflação
    logger.info("\n--- Passo 4: Gerando dados de vendas fictícios ---")
    base_data_file = 'dados.csv'
    output_file = 'vendas_ficticias.csv'
    try:
        generate_mock_sales_data(OUTPUT_FOLDER, base_data_file, inflacao_file_name, output_file)
        logger.info("generate_mock_sales_data() executado.")
        return os.path.join(OUTPUT_FOLDER, output_file)
    except Exception as e:
        logger.error(f"❌ Falha no passo de geração de dados fictícios: {e}")
        return None

def generate_dashboard(csv_file_path: str):
    """
    Cria um dashboard interativo com os principais indicadores de vendas.
    Args:
        csv_file_path (str): O caminho para o arquivo CSV com os dados de vendas.
    """
    try:
        df = pd.read_csv(csv_file_path)
        logger.info(f"Dados de vendas carregados de '{csv_file_path}'.")

        # 1. Análise dos 5 meses com maior volume de vendas
        # Cria uma coluna de data no formato 'AAAA-MM' para facilitar o agrupamento
        df['ano_mes'] = df['ano'].astype(str) + '-' + df['mes'].str.upper().map({
            'JAN': '01', 'FEV': '02', 'MAR': '03', 'ABR': '04', 'MAI': '05', 'JUN': '06',
            'JUL': '07', 'AGO': '08', 'SET': '09', 'OUT': '10', 'NOV': '11', 'DEZ': '12'
        })
        
        vendas_por_mes = df.groupby('ano_mes')['volume_vendas_(kg)'].sum().reset_index()
        top_5_meses = vendas_por_mes.sort_values(by='volume_vendas_(kg)', ascending=False).head(5)

        fig_meses = px.bar(
            top_5_meses,
            x='ano_mes',
            y='volume_vendas_(kg)',
            title='Top 5 Meses com Maior Volume de Vendas',
            labels={'ano_mes': 'Mês (Ano)', 'volume_vendas_(kg)': 'Volume de Vendas (kg)'},
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        fig_meses.update_layout(xaxis_title="Mês (Ano)", yaxis_title="Volume de Vendas (kg)")

        # 2. Análise dos 3 petshops com maior volume de vendas
        vendas_por_petshop = df.groupby('pet_shop')['volume_vendas_(kg)'].sum().reset_index()
        top_3_petshops = vendas_por_petshop.sort_values(by='volume_vendas_(kg)', ascending=False).head(3)

        fig_petshops = px.bar(
            top_3_petshops,
            x='pet_shop',
            y='volume_vendas_(kg)',
            title='Top 3 Pet Shops com Maior Volume de Vendas',
            labels={'pet_shop': 'Pet Shop', 'volume_vendas_(kg)': 'Volume de Vendas (kg)'},
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        fig_petshops.update_layout(xaxis_title="Pet Shop", yaxis_title="Volume de Vendas (kg)")
        
        DASHBOARD_DESTINATION = os.path.dirname(csv_file_path)

        # 3. Combinar os gráficos em um único dashboard HTML
        dashboard_html_path = os.path.join(DASHBOARD_DESTINATION, "index.html")

        
        with open(dashboard_html_path, 'w', encoding='utf-8') as f:
            f.write("<html><head><title>Dashboard de Vendas</title></head><body>")
            f.write("<h1>Dashboard de Vendas da Empresa</h1>")
            f.write(fig_meses.to_html(full_html=False, include_plotlyjs='cdn'))
            f.write("<br>")
            f.write(fig_petshops.to_html(full_html=False, include_plotlyjs='cdn'))
            f.write("</body></html>")
        
        logger.info(f"\n✅ Dashboard gerado e salvo em '{dashboard_html_path}'.")

    except Exception as e:
        logger.error(f"Erro ao gerar o dashboard: {e}")

if __name__ == '__main__':
    # Verifica se o diretório de dados existe. Se não, o cria.
    if not os.path.exists('data/raw'):
        os.makedirs('data/raw')
        logger.info("Diretório 'data/raw' criado.")

    if not os.path.exists('data/docs'):
        os.makedirs('data/docs')
        logger.info("Diretório 'data/docs' criado.")

    csv_file = main()
    
    if csv_file:
        logger.info("\n--- Os dados de vendas foram localizados ---")
        generate_dashboard(csv_file)
    else:
        logger.info("\n🛑 Falha ao gerar dados de vendas.")