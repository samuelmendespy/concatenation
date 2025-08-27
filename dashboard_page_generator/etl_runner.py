import os
import logging
import pandas as pd
import numpy as np
import requests
import zipfile
import glob
import shutil

# --- Seção de Configuração do Logger ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Adiciona handlers ao logger apenas se eles ainda não existirem
if not logger.handlers:
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler('file.log')

    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
# --- Fim da Seção de Configuração do Logger ---

def run_etl_pipeline(source_data_folder: str, output_filename: str, output_source_data_folder: str,):
    """
    Orquestra o processo ETL para os arquivos Excel na pasta especificada.
    As funções auxiliares de ETL são definidas localmente para encapsulamento.

    Args:
        source_data_folder (str): O caminho para as planilhas xlsx com dados base
        output_filename (str): Arquivo csv a ser gerado. exemplo: dados.csv 
        source_data_folder (str): O caminho para salvar o arquivo csv de saída com dados base
    """

    # --- Funções de ETL ---
    def load_excel(file_path: str) -> pd.DataFrame:
        """Extrai dados de um único arquivo Excel usando pandas."""
        try:
            df = pd.read_excel(file_path)
            return df
        except Exception as e:
            raise ConnectionRefusedError(f"Erro ao carregar o arquivo Excel {file_path}: {e}")

    def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa e transforma o DataFrame de dados de vendas,
        incluindo a reorganização das colunas.
        """
        df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
        new_column_order = ['uf', 'id', 'pet_shop', 'compra_maio_2023_(kg)']
        df_reordered = df[new_column_order]
        return df_reordered.dropna()

    def save_to_csv(df: pd.DataFrame, output_source_data_folder: str):
        """Salva o DataFrame como csv na pasta especificada."""
        try:
            file_path = os.path.join(output_source_data_folder, output_filename)
            df.to_csv(file_path, index=False)
            logger.info(f"O arquivo '{file_name}' foi salvo com sucesso em '{file_path}'.")
        except Exception as e:
            logger.error(f"Erro ao salvar dados no arquivo CSV: {e}")
            raise OSError(f"Erro ao carregar dados para o arquivo CSV.")
    # --- Fim das Funções de ETL aninhadas ---

    excel_files = [f for f in os.listdir(source_data_folder) if f.endswith('.xlsx') and not f.startswith('~')]

    if not excel_files:
        logger.warning(f"Nenhum arquivo Excel encontrado em {source_data_folder}. Pulando o ETL.")
        return

    all_cleaned_data = []

    for file_name in excel_files:
        file_path = os.path.join(source_data_folder, file_name)
        logger.info(f"Iniciando ETL para o arquivo: {file_path}")

        try:
            raw_df = load_excel(file_path)
            clean_df = clean_sales_data(raw_df)
            all_cleaned_data.append(clean_df)
            logger.info(f"Processado com sucesso o arquivo {file_name}")
        except Exception as e:
            logger.error(f"Erro ao processar o arquivo {file_name}: {e}")

    if all_cleaned_data:
        final_df = pd.concat(all_cleaned_data, ignore_index=True)
        save_to_csv(final_df, output_source_data_folder)
        logger.info("Todos os dados processados foram carregados no banco de dados.")
    else:
        logger.info("Nenhum dado para carregar no banco de dados após o processamento dos arquivos.")

def download_and_extract_data(url_zip, target_dir):
    """
    Baixa um arquivo ZIP de uma URL, descompacta-o e move o arquivo .xls
    para o diretório de destino.

    Args:
        url_zip (str): A url para baixar o arquivo zip do IPCA.
        target_dir (str): O diretório para destino do arquivo xls.
    """
    local_zip = os.path.join(target_dir, "ipca_data.zip")
    temp_dir = os.path.join(target_dir, "temp_data_ipca")

    logging.info(f"Iniciando o download de '{url_zip}'...")
    resposta = requests.get(url_zip, stream=True)
    resposta.raise_for_status()

    with open(local_zip, 'wb') as arquivo_zip_local:
        for chunk in resposta.iter_content(chunk_size=8192):
            arquivo_zip_local.write(chunk)
    
    logging.info("✅ Download do arquivo ZIP concluído com sucesso!")
    logging.info(f"Descompactando o arquivo '{local_zip}'...")
    
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    with zipfile.ZipFile(local_zip, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
        
    logging.info("✅ Arquivo ZIP descompactado com sucesso!")

    path_to_xls_temp = glob.glob(os.path.join(temp_dir, '**', 'ipca_*.xls'), recursive=True)
    if not path_to_xls_temp:
        raise FileNotFoundError("Nenhum arquivo .xls com o padrão 'ipca_' foi encontrado no diretório descompactado.")
    
    path_to_xls_temp = path_to_xls_temp[0]
    nome_arquivo_xls = os.path.basename(path_to_xls_temp)
    path_to_xls_final = os.path.join(target_dir, nome_arquivo_xls)
    
    shutil.move(path_to_xls_temp, path_to_xls_final)
    
    return path_to_xls_final

def process_xls_data(path_to_xls, years_to_filter, path_to_csv_file):
    """
    Lê o arquivo XLS, processa os dados de inflação e os salva em um arquivo CSV.

    Args:
        path_to_xls (str): O caminho para a planilha xls com dados históricos do IPCA
        years_to_filter (str): Array para intervalo de interesse com um ano de início e um ano de fim ex: [2020, 2024]
        path_to_csv_file : O caminho para o arquivo csv que será exportado.
    """
    logging.info("Iniciando o processamento dos dados do arquivo XLS...")
    
    # Lê o arquivo sem pular linhas, considerando o cabeçalho nulo
    df_raw = pd.read_excel(path_to_xls, header=None)
    logging.info(f"DataFrame bruto lido com {len(df_raw)} linhas.")

    final_data = []
    start_year = min(years_to_filter)
    end_year = max(years_to_filter)

    for index, row in df_raw.iterrows():
        # Converte a célula da coluna 'A' para string para verificar se contém um ano
        col_a_value = str(row[0])

        # Verifica se o valor é um ano de 4 dígitos e está no intervalo desejado
        if len(col_a_value) == 4 and col_a_value.isdigit():
            year_found = int(col_a_value)
            logging.info(f"Encontrado ano {year_found} na linha {index}.")

            # Se o ano for um dos anos para filtrar, processa as 12 linhas seguintes (1 ano = 12 meses)
            if start_year <= year_found <= end_year:
                # O processamento será feito para a linha atual e as 11 próximas
                for i in range(12):
                    current_row_index = index + i
                    # Verifica se a linha existe no DataFrame para evitar erros
                    if current_row_index < len(df_raw):
                        row_ipca_data = df_raw.iloc[current_row_index]

                        # Extrai os dados das colunas B (índice 1) e D (índice 3)
                        mes = row_ipca_data[1] if pd.notna(row_ipca_data[1]) else None
                        inflacao = row_ipca_data[3] if pd.notna(row_ipca_data[3]) else None

                        # Adiciona os dados à lista
                        if mes and inflacao:
                            final_data.append({
                                'ANO': year_found,
                                'MES': str(mes).strip(),
                                'INFLACAO_NO_MES': str(inflacao).replace(',', '.').strip()
                            })

    # Cria o DataFrame final a partir da lista
    df_final = pd.DataFrame(final_data)

    # Converte a coluna de inflação para formato numérico
    df_final['INFLACAO_NO_MES'] = pd.to_numeric(df_final['INFLACAO_NO_MES'], errors='coerce')
    
    # Adiciona a contagem de linhas ao log
    logging.info(f"Dados processados: {len(df_final)} entradas.")

    # Exporta para CSV
    df_final.to_csv(path_to_csv_file, index=False, encoding='utf-8')
    
    logging.info(f"✅ Dados dos anos {years_to_filter} exportados com sucesso para '{path_to_csv_file}'.")

def process_ipca_data(url_zip, years_to_filter, csv_filename, OUTPUT_MOCK_FOLDER):
    """
    Orquestra o processo de obtenção e processamento dos dados de inflação IPCA.

    Args:
        url_zip (str): A url para baixar o arquivo zip do IPCA.
        years_to_filter (str): Array para intervalo de interesse com um ano de início e um ano de fim ex: [2020, 2024]
        csv_filename : O nome do arquivo CSV gerado com os dados de inflação no período de interesse.
    """

    OUTPUT_MOCK_FOLDER = "data/docs"

    target_dir = OUTPUT_MOCK_FOLDER
    path_to_csv_file = os.path.join(target_dir, csv_filename)
    
    # --- Passo 1: Verifica se o arquivo final já existe ---
    found_csv_files = glob.glob(os.path.join(target_dir, f"ipca_*.csv"))
    if found_csv_files:
        logging.info(f"✅ O arquivo '{found_csv_files[0]}' já existe. O script será encerrado.")
        return
    
    path_to_xls = None
    try:
        # --- Verifica se o arquivo xls já foi obtido ---
        sheet_xls_local = glob.glob(os.path.join(target_dir, f"ipca_*.xls"))
        if sheet_xls_local:
            logging.info(f"✅ Foi encontrada a planilha '{sheet_xls_local[0]}'. Pulando o download.")
            path_to_xls = sheet_xls_local[0]
        else:
            # Cria os diretórios necessários
            os.makedirs(target_dir, exist_ok=True)
            # Baixa e extrai os dados
            path_to_xls = download_and_extract_data(url_zip, target_dir)
            
        # --- Processa os dados a partir do arquivo XLS ---
        if path_to_xls:
            process_xls_data(path_to_xls, years_to_filter, path_to_csv_file)

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Erro de download: {e}")
    except zipfile.BadZipFile:
        logging.error("❌ Erro: O arquivo baixado não é um arquivo ZIP válido.")
    except FileNotFoundError as e:
        logging.error(f"❌ Erro: {e}")
    except Exception as e:
        logging.error(f"❌ Ocorreu um erro inesperado: {e}")
    finally:
        # Limpeza de arquivos temporários
        temp_dir = os.path.join(target_dir, "temp_data_ipca")
        local_zip = os.path.join(target_dir, "ipca_data.zip")
        if os.path.exists(local_zip):
            os.remove(local_zip)
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        logging.info("Limpeza de arquivos temporários concluída.")

def generate_mock_sales_data(source_csv_file_folder: str, base_data_file: str, inflacao_file: str, output_file: str):
    """
    Gera dados de vendas fictícios com base em dados de inflação e um arquivo de dados base.

    Args:
        source_csv_file_folder (str): O caminho para a pasta que contém os arquivo csv com dados base.
        base_data_file (str): O nome do arquivo CSV com os dados de maio de 2023.
        inflacao_file (str): O nome do arquivo CSV com os dados de inflação.
        output_file (str): O nome do arquivo de saída para os dados fictícios.
    """
    # Mapeamento dos meses de português para números
    meses_map = {
        'JAN': '01', 'FEV': '02', 'MAR': '03', 'ABR': '04', 'MAI': '05', 'JUN': '06',
        'JUL': '07', 'AGO': '08', 'SET': '09', 'OUT': '10', 'NOV': '11', 'DEZ': '12'
    }
    
    inflacao_file_path = os.path.join(source_csv_file_folder, inflacao_file)
    sales_base_path = os.path.join(source_csv_file_folder, base_data_file)
    output_path = os.path.join(os.path.dirname(sales_base_path), output_file)

    try:
        df_inflacao = pd.read_csv(inflacao_file_path)
        logger.info(f"Arquivo de inflação '{inflacao_file_path}' carregado.")
    except FileNotFoundError:
        logger.error(f"Erro: Arquivo de inflação '{inflacao_file_path}' não encontrado.")
        return
    except Exception as e:
        logger.error(f"Erro ao carregar o arquivo de inflação: {e}")
        return

    try:
        df_sales_maio_2023 = pd.read_csv(sales_base_path)
        logger.info(f"Arquivo de dados base '{sales_base_path}' carregado.")
    except FileNotFoundError:
        logger.error(f"Erro: Arquivo de dados base '{sales_base_path}' não encontrado.")
        return
    except Exception as e:
        logger.error(f"Erro ao carregar o arquivo de dados base: {e}")
        return
        
    # Prepara a tabela de inflação
    # Converte a coluna 'MES' para maiúsculas e usa o .map() para traduzir
    df_inflacao['MES_NUM'] = df_inflacao['MES'].str.upper().map(meses_map)

    # Cria a coluna 'DATA' usando a nova coluna numérica
    df_inflacao['DATA'] = pd.to_datetime(
        df_inflacao['ANO'].astype(str) + '-' + df_inflacao['MES_NUM'].astype(str) + '-01'
    )
    
    df_inflacao = df_inflacao.sort_values(by='DATA', ascending=True)

    # Renomear a coluna para facilitar o processamento
    df_sales_maio_2023 = df_sales_maio_2023.rename(columns={'compra_maio_2023_(kg)': 'venda_base'})

    df_ficticio = pd.DataFrame()
    
    for index, row in df_inflacao.iterrows():
        temp_df = df_sales_maio_2023.copy()

        # Verificar se é o mês de MAI de 2023 para tratar o caso base
        if row['ANO'] == 2023 and row['MES'].upper() == 'MAI':
            # Atribui diretamente os valores de 'venda_base' para o mês base
            temp_df['volume_vendas_(kg)'] = df_sales_maio_2023['venda_base']
        else:
            # Para os outros meses, aplica a lógica de inflação
            inflacao_acumulada = (1 + df_inflacao[df_inflacao['DATA'] <= row['DATA']]['INFLACAO_NO_MES'] / 100).product()
            temp_df['volume_vendas_(kg)'] = df_sales_maio_2023['venda_base'] / inflacao_acumulada

        # Arredonda os valores para baixo e converte para inteiro
        temp_df['volume_vendas_(kg)'] = temp_df['volume_vendas_(kg)'].astype(int)

        temp_df['ano'] = row['ANO']
        temp_df['mes'] = row['MES']
        
        df_ficticio = pd.concat([df_ficticio, temp_df], ignore_index=True)

    # Reorganiza as colunas na nova ordem
    new_column_order = [
        'ano',
        'mes',
        'uf',
        'id',
        'pet_shop',
        'volume_vendas_(kg)'
    ]

    # Cria o DataFrame final com a nova ordem das colunas
    final_df = df_ficticio[new_column_order]

    # Salva os dados fictícios em um arquivo CSV
    try:
        final_df.to_csv(output_path, index=False)
        logger.info(f"Arquivo '{output_file}' gerado com sucesso em '{output_path}'!")
    except Exception as e:
        logger.error(f"Erro ao salvar o arquivo '{output_file}': {e}")
        raise OSError(f"Não foi possível salvar o arquivo '{output_file}'.")

