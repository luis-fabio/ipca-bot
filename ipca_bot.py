import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

def baixar_dados_ipca(url: str) -> dict:
    """
    Baixa os dados do IPCA da URL fornecida no formato JSON.
    Parâmetro: url (str): URL da fonte de dados
    Retorna: dict com os dados em JSON
    """
    response = requests.get(url)
    response.raise_for_status()  # Lança erro se a requisição falhar
    return response.json()

def json_para_dataframe(json_data: dict) -> pd.DataFrame:
    """
    Converte o JSON recebido do IBGE para um DataFrame estruturado.
    Parâmetro: json_data (dict): Dados em formato JSON
    Retorna: pd.DataFrame com os dados tabulares
    """
    # Os dados estão em json_data['Valores']
    # Cada registro é um dicionário com chaves de identificação
    registros = []
    valores = json_data.get('Valores', {})
    for k, v in valores.items():
        registro = v.copy()
        registro['Chave'] = k
        registros.append(registro)
    df = pd.DataFrame(registros)
    return df

def salvar_em_parquet(df: pd.DataFrame, nome_arquivo: str):
    """
    Salva o DataFrame no formato Parquet.
    Parâmetro: df (pd.DataFrame): DataFrame a ser salvo
    Parâmetro: nome_arquivo (str): Caminho do arquivo de saída
    """
    # Converte para tabela Arrow
    tabela = pa.Table.from_pandas(df)
    pq.write_table(tabela, nome_arquivo)

def main():
    # URL dos dados do IPCA
    url = "https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1"
    
    print("Baixando dados do IPCA...")
    dados_json = baixar_dados_ipca(url)
    
    print("Convertendo dados para DataFrame...")
    df_ipca = json_para_dataframe(dados_json)
    
    arquivo_saida = "ipca.parquet"
    print(f"Salvando dados no arquivo {arquivo_saida}...")
    salvar_em_parquet(df_ipca, arquivo_saida)
    
    print("Processo concluído com sucesso!")

if __name__ == "__main__":
    main()