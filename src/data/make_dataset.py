
import os
import pandas as pd
import glob
from joblib import Parallel, delayed
from joblib import parallel_backend
from tqdm import tqdm
from sklearn.preprocessing import OneHotEncoder
import warnings
# import dask.dataframe as dd
# from dask.distributed import Client
# from dask.diagnostics import ProgressBar

class InsuranceDataProcessor:
    """
    Processador de dados de seguros para limpeza, transformação e agregação de dados.
    
    Realiza um pipeline completo de processamento de dados de seguros, incluindo:
    - Combinação de múltiplos arquivos CSV
    - Limpeza e filtragem de dados
    - Codificação de variáveis categóricas
    - Agregação temporal dos dados
    
    Parâmetros:
    ----------
    input_files : str ou list
        Caminho para arquivo(s) CSV ou diretório contendo os arquivos
    combined_path : str, opcional
        Caminho para salvar os dados brutos combinados (padrão: 'combined_data.csv')
    cleaned_path : str, opcional
        Caminho para salvar os dados limpos (padrão: 'cleaned_data.csv')
    final_path : str, opcional
        Caminho para salvar os dados processados finais (padrão: 'final_data.csv')
    n_jobs : int, opcional
        Número de jobs paralelos para processamento (padrão: -1, usa todos os cores)
    
    Métodos:
    -------
    combine_files(sep=';')
        Combina múltiplos arquivos CSV em um único DataFrame
    clean_data(df, sep=';')
        Realiza limpeza dos dados (remove duplicatas, filtra valores válidos, etc.)
    process_data(df, sep=';')
        Processa os dados com one-hot encoding e agregação temporal
    run()
        Executa o pipeline completo (combine → clean → process)
    
    Exemplo:
    -------
    processor = InsuranceDataProcessor('dados/raw/')
    resultado = processor.run()
    """
    def __init__(self, input_files, combined_path=None, cleaned_path=None, final_path=None,
                 n_jobs=-1, inner_max_num_threads=2):
        """
        Inicializa o processador de dados de seguros.
        
        Args:
            input_files (str/list): Caminho para arquivo(s) CSV ou diretório
            combined_path (str, optional): Onde salvar os dados brutos combinados
            cleaned_path (str, optional): Onde salvar os dados limpos
            final_path (str, optional): Onde salvar os dados processados finais
            n_jobs (int, optional): Número de jobs paralelos (padrão: -1)
        """
        # Configura paths
        if isinstance(input_files, str):
            if os.path.isdir(input_files):
                self.input_files = glob.glob(os.path.join(input_files, '*.csv'))
            else:
                self.input_files = [input_files]
        else:
            self.input_files = input_files

        self.state_to_code = {
            'RO': 11, 'AC': 12, 'AM': 13, 'RR': 14, 'PA': 15, 'AP': 16, 'TO': 17, 'MA': 21, 
            'PI': 22, 'CE': 23, 'RN': 24, 'PB': 25, 'PE': 26, 'AL': 27, 'SE': 28, 'BA': 29,
            'MG': 31, 'ES': 32, 'RJ': 33, 'SP': 35, 'PR': 41, 'SC': 42, 'RS': 43, 'MS': 50,
            'MT': 51, 'GO': 52, 'DF': 53
        }
        self.valid_state_codes = list(self.state_to_code.values())

        self.combined_path = combined_path or 'combined_data.csv'
        self.cleaned_path = cleaned_path or 'cleaned_data.csv'
        self.final_path = final_path or 'final_data.csv'
        self.n_jobs = n_jobs
        self.inner_max_num_threads = inner_max_num_threads

    # def combine_files(self, sep=';'):
    #     """
    #     Combina múltiplos arquivos CSV em um único DataFrame.
        
    #     Args:
    #         sep (str): Separador de campos (padrão: ';')
            
    #     Returns:
    #         pd.DataFrame: DataFrame combinado
    #     """
    #     if os.path.exists(self.combined_path):
    #         print(f"Usando arquivo existente: {self.combined_path}")
    #         return pd.read_csv(self.combined_path, sep=sep)

    #     print(f"Combinando {len(self.input_files)} arquivos...")
    #     dfs = Parallel(n_jobs=self.n_jobs, verbose=10)(
    #         delayed(pd.read_csv)(f, sep=sep) for f in self.input_files
    #     )
    #     combined = pd.concat(dfs, ignore_index=True)
    #     combined.to_csv(self.combined_path, index=False, sep=sep)
    #     return combined


    def combine_files(self, sep=';'):
        """
        Combina múltiplos arquivos CSV em um único DataFrame usando paralelismo controlado.
        
        Args:
            sep (str): Separador de campos (padrão: ';')
            
        Returns:
            pd.DataFrame: DataFrame combinado
        
        Otimizado com:
        - Loky backend para melhor gerenciamento de processos
        - Controle de threads internos para evitar sobrecarga
        - Gerenciamento automático de recursos com context manager
        """
        if os.path.exists(self.combined_path):
            print(f"Usando arquivo existente: {self.combined_path}")
            return pd.read_csv(self.combined_path, sep=sep)

        print(f"Combinando {len(self.input_files)} arquivos com {self.n_jobs} workers...")
        
        with parallel_backend('loky', inner_max_num_threads=self.inner_max_num_threads):
            dfs = Parallel(n_jobs=self.n_jobs, verbose=10)(
                delayed(pd.read_csv)(f, sep=sep) for f in self.input_files
            )
        
        combined = pd.concat(dfs, ignore_index=True)
        combined.to_csv(self.combined_path, index=False, sep=sep)
        print("Combinação concluída e salva em:", self.combined_path)
        return combined

    # def combine_files(self, sep=';'):
    #     """
    #     Combina arquivos usando Dask com controle de paralelismo
        
    #     Args:
    #         sep (str): Separador de campos
            
    #     Returns:
    #         pd.DataFrame: DataFrame combinado
    #     """
    #     if os.path.exists(self.combined_path):
    #         print(f"Usando arquivo existente: {self.combined_path}")
    #         return pd.read_csv(self.combined_path, sep=sep)
        
    #     print(f"Combinando {len(self.input_files)} arquivos com Dask...")
        
    #     # Leitura paralela com inferência de tipos
    #     ddf = dd.read_csv(
    #         self.input_files,
    #         sep=sep,
    #         assume_missing=True  # Melhor para inferência de tipos
    #     )
        
    #     # Computação paralela com controle de threads
    #     with warnings.catch_warnings():
    #         warnings.filterwarnings('ignore', category=UserWarning)
    #         with ProgressBar():
    #             combined = ddf.compute(scheduler='threads', num_workers=self.n_jobs)
        
    #     combined.to_csv(self.combined_path, index=False, sep=sep)
    #     print(f"Arquivo combinado salvo em: {self.combined_path}")
    #     return combined

    def clean_data(self, df, sep=';'):
        """
        Realiza limpeza dos dados de seguros.
        
        Operações:
        - Remove duplicatas
        - Seleciona colunas relevantes
        - Filtra sexos válidos (M/F)
        - Padroniza códigos de região
        - Filtra valores válidos para CAUSA (1-8) e EVENTO (1-9)
        
        Args:
            df (pd.DataFrame): DataFrame com dados brutos
            sep (str): Separador de campos (padrão: ';')
            
        Returns:
            pd.DataFrame: DataFrame limpo
        """
        if os.path.exists(self.cleaned_path):
            print(f"Usando dados limpos existentes: {self.cleaned_path}")
            return pd.read_csv(self.cleaned_path, sep=sep)

        print("Limpando dados...")
        
        # 1. Remover duplicatas
        print(f"Número de linhas antes de remover duplicatas: {df.shape[0]}")
        cleaned = df.drop_duplicates()
        print(f"Número de linhas após remover duplicatas: {cleaned.shape[0]}")
        
        # 2. Selecionar colunas relevantes
        cleaned = cleaned[['D_OCORR', 'INDENIZ', 'VAL_SALVAD', 'VAL_RESS', 
                        'EVENTO', 'CAUSA', 'MODALIDADE', 'TIPO_PROD', 
                        'REGIAO', 'SEXO']].copy()
        
        # 3. Filtrar sexos válidos
        cleaned = cleaned[cleaned['SEXO'].isin(['M', 'F'])]
        
        # 4. Ajustar regiões
        cleaned['REGIAO'] = cleaned['REGIAO'].astype(str).str.strip()
        cleaned.loc[:, 'REGIAO'] = cleaned['REGIAO'].replace(self.state_to_code)
        cleaned.loc[:, 'REGIAO'] = pd.to_numeric(cleaned['REGIAO'], errors='coerce')
        cleaned = cleaned[cleaned['REGIAO'].isin(self.valid_state_codes)]
        
        # 5. Filtrar CAUSA e EVENTO
        cleaned['CAUSA'] = cleaned['CAUSA'].astype(str).str.strip()
        cleaned['EVENTO'] = cleaned['EVENTO'].astype(str).str.strip()
        cleaned = cleaned[
            (cleaned['CAUSA'].isin([str(i) for i in range(1, 9)])) & 
            (cleaned['EVENTO'].isin([str(i) for i in range(1, 10)]))
        ]
        
        # 6. Salvar dados limpos
        cleaned.to_csv(self.cleaned_path, index=False, sep=sep)
        print(f"Dados limpos salvos em: {self.cleaned_path}")
        
        return cleaned

    def process_data(self, df, sep=';'):
        """
        Processa os dados com one-hot encoding e agregação temporal.
        
        Operações:
        - Aplica one-hot encoding nas variáveis categóricas
        - Converte valores para absolutos
        - Agrega dados por data (D_OCORR)
        - Formata datas corretamente
        
        Args:
            df (pd.DataFrame): DataFrame com dados limpos
            sep (str): Separador de campos (padrão: ';')
            
        Returns:
            pd.DataFrame: DataFrame processado final
        """
        if os.path.exists(self.final_path):
            print(f"Usando dados processados existentes: {self.final_path}")
            return pd.read_csv(self.final_path, sep=sep)

        print("Processando dados...")
        
        # 1. One-Hot Encoding para variáveis categóricas
        categorical_cols = ['EVENTO', 'CAUSA', 'MODALIDADE', 'TIPO_PROD', 'REGIAO', 'SEXO']
        df_encoded = df.copy()
        
        # Converter colunas categóricas para string
        df_encoded[categorical_cols] = df_encoded[categorical_cols].astype(str)
        df_encoded.set_index('D_OCORR', inplace=True)
        
        # Aplicar one-hot encoding
        onehot_encoder = OneHotEncoder(drop=None, sparse_output=False)
        encoded_data = onehot_encoder.fit_transform(df_encoded[categorical_cols])
        encoded_df = pd.DataFrame(
            encoded_data,
            columns=onehot_encoder.get_feature_names_out(categorical_cols),
            index=df_encoded.index
        )
        
        # Combinar com o DataFrame original
        processed = pd.concat([df_encoded, encoded_df], axis=1)
        processed.drop(columns=categorical_cols, inplace=True)
        processed.reset_index(inplace=True)
        
        # 2. Garantir valores absolutos
        processed['INDENIZ'] = processed['INDENIZ'].abs()
        processed['VAL_SALVAD'] = processed['VAL_SALVAD'].abs()
        processed['VAL_RESS'] = processed['VAL_RESS'].abs()
        
        # 3. Agregação por data
        encoded_columns = [col for col in processed.columns 
                        if any(x in col for x in ['EVENTO_', 'CAUSA_', 'MODALIDADE_', 
                                                'TIPO_PROD_', 'REGIAO_', 'SEXO_'])]
        
        aggregations = {
            'INDENIZ': 'sum',
            **{col: lambda x: (x * processed.loc[x.index, 'INDENIZ']).sum() / 
            processed.loc[x.index, 'INDENIZ'].sum() 
            for col in encoded_columns},
            'VAL_SALVAD': 'sum',
            'VAL_RESS': 'sum'
        }
        
        processed = processed.groupby('D_OCORR').agg(aggregations).reset_index()
        
        # 4. Filtrar e formatar datas
        processed = processed[processed['D_OCORR'].astype(str).str.match(r'^\d{8}$')]
        processed['D_OCORR'] = pd.to_datetime(processed['D_OCORR'], format='%Y%m%d')
        
        # 5. Salvar dados processados
        processed.to_csv(self.final_path, index=False, sep=sep)
        print(f"Dados processados salvos em: {self.final_path}")
        
        return processed

    def run(self):
        """
        Executa o pipeline completo de processamento.
        
        Etapas:
        1. combine_files(): Combina arquivos de entrada
        2. clean_data(): Limpa os dados combinados
        3. process_data(): Processa os dados limpos
        
        Returns:
            pd.DataFrame: Dados finais processados
        """
        combined = self.combine_files()
        cleaned = self.clean_data(combined)
        final = self.process_data(cleaned)
        return final


# Exemplo de uso SIMPLES:


# home_path = os.path.expanduser('~')
# path_project = os.path.join(home_path,'Meu Drive (pesquisaursulino@gmail.com)','Colab Notebooks','IBA_PureRiskConformal')
# path_raw = os.path.join(path_project,'data','raw')
# path_processed = os.path.join(path_project,'data','processed')

# processor = InsuranceDataProcessor(
#     input_files=os.path.join(path_raw,'susep'),  # Ou lista de arquivos
#     combined_path=os.path.join(path_raw,'combined_data.csv'),
#     cleaned_path=os.path.join(path_processed,'cleaned_data.csv'),
#     final_path=os.path.join(path_processed,'trated_data.csv'),
#     n_jobs=10
# )

# resultado = processor.run()
