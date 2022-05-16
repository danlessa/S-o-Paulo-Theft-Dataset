# %%
"""

"""
# %%
import pandas as pd
from tqdm.auto import tqdm
import sys
import csv
%load_ext autotime
# %%
RAW_DATA_PATH = '../data/raw/SIC 1751223794-003.csv'
# %% [markdown]

# ## Initial checks
# ### Checking number of columns
# There are some rows with invalid number of columns (the expected is 43). Given the dataframe size,
# it is important to make sure that we understand exactly why before normalizing

# %%
csv.field_size_limit(sys.maxsize)

N_rows = 17_563_443  # according to wc -l &  the following lines
# N_rows = None
if N_rows is None:
    with open(RAW_DATA_PATH, 'r') as csv_file:
        N_rows = sum(1 for line in csv_file)
        print(N_rows)

HEAD_ROWS = N_rows
HEAD_ROWS = 1_000_000

with open(RAW_DATA_PATH, 'r') as csv_file:
    reader = csv.reader(csv_file,
                        delimiter=';',
                        quoting=csv.QUOTE_NONE
                        )
    i_row = 0
    for row in tqdm(reader, total=N_rows):
        if i_row >= HEAD_ROWS:
            break
        else:
            i_row += 1
            if len(row) != 43:
                print(f'{i_row}: {len(row)}')


# %% [markdown]

# #### Results:
#
# - Rows with 65 cols: `{609792, 616425, 1927626, 1941976, 3959232, 4108518, 4376309, 4509411,
# 6786690, 10192794, 10557909, 16105658}`
# - Rows with less than 65 cols: `{8700988 (32), 10288055 (36), 10562948 (47),
# 11876539 (36), 14915607 (36), 14929051 (36)}`

# ### Investigating the individual inconsistent rows
# - It seems that the rows with 65 cols have some missing newlines.

# %%

ROW_INDEX = 609792

with open(RAW_DATA_PATH,
          'r',
          newline='') as csv_file:
    reader = csv.reader(csv_file,
                        delimiter=';',
                        quoting=csv.QUOTE_NONE)
    i_row = 0
    for row in tqdm(reader, total=N_rows):
        if i_row >= ROW_INDEX:
            break
        else:
            i_row += 1
            if i_row == ROW_INDEX:
                print(f'{i_row}: {len(row)}')
                print(row)
            else:
                continue
# %%
csv.list_dialects()
# %%
# %%
dtypes = {
    'ID_DELEGACIA': int,
    'NOME_DEPARTAMENTO': 'category',
    'NOME_SECCIONAL': 'category',
    'NOME_DELEGACIA': 'category',
    'CIDADE': 'category',
    'ANO_BO': int,
    'NUM_BO': int,
    'DESCR_SOLUCAO': 'category',
    'DESCR_PROVIDENCIA': 'category',
    'NOME_DEPARTAMENTO_CIRC': 'category',
    'NOME_SECCIONAL_CIRC': 'category',
    'NOME_DELEGACIA_CIRC': 'category',
    'NOME_MUNICIPIO_CIRC': 'category',
    'DESCR_TIPO_BO': 'category',
    'DATA_OCORRENCIA_BO': str,
    'HORA_OCORRENCIA_BO': str,
    'DESCRICAO_APRESENTACAO': 'category',
    'DATAHORA_REGISTRO_BO': str,
    'DATA_COMUNICACAO_BO': str,
    'DATAHORA_IMPRESSAO_BO': str,
    'DESCR_PERIODO': 'category',
    'AUTORIA_BO': 'category',
    'FLAG_INTOLERANCIA': 'category',
    'TIPO_INTOLERANCIA': 'category',
    'FLAG_FLAGRANTE': 'category',
    'FLAG_STATUS': 'category',
    'RUBRICA': 'category',
    'DESCR_CONDUTA': 'category',
    'DESDOBRAMENTO': 'category',
    'BAIRRO': 'category',
    'DESCR_TIPOLOCAL': 'category',
    'DESCR_SUBTIPOLOCAL': 'category',
    'CEP': str,
    'LOGRADOURO': str,
    'NUMERO_LOGRADOURO': 'Int64',
    'LATITUDE': str,
    'LONGITUDE': str,
    'CONT_OBJETO': 'Int64',
    'DESCR_MODO_OBJETO': 'category',
    'DESCR_TIPO_OBJETO': 'category',
    'DESCR_SUBTIPO_OBJETO': 'category',
    'DESCR_UNIDADE': 'category',
    'QUANTIDADE_OBJETO': float

}
# %%

N_ROWS = 1_000_000
df = pd.read_csv(RAW_DATA_PATH,
                 sep=';',
                 #nrows=N_ROWS,
                 dtype=dtypes,
                 quoting=csv.QUOTE_NONE)
df.sample(1).T
# %%
i_col = 23
print(f'{df.columns[i_col]} {df.dtypes[i_col]}')
# %%
df.nunique().sort_values()
# %%
df.to_feather('../data/bulletins.feather')
# %%
df.to_parquet('../data/bulletins.parquet')
# %%
# %%
