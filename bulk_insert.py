import re
from os import listdir, sep
from os.path import isfile, join
import os
from tkinter.ttk import Separator
from typing import Iterator
import pandas as pd
import sqlalchemy as sal
from sqlalchemy.engine import URL
from sqlalchemy.types import NVARCHAR
import numpy as np
import sys
import math
import csv
import time

file = "../Chronos Database/rawdata/compras.ComprasSemLicitacao.csv"
encoding = "mbcs"  # "utf-8", " cp1252", "mbcs", "raw_unicode_escape", "utf_16_le"
fileEngine = 'python'  # 'c', 'python'
# delimiter = '\<\|\>'
# delimiter = r'~'
delimiter = '\¢\~\¢'
# delimiter = '|'
chunkSize = 500000
quoteChar = None
# quoteChar = r'¢'  # None if there is no quoting in the file | ¢

# %% DB Connection

connection_string = (
    r"Driver=ODBC Driver 17 for SQL Server;"
    r"Server=.;"
    r"Database=eydna-bis-chronos-preprod;"
    r"Trusted_Connection=yes;"
)

connection_url = URL.create(
    "mssql+pyodbc", query={"odbc_connect": connection_string})

engine = sal.create_engine(connection_url, fast_executemany=True)
conn = engine.connect()

# %% File Retrieval


def get_df():
    df = pd.read_csv(
        file,
        sep=delimiter,
        chunksize=chunkSize,
        encoding=encoding,
        dtype=str,
        header=0,
        quoting=csv.QUOTE_ALL if quoteChar is not None else csv.QUOTE_NONE,
        quotechar=quoteChar,  # qualificador
        engine=fileEngine,
        on_bad_lines="error",
    )

    return df

# strip accents/remove double spaces and convert number cols to int


def normalize_df(df):
    print('normalizing data...')

    start = time.time()
    cols = df.select_dtypes(include=[object]).columns
    searchFor = ['target', 'name', 'raz', 'social',
                 'doador', 'nome', 'razao']

    notSearchFor = ['link', 'url', 'projeto']

    cols = [col for col in cols if any(substring in col.lower() for substring in searchFor) and not any(
        substring in col.lower() for substring in notSearchFor)]

    if(len(cols) > 0):
        df[cols] = df[cols].apply(
            lambda x: x
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('\s{2,}', ' ', regex=True)
            .str.upper()
        )

    end = time.time()
    print(f'normalization took {getTime(start,end)}')

    return df


def getTime(start, end):
    return time.strftime('%H:%M:%S', time.gmtime(end - start))


# onlyfiles = [f for f in listdir("./") if isfile(join("./", f)) and ".csv" in f]
# print(onlyfiles)

# for file in onlyfiles:


errorsFile = f"{file}_errors.csv"
auxErrorsFile = f'{file}_errors_aux.csv'
name = os.path.basename(file).split(".")[1]
schema = os.path.basename(file).split(".")[0]

print("===========Starting Import============")
print(f"File: {file}")
print(f"Schema: {schema}")
print(f"Table: {name}")

importedRowCount = 0
totalImportTime = 0.0
importStartTime = time.time()
with open(auxErrorsFile, 'w') as fp:
    sys.stderr = fp

# %% File Import
    print('Reading file...')

    df = get_df()
    add = 0
    i = 0
    number_of_chunks = math.ceil(sum(1 for row in open(file, 'r', encoding=encoding))/chunkSize)

    for chunk in df:
        print(f'-----------------------------------------------')
        print(f'Importing chunk {i+1} of {number_of_chunks}')

        chunk = normalize_df(chunk)

        if i == 0:
            print(chunk.head())

        colCount = [header for header in chunk]

        add = chunk.shape[0]
        importedRowCount = importedRowCount + add
        i = i + 1

        start = time.time()
        chunk.to_sql(
            name,
            conn,
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=chunkSize,
            dtype={col_name: NVARCHAR for col_name in chunk},
        )
        end = time.time()
        print(f'Chunk import took {getTime(start,end)}\n')

# %% Error Reporting

with open(auxErrorsFile, 'r') as fp:

    output = fp.read()
    fp.seek(0)

    skipped_lines = re.findall(r"[0-9]+:", output)
    skipped_lines = [x.replace(':', '') for x in skipped_lines]

    print("\n\n=================END======================")
    print(f"[{schema}].[{name}] imported with {importedRowCount - len(skipped_lines)} rows in {getTime(importStartTime, time.time())}")

    if(len(skipped_lines) > 0):
        print("=================ERRORS======================")
        print("Rows skipped: ")
        print(skipped_lines)
        skipped_lines = [int(x) - 1 for x in skipped_lines]

skipped_lines_data = []

with open(file, 'r', encoding=encoding) as fp:
    for i, line in enumerate(fp):
        if i == 0 or i in skipped_lines:
            skipped_lines_data.append(line)

with open(errorsFile, 'w', encoding=encoding) as fp:
    fp.writelines(skipped_lines_data)

# os.remove(auxErrorsFile)
