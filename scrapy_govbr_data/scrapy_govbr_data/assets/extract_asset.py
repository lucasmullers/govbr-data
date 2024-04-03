import zipfile

import pandas as pd
from shutil import rmtree
from os import remove, path
from loguru import logger
from dagster import asset
from urllib import request
from datetime import datetime

from scrapy_govbr_data.utils.s3 import S3


UFS = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    # "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]


@asset(group_name="scrapy_tse_data")
def extract_candidatos() -> None:
    """
    Function to extract candidatos data from TSE website and upload it to S3.
    """
    start_date = 2000
    end_date = datetime.now().year + 1
    s3 = S3()

    for year in range(start_date, end_date, 2):
        url = f"https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_{year}.zip"
        file_name = f"consulta_cand_{year}"
        zip_file = f"{file_name}.zip"

        logger.info("Downloading candidates from {} election.".format(year))

        try:
            resp = request.urlretrieve(url, zip_file)

            logger.info(f"Response received: {resp}")

            with zipfile.ZipFile(f"./{zip_file}", "r") as zip_ref:
                zip_ref.extractall(f"./{file_name}")

            if path.exists(zip_file):
                logger.info(f"Deleting zip file: {zip_file}")
                remove(zip_file)

            for uf in UFS:
                logger.info(f"Converting encoding from file: {file_name}_{uf}.csv")
                df = pd.read_csv(
                    f"./{file_name}/{file_name}_{uf}.csv", encoding="latin-1", sep=";"
                )
                df.to_csv(
                    f"./{file_name}/{file_name}_{uf}.csv",
                    encoding="utf-8",
                    sep=";",
                    index=False,
                )

                logger.info(f"Uploading file '{file_name}_{uf}.csv'to S3.")
                s3.upload_file(
                    file_path=f"./{file_name}/{file_name}_{uf}.csv",
                    bucket_name="govbr-data",
                    object_name=f"LANDING/TSE/CANDIDATOS/{file_name}_{uf}.csv",
                )

            if path.exists(f"./{file_name}"):
                logger.info(f"Deleting folder: {file_name}")
                rmtree(f"./{file_name}")
        except Exception as e:
            print(f"Não foi possível coletar o arquivo: {e}")


@asset(group_name="scrapy_tse_data")
def extract_bens_candidatos() -> None:
    """
    Function to extract candidatos data from TSE website and upload it to S3.
    """
    start_date = 2000
    end_date = datetime.now().year + 1
    s3 = S3()

    for year in range(start_date, end_date, 2):
        url = f"https://cdn.tse.jus.br/estatistica/sead/odsele/bem_candidato/bem_candidato_{year}.zip"
        file_name = f"bem_candidato_{year}"
        zip_file = f"{file_name}.zip"

        logger.info("Downloading candidates from {} election.".format(year))

        try:
            resp = request.urlretrieve(url, zip_file)

            logger.info(f"Response received: {resp}")

            with zipfile.ZipFile(f"./{zip_file}", "r") as zip_ref:
                zip_ref.extractall(f"./{file_name}")

            if path.exists(zip_file):
                logger.info(f"Deleting zip file: {zip_file}")
                remove(zip_file)

            for uf in UFS:
                logger.info(f"Converting encoding from file: {file_name}_{uf}.csv")
                df = pd.read_csv(
                    f"./{file_name}/{file_name}_{uf}.csv", encoding="latin-1", sep=";"
                )
                df.to_csv(
                    f"./{file_name}/{file_name}_{uf}.csv",
                    encoding="utf-8",
                    sep=";",
                    index=False,
                )

                logger.info(f"Uploading file '{file_name}_{uf}.csv'to S3.")
                s3.upload_file(
                    file_path=f"./{file_name}/{file_name}_{uf}.csv",
                    bucket_name="govbr-data",
                    object_name=f"LANDING/TSE/BENS_CANDIDATOS/{file_name}_{uf}.csv",
                )

            if path.exists(f"./{file_name}"):
                logger.info(f"Deleting folder: {file_name}")
                rmtree(f"./{file_name}")
        except Exception as e:
            print(f"Não foi possível coletar o arquivo: {e}")