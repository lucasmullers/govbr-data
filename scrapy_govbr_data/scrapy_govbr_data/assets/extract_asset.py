from loguru import logger
from dagster import asset
from scrapy_govbr_data.utils.extract_tse import ExtractTSEData


@asset(group_name="scrapy_tse_data")
def extract_candidatos() -> None:
    """
    Function to extract candidatos data from TSE website and upload it to S3.
    """
    starting_year = 2022
    # end_date = datetime.now().year + 1
    ending_year = 2024

    try:
        ExtractTSEData(
            start_year=starting_year, end_year=ending_year, file_name="consulta_cand", s3_folder="CANDIDATOS"
        ).download_and_upload_to_s3()
    except Exception as e:
        logger.error(f"Não foi possível coletar o arquivo: {e}")


@asset(group_name="scrapy_tse_data")
def extract_bens_candidatos() -> None:
    """
    Function to extract candidatos data from TSE website and upload it to S3.
    """
    starting_year = 2022
    # end_date = datetime.now().year + 1
    ending_year = 2024

    try:
        ExtractTSEData(
            start_year=starting_year, end_year=ending_year, file_name="bem_candidato", s3_folder="BENS_CANDIDATOS"
        ).download_and_upload_to_s3()
    except Exception as e:
        logger.error(f"Não foi possível coletar o arquivo: {e}")


@asset(group_name="scrapy_tse_data")
def extract_votacao_secao() -> None:
    """
    Function to extract candidatos data from TSE website and upload it to S3.
    """
    starting_year = 2022
    # end_date = datetime.now().year + 1
    ending_year = 2024

    try:
        ExtractTSEData(
            start_year=starting_year, end_year=ending_year, file_name="votacao_secao", s3_folder="VOTACAO_POR_SECAO"
        ).download_and_upload_to_s3(uf="MG")
    except Exception as e:
        logger.error(f"Não foi possível coletar o arquivo: {e}")


@asset(group_name="scrapy_tse_data")
def extract_vagas() -> None:
    """
    Function to extract vagas data from TSE website and upload it to S3.
    """
    starting_year = 2022
    # end_date = datetime.now().year + 1
    ending_year = 2024

    try:
        ExtractTSEData(
            start_year=starting_year, end_year=ending_year, file_name="consulta_vagas", s3_folder="VAGAS"
        ).download_and_upload_to_s3()
    except Exception as e:
        logger.error(f"Não foi possível coletar o arquivo: {e}")


@asset(group_name="scrapy_tse_data")
def extract_motivo_cassacao() -> None:
    """
    Function to extract motivo da cassação data from TSE website and upload it to S3.
    """
    starting_year = 2022
    # end_date = datetime.now().year + 1
    ending_year = 2024

    try:
        ExtractTSEData(
            start_year=starting_year, end_year=ending_year, file_name="motivo_cassacao", s3_folder="MOTIVO_DA_CASSACAO"
        ).download_and_upload_to_s3()
    except Exception as e:
        logger.error(f"Não foi possível coletar o arquivo: {e}")
