from loguru import logger

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


class ExtractTSEData:
    def __init__(self, start_year: int, end_year: int, file_name: str, s3_folder: str) -> None:
        self._start_year = start_year
        self._end_year = end_year
        self._file_name = file_name

        self._base_url = "https://cdn.tse.jus.br/estatistica/sead/odsele/"
        self._file_name = file_name
        self._s3_folder = s3_folder

    def _download_file_and_unzip(self, year: int, uf=None) -> None:
        from urllib import request
        from zipfile import ZipFile
        from os import remove, path

        logger.info("Downloading database from {} election.".format(year))

        if uf is None:
            url = f"{self._base_url}{self._file_name}/{self._file_name}_{year}.zip"
            zip_file = f"{self._file_name}_{year}.zip"
        else:
            url = f"{self._base_url}{self._file_name}/{self._file_name}_{year}_{uf}.zip"
            zip_file = f"{self._file_name}_{year}_{uf}.zip"

        logger.info(f"Downloading {url} to {zip_file}.")

        resp = request.urlretrieve(url, zip_file)

        logger.info(f"Response received: {resp}")

        with ZipFile(f"./{zip_file}", "r") as zip_ref:
            zip_ref.extractall(f"./{self._file_name}_{year}")

        if path.exists(zip_file):
            logger.info(f"Deleting zip file: {zip_file}")
            remove(zip_file)

    def _convert_encoding(self, year: int, uf=None) -> None:
        import dask.dataframe as dd

        ufs_list = UFS if uf is None else [uf]

        for uf in ufs_list:
            try:
                logger.info(f"Converting encoding from file: {self._file_name}_{year}_{uf}.csv")
                df = dd.read_csv(
                    f"./{self._file_name}_{year}/{self._file_name}_{year}_{uf}.csv",
                    engine="python",
                    encoding="latin-1",
                    assume_missing=True,
                    sep=";",
                )
                df.to_csv(
                    f"./{self._file_name}_{year}/{self._file_name}_{year}_{uf}.csv",
                    encoding="utf-8",
                    sep=";",
                    single_file=True,
                    index=False,
                )
            except Exception as e:
                logger.error(f"Error converting encoding: {e}")

    def _upload_files_to_s3(self, year: int, uf=None) -> None:
        from os import path
        from shutil import rmtree
        from scrapy_govbr_data.utils.s3 import S3

        s3 = S3()

        ufs_list = UFS if uf is None else [uf]

        for uf in ufs_list:
            try:
                logger.info(f"Uploading file '{self._file_name}_{year}_{uf}.csv'to S3.")
                s3.upload_file(
                    file_path=f"./{self._file_name}_{year}/{self._file_name}_{year}_{uf}.csv",
                    bucket_name="govbr-data",
                    object_name=f"LANDING/TSE/{self._s3_folder}/{self._file_name}_{year}_{uf}.csv",
                )
            except Exception as e:
                logger.error(f"Error uploading file to S3: {e}")

        if path.exists(f"./{self._file_name}_{year}"):
            logger.info(f"Deleting folder: {self._file_name}_{year}")
            rmtree(f"./{self._file_name}_{year}")

    def download_and_upload_to_s3(self, step: int = 2, uf=None) -> None:
        for year in range(self._start_year, self._end_year, step):
            self._download_file_and_unzip(year, uf)
            self._convert_encoding(year, uf)
            self._upload_files_to_s3(year, uf)
