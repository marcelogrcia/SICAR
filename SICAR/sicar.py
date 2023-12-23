"""
SICAR Class Module.

This module defines a class representing the Sicar system for managing environmental rural properties in Brazil.

Classes:
    Sicar: Class representing the Sicar system.
"""

import io
import os
import re
import time
import random
import requests
from PIL import Image, UnidentifiedImageError
from tqdm import tqdm
from typing import Dict
from pathlib import Path
from html import unescape
from urllib.parse import urlencode

from SICAR.drivers import Captcha, Tesseract
from SICAR.output_format import OutputFormat
from SICAR.state import State
from SICAR.url import Url
from SICAR.exceptions import (
    EmailNotValidException,
    UrlNotOkException,
    StateCodeNotValidException,
    FailedToDownloadCaptchaException,
    FailedToDownloadShapefileException,
    FailedToDownloadCsvException,
)

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class Sicar(Url):
    """
    Class representing the Sicar system.

    Sicar is a system for managing environmental rural properties in Brazil.

    It inherits from the Url class to provide access to URLs related to the Sicar system.

    Attributes:
        _driver (Captcha): The driver used for handling captchas. Default is Tesseract.
        _email (str): The personal email for communication or identification purposes.
    """

    def __init__(
        self,
        driver: Captcha = Tesseract,
        headers: Dict = None,
    ):
        """
        Initialize an instance of the Sicar class.

        Parameters:
            driver (Captcha): The driver used for handling captchas. Default is Tesseract.
            email (str): The personal email for communication or identification purposes. Default is "sicar@sicar.com".
            headers (Dict): Additional headers for HTTP requests. Default is None.

        Returns:
            None
        """
        self._driver = driver()
        self._create_session(headers=headers)
        self._initialize_cookies()

    def _create_session(self, headers: Dict = None):
        """
        Create a new session for making HTTP requests.

        Parameters:
            headers (Dict): Additional headers for the session. Default is None.

        Returns:
            None
        """
        self._session = requests.Session()
        self._session.headers.update(
            headers
            if isinstance(headers, dict)
            else {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36 Edg/88.0.705.56",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            }
        )

    def _initialize_cookies(self):
        """
        Initialize cookies by making the initial request and accepting any redirections.

        This method is intended to be called in the constructor to set up the session cookies.

        Returns:
            None
        """
        self._get(self._INDEX)

    def _get(self, url: str, *args, **kwargs):
        """
        Send a GET request to the specified URL using the session.

        Parameters:
            url (str): The URL to send the GET request to.
            *args: Variable-length positional arguments.
            **kwargs: Variable-length keyword arguments.

        Returns:
            requests.Response: The response from the GET request.

        Raises:
            UrlNotOkException: If the response from the GET request is not OK (status code is not 200).

        Note:
            The SSL certificate verification is disabled by default using `verify=False`. This allows connections to servers
            with self-signed or invalid certificates. Disabling SSL certificate verification can expose your application to
            security risks, such as man-in-the-middle attacks. If the server has a valid SSL certificate issued by a trusted
            certificate authority, you can remove the `verify=False` parameter to enable SSL certificate verification by
            default.
        """
        response = self._session.get(url, verify=False, *args, **kwargs)

        if not response.ok:
            raise UrlNotOkException(url)

        return response

   

    def _download_captcha(self) -> Image:
        """
        Download a captcha image from the SICAR system.

        Returns:
            Image: The captcha image.

        Raises:
            FailedToDownloadCaptchaException: If the captcha image fails to download.
        """
        
        url = f"https://www.car.gov.br/publico/municipios/ReCaptcha?{urlencode({'id': int(random.random() * 1000000)})}"
        response = self._get(url)

        if not response.ok:
            raise FailedToDownloadCaptchaException()

        try:
            
            
            captcha = Image.open(io.BytesIO(response.content))
        except UnidentifiedImageError as error:
            raise FailedToDownloadCaptchaException() from error

        
        return captcha

    def _download_shapefile(
        self,
        state: str | int,
        captcha: str,
        type: str,
        folder: str,
        chunk_size: int = 1024,
    ) -> Path:
        """
        Download the shapefile for the specified city code.

        Parameters:
            city_code (str | int): The code of the city for which to download the shapefile.
            captcha (str): The captcha value for verification.
            folder (str): The folder path where the shapefile will be saved.
            chunk_size (int, optional): The size of each chunk to download. Defaults to 1024.

        Returns:
            Path: The path to the downloaded shapefile.

        Raises:
            FailedToDownloadShapefileException: If the shapefile download fails.

        Note:
            This method performs the shapefile download by making a GET request to the shapefile URL with the specified
            city code and captcha. The response is then streamed and saved to a file in chunks. A progress bar is displayed
            during the download. The downloaded file path is returned.
        """
        query = urlencode(
            {"idEstado": state, "tipoBase": type, "ReCaptcha": captcha}
        )

        try:
            response = self._get(f"{self._BASE}/estados/downloadBase?{query}", stream=True)
        except UrlNotOkException as error:
            raise FailedToDownloadShapefileException() from error

        content_length = int(response.headers.get("Content-Length", 0))

        content_type = response.headers.get("Content-Type", "")

        if content_length == 0 or not content_type.startswith("application/zip"):
            raise FailedToDownloadShapefileException()

        path = Path(os.path.join(folder, f"SHAPE_{state}_{type}")).with_suffix(".zip")

        with open(path, "wb") as fd:
            with tqdm(
                total=content_length,
                unit="iB",
                unit_scale=True,
                desc=f"Downloading Shapefile for '{state}'",
            ) as progress_bar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    fd.write(chunk)
                    progress_bar.update(len(chunk))
        return path

    

    def download_state(
        self,
        state: State | str,
        output_format: OutputFormat = OutputFormat.SHAPEFILE,
        folder: Path | str = Path("temp"),
        tries: int = 25,
        type: str = None,
        debug: bool = False,
        chunk_size: int = 1024,
    ):
        """
        Download shapefiles or CSVs for a state.

        Parameters:
            state (State | str): The state for which to download the files. It can be either a `State` enum value or a string representing the state's abbreviation.
            output_format (OutputFormat, optional): The format of the files to download. Defaults to OutputFormat.SHAPEFILE.
            folder (Path | str, optional): The folder path where the downloaded files will be saved. Defaults to 'temp'.
            tries (int, optional): The number of download attempts allowed per city. Defaults to 25.
            debug (bool, optional): Whether to enable debug mode with additional print statements. Defaults to False.
            chunk_size (int, optional): The size of each chunk to download. Defaults to 1024.

        Returns:
            Dict: A dictionary containing the results of the download operation.
                The keys are tuples of city name and code, and the values are the paths to the downloaded files.
                If a download fails for a city, the corresponding value will be False.
        """
        
    
        Path(folder).mkdir(parents=True, exist_ok=True)

        captcha = ""
        info = f"State '{state}' in '{output_format}' format"
        while tries > 0:
            try:
                captcha = self._driver.get_captcha(self._download_captcha())

                if len(captcha) == 5:
                    if debug:
                        print(
                            f"[{tries:02d}] - Requesting {info} with captcha '{captcha}'"
                        )

                    return self._download_shapefile(
                        state=state,
                        captcha=captcha,
                        type=type,
                        folder=folder,
                        chunk_size=chunk_size,
                    )
                elif debug:
                    print(
                        f"[{tries:02d}] - Invalid captcha '{captcha}' to request {info}"
                    )
            except (
                FailedToDownloadCaptchaException,
                FailedToDownloadShapefileException,
                FailedToDownloadCsvException,
            ) as error:
                if debug:
                    print(f"[{tries:02d}] - {error} When requesting {info}")
            finally:
                tries -= 1
                time.sleep(random.random() + random.random())

        return False

