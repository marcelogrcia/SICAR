"""
Custom Exception Classes Module.

This module provides custom exception classes for specific error conditions.

Classes:
    EmailNotValidException: Exception raised when an invalid email is encountered.
    UrlNotOkException: Exception raised when a URL is inaccessible or returns an error.
    StateCodeNotValidException: Exception raised when an invalid state code is encountered.
    FailedToDownloadCaptchaException: Exception raised when downloading a captcha fails.
    FailedToDownloadShapefileException: Exception raised when downloading a shapefile fails.
    FailedToDownloadCsvException: Exception raised when downloading a CSV fails.
"""


class EmailNotValidException(Exception):
    """
    Exception raised when an invalid email is encountered.

    Attributes:
        email (str): The invalid email address.
    """

    def __init__(self, email: str):
        """
        Initialize an instance of EmailNotValidException.

        Parameters:
            email (str): The invalid email address.

        Returns:
            None
        """
        self.email = email
        super().__init__(f"Email {self.email} not valid!")


class UrlNotOkException(Exception):
    """
    Exception raised when a URL is inaccessible or returns an error.

    Attributes:
        url (str): The problematic URL.
    """

    def __init__(self, url: str):
        """
        Initialize an instance of UrlNotOkException.

        Parameters:
            url (str): The problematic URL.

        Returns:
            None
        """
        self.url = url
        super().__init__(f"Oh no! Failed to access {self.url}!")


class StateCodeNotValidException(Exception):
    """
    Exception raised when an invalid state code is encountered.

    Attributes:
        state (str): The invalid state code.
    """

    def __init__(self, state: str):
        """
        Initialize an instance of StateCodeNotValidException.

        Parameters:
            state (str): The invalid state code.

        Returns:
            None
        """
        self.state = state
        super().__init__(f"State code {self.state} not valid!")


class FailedToDownloadCaptchaException(Exception):
    """Exception raised when downloading a captcha fails."""

    def __init__(self):
        """
        Initialize an instance of FailedToDownloadCaptchaException.

        Parameters:
            None

        Returns:
            None
        """
        super().__init__("Failed to download captcha!")


class FailedToDownloadShapefileException(Exception):
    """Exception raised when downloading a shapefile fails."""

    def __init__(self):
        """
        Initialize an instance of FailedToDownloadShapefileException.

        Parameters:
            None

        Returns:
            None
        """
        super().__init__("Failed to download shapefile!")


class FailedToDownloadCsvException(Exception):
    """Exception raised when downloading a CSV fails."""

    def __init__(self):
        """
        Initialize an instance of FailedToDownloadCsvException.

        Parameters:
            None

        Returns:
            None
        """
        super().__init__("Failed to download CSV!")
