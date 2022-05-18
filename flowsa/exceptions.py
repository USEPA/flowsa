# exceptions.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""Defines custom exceptions for flowsa"""


class FBANotAvailableError(Exception):
    def __init__(self, method=None, year=None, message=None):
        if message is None:
            message = ("FBA not available for requested year")
            if method:
                message = message.replace("FBA", method)
            if year:
                message = message.replace("requested year", str(year))
        self.message = message
        super().__init__(self.message)


class MethodNotFoundError(FileNotFoundError):
    def __init__(self, method_type=None, method=None):
        message = (f"{method_type} method file not found")
        if method:
            message = " ".join((message, f"for {method}"))
        self.message = message
        super().__init__(self.message)


class APIError(Exception):
    def __init__(self, api_source):
        message = (f"Key file {api_source} not found. See github wiki for help "
                  "https://github.com/USEPA/flowsa/wiki/Using-FLOWSA#api-keys")
        self.message = message
        super().__init__(self.message)


class MethodConstructionError(Exception):
    """Errors in FBS methods which result in incompatible models"""
    def __init__(self, message=None):
        if message is None:
            message = ("Error in method construction.")
        self.message = message
        super().__init__(self.message)
