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


class FlowsaMethodNotFoundError(FileNotFoundError):
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


class EnvError(Exception):
    def __init__(self, key):
        message = (f"The key {key} was not found in external_paths.env. "
                   f"Create key or see examples folder for help.")
        self.message = message
        super().__init__(self.message)


class FBSMethodConstructionError(Exception):
    """Errors in FBS methods which result in incompatible models"""
    def __init__(self, message=None, error_type=None):
        if message is None:
            message = ("Error in method construction.")
        if error_type == 'fxn_call':
            message = ("Calling functions in method files must be preceded "
                       "by '!script_function:<data_source_module>'")
        self.message = message
        super().__init__(self.message)
