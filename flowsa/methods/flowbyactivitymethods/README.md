# Data source configuration 
Standard source configuration files provide information needed for pulling 
data sources. They are stored in YAML format using a .yaml extension

```
#Source configuration format
---
author: Data author, for use in bibliography entry
source_name: Data title, for use bibliography entry
source_url: Full data url, for use in bibliography entry
bib_id: Shorthand data name used for citation purposes in a .bib entry
api_name: Name internally for API used for identifying api keys
api_key_required: true or false 
format: #format, json, txt, xlsx
url 
  base_url: base url
  api_path: path to api
  url_params: 
    # A set of url parameters for query string, specific to data set
  year_param: name of year parameter
  key_param: name of key parameter 
url_replace_fxn: name of the source specific function that replaces the dynamic values in the URL
call_response_fxn: name of the source specific function that specifies how data should be loaded
parse_response_fxn: name of the source specific function that parses and formats the dataframe
call_all_years: bool, allows the passing of a year range to generateflowbyactivity.main() while only calling and parsing the url a single time
time_delay: int (in seconds), allows pausing between requests
years: 
    #years of data as separate lines like - 2015

* can add additional yaml dictionary items specific to calling on a data set
```

To declare a value that needs to be dynamically replaced, surround
a variable name in double underscores like `__foo__` so that a string
function will do a dynamic replacement

Specify the functions to use in the FBA creation using the tag 
`!script_function:PythonFileName FunctionName`
where _PythonFileName_ is the name of the Python file (e.g., 
"Census_PEP_Population.py") and _FunctionName_ is the name of the function 
(e.g., "Census_pop_URL_helper"). 

Based on [YAML v1.1 schema](https://yaml.org/spec/1.1/). Use 
[YAMLlint](http://www.yamllint.com/) to assure the file is valid YAML.
