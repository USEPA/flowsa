# Data source configuration 
Standard source configuration files provide information needed for pulling data sources

They are stored in YAML format using a .yaml extension

```
#Source configuration format
---
api_name: #Name internally for API used for identifying api keys
api_key_required:  #true or false 
format: #format, either json or txt
url 
  base_url: # base url
  api_path: #path to api
  url_params 
    # A set of url parameters for query string
  year_param: #name of year parameter
  key_param: # name of key parameter 
url_replace_fxn:  #name of the source specific function that replaces the dynamic values in the URL
years: 
    #years of data as separate lines like - 2015 
```

To declare a value that needs to be dynamically replaced, surround
a variable name in double underscores like \__foo__ so that a string
function will do a dynamic replacement

Based on [YAML v1.1 schema](https://yaml.org/spec/1.1/)

Use [YAMLlint](http://www.yamllint.com/) to assure the file is valid YAML

