from string import digits
from flowsa.common import *
from flowsa.settings import log


def usgs_myb_year(years, current_year_str):
    """Sets the column for the string based on the year.
        Checks that the year you picked is in the last file."""
    years_array = years.split("-")
    lower_year = int(years_array[0])
    upper_year = int(years_array[1])
    current_year = int(current_year_str)
    if lower_year <= current_year <= upper_year:
        column_val = current_year - lower_year + 1
        return "year_" + str(column_val)
    else:
        log.info("Your year is out of scope. Pick a year between %s and %s", lower_year, upper_year)


def usgs_myb_name(USGS_Source):
    """Takes the USGS source name and parses it so it can be used in other parts of Flow by activity."""
    #USGS_MYB_Kyanite
    source_split = USGS_Source.split("_")
    name_cc = str(source_split[2])
    name = ""
    for char in name_cc:
        if char.isupper():
            name = name + " " + char
        else:
            name = name + char
    name = name.lower()
    name = name.strip()
    return name

def usgs_myb_static_varaibles():
    """Populates the data values for Flow by activity that are the same for all of USGS_MYB Files"""
    data = {}
    data["Class"] = "Geological"
    data['FlowType'] = "ELEMENTARY_FLOWS"
    data["Location"] = US_FIPS
    data["Compartment"] = "ground"
    data["Context"] = None
    data["ActivityConsumedBy"] = None
    return data

def usgs_myb_remove_digits(value_string):
    """Eliminates numbers in a string"""
    remove_digits = str.maketrans('', '', digits)
    return_string = value_string.translate(remove_digits)
    return return_string

