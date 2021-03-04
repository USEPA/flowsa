def usgs_myb_year(years, current_year_str):
    years_array = years.split("-")
    lower_year = int(years_array[0])
    upper_year = int(years_array[1])
    current_year = int(current_year_str)
    if lower_year <= current_year <= upper_year:
        column_val = current_year - lower_year + 1
        return "year_" + str(column_val)


def usgs_myb_name(USGS_Source):
    #USGS_MYB_Kyanite
    source_split = USGS_Source.split("_")
    name = str(source_split[2]).lower()
    return name

def usgs_myb_static_varaibles():
    data = {}
    data["Class"] = "Geological"
    data['FlowType'] = "ELEMENTARY_FLOWS"
    data["Location"] = "00000"
    data["Compartment"] = "ground"
    data["Context"] = None
    data["ActivityConsumedBy"] = None
    return data