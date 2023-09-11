"""
Test functions work, used for CI/CD testing
"""
import flowsa
from flowsa import seeAvailableFlowByModels
from flowsa.common import check_method_status, load_yaml_dict


def test_get_flows_by_activity():
    flowsa.getFlowByActivity(datasource="EIA_MECS_Land", year=2014,
                             download_FBA_if_missing=False)


def test_get_flows_by_sector():
    # set function to download any FBAs that are missing
    flowsa.getFlowBySector('Water_national_2015_m1',
                           download_FBAs_if_missing=True)


    flowsa.getFlowBySector('TRI_DMR_state_2017',
                           download_FBAs_if_missing=True)

    flowsa.getFlowBySector('GHG_national_2016_m1',
                           download_FBAs_if_missing=True)

    flowsa.getFlowBySector('CNHW_national_2018',
                           download_FBAs_if_missing=True,
                           download_FBS_if_missing=True)


# todo: reinstate after modifying bib function for recursive method
# def test_write_bibliography():
#     flowsa.writeFlowBySectorBibliography('Water_national_2015_m1')


def test_FBS_methods():
    """Test succesful loading of FBS yaml files, skip files know to cause
    errors"""
    method_status = check_method_status()
    for m in seeAvailableFlowByModels("FBS", print_method=False):
        print(f"Testing method: {m}")
        if method_status.get(m) is not None:
            print(f"{m} skipped due to "
                  f"{method_status.get(m).get('Status', 'Unknown')}")
            continue
        load_yaml_dict(m, flowbytype='FBS')
