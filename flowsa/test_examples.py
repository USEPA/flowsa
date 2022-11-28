"""
Test functions work, used for CI/CD testing
"""
import flowsa
from flowsa import seeAvailableFlowByModels
from flowsa.flowbyactivity import load_yaml_dict


def test_get_flows_by_activity():
    flowsa.getFlowByActivity(datasource="EIA_MECS_Land", year=2014,
                             download_FBA_if_missing=False)


def test_get_flows_by_sector():
    # set function to download any FBAs that are missing
    flowsa.getFlowBySector('Water_national_2015_m1',
                           download_FBAs_if_missing=True)


def test_write_bibliography():
    flowsa.writeFlowBySectorBibliography('Water_national_2015_m1')


def test_FBS_methods():
    """Test succesful loading of FBS yaml files"""
    for m in seeAvailableFlowByModels("FBS", print_method=False):
        print(f"Testing method: {m}")
        load_yaml_dict(m, flowbytype='FBS')
