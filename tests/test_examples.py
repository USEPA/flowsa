"""
Test functions work, used for CI/CD testing
"""
import flowsa
import flowsa.flowbyactivity
import flowsa.flowbysector
from flowsa.common import check_method_status, load_yaml_dict, \
    seeAvailableFlowByModels


def test_get_flows_by_activity():
    flowsa.flowbyactivity.getFlowByActivity(datasource="EIA_MECS_Land", year=2014,
                                            download_FBA_if_missing=False)


def test_get_flows_by_sector():
    # set function to download any FBAs that are missing
    flowsa.flowbysector.getFlowBySector('Water_national_2015_m1',
                                        download_FBAs_if_missing=True)


    flowsa.flowbysector.getFlowBySector('TRI_DMR_state_2017',
                                        download_FBAs_if_missing=True)

    flowsa.flowbysector.getFlowBySector('GHG_national_2022_m1',
                                        download_FBAs_if_missing=True)

    flowsa.flowbysector.getFlowBySector('CNHW_national_2018',
                                        download_FBAs_if_missing=True,
                                        download_FBS_if_missing=True)


# todo: reinstate after modifying bib function for recursive method
# def test_write_bibliography():
#     flowsa.writeFlowBySectorBibliography('Water_national_2015_m1')
