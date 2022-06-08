"""
Test functions work
"""
import pytest
import flowsa


def test_get_flows_by_activity():
    flowsa.getFlowByActivity(datasource="EIA_MECS_Land", year=2014)


def test_get_flows_by_sector():
    # set function to download any FBAs that are missing
    flowsa.getFlowBySector('Water_national_2015_m1',
                           download_FBAs_if_missing=True)


def test_write_bibliography():
    flowsa.writeFlowBySectorBibliography('Water_national_2015_m1')


@pytest.mark.generate_fbs
def test_generate_fbs():
    """Generate all FBS from methods in repo."""
    for m in flowsa.seeAvailableFlowByModels("FBS", print_method=False):
        if m not in ['BEA_summary_target',
                     'USEEIO_summary_target',
                     'Electricity_gen_emissions_national_2016']:
            print("--------------------------------\n"
                  f"Method: {m}\n"
                  "--------------------------------")
            flowsa.flowbysector.main(method=m, download_FBAs_if_missing=True)

if __name__ == "__main__":
    test_generate_fbs()