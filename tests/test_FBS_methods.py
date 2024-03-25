"""
Test FBS method yaml during github action for succesful loading
"""
import pytest
from flowsa.common import check_method_status, load_yaml_dict, \
    seeAvailableFlowByModels


@pytest.mark.skip(reason="Perform targeted action for test_FBS_methods")
def test_FBS_methods():
    """Test succesful loading of FBS yaml files, skip files know to cause
    errors"""
    method_status = check_method_status()
    for m in seeAvailableFlowByModels("FBS", print_method=False):
        if method_status.get(m) is not None:
            print(f"{m} skipped due to "
                  f"{method_status.get(m).get('Status', 'Unknown')}")
            continue
        elif any(s in m for s in (
                'GHG_national_2013',
                'GHG_national_2014',
                'GHG_national_2015',
                'GHG_national_2016',
                'GHG_national_2018',
                'GHG_national_2019',
                'GHG_national_2020',
                )):
            # Skip select methods to expedite testing
            continue
        print(f"Testing method: {m}")
        load_yaml_dict(m, flowbytype='FBS')


if __name__ == "__main__":
    test_FBS_methods()
