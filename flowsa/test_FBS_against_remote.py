"""
Tests to run during github action
"""
import sys

import pytest
import os
import flowsa
from flowsa import seeAvailableFlowByModels
from flowsa.settings import diffpath, memory_limit
from flowsa.common import check_method_status
from flowsa.test_single_FBS import compare_single_FBS_against_remote


@pytest.mark.generate_fbs
def test_generate_fbs():
    """Generate all FBS from methods in repo."""
    for m in flowsa.seeAvailableFlowByModels("FBS", print_method=False):
        if m not in ['BEA_summary_target',
                     'Electricity_gen_emissions_national_2016',
                     'Employment_common',
                     'USEEIO_summary_target'
                     ]:
            print("--------------------------------\n"
                  f"Method: {m}\n"
                  "--------------------------------")
            flowsa.flowbysector.main(method=m, download_FBAs_if_missing=True)


@pytest.mark.skip(reason="Perform targeted test for compare_FBS on PR")
def test_FBS_against_remote(only_run_m=None):
    """Compare results for each FBS method at current HEAD with most
    recent FBS stored on remote server."""
    error_list = []
    outdir = diffpath
    method_status = check_method_status()
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    for m in ['Land_national_2012', 'Water_national_2015_m1']: #seeAvailableFlowByModels("FBS", print_method=False):
        if only_run_m is not None and m != only_run_m:
            continue
        if method_status.get(m) is not None:
            print(f"{m} skipped due to "
                  f"{method_status.get(m).get('Status', 'Unknown')}")
            continue
        try:
            compare_single_FBS_against_remote(m)
        except:
            error_list.append(m)
            continue
    if error_list:
        pytest.fail(f"Error generating:"
                    f" {', '.join([x for x in [*error_list]])}")


if __name__ == "__main__":
    # memory_limit()  # Not functioning
    if len(sys.argv) < 2:
        test_FBS_against_remote()
    elif sys.argv[1] == "list":
        print("\n".join(seeAvailableFlowByModels("FBS", print_method=False)))
    else:
        test_FBS_against_remote(sys.argv[1])
