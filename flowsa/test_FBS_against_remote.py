"""
Tests to run during github action
"""
import sys

import pytest
import os
from flowsa import seeAvailableFlowByModels
from flowsa.metadata import set_fb_meta
from flowsa.settings import paths, diffpath, memory_limit
from flowsa.validation import compare_FBS_results
from flowsa.common import check_method_status
from flowsa.test_single_FBS import compare_single_FBS_against_remote



@pytest.mark.skip(reason="Perform targeted test for compare_FBS on PR")
def test_FBS_against_remote(only_run_m=None):
    """Compare results for each FBS method at current HEAD with most
    recent FBS stored on remote server."""
    error_list = []
    outdir = diffpath
    method_status = check_method_status()
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    for m in seeAvailableFlowByModels("FBS", print_method=False):
        if only_run_m is not None and m != only_run_m:
            continue
        if method_status.get(m) is not None:
            print(f"{m} skipped due to "
                  f"{method_status.get(m).get('Status', 'Unknown')}")
            continue
        try:
            compare_single_FBS_against_remote(m)
        except Exception as e:
            error_list.append(m)
    if error_list:
        pytest.fail(f"Error generating:"
                    f" {', '.join([x for x in [*error_list]])}")



if __name__ == "__main__":
    memory_limit()
    if len(sys.argv) < 2:
        test_FBS_against_remote()
    elif sys.argv[1] == "list":
        print("\n".join(seeAvailableFlowByModels("FBS", print_method=False)))
    else:
        test_FBS_against_remote(sys.argv[1])
