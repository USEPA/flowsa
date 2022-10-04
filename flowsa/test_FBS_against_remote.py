"""
Tests to run during github action
"""
import pytest
import os
from flowsa import seeAvailableFlowByModels
from flowsa.settings import diffpath
from flowsa.common import check_method_status
from flowsa.test_single_FBS import compare_single_FBS_against_remote


@pytest.mark.skip(reason="Perform targeted test for compare_FBS on PR")
def test_FBS_against_remote():
    """Compare results for each FBS method at current HEAD with most
    recent FBS stored on remote server."""
    outdir = diffpath
    method_status = check_method_status()
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    for m in seeAvailableFlowByModels("FBS", print_method=False):
        if method_status.get(m) is not None:
            print(f"{m} skipped due to "
                  f"{method_status.get(m).get('Status', 'Unknown')}")
            continue
        compare_single_FBS_against_remote(m)


if __name__ == "__main__":
    test_FBS_against_remote()
