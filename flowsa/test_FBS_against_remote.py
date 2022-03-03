"""
Tests to run during github action
"""
import pytest
import os
from flowsa import seeAvailableFlowByModels
from flowsa.metadata import set_fb_meta
from flowsa.settings import paths, datapath
from flowsa.validation import compare_FBS_results
from esupy.processed_data_mgmt import download_from_remote

@pytest.mark.skip(reason="Perform targeted test for compare_FBS on PR")
def test_FBS_against_remote():
    """Compare results for each FBS method at current HEAD with most
    recent FBS stored on remote server."""
    outdir = f"{datapath}fbs_diff/"
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    for m in seeAvailableFlowByModels("FBS"):
        if m.startswith('CAP_HAP_'): continue
        status = download_from_remote(set_fb_meta(m, "FlowBySector"),
                                      paths)

        if not status:
            print(f"{m} not found in remote server. Skipping...")
            continue

        df = compare_FBS_results(m, m, download=True)
        if len(df) > 0:
            print(f"Saving differences in {m}")
            df.to_csv(f"{outdir}{m}_diff.csv", index=False)
        else:
            print(f"No differences found in {m}")

if __name__ == "__main__":
    test_FBS_against_remote()