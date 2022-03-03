"""
Test functions work
"""
import pytest
import os
import flowsa
from flowsa.metadata import set_fb_meta
from esupy.processed_data_mgmt import download_from_remote

def test_get_flows_by_activity():
    flowsa.getFlowByActivity(datasource="EIA_MECS_Land", year=2014)


def test_get_flows_by_sector():
    # set function to download any FBAs that are missing
    flowsa.getFlowBySector('Water_national_2015_m1',
                           download_FBAs_if_missing=True)


def test_write_bibliography():
    flowsa.writeFlowBySectorBibliography('Water_national_2015_m1')


@pytest.mark.skip(reason="Perform targeted test for compare_FBS on PR")
def test_FBS_against_remote():
    """Compare results for each FBS method at current HEAD with most
    recent FBS stored on remote server."""
    outdir = f"{flowsa.settings.datapath}fbs_diff/"
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    with os.scandir(flowsa.settings.flowbysectormethodpath) as files:
        for method in files:
            if method.name.endswith(".yaml"):
                m = method.name.split(".yaml")[0]
                if m.startswith('CAP_HAP_'): continue
                status = download_from_remote(set_fb_meta(m, "FlowBySector"),
                                              flowsa.settings.paths)

                if not status:
                    print(f"{m} not found in remote server. Skipping...")
                    continue

                df = flowsa.validation.compare_FBS_results(m, m,
                                                           download=True)
                if len(df) > 0:
                    print(f"Saving differences in {m}")
                    df.to_csv(f"{outdir}{m}_diff.csv", index=False)
                else:
                    print(f"No differences found in {m}")

if __name__ == "__main__":
    test_FBS_against_remote()