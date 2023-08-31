"""
Targeted comparison of FBS against remote
"""
import pytest
import os
from flowsa.flowbysector import FlowBySector
from flowsa.metadata import set_fb_meta
from flowsa.settings import paths, diffpath
from flowsa.validation import compare_FBS_results
from esupy.processed_data_mgmt import download_from_remote


@pytest.mark.skip(reason="Perform targeted test on manual trigger")
def compare_single_FBS_against_remote(m, outdir=diffpath,
                                      run_single=False):
    downloaded = download_from_remote(set_fb_meta(m, "FlowBySector"),
                                      paths)
    if not downloaded:
        if run_single:
            # Run a single file even if no comparison available
            FlowBySector.generateFlowBySector(
                method=m, download_sources_ok=True)
        else:
            print(f"{m} not found in remote server. Skipping...")
        return
    print("--------------------------------\n"
          f"Method: {m}\n"
          "--------------------------------")
    df = compare_FBS_results(m, m, ignore_metasources=True,
                             compare_to_remote=True)
    df.rename(columns = {'FlowAmount_fbs1': 'FlowAmount_remote',
                         'FlowAmount_fbs2': 'FlowAmount_HEAD'},
              inplace=True)
    if len(df) > 0:
        print(f"Saving differences in {m} to csv")
        df.to_csv(f"{outdir}/{m}_diff.csv", index=False)
    else:
        print(f"***No differences found in {m}***")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--method', help='FBS method name')
    args = vars(parser.parse_args())

    outdir = diffpath
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    compare_single_FBS_against_remote(m=args['method'],
                                      run_single=True)
