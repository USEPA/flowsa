"""
Targeted comparison of FBS against remote
"""
import pytest
import os
from flowsa.settings import diffpath
from flowsa.validation import compare_single_FBS_against_remote


@pytest.mark.skip(reason="Perform targeted test on manual trigger")
def test_against_remote(m):
    compare_single_FBS_against_remote(m,
                                      outdir=diffpath,
                                      run_single=True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--method', help='FBS method name')
    args = vars(parser.parse_args())

    outdir = diffpath
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    test_against_remote(m=args['method'])
