"""
Targeted comparison of FBS against remote
"""
import pytest
import os
from flowsa.settings import diffpath
from flowsa.validation import compare_single_FBA_against_remote


@pytest.mark.skip(reason="Perform targeted test on manual trigger")
def test_fba_against_remote(source, year):
    compare_single_FBA_against_remote(
        source, year, outdir=diffpath, run_single=True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', help='FBA source name')
    parser.add_argument('--year', help='FBA year')
    args = vars(parser.parse_args())

    outdir = diffpath
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    test_fba_against_remote(source=args['source'], year=args['year'])
