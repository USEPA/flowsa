"""
Tests to run during github action
"""
import sys

import pytest
import os
import pandas as pd
import numpy as np
from flowsa.flowbysector import FlowBySector
from flowsa.common import check_method_status, seeAvailableFlowByModels
from flowsa.settings import diffpath
from flowsa.validation import compare_single_FBS_against_remote


@pytest.mark.skip(reason="Perform targeted test for compare_FBS on PR")
def test_FBS_against_remote(only_run_m=None):
    """Compare results for each FBS method (latest year) at current HEAD
    with most recent FBS stored on remote server."""
    error_list = []
    outdir = diffpath
    method_status = check_method_status()
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    models = pd.DataFrame(seeAvailableFlowByModels("FBS", print_method=False))
    models['year'] = models[0].str.extract('.*(\d{4})', expand = False)
    models = models.dropna()
    models['model'] = models.apply(lambda x: x[0].split(x['year']),
                                   axis=1).str[0]
    m_last_two = models[0].str.slice(start=-2)
    models['model'] = np.where(m_last_two.str.startswith('m'),
                               models['model'] + m_last_two,
                               models['model'])
    model_list = (models.sort_values(by='year')
                        .drop_duplicates(subset='model', keep='last')[0])
    for m in model_list:
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
    # memory_limit()  # Not functioning
    if len(sys.argv) < 2:
        test_FBS_against_remote()
    elif sys.argv[1] == "list":
        print("\n".join(seeAvailableFlowByModels("FBS", print_method=False)))
    else:
        test_FBS_against_remote(sys.argv[1])
