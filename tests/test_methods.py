"""
Test FBA config and urls during github action
"""
import pytest
import flowsa.exceptions
from flowsa.generateflowbyactivity import assemble_urls_for_query,\
    call_urls
from flowsa.common import check_method_status, load_yaml_dict, \
    seeAvailableFlowByModels


@pytest.mark.skip(reason="Perform targeted test for test_FBA_urls on PR")
def test_FBA_urls(only_run_m=None):
    """Test yaml_load and url access for each FBA at the latest year.
    FBA requiring API key are skipped."""
    error_list = []
    method_status = check_method_status()
    for m in seeAvailableFlowByModels("FBA", print_method=False):
        if only_run_m is not None and m != only_run_m:
            continue
        m_status = method_status.get(m)
        config = load_yaml_dict(m, flowbytype='FBA')
        year = max(config['years'])

        if ((config.get('url', 'None') == 'None') or
            (m == 'EPA_EQUATES')):
            continue

        print("--------------------------------\n"
              f"Method: {m} - {year}\n"
              "--------------------------------")
        # Remove call_response so the dataframe is not generated
        if 'call_response_fxn' in config:
            config.pop('call_response_fxn')
        try:
            urls = assemble_urls_for_query(source=m, year=str(year),
                                           config=config)
            call_urls(url_list=urls, source=m, year=str(year),
                      config=config)
        except flowsa.exceptions.APIError:
            print('API Key required, skipping url')
            continue
        except Exception as e:
            if ((m_status is not None) and
                (e.__class__.__name__ == m_status.get('Type'))):
                print(f'Known {m_status.get("Type")} in {m}')
            else:
                error_list.append(m)
    if error_list:
        pytest.fail(f"Error retrieving: {', '.join([x for x in [*error_list]])}")


if __name__ == "__main__":
    test_FBA_urls()
