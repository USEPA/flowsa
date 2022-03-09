"""
Test FBA config and urls during github action
"""
import pytest
from flowsa import seeAvailableFlowByModels
from flowsa.flowbyactivity import load_yaml_dict, assemble_urls_for_query,\
    call_urls


@pytest.mark.skip(reason="Perform targeted test for test_FBA_urls on PR")
def test_FBA_urls():
    """Test yaml_load and url access for each FBA at the latest year.
    FBA requiring API key are skipped."""
    error_list = []
    for m in seeAvailableFlowByModels("FBA", print_method=False):
        config = load_yaml_dict(m, flowbytype='FBA')
        year = max(config['years'])

        if ((config.get('url', 'None') == 'None') or
            (config.get('api_key_required', False))):
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
        except Exception:
            error_list.append(m)
    if error_list:
        pytest.fail(f"Error retrieving: {', '.join([x for x in [*error_list]])}")

if __name__ == "__main__":
    test_FBA_urls()
