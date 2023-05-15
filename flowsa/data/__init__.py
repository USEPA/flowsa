from importlib_resources import files
import pandas as pd


def crosswalk(
    crosswalk: str,
) -> pd.DataFrame:
    # read crosswalk columns as dtype object - otherwise sectors can be 
    # imported as int and cause merge errors
    return pd.read_csv(files(f'{__package__}.activitytosectormapping')
                       .joinpath(f'{crosswalk}.csv'), dtype=object)
