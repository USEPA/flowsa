from importlib_resources import files
import pandas as pd


def crosswalk(
    crosswalk: str,
) -> pd.DataFrame:
    return pd.read_csv(files(f'{__package__}.activitytosectormapping')
                       .joinpath(f'{crosswalk}.csv'))
