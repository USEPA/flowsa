from importlib_resources import files
import pandas as pd
from typing import Callable


def crosswalk(
    crosswalk: str or Callable,
    crosswalk_config: dict
) -> pd.DataFrame:
    if isinstance(crosswalk, str):
        return pd.read_csv(files(f'{__package__}.activitytosectormapping')
                           .joinpath(f'{crosswalk}.csv'))
    elif callable(crosswalk):
        return crosswalk(crosswalk_config)
