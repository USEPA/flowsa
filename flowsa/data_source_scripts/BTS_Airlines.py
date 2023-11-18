import pandas as pd
import numpy as np
from flowsa import flowsa_yaml, settings
from flowsa.flowbyactivity import FlowByActivity


def generate_fba():
    with open(f'{settings.sourceconfigpath}BTS_Airlines.yaml') as f:
        config = flowsa_yaml.load(f)

    year_list = config['years']

    df = pd.DataFrame(FlowByActivity((
        pd.read_csv(config['file_path'])
        [[c for c in config['parse']['rename_columns']]]
        .rename(columns=config['parse']['rename_columns'])
        .query('Year in @year_list')
        .reset_index(drop=True)
        .groupby('Year').agg('sum').reset_index()
        .assign(
            Class='Fuel',
            Compartment='air',
            SourceName='BTS_Airlines',
            LocationSystem='FIPS_2015',
            ActivityConsumedBy='Commercial Aircraft',
            Unit='gal',
            FlowName='Total Gallons Consumed',
            FlowType='TECHNOSPHERE_FLOW',
            Location='00000',
            DataReliability=3,
            DataCollection=5
        )
    )))

    df.to_parquet(f'{settings.fbaoutputpath}BTS_Airlines.parquet')
