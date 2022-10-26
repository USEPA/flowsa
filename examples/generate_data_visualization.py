# generate_data_visualization.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Generate plots to explore Flow-By-Sector model outputs
"""

import flowsa
import flowsa.datavisualization as dv
from flowsa.settings import plotoutputpath
import matplotlib.pyplot as plt


########## Produce facet graph of resources associated with cropland sectors ##########
sectors = ['112']
sector_length_display = 6
plottype = 'facet_graph'
method_dict = {'Water Withdrawal 2015': 'Water_national_2015_m1',
               'Land Use 2012': 'Land_national_2012',
               'Employment 2017': 'Employment_national_2017'}


dv.FBSscatterplot(method_dict, plottype,
                  sector_length_display=sector_length_display,
                  sectors_to_include=sectors,
                  plot_title='Direct Resource Use for Livestock'
                  )
# Can manually adjust the figure pop up before saving
plt.savefig(f"{plotoutputpath}livestock_resource_use.png", dpi=300)


########## Compare the results between water method 1 and method 2 ##########
sectors = ['21']
sector_length_display = 6
plottype = 'method_comparison'
method_dict = {'Water Withdrawal 2015 M1': 'Water_national_2015_m1',
               'Water Withdrawal 2015 M2': 'Water_national_2015_m2'}

flowsa.generateFBSplot(method_dict, plottype,
                       sector_length_display=sector_length_display,
                       sectors_to_include=sectors,
                       plot_title='Comparison of 2015 National Water '
                                  'Withdrawals Method 1 and Method 2 for '
                                  'Mining Sectors'
                       )
# Can manually adjust the figure pop up before saving
plt.savefig(f"{plotoutputpath}mining_water_comp.png", dpi=300)


########## Compare food waste flows via Sankey ##########
methodnames = ['Food_Waste_national_2018_m3', 'Food_Waste_national_2018_m2']
target_sector_level = 'NAICS_2'
target_subset_sector_level = {
    'Food_Waste_national_2018_m3': {'NAICS_4': ['2212'],
                                    'NAICS_6': ['62421', '31111', '32411',
                                                '56221', '62421', '115112',
                                                '22132'],
                                    'NAICS_7': ['562212', '562219']
                                    },
    'Food_Waste_national_2018_m2': {
        'SectorConsumedBy': {'NAICS_6': ['115112', '22132','311119',
                                         '32411', '562213','62421'],
                             'NAICS_7': ['562212', '562219']
                               }}
}

dv.generateSankeyDiagram(
    methodnames,
    target_sector_level=target_sector_level,
    target_subset_sector_level=target_subset_sector_level,
    use_sectordefinition=True,
    sectors_to_include=None,
    fbsconfigpath=None,
    orientation='horizontal'
)
