# generate_data_visualization.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Data visualization will be revised at a later date to work for flowsav2.0

Generate plots to explore Flow-By-Sector model outputs
"""

import flowsa
from flowsa.settings import plotoutputpath
import matplotlib.pyplot as plt


### Produce facet graph of resources associated with livestock sectors ###
sectors = ['112']
sector_length_display = 6
plottype = 'facet_graph'
method_dict = {'Water Withdrawal 2015': 'Water_national_2015_m1',
               'Land Use 2012': 'Land_national_2012',
               'Employment 2017': 'Employment_national_2017'}


flowsa.FBSscatterplot(method_dict, plottype,
                  sector_length_display=sector_length_display,
                  sectors_to_include=sectors,
                  plot_title='Direct Resource Use for Livestock'
                  )
# Can manually adjust the figure pop up before saving
plt.savefig(plotoutputpath / "livestock_resource_use.png", dpi=300)


### Compare the results between national employment for 2015 and 2018 ###
sectors = ['21']
sector_length_display = 6
plottype = 'method_comparison'
method_dict = {'National Employment 2015': 'Employment_national_2015',
               'National Employment 2018': 'Employment_national_2018'}

flowsa.FBSscatterplot(method_dict, plottype,
                  sector_length_display=sector_length_display,
                  sectors_to_include=sectors,
                  plot_title=('Comparison of 2015 and 2018 Employment '
                              'for Mining Sectors')
                  )
# Can manually adjust the figure pop up before saving
plt.savefig(plotoutputpath / "mining_employment_comp.png", dpi=300)


### GHG Bar Chart ###
# Option 1 - GHG emissions by GHG
flowsa.stackedBarChart('GHG_national_2018_m1',
                       generalize_AttributionSources=True,
                       selection_fields={'SectorProducedBy': ['111', '324'],
                                         'Flowable': ['Carbon dioxide',
                                                      'Methane']},
                       industry_spec={'default': 'NAICS_3'},
                       filename='GHG_national_2018_m1_emissions_barchart',
                       axis_title='Emissions (MMT CO2e)'
                       )

# Option 2 - specify indicator, much have LCIAformatter installed
# https://github.com/USEPA/LCIAformatter
flowsa.stackedBarChart('GHG_national_2018_m1',
                       generalize_AttributionSources=True,
                       impact_cat="Global warming",
                       selection_fields={'SectorProducedBy': ['111', '324'],
                                         'Flowable': ['Carbon dioxide']},
                       industry_spec={'default': 'NAICS_3'},
                       filename='GHG_national_2018_m1_CO2_barchart',
                       axis_title='Global Warming Potential (MMT CO2e)'
                       )


# todo: will update the sankey code for recursive method post v2.0 release
########## Compare food waste flows via Sankey ##########
# methodnames = ['Food_Waste_national_2018_m1', 'Food_Waste_national_2018_m2']
# target_sector_level = 'NAICS_2'
# target_subset_sector_level = {
#     'Food_Waste_national_2018_m1': {'NAICS_4': ['2212'],
#                                     'NAICS_6': ['62421', '31111', '32411',
#                                                 '56221', '62421', '115112',
#                                                 '22132'],
#                                     'NAICS_7': ['562212', '562219']
#                                     },
#     'Food_Waste_national_2018_m2': {
#         'SectorConsumedBy': {'NAICS_6': ['115112', '22132','311119',
#                                          '32411', '562213','62421'],
#                              'NAICS_7': ['562212', '562219']
#                                }}
# }
# # set domain to scale sankey diagrams
# domain_dict = {0: {'x': [0.01, 0.49], 'y': [0, 1]},
#                1: {'x': [.51, 0.99], 'y': [.12, .88]}
#                }
#
# flowsa.generateSankeyDiagram(
#     methodnames,
#     target_sector_level=target_sector_level,
#     target_subset_sector_level=target_subset_sector_level,
#     use_sectordefinition=True,
#     sectors_to_include=None,
#     fbsconfigpath=None,
#     orientation='horizontal',
#     domain_dict=domain_dict,
#     value_label_format='brackets',
#     subplot_titles=['m1', 'm2'],
#     filename='FoodWasteSankey'
# )
