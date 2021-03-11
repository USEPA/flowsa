import os
from bibtexparser.bwriter import BibTexWriter
from bibtexparser.bibdatabase import BibDatabase
from flowsa.flowbysector import load_method
from flowsa.common import outputpath, biboutputpath, load_sourceconfig

# test
# methodname = 'Land_national_2012'

def generate_fbs_bibliography(methodname):
    """
    Generate bibliography for a FlowBySector
    :param methodname:
    :return:
    """
    # load the fbs method yaml
    fbs_yaml = load_method(methodname)

    # create list of data and allocation data sets
    fbs = fbs_yaml['source_names']
    fbas = []
    for fbs_k, fbs_v in fbs.items():
        fbas.append([fbs_k, fbs_v['year']])
        activities = fbs_v['activity_sets']
        for aset, attr in activities.items():
            if attr['allocation_source'] != 'None':
                fbas.append([attr['allocation_source'], attr['allocation_source_year']])
            if 'helper_source' in attr:
                fbas.append([attr['allocation_source'], attr['helper_source_year']])
    # drop list duplicates and any where year is None (because allocation is a function, not a datasource)
    fba_list = []
    for fba in fbas:
        if fba not in fba_list:
            if fba[1] != 'None':
                fba_list.append(fba)

    # loop through list of fbas, load fba method yaml, and create bib entry
    bib_list = []
    for f in fba_list:
        config = load_sourceconfig(f[0])

        db = BibDatabase()
        db.entries = [{
            'title': config['source_name'],
            'author': config['author'],
            'year': str(f[1]),
            'url': config['citeable_url'],
            'urldate': config['date_of_access'],
            'ID': f[0] + '_' + str(f[1]),
            'ENTRYTYPE': 'misc'
        }]
        # append each entry to a list of BibDatabase entries
        bib_list.append(db)

    # write out bibliography
    writer = BibTexWriter()
    # create directory if missing
    os.makedirs(outputpath + '/Bibliography', exist_ok=True)
    with open(biboutputpath + methodname + '.bib', 'w') as bibfile:
        # loop through all entries in bib_list
        for b in bib_list:
            bibfile.write(writer.write(b))

    return None
