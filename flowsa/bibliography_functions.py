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

    # create dictionary of data and allocation datasets
    fbs = fbs_yaml['source_names']
    bib_list = []
    for fbs_k, fbs_v in fbs.items():

        # load the fba yaml
        fba = load_sourceconfig(fbs_k)

        db = BibDatabase()
        db.entries = [{
            'title': fba['source_name'],
            'author': fba['author'],
            'year': str(fbs_v['year']),
            'url': fba['citeable_url'],
            'urldate': fba['date_of_access'],
            'ID': fbs_k + '_' + str(fbs_v['year']),
            'ENTRYTYPE': 'misc'
        }]
        # append each entry to a list of BibDatabase entries
        bib_list.append(db)

        # add information for activity sets
        activities = fbs_v['activity_sets']
        for aset, attr in activities.items():
            if attr['allocation_source'] != 'None':

                # load the fba yaml
                fba_alloc = load_sourceconfig(attr['allocation_source'])

                db = BibDatabase()
                db.entries = [{
                     'title': fba_alloc['source_name'],
                     'author': fba_alloc['author'],
                     'year': str(attr['allocation_source_year']),
                     'url': fba_alloc['citeable_url'],
                     'urldate': fba_alloc['citeable_url'],
                     'ID': attr['allocation_source'] + '_' + str(attr['allocation_source_year']),
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
