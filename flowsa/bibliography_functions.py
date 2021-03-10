import os
from bibtexparser.bwriter import BibTexWriter
from bibtexparser.bibdatabase import BibDatabase
from flowsa.flowbysector import load_method
from flowsa.common import outputpath, biboutputpath


def generate_fbs_bibliography(methodname):
    """
    Generate bibliography for a FlowBySector
    :param methodname:
    :return:
    """

    # load the fbs method yaml
    fbs_yaml = load_method(methodname)

    # create dictionary of data and allocation datasets
    fb = fbs_yaml['source_names']
    bib_list = []
    for k, v in fb.items():
        db = BibDatabase()
        db.entries = [{
             'title': k,
             'author': 'test',
             'year': str(v['year']),
             'url': 'test',
             'urldate': 'test',
             'ID': k + '_' + str(v['year']),
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
