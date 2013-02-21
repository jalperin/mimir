import sys
import networkx as nx
import csv
import cPickle

# can be overriden by command line
YEAR = 2005
CITATION_WINDOW = 5
CODE = 'SS'

SSH_SUBJECT_CODES = set(['BF', 'BI', 'BK', 'BM', 'BP', 'OR', 'CN', 'DI', 'DK', 'EO', 'EU', 'FA', 'FE', 'EN', 'FS', 'FU', 'GY', 'HA', 'HB', 'HE', 'JB', 'HF', 'JM', 'JO', 'JS', 'JW', 'KU', 'KU', 'KV', 'MM', 'MQ', 'MR', 'MW', 'BQ', 'NM', 'NU', 'OE', 'OY', 'OM', 'OT', 'OZ', 'OX', 'PA', 'PD', 'PF', 'PG', 'PH', 'PH', 'QC', 'QD', 'QL', 'PC', 'OO', 'RO', 'RP', 'PE', 'UA', 'UQ', 'UT', 'UU', 'VI', 'NQ', 'BV', 'EQ', 'MY', 'MY', 'HI', 'VX', 'VS', 'VJ', 'VP', 'WQ', 'VM', 'NE', 'YI', 'WM', 'WV', 'WU', 'PS', 'WY', 'XA', 'YE', 'YG', 'YY'])

SS_SUBJECT_CODES = set(['BF', 'BI', 'BM', 'OR', 'CN', 'DI', 'DK', 'EU', 'FE', 'EN', 'FU', 'GY', 'HA', 'HB', 'HE', 'JB', 'JM', 'JO', 'JS', 'JW', 'KU', 'KU', 'KV', 'MR', 'MW', 'NM', 'NU', 'OE', 'PC', 'RO', 'PE', 'UQ', 'UU', 'VI', 'NQ', 'BV', 'EQ', 'MY', 'MY', 'HI', 'VX', 'VS', 'VJ', 'VP', 'WQ', 'VM', 'NE', 'WM', 'WV', 'WU', 'PS', 'WY', 'XA', 'YE', 'YY'])

# command line options
if len(sys.argv) > 1:
    YEAR = int(sys.argv[1])
if len(sys.argv) > 2:
    CITATION_WINDOW = int(sys.argv[2])

print "Using year = %s" % YEAR
print "Using a %s year citation window" % CITATION_WINDOW


fieldnames = ['isi_country_name', 'iso', 'iso2', 'country_name', 'region', 'region_name', 'income_level', 'lat', 'lon']
wbCsv = csv.reader(open('data/worldbank.csv'))
#scrap header
wbCsv.next()

worldbankdata = {}
for entry in wbCsv:
    worldbankdata[entry[0]] = entry[1:]

nodes = cPickle.load(open('nodes_2005.cPickle'))

for k,v in nodes.iteritems():
    country = nodes[k][0]
    if country:
        v += worldbankdata[country]
    else:
        v += [None for x in range(0,8)]

outfilename = 'G%s_%s' % (YEAR, CITATION_WINDOW)

G_weighted=cPickle.load(open(outfilename + '_weighted.cPickle'))

outfilename += '_' + CODE

# keep only journals classified as SSH in the current year
filtered = dict((k,v) for k,v in nodes.iteritems() if v[3].intersection(SS_SUBJECT_CODES))

# make a subgraph of only those journals
SG = G_weighted.subgraph(filtered.keys())

# convert nodes to numeric (statnet requires it)
SG_numeric=nx.convert_node_labels_to_integers(SG,first_label=1, discard_old_labels=False)

nx.write_edgelist(SG_numeric, outfilename + '_numeric.edgelist', data=True)

nodeoutfile = open('nodes' + str(YEAR) + '_' + CODE + '_numeric.csv', 'wb')
nodesCsv = csv.writer(nodeoutfile)

headers = ['id', 'name', 'isi_country', 'missing', 'num_citations_out', 'subject_codes'] + fieldnames[1:]
nodesCsv.writerow(headers)
for k, v in SG_numeric.node_labels.iteritems():
    nodesCsv.writerow([v, k] + nodes[k][0:3] + [';'.join(nodes[k][3])] + nodes[k][4:])

nodeoutfile.close()





