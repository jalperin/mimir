import sys, os
import re
import time
import csv, cPickle

import MySQLdb as mdb
import networkx as nx

import collections


# os.system('killall ssh')
# os.system('ssh -L 3307:localhost:5029 iriss-ice')

# can be overriden by command line
YEAR = 2005
CITATION_WINDOW = 5

con = None
try:
	# laptop
	# con = mdb.connect('localhost', 'root', '123456789', 'isi');

	# choroid
	# con = mdb.connect('choroid.stanford.edu', 'minerva_r2', 'hohNeet4', 'mimir_dev');

	# on janet
	con = mdb.connect(host = '127.0.0.1', user = 'iriss', passwd = '1s1data', db = 'I', port = 5029);

	cur = con.cursor()
	cur.execute("SELECT VERSION()")

	data = cur.fetchone()
	print "Database version : %s " % data

except mdb.Error, e:
	print "Error %d: %s" % (e.args[0],e.args[1])
	sys.exit(1)

# command line options
if len(sys.argv) > 1:
    YEAR = int(sys.argv[1])
if len(sys.argv) > 2:
    CITATION_WINDOW = int(sys.argv[2])

print "Using year = %s" % YEAR
print "Using a %s year citation window" % CITATION_WINDOW

cur = con.cursor()

# Construct a node list
try:
	cur.execute("SELECT DISTINCT Name, Country FROM Issue WHERE Year = %s AND Publication_Type = 'J'", (YEAR))
except mdb.Error, e:
	print "Error on Issue SELECT %d: %s" % (e.args[0],e.args[1])
	sys.exit(1)


init_time = time.time()


SSH_SUBJECT_CODES = set(['BF', 'BI', 'BK', 'BM', 'BP', 'OR', 'CN', 'DI', 'DK', 'EO', 'EU', 'FA', 'FE', 'EN', 'FS', 'FU', 'GY', 'HA', 'HB', 'HE', 'JB', 'HF', 'JM', 'JO', 'JS', 'JW', 'KU', 'KU', 'KV', 'MM', 'MQ', 'MR', 'MW', 'BQ', 'NM', 'NU', 'OE', 'OY', 'OM', 'OT', 'OZ', 'OX', 'PA', 'PD', 'PF', 'PG', 'PH', 'PH', 'QC', 'QD', 'QL', 'PC', 'OO', 'RO', 'RP', 'PE', 'UA', 'UQ', 'UT', 'UU', 'VI', 'NQ', 'BV', 'EQ', 'MY', 'MY', 'HI', 'VX', 'VS', 'VJ', 'VP', 'WQ', 'VM', 'NE', 'YI', 'WM', 'WV', 'WU', 'PS', 'WY', 'XA', 'YE', 'YG', 'YY'])

journals = cur.fetchall()

#construct a set to make test faster
journal_set = set([j[0] for j in journals])

# build the graph
G=nx.MultiDiGraph()

# add all the nodes first
nodes = collections.defaultdict(list)
for j in journals:
    G.add_node(j[0])

    # report duplicates
    if j[0] in nodes:
        print 'repeat journal name ', j
        print 'already had ', nodes[j[0]]
        continue

    # print j[0]
    nodes[j[0]].append(j[1])     # add country
    try:
        cur.execute("""SELECT  Citing_Issue_Name,
                                SUM(IF(Cited_Article_Key = 0,1,0))/COUNT(*)*100 AS missing,
                                SUM(IF(Cited_Article_Key > 0,1,0))/COUNT(*)*100 AS not_missing,
                                COUNT(*) as num_citations
                        FROM Citation
                        WHERE Citing_Year = %s AND Citing_Issue_Name = %s GROUP BY 1;""", (YEAR, j[0]))
        row = cur.fetchone()

        # some journals do not have records in the citation table for some reason
        if (row):
            nodes[j[0]].append(row[1])    # add missing citations percentage
            nodes[j[0]].append(row[3])    # add number of outgoing citations (all)
        else:
            # put placeholders to keep things consistent
            nodes[j[0]].append(0)
            nodes[j[0]].append(0)
            print "'%s' does not have an entry in Citation" % j[0]
    except mdb.Error, e:
        print "Error on Citation Missing SELECT %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)

    try:
        cur.execute("""SELECT DISTINCT Subject_Code
                        FROM Issue i JOIN Issue_Subject s ON (i.Issue_Key = s.Issue_Key)
                        WHERE i.Year = %s AND i.Name = %s;""", (YEAR, j[0]))
        data = cur.fetchall()

        nodes[j[0]].append(set([x[0] for x in data]))    # add a set with all the subjects
    except mdb.Error, e:
        print "Error on Citation Missing SELECT %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)

cPickle.dump(nodes, open('nodes_'+ str(YEAR) + '.cPickle', 'wb'))
print "added %s nodes in %s minutes" % (len(nodes), (time.time()-init_time) / 60.0)

cite_counts = []
skipped_cites = 0

for j in journals:
    try:
        cur.execute("""SELECT Citing_Issue_Name
                        FROM Citation
                        WHERE Cited_Issue_Name = %s
                        AND Cited_Year = %s AND Citing_Year BETWEEN %s AND %s""", (j[0], YEAR, YEAR, (YEAR + CITATION_WINDOW)))
    except mdb.Error, e:
        print "Error on Citation SELECT %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)

    citations = cur.fetchall()
    cite_counts.append(len(citations))
    for cite in citations:
        # print cite
        if cite[0] in journal_set:
            G.add_edge(j[0], cite[0])
        else:
            skipped_cites += 1


print "total time %s" % (time.time() - init_time)
print "average number of citations (incl. skipped): %s" % (float(sum(cite_counts)) / len(cite_counts))
print "skipped: %s" % skipped_cites
print "Nodes: %s (%s)" % (len(journals), G.number_of_nodes())
print "Edges: %s" % G.size()


# save the file for future use
outfilename = 'G%s_%s' % (YEAR, CITATION_WINDOW)

cPickle.dump(G, open(outfilename + '.cPickle', 'wb'))

# convert to weighted

G_weighted = nx.DiGraph()
for node in G.nodes_iter():
    # add nodes to make sure we keep isolates
    G_weighted.add_node(node)
    for neighbor in G.neighbors(node):
        G_weighted.add_edge(node, neighbor, weight=len(G[node][neighbor]))

# save weighted for future use also
cPickle.dump(G_weighted, open(outfilename + '_weighted.cPickle', 'wb'))


# from networkx.readwrite import json_graph
# import json
# data = json_graph.node_link_data(G)
# s = json.dumps(data)
# f=open(outfilename + '.json', 'wb')
# f.write(s)
# f.close()
#
# # give things a numeric ID, but hold on to the journal name
# G=nx.convert_node_labels_to_integers(G,first_label=1, discard_old_labels=False)
# for name, label in G.node_labels.iteritems():
#     G.node[label]['name'] = name
#
# nx.write_edgelist(G, outfilename + '.edgelist')
#
# cPickle.dump(G, open(outfilename + '_numeric.cPickle', 'wb'))


# close the DB conncetion
con.close()

