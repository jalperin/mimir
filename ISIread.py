"""
This is a script to read in gzip files provided by Thomson Reuters
into a relational mysql database. It has not been extensively tested
but it seems to do the trick for importing the 40+GB of data.
"""

__author__ = "Juan Pablo Alperin"
__copyright__ = "Copyright 2012, Juan Pablo Alperin"
__credits__ = ["Juan Pablo Alperin"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Juan Pablo Alperin"
__email__ = "juan@alperin.ca"

import os
import fnmatch
import gzip
import re
import string
import sys
from time import time, strftime, gmtime
import datetime

import MySQLdb as mdb

con = None
try:
	con = mdb.connect('##host##', '##username##', '##password##', '##dbname##');

	cur = con.cursor()
	cur.execute("SELECT VERSION()")

	data = cur.fetchone()
	print "Database version : %s " % data

except mdb.Error, e:
	print "Error %d: %s" % (e.args[0],e.args[1])
	sys.exit(1)

cur = con.cursor()
ISIdir='/u/nlp/mimir/ISI/data/'
# ISIdir='/Users/juan/Code/sandbox/isi/'

class Issue():
	"""An Issue object"""
	def __init__(self):
		self.change_code = None # T1
		self.id = None # UI
		self.sequence = None # SQ
		self.pubtype = None # PT
		self.name = None # SO
		self.name_abbrev = None # JI
		self.name_abbrev11 = None # J1
		self.name_abbrev20 = None # J2
		self.name_abbrev29 = None # J9
		self.subject = None # SC
		self.issn = None # SN
		self.series_name = None # SE
		self.publisher = None # PU
		self.publisher_city = None # PI
		self.publisher_address = None # PA (not all years)
		self.volume = None # VL
		self.issue = None # IS
		self.year = None # PY
		self.date = None # PD
		self.supplement = None # SU
		self.special_issue = None # SI

class Article():
	"""An article object"""
	def __init__(self):
		self.change_code = None # T2
		self.id = None # UT
		self.id_when_cited = None # T9
		self.id_at_source = [] # AR
		self.title = None # TI
		self.abstract = None # AB
		# RL, # RW (reviewed work language and author)
		self.doctype = None # DT
		self.fpage = None # BP
		self.epage = None # EP
		self.page_count = None # PG
		self.language = None # LA
		self.num_citations = None # NR
		self.keywords = [] # DE, ID
		self.authors = []
		self.citation_references = []
		self.citation_patents = []
		self.addresses = []

class Author():
	"""An author object"""
	def __init__(self):
		self.name = None # AU, AG
		self.name_formatted = None # AU (formatted)
		self.role = None # RO
		self.last_name = None # LN
		self.first_name = None # AF
		self.suffix = None # AS
		self.email = None # EM
		self.address = None # AA
		self.sequence = None # Order author was found within article
		self.address_sequence = None # AD

class Address():
	"""An address object"""
	def __init__(self):
		self.adtype = None # R=reprint/A=author
		self.author = None # RA
		self.full_address = None # NF
		self.organization = None # NC
		self.sub_organization = None # ND
		self.street = None # NN
		self.city = None # NY
		self.province = None # NP
		self.postal_code = None # NZ
		self.country = None # NU
		self.sequence = None # CN or Order address was found within article

class CitationPatent():
	"""A patent citation object"""
	def __init__(self):
		self.assignee = None # /A
		self.year = None # /Y
		self.number = None # /W
		self.country = None # /N
		self.patent_type = None # /C

class CitationReference():
	"""A regular citation object"""
	def __init__(self):
		self.cited_article_id = None # R9
		self.id_when_cited = None # RS
		self.id_at_source_all = [] # use to hold a list of all the id at source
		self.id_at_source_type = None # AR, use to hold the string after splitting
		self.id_at_source = None # AR, use to hold the string after splitting
		self.author = None # /A
		self.year = None # /Y
		self.title = None # /W
		self.volume = None # /V
		self.page = None # /P

def _split(line):
	'''the ISI files have 2 char code followed by content'''
	return line[0:2], line[3:]

def _split_id_at_source(l):
	'''Everything before the first space is the type, everything after is the id'''
	split_list=[x.split(' ') for x in l]
	ret1 = ';'.join([y[0] for y in split_list])
	ret2 = ';'.join([' '.join(x[1:]) for x in split_list])
	if ret1.strip() == '' or ret2.strip() == '':
		return None, None
	else:
		return ret1, ret2

# for _format_names to use
_delchars = ''.join(c for c in map(chr, range(256)) if not c.isalnum())
_blank_trans_table = string.maketrans('', '')

def _format_names(name):
	'''how do you want the name formatted?'''
	l=content.split(',')
	formatted_name=l.pop(0).translate(_blank_trans_table, _delchars)
	if len(l) > 0:
		formatted_name+='_'+(''.join(l)).translate(_blank_trans_table, _delchars)
	return formatted_name.lower()

# Grab all the files in the ISIdir and find the creation date
# we need to process files in order so that we handle changes correctly
filelist=[]

for filename in os.listdir( ISIdir ):
	if not fnmatch.fnmatch(filename, '*.gz'):
		continue

	# open the file
	file = gzip.open(ISIdir+filename, 'rb')

	# try top 5 lines for a date
	for i in range(0,5):
		line = file.readline()
		code, content = _split(line)
		if code == 'H8':
			filelist.append(content.strip()+filename)
			break

# sort to process the files by creation date
filelist=sorted(filelist)

issues_processed=0
articles_processed=0
prev_articles_processed=0

# go through all the files and process
for x in filelist:
	# use
	filename = x[8:]

	# open the file
	file = gzip.open(ISIdir+filename, 'rb')
	print "current file is: " + filename
	start_time=time()
	prev_time=start_time

	i = 0
	issue=None
	article=None
	author=None
	address=None
	citation=None

	# junk the first line (should just be filename)
	line = file.readline()

	# go until file is empty
	while line != '':
		code, content = _split(line)

		# end of file detected
		if code == 'EF': break;

		# read ahead to check for continue lines
		line2 = file.readline()
		if line2 != '':
			code2, content2 = _split(line2)
			# while continue line
			while code2 == '--' or ((code == code2) and code in ['AB', 'TI', 'NZ', 'ND', 'SC']):
				# join lines (removing newline with continue and keeping with same tag)
				if code in ['NZ', 'ND', 'SC']:
					content+=';'+content2
				elif code == code2:
					content+=content2
				else:
					content = content[:-1]+content2
				line2=file.readline()
				code2, content2 = _split(line2)

		# remove preceeding and trailing newlines (if present)
		content=content.strip()

		# Set some flags to keep track of open/close
		# start issue
		if code == 'UI':
			issue=Issue()

			issue.id = content
			# clear out this issue
			try:
				cur.execute("DELETE FROM isi_issues WHERE issue_id = %s", issue.id)
				cur.execute("DELETE FROM isi_issue_subjects WHERE issue_id = %s", issue.id)
			except mdb.Error, e:
				print "Error on Issue Delete %d: %s" % (e.args[0],e.args[1])
				print "Filename: %s" % filename
				print "Issue %s" % issue.id
				sys.exit(1)
		# end issue
		elif issue and code == 'RE' and issue.change_code != 'D':
			# insert issue
			try:
				cur.execute("INSERT INTO isi_issues (issue_id, sequence, pubtype, name, name_abbrev, name_abbrev11, name_abbrev20, name_abbrev29, issn, series_name, publisher, publisher_city, publisher_address, volume, issue, year, date, supplement, special_issue) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (issue.id, issue.sequence, issue.pubtype, issue.name, issue.name_abbrev, issue.name_abbrev11, issue.name_abbrev20, issue.name_abbrev29, issue.issn, issue.series_name, issue.publisher, issue.publisher_city, issue.publisher_address, issue.volume, issue.issue, issue.year, issue.date, issue.supplement, issue.special_issue))
			except mdb.Error, e:
				print "Error on Issue Insert %d: %s" % (e.args[0],e.args[1])
				print "Filename: %s" % filename
				print "Issue %s" % issue.id
				sys.exit(1)
			# insert the subject codes
			if issue.subject:
				try:
					cur.executemany("INSERT INTO isi_issue_subjects (issue_id, subject) VALUES (%s, %s)", [(issue.id, s) for s in issue.subject])
				except mdb.Error, e:
					print "Error on Issue Subject Insert %d: %s" % (e.args[0],e.args[1])
					print "Filename: %s" % filename
					print "Issue %s" % issue.id
					sys.exit(1)

			issues_processed+=1
			if (issues_processed % 500) == 0:
				t=time()
				print '%s::%s: Issues: %s, Articles: %s, sec/art: %3.5f, art/sec: %5.5f' % (strftime('%D %H:%M:%S', gmtime()), datetime.timedelta(seconds=round(t-start_time)), issues_processed, articles_processed, (t-prev_time)/(articles_processed-prev_articles_processed), (articles_processed-prev_articles_processed)/(t-prev_time))
				prev_time=t
				prev_articles_processed=articles_processed

			# reset
			issue=None
			article=None
			author=None
			address=None
			citation=None
		# start article
		elif code == 'UT':
			address_sequence = 0
			author_sequence = 0
			# reset
			article=Article()
			author=None
			address=None
			citation=None

			# clear out previous versions of article
			article.id = content
			try:
				cur.execute("DELETE FROM isi_articles WHERE article_id = %s", article.id)
				cur.execute("DELETE FROM isi_authors WHERE article_id = %s", article.id)
				cur.execute("DELETE FROM isi_addresses WHERE article_id = %s", article.id)
				cur.execute("DELETE FROM isi_citation_references WHERE article_id = %s", article.id)
				cur.execute("DELETE FROM isi_citation_patents WHERE article_id = %s", article.id)
			except mdb.Error, e:
				print "Error on Issue Article Delete %d: %s" % (e.args[0],e.args[1])
				print "Filename: %s" % filename
				print "Article %s" % article.id
				sys.exit(1)
		# end article
		elif article and code == 'EX' and article.change_code != 'D':
			id_at_source_type, id_at_source = _split_id_at_source(article.id_at_source)
			try:
				cur.execute("INSERT INTO isi_articles (article_id, id_when_cited, id_at_source_type, id_at_source, doctype, keywords, language, title, abstract_text, issue_id, data_file, fpage, epage, page_count, num_citations) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , (article.id, article.id_when_cited, id_at_source_type, id_at_source, article.doctype, ';'.join(article.keywords), article.language, article.title, article.abstract, issue.id, filename, article.fpage, article.epage, article.page_count, article.num_citations))
			except mdb.Error, e:
				print "Error on Article Insert %d: %s" % (e.args[0],e.args[1])
				print "Filename: %s" % filename
				print "Article %s" % article.id
				sys.exit(1)

			# add the last author
			if author:
				author_sequence += 1
				author.sequence = author_sequence
				article.authors.append(author)

			# insert all the authors
			if len(article.authors) > 0:
				try:
					cur.executemany("INSERT INTO isi_authors (article_id, name_first, name_last, name_suffix, name, name_formatted, email, role, address, sequence, address_sequence)	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", [(article.id, au.first_name, au.last_name, au.suffix, au.name, au.name_formatted, au.email, au.role, au.address, au.sequence, au.address_sequence) for au in article.authors])
				except mdb.Error, e:
					print "Error on Author %d: %s" % (e.args[0],e.args[1])
					print "Filename: %s" % filename
					print "article %s" % article.id
					sys.exit(1)

			# insert all addresses
			if len(article.addresses) > 0:
				try:
					cur.executemany("INSERT INTO isi_addresses (article_id, adtype, author, full_address, organization, sub_organization, street, city, province, postal_code, country, sequence) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", [(article.id, ad.adtype, ad.author, ad.full_address, ad.organization, ad.sub_organization, ad.street, ad.city, ad.province, ad.postal_code, ad.country, ad.sequence) for ad in article.addresses])
				except mdb.Error, e:
					print "Error on Addresses %d: %s" % (e.args[0],e.args[1])
					print "Filename: %s" % filename
					print "article %s" % article.id
					sys.exit(1)

			# insert all references
			if len(article.citation_references) > 0:
				try:
					cur.executemany("INSERT INTO isi_citation_references (article_id, id_when_cited, id_at_source_type, id_at_source, author, year, volume, title_abbrev, pages) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", [(article.id, cit.id_when_cited, cit.id_at_source_type, cit.id_at_source, cit.author, cit.year, cit.volume, cit.title, cit.page) for cit in article.citation_references])
				except mdb.Error, e:
					print "Error on references %d: %s" % (e.args[0],e.args[1])
					print "Filename: %s" % filename
					print "Article %s" % article.id
					sys.exit(1)

			# insert all patents
			if len(article.citation_patents) > 0:
				try:
					cur.executemany("INSERT INTO isi_citation_patents (article_id, assignee, year, patent_number, country, type) VALUES (%s, %s, %s, %s, %s, %s)", [(article.id, cit.assignee, cit.year, cit.number, cit.country, cit.patent_type) for cit in article.citation_patents])
				except mdb.Error, e:
					print "Error on references %d: %s" % (e.args[0],e.args[1])
					print "Filename: %s" % filename
					print "Article %s" % article.id
					sys.exit(1)

			articles_processed+=1
			# reset
			article=None
		# start author
		elif code == 'AU':
			if author:
				author_sequence += 1
				author.sequence = author_sequence

				# add to list of authors
				article.authors.append(author)
			author=Author()
		elif code == 'C1':
			address = Address()
			address.adtype = 'A'
		elif code == 'RP':
			address = Address()
			address.adtype = 'R'
		elif code == 'EA':
			if article:
				if not address.sequence:
					address_sequence+=1
					address.sequence=address_sequence

				article.addresses.append(address)
			address = None
		elif code == 'CP':
			citation = CitationPatent()
		elif code == 'CR':
			citation = CitationReference()
		elif code == 'EC':
			if isinstance(citation, CitationReference):
				id_at_source_type, id_at_source = _split_id_at_source(citation.id_at_source_all)
				citation.id_at_source_type = id_at_source_type
				citation.id_at_source = id_at_source
				article.citation_references.append(citation)

			elif isinstance(citation, CitationPatent):
				article.citation_patents.append(citation)

			# reset the citation
			citation = None

		###################
		#  PROCESS CODES  #
		###################
		if address:
			if code == 'RA':
				address.author = content
			elif code == 'NF':
				address.full_address = content
			elif code == 'NC':
				address.organization = content
			elif code == 'ND':
				address.sub_organization = content
			elif code == 'NN':
				address.street = content
			elif code == 'NY':
				address.city = content
			elif code == 'NP':
				address.province = content
			elif code == 'NZ':
				address.postal_code = content
			elif code == 'NU':
				address.country = content
			elif code == 'CN':
				address.sequence = content # or Order address was found within article
				address_sequence = content
		elif citation:
			if isinstance(citation, CitationReference):
				if code == '/A':
					citation.author = content
				elif code == 'R9':
					citation.id_when_cited = content
				elif code == 'AR' or code == 'RS':
					citation.id_at_source_all.append(content)
				elif code == '/Y':
					citation.year = content
				elif code == '/W':
					citation.title = content
				elif code == '/V':
					citation.volume = content
				elif code == '/P':
					citation.page = content
			elif isinstance(citation, CitationPatent):
				if code == '/A':
					citation.assignee = content
				elif code == '/Y':
					citation.year = content
				elif code == '/W':
					citation.number = content
				elif code == '/N':
					citation.country = content
				elif code == '/C':
					citation.patent_type = content
		elif article:
			if code == 'T2':
				article.change_code = content[0].upper()
			elif code == 'UT':
				article.id = content
			elif code == 'AR':
				article.id_at_source.append(content)
			elif code == 'T9':
				article.id_when_cited = content
			elif code == 'TI':
				article.title = content
			elif code == 'AB':
				article.abstract = content
			# RL , RW (reviewed work language and author)
			elif code == 'DT':
				# take only 1 char?
				article.doctype = content[0]
			elif code == 'BP':
				article.fpage = content
			elif code == 'EP':
				article.epage = content
			elif code == 'PG':
				article.page_count = content
			elif code == 'LA':
				article.language = content[0:2]
			elif code == 'DE' or code == 'ID':
				# replace ';' with ' ', since we use ';' as delimiter in DB
				article.keywords.append(content.replace(';', ' '))
			elif code == 'NR':
				if content[0:2] != 'NK':
					article.num_citations = content
			#
			# author codes
			#
			elif code == 'AU':
				author.name = content
				author.name_formatted = _format_names(content)
			elif code == 'AA':
				author.address = content
			elif code == 'AG':
				author.name = content
			elif code == 'RO':
				author.role = content
			elif code == 'LN':
				author.last_name = content
			elif code == 'AF':
				author.first_name = content
			elif code == 'AS':
				author.suffix = content
			elif code == 'EM':
				author.email = content
			elif code == 'AD':
				author.address_sequence = content # AD
		elif issue:
			if code == 'T1':
				issue.change_code = content[0].upper()
			elif code == 'UI':
				issue.id = content
			elif code == 'SQ':
				issue.sequence = content
			elif code == 'PT':
				issue.pubtype = content
			elif code == 'SO':
				issue.name = content
			elif code == 'JI':
				issue.name_abbrev = content
			elif code == 'J1':
				issue.name_abbrev11 = content
			elif code == 'J2':
				issue.name_abbrev20 = content
			elif code == 'J9':
				issue.name_abbrev29 = content
			elif code == 'SC':
				# this is a little ugly
				# content is in the form: X XX XXXXXXX;XX XX XXXXXX; ...
				# split on ; (added by multi-code above)
				# split based on spaces
				# grab the second item (the 2 letter code)
				# make set, then back to list, to uniquify
				issue.subject = list(set([y[1] for y in [x.split(' ') for x in content.split(';')]]))
			elif code == 'SN':
				issue.issn = content
			elif code == 'SE':
				issue.series_name = content
			elif code == 'PU':
				issue.publisher = content
			elif code == 'PI':
				issue.publisher_city = content
			elif code == 'PA':
				issue.publisher_address = content
			elif code == 'VL':
				issue.volume = content
			elif code == 'IS':
				issue.issue = content
			elif code == 'PY':
				issue.year = content
			elif code == 'PD':
				issue.date = content
			elif code == 'SU':
				issue.supplement = content
			elif code == 'SI':
				issue.special_issue = content

		# move onto the next line
		line = line2
		sys.stdout.flush()

# close the DB conncetion
con.close()

