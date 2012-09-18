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
	# connect
	con = mdb.connect('host', 'user', 'password', 'db_name');

	cur = con.cursor()
	cur.execute("SELECT VERSION()")

	data = cur.fetchone()
	print "Database version : %s " % data

except mdb.Error, e:
	print "Error %d: %s" % (e.args[0],e.args[1])
	sys.exit(1)

cur = con.cursor()
# ISIdir='/u/nlp/mimir/ISI/data/'
# ISIdir='/Users/juan/Code/sandbox/isi/'
ISIdir='/janet/scr3/mimir/data/'

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
		self.doctype = '-' # DT
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
# filelist=[]
#
# for filename in os.listdir( ISIdir ):
# 	if not fnmatch.fnmatch(filename, '*.gz'):
# 		continue
#
# 	# open the file
# 	file = gzip.open(ISIdir+filename, 'rb')
#
# 	# try top 5 lines for a date
# 	for i in range(0,5):
# 		line = file.readline()
# 		code, content = _split(line)
# 		if code == 'H8':
# 			filelist.append(content.strip()+filename)
# 			break
#
# # sort to process the files by creation date
# filelist=sorted(filelist)

#  original import (2009)
# filelist=['19961206MN3A95AA.gz', '19961208MN3A95AB.gz', '19961210MN3A95AD.gz', '19961213MN3A95AC.gz', '19970119MN3A96AA.gz', '19970121MN3A96AB.gz', '19970124MN3A96AC.gz', '19970127MN3A96AD.gz', '19970128MN3A91AA.gz', '19970130MN3A91AB.gz', '19970201MN3A91AC.gz', '19970202MN3A91AD.gz', '19970216MN3A94AA.gz', '19970218MN3A94AB.gz', '19970221MN3A94AC.gz', '19970223MN3A94AD.gz', '19970224MN3A93AA.gz', '19970226MN3A93AB.gz', '19970228MN3A93AC.gz', '19970303MN3A93AD.gz', '19970324MN3A92AA.gz', '19970327MN3A92AB.gz', '19970329MN3A92AC.gz', '19970402MN3A92AD.gz', '19990209MN3A97AB.gz', '19990217MN3A97AC.gz', '19990221MN3A97AA.gz', '19990224MN3A97AD.gz', '19990226MN3A97OV.gz', '20011025IN3A00AA.gz', '20011101IN3A00AB.gz', '20011106IN3A00AC.gz', '20011109IN3A00AD.gz', '20020107IN3A01AA.gz', '20020110IN3A01AB.gz', '20020114IN3A01AC.gz', '20020116IN3A01AD.gz', '20020122IN3A98AA.gz', '20020124IN3A98AB.gz', '20020128IN3A98AC.gz', '20020129IN3A98AD.gz', '20020201IN3A99AA.gz', '20020204IN3A99AB.gz', '20020205IN3A99AC.gz', '20020207IN3A99AD.gz', '20040113IN3O030113.gz', '20040120IN3O031426.gz', '20040202IN3O034052.gz', '20040831IC3N010152.gz', '20040901IG3N010152.gz', '20040902IC3N000152.gz', '20050531IN3O040113.gz', '20050601IN3O041426.gz', '20050701IN3O044052.gz', '20050708IN3O042739.gz', '20050805IN3O022739.gz', '20050809IN3O020113.gz', '20050809IN3O021426.gz', '20050809IN3O024052.gz', '20060111IN3O050113.gz', '20060113IN3O052739.gz', '20060117IN3O054052.gz', '20060127IN3O051426.gz', '20070118IN3O060113.gz', '20070126IN3O061426.gz', '20070202IN3O062739.gz', '20070207IN3O064052.gz', '20080110IN3O070113.gz', '20080114IN3O071426.gz', '20080128IN3O072739.gz', '20080204IN3O074052.gz', '20080217IN3O032739.gz', '20090306IN3O080113.gz', '20090309IN3O081426.gz', '20090310IN3O082739.gz', '20090310IN3O084052.gz', '20090528IG3O07AA.gz', '20090528IG3O080152.gz', '20090608IC3O080113.gz', '20090610IC3O070152.gz', '20090611IC3O060152.gz', '20090611IC3O082739.gz', '20090611IC3O084052.gz', '20090611IG3O070152.gz', '20090612IC3O050152.gz', '20090612IG3O054652.gz', '20090612IG3O060152.gz', '20090623IC3O040110.gz', '20090624IC3O041121.gz', '20090624IC3O042239.gz', '20090624IC3O044052.gz', '20090624IG3O041252.gz', '20090630IC3O030113.gz', '20090702IC3O031426.gz', '20090706IC3O032735.gz', '20090708IC3O033644.gz', '20090713IC3O034552.gz', '20090713IG3O030152.gz', '20090714IC3O020126.gz', '20090720IC3O022752.gz', '20090720IG3O020152.gz']

# second import (through to 2012)
# filelist= ['20040831IC3N010152.gz', '20040901IG3N010152.gz', '20090528IG3O080152.gz', '20090608IC3O080126.gz', '20090610IC3O070152.gz', '20090611IC3O060152.gz', '20090611IC3O082739.gz', '20090611IC3O084052.gz', '20090611IG3O070152.gz', '20090612IC3O050152.gz', '20090612IG3O054652.gz', '20090612IG3O060152.gz', '20090623IC3O040110.gz', '20090624IC3O041121.gz', '20090624IC3O042239.gz', '20090624IC3O044052.gz', '20090624IG3O041252.gz', '20090630IC3O030113.gz', '20090702IC3O031426.gz', '20090706IC3O032735.gz', '20090708IC3O033644.gz', '20090713IC3O034552.gz', '20090713IG3O030152.gz', '20090714IC3O020126.gz', '20090720IC3O022752.gz', '20090720IG3O020152.gz', '20090721IC3N090113.gz', '20090723IG3N090113.gz', '20090723IG3N091427.gz', '20090811IC3N000252.gz', '20090909IC3N091426.gz', '20091007IC3N092739.gz', '20091007IG3N092839.gz', '20100112IC3N094052.gz', '20100112IG3N094052.gz', '20100201IN3O090113.gz', '20100202IN3O091426.gz', '20100204IN3O092739.gz', '20100205IN3O094052.gz', '20100401IC3N100113.gz', '20100726IC3N101426.gz', '20101122IC3N102739.gz', '20110104IC3N104052.gz', '20110106IG3N100152.gz', '20110202IN3O100113.gz', '20110208IN3O101426.gz', '20110214IN3O102739.gz', '20110217IN3O104052.gz', '20110330IC3N110113.gz', '20110330IG3N110113.gz', '20110714IC3N111426.gz', '20110714IG3N111426.gz', '20110927IC3N112739.gz', '20111206IG3N112739.gz', '20120104IC3N114052.gz', '20120104IG3N114052.gz', '20120202IN3O110113.gz', '20120202IN3O111426.gz', '20120202IN3O112739.gz', '20120208IN3O114052.gz']

filelist= ['20110202IN3O100113.gz', '20110208IN3O101426.gz', '20110214IN3O102739.gz', '20110217IN3O104052.gz', '20110330IC3N110113.gz', '20110330IG3N110113.gz', '20110714IC3N111426.gz', '20110714IG3N111426.gz', '20110927IC3N112739.gz', '20111206IG3N112739.gz', '20120104IC3N114052.gz', '20120104IG3N114052.gz', '20120202IN3O110113.gz', '20120202IN3O111426.gz', '20120202IN3O112739.gz', '20120208IN3O114052.gz']

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

