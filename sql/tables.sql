CREATE TABLE `isi_addresses` (
  `article_id` char(16) NOT NULL default '',
  `adtype` char(1) NOT NULL default '',
  `author` varchar(64) default NULL,
  `full_address` varchar(256) default NULL,
  `organization` varchar(256) default NULL,
  `sub_organization` varchar(256) default NULL,
  `street` varchar(256) default NULL,
  `city` varchar(256) default NULL,
  `province` varchar(256) default NULL,
  `postal_code` varchar(256) default NULL,
  `country` varchar(256) default NULL,
  `sequence` smallint(6) default NULL,
  KEY `isi_addresses_article_id` (`article_id`),
  KEY `isi_addresses_adtype` (`adtype`),
  KEY `isi_addresses_author` (`author`),
  KEY `isi_addresses_organization` (`organization`),
  KEY `isi_addresses_city` (`city`),
  KEY `isi_addresses_country` (`country`),
  KEY `isi_addresses_sequence` (`sequence`),
  KEY `isi_addresses_province` (`province`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `isi_articles` (
  `article_id` char(16) character set latin1 NOT NULL default '' COMMENT 'Unique alphanumeric ID.',
  `issue_id` char(16) default NULL COMMENT 'Alphanumeric ID of the source journal.',
  `id_when_cited` char(16) character set latin1 default NULL COMMENT 'Cited ID used for this article.',
  `id_at_source_type` char(16) character set latin1 default NULL COMMENT '(multiple) ARTN, DOI, PII, UNSP',
  `id_at_source` char(255) character set latin1 default NULL COMMENT '(multiple) Identifier provided by source',
  `doctype` char(1) NOT NULL default '' COMMENT 'Document Type (@ = article)',
  `keywords` varchar(1024) default NULL COMMENT 'Space-delimited list of article keywords.',
  `language` char(2) default NULL COMMENT 'Language code (see ISI documentation).',
  `title` varchar(1024) default NULL COMMENT 'Title of the article.',
  `abstract_text` varchar(10240) default NULL COMMENT 'Full text of the article abstract.',
  `data_file` char(32) NOT NULL COMMENT 'Path to ISI gzip file of origin.',
  `fpage` char(16) default NULL,
  `epage` char(16) default NULL,
  `page_count` int(6) default NULL,
  `num_citations` smallint(6) default NULL COMMENT 'Number of citations',
  PRIMARY KEY  (`article_id`),
  KEY `isi_articles_language` (`language`),
  KEY `isi_articles_title` (`title`(333)),
  KEY `isi_articles_id_when_cited` (`id_when_cited`),
  KEY `isi_articles_issue_id` (`issue_id`),
  KEY `isi_articles_doctype` (`doctype`),
  KEY `isi_articles_num_citations` (`num_citations`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='The ISI Web of Knowledge article data.';

CREATE TABLE `isi_authors` (
  `article_id` char(16) NOT NULL COMMENT 'Unique alphanumeric ID of the article associated with this author instance.',
  `name` varchar(64) NOT NULL COMMENT 'Name in lastname_ii format.',
  `name_formatted` varchar(64) NOT NULL default '' COMMENT 'Name in lastname_ii format.',
  `name_first` varchar(64) default NULL COMMENT 'Full first name.',
  `name_last` varchar(64) default NULL COMMENT 'Full last name.',
  `name_suffix` char(8) default NULL COMMENT 'Abbreviated name suffix (e.g. "Jr.").',
  `address` varchar(256) default NULL COMMENT 'Mailing address.',
  `organization` varchar(256) default NULL COMMENT 'Organization associated with this author.',
  `email` varchar(64) default NULL COMMENT 'Email address.',
  `sequence` smallint(6) NOT NULL default '0' COMMENT 'The order it appears in the record',
  `role` varchar(64) default NULL COMMENT 'The role of this author (Author, Reprint Author)',
  `address_sequence` smallint(6) default NULL,
  PRIMARY KEY  (`article_id`,`name_formatted`,`sequence`),
  KEY `isi_authors_address` (`address`),
  KEY `isi_authors_organization` (`organization`),
  KEY `isi_authors_email` (`email`),
  KEY `isi_authors_name_last` (`name_last`),
  KEY `isi_authors_name_suffix` (`name_suffix`),
  KEY `isi_authors_article_id` (`article_id`),
  KEY `isi_authors_name` (`name`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COMMENT='The ISI Web of Knowledge author data.';

CREATE TABLE `isi_citation_patents` (
  `article_id` char(16) NOT NULL default '',
  `assignee` varchar(64) default NULL,
  `year` smallint(6) default NULL,
  `patent_number` varchar(18) default NULL,
  `country` varchar(4) default NULL,
  `type` varchar(4) default NULL,
  KEY `isi_citation_patents_article_id` (`article_id`),
  KEY `isi_citation_patents_assignee` (`assignee`),
  KEY `isi_citation_patents_year` (`year`),
  KEY `isi_citation_patents_country` (`country`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `isi_citation_references` (
  `article_id` char(16) NOT NULL default '',
  `id_when_cited` char(16) default '',
  `id_at_source_type` char(16) default NULL,
  `id_at_source` char(255) default NULL,
  `author` varchar(64) default NULL,
  `year` smallint(6) default NULL,
  `volume` varchar(4) default NULL,
  `title_abbrev` varchar(20) default NULL,
  `pages` varchar(5) default NULL,
  KEY `isi_citation_references_id_when_cited` (`id_when_cited`),
  KEY `isi_citation_references_article_id` (`article_id`),
  KEY `isi_citation_references_id_at_source_type` (`id_at_source_type`),
  KEY `isi_citation_references_id_at_source` (`id_at_source`),
  KEY `isi_citation_references_author` (`author`),
  KEY `isi_citation_references_year` (`year`),
  KEY `isi_citation_references_title_abrev` (`title_abbrev`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `isi_issue_subjects` (
  `issue_id` char(10) NOT NULL default '',
  `subject` char(2) NOT NULL default '',
  KEY `isi_issue_subjects_issue_id` (`issue_id`),
  KEY `isi_issue_subjects_subject` (`subject`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `isi_issues` (
  `issue_id` char(10) NOT NULL default '',
  `sequence` varchar(7) default NULL,
  `pubtype` char(1) default NULL,
  `name` varchar(255) default NULL,
  `name_abbrev` varchar(170) default NULL,
  `name_abbrev11` varchar(11) default NULL,
  `name_abbrev20` varchar(20) default NULL,
  `name_abbrev29` varchar(29) default NULL,
  `issn` varchar(9) default NULL,
  `series_name` varchar(255) default NULL,
  `publisher` varchar(75) default NULL,
  `publisher_city` varchar(75) default NULL,
  `publisher_address` varchar(255) default NULL,
  `volume` varchar(10) default NULL,
  `issue` varchar(10) default NULL,
  `year` smallint(6) default NULL,
  `date` varchar(20) default NULL,
  `supplement` varchar(10) default NULL,
  `special_issue` varchar(10) default NULL,
  KEY `isi_issues_issue_id` (`issue_id`),
  KEY `isi_issues_pubtype` (`pubtype`),
  KEY `isi_issues_name` (`name`),
  KEY `isi_issues_name_abbrev` (`name_abbrev`),
  KEY `isi_issues_issn` (`issn`),
  KEY `isi_issues_publisher_city` (`publisher_city`),
  KEY `isi_issues_year` (`year`),
  KEY `isi_issues_date` (`date`),
  KEY `isi_issues_publisher_address` (`publisher_address`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

