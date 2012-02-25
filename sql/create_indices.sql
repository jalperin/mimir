# when importing the data, it is better to drop all your indices
# these SQL statements will bring them back

ALTER TABLE isi_articles
ADD INDEX isi_articles_language (language),
ADD INDEX isi_articles_title (title(333)),
ADD INDEX isi_articles_id_when_cited (id_when_cited),
ADD INDEX isi_articles_issue_id (issue_id),
ADD INDEX isi_articles_doctype (doctype),
ADD INDEX isi_articles_num_citations (num_citations);

ALTER TABLE isi_addresses
ADD INDEX isi_addresses_adtype (adtype),
ADD INDEX isi_addresses_author (author),
ADD INDEX isi_addresses_organization (organization),
ADD INDEX isi_addresses_city (city),
ADD INDEX isi_addresses_country (country),
ADD INDEX isi_addresses_sequence (sequence),
ADD INDEX isi_addresses_province (province);

ALTER TABLE isi_authors
ADD INDEX isi_authors_address (address),
ADD INDEX isi_authors_organization (organization),
ADD INDEX isi_authors_email (email),
ADD INDEX isi_authors_name_last (name_last),
ADD INDEX isi_authors_name_suffix (name_suffix),
ADD INDEX isi_authors_article_id (article_id),
ADD INDEX isi_authors_name (name);

ALTER TABLE isi_citation_patents
ADD INDEX isi_citation_patents_assignee (assignee),
ADD INDEX isi_citation_patents_year (year),
ADD INDEX isi_citation_patents_country (country);

ALTER TABLE isi_citation_references
ADD INDEX isi_citation_references_id_when_cited (id_when_cited),
ADD INDEX isi_citation_references_id_at_source_type (id_at_source_type),
ADD INDEX isi_citation_references_id_at_source (id_at_source),
ADD INDEX isi_citation_references_author (author),
ADD INDEX isi_citation_references_year (year),
ADD INDEX isi_citation_references_title_abrev (title_abbrev);

ALTER TABLE isi_issue_subjects
ADD INDEX isi_issue_subjects_subject (subject);

ALTER TABLE isi_issues
ADD INDEX isi_issues_pubtype (pubtype),
ADD INDEX isi_issues_name (name),
ADD INDEX isi_issues_name_abbrev (name_abbrev),
ADD INDEX isi_issues_issn (issn),
ADD INDEX isi_issues_publisher_city (publisher_city),
ADD INDEX isi_issues_year (year),
ADD INDEX isi_issues_date (date),
ADD INDEX isi_issues_publisher_address (publisher_address);


