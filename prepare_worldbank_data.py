import json
import csv

# queried from isi_data: SELECT DISTINCT Country FROM Issue
isi_countries = ['ARGENTINA', 'ARMENIA', 'AUSTRALIA', 'AUSTRIA', 'AZERBAIJAN', 'BAHRAIN', 'BANGLADESH', 'BARBADOS', 'BELGIUM', 'BOSNIA & HERCEG', 'BRAZIL', 'BULGARIA', 'BYELARUS', 'CANADA', 'CHILE', 'COLOMBIA', 'COSTA RICA', 'CROATIA', 'CUBA', 'CZECH REPUBLIC', 'CZECHOSLOVAKIA', 'DENMARK', 'ECUADOR', 'EGYPT', 'ENGLAND', 'ESTONIA', 'ETHIOPIA', 'FINLAND', 'FRANCE', 'GERMANY', 'GREECE', 'HUNGARY', 'ICELAND', 'INDIA', 'IRAN', 'IRELAND', 'ISRAEL', 'ITALY', 'JAMAICA', 'JAPAN', 'JORDAN', 'KENYA', 'KUWAIT', 'LATVIA', 'LIBYA', 'LITHUANIA', 'MACEDONIA', 'MALAWI', 'MALAYSIA', 'MALTA', 'MEXICO', 'MOLDOVA', 'MONACO', 'NEPAL', 'NETHERLANDS', 'NEW ZEALAND', 'NIGERIA', 'NORTH IRELAND', 'NORWAY', 'PAKISTAN', 'PAPUA N GUINEA', 'PEOPLES R CHINA', 'PERU', 'PHILIPPINES', 'POLAND', 'PORTUGAL', 'REP OF GEORGIA', 'ROMANIA', 'RUSSIA', 'SAUDI ARABIA', 'SCOTLAND', 'SERBIA', 'SINGAPORE', 'SLOVAKIA', 'SLOVENIA', 'SOUTH AFRICA', 'SOUTH KOREA', 'SPAIN', 'SRI LANKA', 'SWEDEN', 'SWITZERLAND', 'TAIWAN', 'THAILAND', 'TRINID & TOBAGO', 'TURKEY', 'U ARAB EMIRATES', 'UGANDA', 'UKRAINE', 'UNITED KINGDOM', 'UNITED STATES', 'URUGUAY', 'USSR', 'UZBEKISTAN', 'VENEZUELA', 'WALES', 'YUGOSLAVIA', 'ZIMBABWE']

# worldbank.json queried from: http://api.worldbank.org/countries?per_page=250
wbJson=json.load(open('data/worldbank.json'))
# pop off the paging data
wbJson = wbJson.pop()

# Entries look like this:
# {"id":"ARG","iso2Code":"AR","name":"Argentina","region":{"id":"LCN","value":"Latin America & Caribbean (all income levels)"},"adminregion":{"id":"LAC","value":"Latin America & Caribbean (developing only)"},"incomeLevel":{"id":"UMC","value":"Upper middle income"},"lendingType":{"id":"IBD","value":"IBRD"},"capitalCity":"Buenos Aires","longitude":"-58.4173","latitude":"-34.6118"},

# Find missing values with the following:
# countries = []
# for entry in wbJson:
#     countries.append(entry['name'].upper())
#
# for country in isi_countries:
#     if country not in countries and country not in isi_country_corrections.keys():
#         print country

# make a dict of corrections based on missing values (by finding the appropriate match in the worldbank data manually)
isi_country_corrections = {'PAPUA N GUINEA': 'PAPUA NEW GUINEA', 'BOSNIA & HERCEG': 'BOSNIA AND HERZEGOVINA', 'LIBYA': 'LIBYA', 'SERBIA': 'SERBIA', 'MOLDOVA': 'MOLDOVA', 'YUGOSLAVIA': 'SERBIA', 'NORTH IRELAND': 'IRELAND', 'WALES': 'UNITED KINGDOM', 'TRINID & TOBAGO': 'TRINIDAD AND TOBAGO', 'REP OF GEORGIA': 'GEORGIA', 'EGYPT': 'EGYPT, ARAB REP.', 'PEOPLES R CHINA': 'CHINA', 'U ARAB EMIRATES': 'UNITED ARAB EMIRATES', 'IRAN': 'IRAN, ISLAMIC REP.', 'BYELARUS': 'BELARUS', 'CZECHOSLOVAKIA': 'CZECH REPUBLIC', 'MACEDONIA': 'MACEDONIA, FYR', 'USSR': 'RUSSIAN FEDERATION', 'VENEZUELA': 'VENEZUELA, RB', 'ENGLAND': 'UNITED KINGDOM', 'SCOTLAND': 'UNITED KINGDOM', 'SLOVAKIA': 'SLOVAK REPUBLIC', 'SOUTH KOREA': 'KOREA, DEM. REP.', 'RUSSIA': 'RUSSIAN FEDERATION', 'TAIWAN': 'CHINA'}

wbdata = {}

fieldnames = ['isi_country_name', 'iso', 'iso2', 'name', 'region', 'region_name', 'income_level', 'lat', 'lon']
for entry in wbJson:
    region = entry['region']
    income = entry['incomeLevel']
    wbdata[entry['name'].upper()] = [entry['id'], entry['iso2Code'], entry['name'], region['id'], region['value'], income['id'], entry['latitude'], entry['longitude']]

# now repeat some of the data with the corrections

for k,v in isi_country_corrections.iteritems():
    wbdata[k] = wbdata[v]

wbcsv = open('data/worldbank.csv', 'wb')
wbWriter = csv.writer(wbcsv)
wbWriter.writerow(fieldnames)

for k,v in wbdata.iteritems():
    wbWriter.writerow([k]+v)

wbcsv.close()