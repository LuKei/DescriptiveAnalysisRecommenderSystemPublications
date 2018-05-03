import sqlite3
import pandas
import pickle
from datetime import datetime

dbname = 'publications20180322_184739 _erweitert'
connIn = sqlite3.connect(dbname + '.db')
dfPublications = pandas.read_sql('select * from publication', connIn)
citations = pickle.load(open('pickles/publicationCitations.pkl', 'rb'))

citedCSSeries = pandas.Series(dtype=int)
citedISSeries = pandas.Series(dtype=int)
citedTotalSeries = pandas.Series(dtype=int)

i = 0
for publication in dfPublications.itertuples():
    # get publications of the each field that cite the publication
    citation_set = citations[publication.id]
    ind_series_CS = dfPublications.apply(lambda x: x['id'] in citation_set and x['field'] == 'CS', axis=1)
    ind_series_IS = dfPublications.apply(lambda x: x['id'] in citation_set and x['field'] == 'IS', axis=1)
    publications_CS_citing = dfPublications[ind_series_CS]
    publications_IS_citing = dfPublications[ind_series_IS]
    citedCSSeries = citedCSSeries.append(pandas.Series([len(publications_CS_citing)]), ignore_index=True)
    citedISSeries = citedISSeries.append(pandas.Series([len(publications_IS_citing)]), ignore_index=True)
    citedTotalSeries = citedTotalSeries.append(pandas.Series([len(citation_set)]), ignore_index=True)

    i += 1
    if i % 100 == 0:
        print('iteration: #' + str(i))

dfPublications['countCitedFromCS'] = citedCSSeries
dfPublications['countCitedFromIS'] = citedISSeries
dfPublications['countCitedTotal'] = citedTotalSeries

timestring = datetime.now().strftime("%Y%m%d_%H%M%S")

connOut = sqlite3.connect(dbname + '_mostCited' + timestring + '.db')
cursorOut = connOut.cursor()
cursorOut.execute('CREATE TABLE IF NOT EXISTS publication('
                  'id INTEGER UNIQUE NOT NULL, '
                  'title TEXT NOT NULL, '
                  'journal TEXT NOT NULL, '
                  'abstract TEXT NOT NULL, '
                  'authors TEXT NOT NULL,'
                  'year INTEGER,'
                  'field TEXT NOT NULL,'
                  'countCitedFromCS INTEGER NOT NULL,'
                  'countCitedFromIS INTEGER NOT NULL,'
                  'countCitedTotal INTEGER NOT NULL'
                  ')')
for publication in dfPublications.itertuples():
    cursorOut.execute('INSERT INTO publication VALUES(?,?,?,?,?,?,?,?,?,?)', (publication.id,
                                                                              publication.title,
                                                                              publication.journal,
                                                                              publication.abstract,
                                                                              publication.authors,
                                                                              publication.year,
                                                                              publication.field,
                                                                              publication.countCitedFromCS,
                                                                              publication.countCitedFromIS,
                                                                              publication.countCitedTotal))
connOut.commit()
