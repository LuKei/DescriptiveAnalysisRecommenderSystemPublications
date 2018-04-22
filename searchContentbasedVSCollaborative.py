import pandas
import sqlite3
from datetime import datetime



def checkCollaborativeCat(publication):
    searchString = 'collaborative'
    return True if (searchString in publication.title or
                    searchString in publication.abstract) \
        else False

def checkContentBasedCat(publication):
    searchString = 'content'
    return True if (searchString in publication.title or
                    searchString in publication.abstract) \
        else False

def checkHybrid(publication, isCollaborative=False, isContentBased=False):
    searchString = 'hybrid'
    return True if ((isCollaborative and isContentBased) or
                    searchString in publication.title or
                    searchString in publication.abstract) \
        else False

dbname = 'publications20180322_184739 _erweitert_mostCited20180419_092453'
connIn = sqlite3.connect(dbname + '.db')
dfPublications = pandas.read_sql('select * from publication', connIn)
categorySeries = pandas.Series(dtype=str)

for publication in dfPublications.itertuples():
    isCollaborative = checkCollaborativeCat(publication)
    isContentBased = checkContentBasedCat(publication)
    isHybrid = checkHybrid(publication, isCollaborative, isContentBased)

    if isHybrid:
        categorySeries = categorySeries.append(pandas.Series(['hybrid']), ignore_index=True)
    elif isCollaborative:
        categorySeries = categorySeries.append(pandas.Series(['collaborative']), ignore_index=True)
    elif isContentBased:
        categorySeries = categorySeries.append(pandas.Series(['contentbased']), ignore_index=True)
    else:
        categorySeries = categorySeries.append(pandas.Series(['nocat']), ignore_index=True)

dfPublications['category'] = categorySeries

timestring = datetime.now().strftime("%Y%m%d_%H%M%S")

connOut = sqlite3.connect(dbname + '_category' + timestring + '.db')
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
                  'countCitedTotal INTEGER NOT NULL,'
                  'category'
                  ')')
for publication in dfPublications.itertuples():
    cursorOut.execute('INSERT INTO publication VALUES(?,?,?,?,?,?,?,?,?,?,?)', (publication.id,
                                                                              publication.title,
                                                                              publication.journal,
                                                                              publication.abstract,
                                                                              publication.authors,
                                                                              publication.year,
                                                                              publication.field,
                                                                              publication.countCitedFromCS,
                                                                              publication.countCitedFromIS,
                                                                              publication.countCitedTotal,
                                                                                publication.category))
connOut.commit()