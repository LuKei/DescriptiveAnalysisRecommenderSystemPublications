import pandas
import sqlite3
import ast
import Levenshtein
from fuzzywuzzy import fuzz
import datetime
import copy


def match_title(title_a, title_b, author_match=False, threshold=(0.90, 0.80)):
    ratio = Levenshtein.ratio(title_a, title_b)
    return True if ((ratio > threshold[0]) or (
            ratio > threshold[1] and author_match)) else False


def match_authors(authors_a, authors_b, threshold=0.85):
    return True if Levenshtein.jaro_winkler(
        authors_a, authors_b) > threshold else False


def match_author(author_a, author_b, threshold=0.95):
    return True if (fuzz.token_sort_ratio(
        author_a, author_b) / 100) > threshold else False


def countFieldSwitches(releaseOrderSorted):
    switches = 0
    lastReleaseField = releaseOrderSorted[0][1]
    for release in releaseOrderSorted:
        if release[1] != lastReleaseField:
            switches += 1
        lastReleaseField = release[1]
    return switches


connIn = sqlite3.connect('publications20180322_184739 _erweitert_mostCited20180419_092453_category20180419_175633.db')
dfPublications = pandas.read_sql('select * from publication', connIn)

c = 0
dfPublicationsObserved = pandas.DataFrame(columns=['title', 'authors'])
dfAuthors = pandas.DataFrame(
    columns=['name', 'count', 'countCS', 'countIS', 'releaseOrder', 'countContentbased', 'countCollaborative',
             'countHybrid', 'countNocat'])
for publication in dfPublications.itertuples():
    alreadyObserved = False
    for observedPub in dfPublicationsObserved.itertuples():
        if match_title(publication.title, observedPub.title, match_authors(publication.authors, observedPub.authors)):
            alreadyObserved = True
            break

    if not alreadyObserved:
        authors = ast.literal_eval(publication.authors)
        for author in authors:
            authorExistsIndex = -1
            for existingAuthor in dfAuthors.itertuples():
                if match_author(author, existingAuthor.name):
                    authorExistsIndex = existingAuthor.Index
                    break

            countCS = 1 if publication.field == 'CS' else 0
            countIS = 1 if publication.field == 'IS' else 0
            countContentbased = 1 if publication.category == 'contentbased' else 0
            countCollaborative = 1 if publication.category == 'collaborative' else 0
            countHybrid = 1 if publication.category == 'hybrid' else 0
            countNocat = 1 if publication.category == 'nocat' else 0

            if authorExistsIndex == -1:
                dfAuthors = dfAuthors.append({'name': author, 'count': 1, 'countCS': countCS, 'countIS': countIS,
                                              'releaseOrder': [(publication.year, publication.field)],
                                              'countContentbased': countContentbased,
                                              'countCollaborative': countCollaborative,
                                              'countHybrid': countHybrid,
                                              'countNocat': countNocat}, ignore_index=True)
            else:
                dfAuthors.set_value(authorExistsIndex, 'count', dfAuthors.iloc[authorExistsIndex]['count'] + 1)
                dfAuthors.iloc[authorExistsIndex]['releaseOrder'].append((publication.year, publication.field))
                dfAuthors.set_value(authorExistsIndex, 'countCS',
                                    dfAuthors.iloc[authorExistsIndex]['countCS'] + countCS)
                dfAuthors.set_value(authorExistsIndex, 'countIS',
                                    dfAuthors.iloc[authorExistsIndex]['countIS'] + countIS)
                dfAuthors.set_value(authorExistsIndex, 'countContentbased',
                                    dfAuthors.iloc[authorExistsIndex]['countContentbased'] + countContentbased)
                dfAuthors.set_value(authorExistsIndex, 'countCollaborative',
                                    dfAuthors.iloc[authorExistsIndex]['countCollaborative'] + countCollaborative)
                dfAuthors.set_value(authorExistsIndex, 'countHybrid',
                                    dfAuthors.iloc[authorExistsIndex]['countHybrid'] + countHybrid)
                dfAuthors.set_value(authorExistsIndex, 'countNocat',
                                    dfAuthors.iloc[authorExistsIndex]['countNocat'] + countNocat)

        dfPublicationsObserved = dfPublicationsObserved.append(
            {'title': publication.title, 'authors': publication.authors}, ignore_index=True)

    c += 1
    if c % 100 == 0:
        print('iteration: #' + str(c))

timestring = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
connOut = sqlite3.connect('mostActiveAuthors' + timestring + '.db')
cursorOut = connOut.cursor()

cursorOut.execute('CREATE TABLE IF NOT EXISTS author('
                  'name TEXT NOT NULL,'
                  'count INTEGER NOT NULL,'
                  'countCS INTEGER NOT NULL,'
                  'countIS INTEGER NOT NULL,'
                  'releaseOrder TEXT NOT NULL,'
                  'fieldSwitches INTEGER NOT NULL,'
                  'countContentbased INTEGER NOT NULL,'
                  'countCollaborative INTEGER NOT NULL,'
                  'countHybrid INTEGER NOT NULL,'
                  'countNocat INTEGER NOT NULL'
                  ')')
for author in dfAuthors.itertuples():
    releaseOrderSorted = copy.deepcopy(author.releaseOrder)
    releaseOrderSorted = sorted(releaseOrderSorted, key=lambda x: x[0])
    fieldSwitches = countFieldSwitches(releaseOrderSorted)
    cursorOut.execute('INSERT INTO author VALUES(?,?,?,?,?,?,?,?,?,?)', (
        author.name, author.count, author.countCS, author.countIS, str(releaseOrderSorted), fieldSwitches,
        author.countContentbased, author.countCollaborative, author.countHybrid, author.countNocat))

connOut.commit()
