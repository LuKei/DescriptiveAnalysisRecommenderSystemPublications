import pandas
import sqlite3
import ast
import Levenshtein
from fuzzywuzzy import fuzz
import datetime

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

connIn = sqlite3.connect('publications20180322_184739 _erweitert.db')
dfPublications = pandas.read_sql('select * from publication', connIn)
dfPublicationsObserved = pandas.DataFrame(columns=['title', 'authors'])
dfAuthors = pandas.DataFrame(columns=['author', 'count'])

c = 0
for pub in dfPublications.itertuples():
    alreadyObserved = False
    for observedPub in dfPublicationsObserved.itertuples():
        if match_title(pub.title, observedPub.title, match_authors(pub.authors, observedPub.authors)):
            alreadyObserved = True
            break

    if not alreadyObserved:
        authors = ast.literal_eval(pub.authors)
        for author in authors:
            authorExistsIndex = -1
            for existingAuthor in dfAuthors.itertuples():
                if match_author(author, existingAuthor.author):
                    authorExistsIndex = existingAuthor.Index
                    break

            if authorExistsIndex == -1:
                dfAuthors = dfAuthors.append({'author': author, 'count': 1}, ignore_index=True)
            else:
                dfAuthors.set_value(authorExistsIndex, 'count', dfAuthors.iloc[authorExistsIndex]['count'] + 1)

        dfPublicationsObserved = dfPublicationsObserved.append({'title': pub.title, 'authors': pub.authors}, ignore_index=True)

    c += 1
    if c % 100 == 0:
        print('iteration: #' + str(c))


#dfAuthors.to_pickle('authorCount.pkl')
timestring = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
connOut = sqlite3.connect('mostActiveAuthors' + timestring + '.db')
cursorOut = connOut.cursor()

cursorOut.execute('CREATE TABLE IF NOT EXISTS author('
                  'name TEXT NOT NULL,'
                  'count INTEGER NOT NULL'
                  ')')
for author in dfAuthors.itertuples():
    cursorOut.execute('INSERT INTO author VALUES(?,?)', (author.author, author.count))

connOut.commit()
















