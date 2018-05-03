import pickle
import pandas
import sqlite3
import time
import Levenshtein
import traceback


def match_title_and_id(pid, idSeries, title_match=False):
    return True if pid in idSeries.values and title_match else False


def match_title(title_a, title_b, author_match=False, threshold=(0.90, 0.80)):
    ratio = Levenshtein.ratio(title_a, title_b)
    return True if ((ratio > threshold[0]) or (
        ratio > threshold[1] and author_match)) else False


def match_authors(authors_a, authors_b, threshold=0.85):
    return True if Levenshtein.jaro_winkler(
        authors_a, authors_b) > threshold else False


def generate_reference_matches(reference_df, publication_df):
    '''
    Iterates of the reference dataframe to resolve duplicate papers and find citation intersections
    :param reference_df:
    :return: matched_df
    '''
    c, matches = 0, {}
    reference_df['ref_id'] = reference_df.index
    for publication in publication_df.itertuples():
        ind_series = reference_df.apply(lambda x: match_title_and_id(x['pid'] ,publication_df['id'], match_title(x['title'], publication.title,
                                                              match_authors(x['authors'], publication.authors)
                                                               )), axis=1)
        try:
            indices = reference_df[ind_series].ref_id.as_matrix()
            pid_indices = reference_df[ind_series].pid.as_matrix()
        except ValueError as err:
            traceback.print_tb(err.__traceback__)
            print('value of c: ' + str(c))
            c += 1
            continue
        if indices.size:
            matches[publication.id] = set(pid_indices)
            reference_df.drop(indices, inplace=True)
        else:
            matches[publication.id] = set(pid_indices)
        c += 1
        if c % 100 == 0:
            print(time.strftime("%H:%M:%S", time.gmtime()) + " #" + str(c))
    return matches


reference_df = pandas.read_pickle('pickles/citationHierarchy.pkl')
connIn = sqlite3.connect('publications20180322_184739 _erweitert.db')
publication_df = pandas.read_sql('select * from publication', connIn)

matches = generate_reference_matches(reference_df, publication_df)

with open('pickles/publicationCitations.pkl', 'wb') as f:
    pickle.dump(matches, f, pickle.HIGHEST_PROTOCOL)

print('success')