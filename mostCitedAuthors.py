import pandas
import time
import Levenshtein
import traceback
import pickle
from datetime import datetime

def match_title(title_a, title_b, author_match=False, threshold=(0.90, 0.80)):
    ratio = Levenshtein.ratio(title_a, title_b)
    return True if ((ratio > threshold[0]) or (
        ratio > threshold[1] and author_match)) else False


def match_authors(authors_a, authors_b, threshold=0.85):
    return True if Levenshtein.jaro_winkler(
        authors_a, authors_b) > threshold else False


def generate_reference_matches(reference_df):
    '''
    Iterates of the reference dataframe to resolve duplicate papers and find citation intersections
    :param reference_df:
    :return: matched_df
    '''
    c, match = 0, []
    reference_df['ref_id'] = reference_df.index
    for reference in reference_df.itertuples():
        ind_series = reference_df.apply(lambda x: match_title(
            x['title'], reference.title, match_authors(x['authors'], reference.authors)), axis=1)
        try:
            indices = reference_df[ind_series].ref_id.as_matrix()
        except ValueError as err:
            traceback.print_tb(err.__traceback__)
            print('value of c: ' + str(c))
            c += 1
            continue
        if indices.size:
            match.append(set(indices))
            reference_df.drop(indices, inplace=True)
        c += 1
        if c % 100 == 0:
            print(time.strftime("%H:%M:%S", time.gmtime()) + " #" + str(c))
    return match


df = pandas.read_pickle('pickles/citationHierarchy.pkl')

matches = generate_reference_matches(df)

with open('pickles/matchedCitationHierarchy.pkl', 'wb') as f:
    pickle.dump(matches, f, pickle.HIGHEST_PROTOCOL)

print('success')