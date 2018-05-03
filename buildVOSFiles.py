import pickle
import pandas
import sqlite3

citations = pickle.load(open('pickles/publicationCitations.pkl', 'rb'))
dbname = 'publications20180322_184739 _erweitert_mostCited20180419_092453_category20180419_175633'
conn_inn = sqlite3.Connection(dbname + '.db')
df_publications = pandas.read_sql('select * from publication', conn_inn)


def category_to_int(category):
    if category == 'contentbased':
        return 1
    if category == 'collaborative':
        return 2
    if category == 'hybrid':
        return 3
    return 4

with open('vos_mapfile' + '.txt', 'w', encoding='utf-8') as f:
    f.write('id\t')
    f.write('cluster\n')
    for publication in df_publications[df_publications['category'] != 'nocat'].itertuples():
        f.write(str(publication.id)+'\t')
        f.write(str(category_to_int(publication.category)) + '\n')



with open('vos_networkfile' + '.txt', 'w', encoding='utf-8') as f:
    for publication in df_publications[df_publications['category'] != 'nocat'].itertuples():
        for citation_id in citations[publication.id]:
            citation_category = df_publications[df_publications['id'] == citation_id]['category'].iloc[0]
            if citation_category != 'nocat':
                f.write(str(citation_id) + '\t')
                f.write(str(publication.id) + '\n')