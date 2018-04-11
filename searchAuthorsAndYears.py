import xml.etree.ElementTree
import sqlite3
import pandas
import datetime
import time
import urllib
import re
from fuzzywuzzy import fuzz
from enum import Enum
from wos import WosClient

sResultFlag = None

class SearchResult(Enum):
    SINGLE = 1
    MULTI = 2
    NOTFOUND = 3

class Publication:
    def __init__(self, title, journal, abstract):
        self.title = title
        self.journal = journal
        self.abstract = abstract
        self.authors = []
        self.year = -1

def rmvSpcChrsAndMkLwer(inputString):
    inputString = re.sub('[^A-Za-z0-9äöü\-\s]', '', inputString)
    inputString = re.sub('\s+', ' ', inputString)
    inputString = inputString.lower()
    return inputString


def setResFlagIfPubComp(publication):
    global sResultFlag
    if (publication.year != -1) and len(publication.authors) > 0:
        sResultFlag = SearchResult.SINGLE


def searchPublicationInfoScopus(publication, scopusDf, publicationId):
    year = scopusDf.iloc[publicationId]['year']
    authors = scopusDf.iloc[publicationId]['authors']
    if (type(year) is int) and (publication.year == -1):
        publication.year = int(year)
    if (type(authors) is list) and (len(authors) > len(publication.authors)):
        publication.authors = authors
    setResFlagIfPubComp(publication)
    return publication


def buildPublicationInfoFromWOSRecord(publication, record):
    year = -1
    authors = []
    for labelValuePair in record.source:
        if labelValuePair.label == 'Published.BiblioYear':
            year = int(labelValuePair.value[0])
            break
    for authorName in record.authors[0].value:
        authors.append(authorName)
    if publication.year == -1:
        publication.year = year
    if len(authors) > len(publication.authors):
        publication.authors = authors
    return publication


def searchPublicationInfoWos(publication, client):
    global sResultFlag
    searchString = rmvSpcChrsAndMkLwer(publication.title)
    result = client.search(query='TI="' + searchString + '"', count=5, offset=1)

    if result.recordsFound > 1:
        sResultFlag = SearchResult.MULTI
        for record in result.records:
            if fuzz.ratio(rmvSpcChrsAndMkLwer(record.title[0].value[0]), rmvSpcChrsAndMkLwer(publication.title)) >= 98:
                try:
                    publication = buildPublicationInfoFromWOSRecord(publication, record)
                except AttributeError:
                    continue
                break
    elif result.recordsFound == 1:
        publication = buildPublicationInfoFromWOSRecord(publication, result.records[0])

    setResFlagIfPubComp(publication)

    time.sleep(0.5)
    return publication


def buildPublicationFromDblpHitNode(publication, hitNode):
    year = int(hitNode.find('info').find('year').text)
    authors = []
    try:
        for authorNode in hitNode.find('info').find('authors').findall('author'):
            authors.append(authorNode.text)
    except AttributeError:
        pass
    if publication.year == -1:
        publication.year = year
    if (len(authors) > len(publication.authors)):
        publication.authors = authors
    return  publication


def searchPublicationInfoDblp(publication):
    global sResultFlag
    searchString = rmvSpcChrsAndMkLwer(publication.title)
    searchString = searchString.replace(' ', '+')
    xmlString = urllib.request.urlopen('http://dblp.org/search/publ/api?q=' + searchString).read().decode('utf-8')
    xmlString = xmlString.replace('\n', '')
    xmlString = xmlString.replace('\r', '')

    root = xml.etree.ElementTree.fromstring(xmlString)
    hitsNode = root.find('hits')
    if int(hitsNode.attrib['total']) > 1:
        sResultFlag = SearchResult.MULTI
        for hitNode in hitsNode.findall('hit'):
            if fuzz.ratio(rmvSpcChrsAndMkLwer(hitNode.find('info').find('title').text), rmvSpcChrsAndMkLwer(publication.title)) >= 98:
                publication = buildPublicationFromDblpHitNode(publication, hitNode)
                break
    elif int(hitsNode.attrib['total']) == 1:
        publication = buildPublicationFromDblpHitNode(publication, hitsNode.find('hit'))

    setResFlagIfPubComp(publication)

    return publication


def createTables(cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS publication('
                   'id INTEGER UNIQUE NOT NULL, '
                   'title TEXT NOT NULL, '
                   'journal TEXT NOT NULL, '
                   'abstract TEXT NOT NULL, '
                   'authors TEXT NOT NULL,'
                   'year INTEGER)')


def main():
    global sResultFlag
    timestring = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    connIn = sqlite3.connect('paper2.db')
    dfSourceTitles = pandas.read_sql('select * from paper2', connIn)
    dfScopus = pandas.read_pickle('pickles/scopusData.pkl')

    connOut = sqlite3.connect('publications' + timestring + '.db')
    cursorOut = connOut.cursor()
    createTables(cursorOut)

    loopIdx = 0
    publicationCount = len(dfSourceTitles.index)

    while loopIdx < publicationCount:
        sid = input('Please put in SID of Web of Science session: ')
        with WosClient(SID=sid, lite=True, close_on_exit=False) as client:
            for i in range(0, 2400):
                if loopIdx >= publicationCount:
                    break
                sResultFlag = SearchResult.NOTFOUND
                publication = Publication(title = dfSourceTitles.loc[loopIdx]['title'],
                                          journal = dfSourceTitles.loc[loopIdx]['journal'],
                                          abstract = dfSourceTitles.loc[loopIdx]['abstract'])


                publication = searchPublicationInfoWos(publication, client)
                if sResultFlag == SearchResult.NOTFOUND:
                    publication = searchPublicationInfoDblp(publication)
                if sResultFlag == SearchResult.NOTFOUND:
                    publication = searchPublicationInfoScopus(publication, dfScopus,
                                                              dfSourceTitles.iloc[loopIdx]['index'])



                if sResultFlag == SearchResult.MULTI:
                    with open('logMulti' + timestring + '.txt', 'a', encoding='utf-8') as f:
                        f.write('mulitple records found: "' + publication.title + '"\n')
                    sResultFlag == None

                if sResultFlag == SearchResult.NOTFOUND:
                    with open('logNotFound' + timestring + '.txt', 'a', encoding='utf-8') as f:
                        f.write('Not found: "' + publication.title + '"\n')
                    sResultFlag == None

                cursorOut.execute("INSERT INTO publication VALUES(?,?,?,?,?,?)",
                                  (int(dfSourceTitles.loc[loopIdx]['id']), publication.title, publication.journal, publication.abstract,
                                   str(publication.authors), publication.year))
                connOut.commit()

                loopIdx += 1
                print('iteration: ' + str(loopIdx))



main()
