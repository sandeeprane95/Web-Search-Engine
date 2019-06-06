
# coding: utf-8

# Name: Sandeep Rane
# UIN: 677515266
# netid: srane3

# In[1]:


import time, nltk, os, re, string, math, queue, threading, ssl, json
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from nltk import PorterStemmer
from pathlib import Path
from bs4 import BeautifulSoup


# In[2]:


# Stopwords parsing

stopwordPath = Path('stopwords.txt')
stopword_list = []

def parseStopwords():
    file_object = open(stopwordPath, 'r')
    for aStopword in file_object:
        aStopword = aStopword.lower()
        aStopword = re.split("\n",aStopword)
        stopword_list.append(aStopword[0])
    file_object.close()

parseStopwords()


# In[3]:


# Initialize stemmer

stemmer = PorterStemmer()


# In[4]:


# Initialize Data structures for storage

invertedIndex = {}
pagerankScores = {}


# In[5]:


# Preprocessing methods

def tokenizer(content):
    content = content.lower()
    generatedTokens = content.split()
    return generatedTokens

def preprocessor(aToken):
    if aToken not in stopword_list:
        exclude = set(string.punctuation)
        new_s = ''.join(ch for ch in aToken if ch not in exclude)
        aToken = ''.join([i for i in new_s if not i.isdigit()])
        stemWord = stemmer.stem(aToken)
        if stemWord not in stopword_list:
            return stemWord
        return 'x'  
    return 'x'              


# In[ ]:


# Inverted Index Modules



# In[6]:


# PageRank Modules

class graphNode():    
    def __init__(self, srcUrl):
        self.url = srcUrl
        self.outEdges = set()
        self.score = 0
        self.timesAdj = 0
    def addEdge(self, destUrl):
        if destUrl not in self.outEdges:
            self.outEdges.add(destUrl)
            self.timesAdj += 1
            
def createNode(url, urlGraph):
    if url not in urlGraph:
        urlGraph[url] = graphNode(url)
        
def createEdge(srcUrl, destUrl, urlGraph):
    createNode(destUrl, urlGraph)
    urlGraph[srcUrl].addEdge(destUrl)
    
def computePageRank(urlGraph):
    dF = 0.85
    n = 10
    v = len(urlGraph)
    
    # Initialize pagerank scores
    for url, urlNode in urlGraph.items():
        urlNode.score = 1/float(v)
    
    # Compute actual scores - If a node has no outlinks, assign it a dangling score 
    for i in range(n):
        dangling = 0
        urlScores = {}
        for url, urlNode in urlGraph.items():
            if(len(urlNode.outEdges) == 0):
                dangling += urlNode.score/v
                continue
            temp = urlNode.score/urlNode.timesAdj
            for destUrl in urlNode.outEdges:
                if destUrl not in urlScores:
                    urlScores[destUrl] = temp
                else:
                    urlScores[destUrl] += temp
        for url in urlGraph:
            val = 0
            if url in urlScores:
                val = urlScores[url]
            urlGraph[url].score = (1-dF)*(1/float(v)) + dF*(val+dangling)
            
    for url, urlNode in urlGraph.items():
        pagerankScores[url] = urlNode.score


# In[7]:


# Single Web Page Parser

def webpageParse(currUrl, q, uniqueUrls, vocabulary, urlGraph):
    req = Request(currUrl)
    try:
        response = urlopen(currUrl)
    except HTTPError as e:
        return 
    except URLError as e:
        return
    except ssl.CertificateError:
        return
    
    soup = BeautifulSoup(response, from_encoding=response.info().get_param('charset'))
    
    # Extract links in a webpage
    urls = []
    for aLink in soup.find_all('a', href=True):
        aLink = aLink['href']
        if aLink.find('#'):
            aLink = aLink.split('#')
            aLink=aLink[0]
        if len(aLink)>=1 and aLink[-1]!='/':
            aLink += '/'
        aLinkParts = aLink.split('://')
        if len(aLinkParts)>1 and aLinkParts[0][:4]=='http':
            if len(aLinkParts[0])>4 and aLinkParts[0][4]=='s':
                aLinkParts[0] = 'http'
            if aLinkParts[1][:4] == "www.":
                aLinkParts[1] = aLinkParts[1][4:]
            parts = aLinkParts[1].split('/')
            if 'uic.edu' in parts[0]:
                urls.append(aLinkParts[0]+'://'+aLinkParts[1])
        if len(aLinkParts)==1:
            if len(aLinkParts[0])>1 and aLinkParts[0][0]=='/':
                urls.append(currUrl + aLinkParts[0][1:])
                    
    # Update the queue and add an edge from current URL to all URLs connected to it
    createNode(currUrl, urlGraph)
    for aUrl in urls:
        if aUrl not in uniqueUrls:
            uniqueUrls.add(aUrl)
            q.put(aUrl)
        createEdge(currUrl, aUrl, urlGraph)
    
    # Extract the contents of a webpage
    # Compute inverted index : Dictionary with keys as words and values as dictionary(urls as keys and occurence in url as vallues)    
    content = soup.find_all(('p', 'a', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'))
    for aObject in content:
        text = aObject.text.strip()
        tokens = tokenizer(text)
        for aToken in tokens:
            newToken = preprocessor(aToken)
            if not (len(newToken)<3):
                if newToken not in invertedIndex:
                    invertedIndex[newToken] = {}
                    invertedIndex[newToken][currUrl] = 1
                    vocabulary[newToken] = 1
                else:
                    if currUrl in invertedIndex[newToken]:
                        invertedIndex[newToken][currUrl] += 1
                    else:
                        invertedIndex[newToken][currUrl] = 1
                    vocabulary[newToken] += 1


# In[8]:


# File write methods

def saveToFile():
    #Store the inverted index and the page rank in two separate json files
    with open('invertedIndex.json', 'w') as fp:
        json.dump(invertedIndex, fp)
    with open('pagerankScores.json', 'w') as fp:
        json.dump(pagerankScores, fp)


# In[9]:


# Web Crawler

def crawlWeb(startUrl):
    a = time.time()
    q = queue.Queue()
    uniqueUrls = set()
    q.put(startUrl)
    uniqueUrls.add(startUrl)
    urlCount = 1
    vocabulary = {}
    urlGraph = {}
    while urlCount<3000 and not q.empty():
        if q.qsize()>75:
            urlCrawlers = [threading.Thread(target=webpageParse, args=([q.get(), q, uniqueUrls, vocabulary, urlGraph]), kwargs={}) for x in range(75)]
            for subCrawler in urlCrawlers:
                subCrawler.start()
            for subCrawler in urlCrawlers:
                subCrawler.join()
            urlCount += 75
        else:
            webpageParse(q.get(), q, uniqueUrls, vocabulary, urlGraph)
            urlCount += 1
        print(urlCount)
    computePageRank(urlGraph)
    print(time.time()-a)
    
    saveToFile()


# In[10]:


crawlWeb('http://www.cs.uic.edu/')

