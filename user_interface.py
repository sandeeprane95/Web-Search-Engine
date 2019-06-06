# coding: utf-8

# Name: Sandeep Rane
# UIN: 677515266
# netid: srane3

# In[1]:


import json, nltk, os, re, math, string
from nltk import PorterStemmer
from pathlib import Path
from flask import Flask, render_template, request


# In[2]:


invertedIndexPath = Path('invertedIndex.json')
pageRankPath = Path('pagerankScores.json')


# In[3]:


# Stopwords parsing

stopwordPath = Path('stopwords.txt')
stopword_list = set()

def parseStopwords():
    file_object = open(stopwordPath, 'r')
    for aStopword in file_object:
        aStopword = aStopword.lower()
        aStopword = re.split("\n",aStopword)
        stopword_list.add(aStopword[0])
    file_object.close()

parseStopwords()


# In[4]:


# Initialize stemmer

stemmer = PorterStemmer()


# In[5]:


# Initialize data structures for retrieval

invertedIndex = {}
pagerankScores = {}


# In[6]:


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


# In[7]:


#Retrieve the inverted index and the page rank from the json files
with open(invertedIndexPath) as fp:
    invertedIndex = json.load(fp)
with open(pageRankPath) as fp:
    pagerankScores = json.load(fp)


# In[8]:


# Compute idf for the entire corpus

def computeIdf(totalUrls):
    for word in invertedIndex:
        idfDict[word] = math.log((float(totalUrls)/urlFreq[word]),2)
        for url in invertedIndex[word]:
            invertedIndex[word][url] *= idfDict[word]
            if url in urlDen:
                urlDen[url] += invertedIndex[word][url] ** 2
            else:
                urlDen[url] = invertedIndex[word][url] ** 2


# In[9]:


# Compute the rank of pages based on the page ranking scores

def rankPages():
    rankList = []
    for url,score in pagerankScores.items():
        rankList.append((score,url))
    rankList.sort(reverse=True)
    for i,v in enumerate(rankList):
        rankDict[v[1]] = i + 1


# In[10]:


# Method for parsing the query and finding its tf-idf and cosine similarity scores to find top 200 pages 

def queryParse(user_query, simList, idfDict, urlDen, rankDict):
    queryIndex = {}
    tokens = tokenizer(user_query)
    
    for aToken in tokens:
        text = preprocessor(aToken)
        if(text == "x"):
            continue
        if text not in queryIndex:
            queryIndex[text] = 1
        else:
            queryIndex[text] += 1
    
    queryUrlFreq = {}
    queryDen = 0
    
    #Compute query scores
    for word in queryIndex:
        if word in invertedIndex:
            for url in invertedIndex[word]:
                if url in queryUrlFreq:
                    queryUrlFreq[url] += invertedIndex[word][url]*idfDict[word]*queryIndex[word]
                else:
                    queryUrlFreq[url] = invertedIndex[word][url]*idfDict[word]*queryIndex[word]
            queryDen += (queryIndex[word] * idfDict[word]) ** 2
    
    #Compute query similary score
    simScore = []
    for url in urlDen:
        if url in queryUrlFreq:
            score = queryUrlFreq[url]/math.sqrt(urlDen[url]*queryDen)
        else:
            score = 0
        simScore.append((score,url))
    simScore.sort(reverse=True)
    simList.append(simScore[0:30])


# In[21]:


# Find the top 30 pages out of the 200 pages using Page Rank Scores 

def assignRanks(simList,rankDict):
    finalRank = []
    for page in simList:
        tempArr = []
        for val in page:
            if val[1] in rankDict:
                tempArr.append((rankDict[val[1]], val[1]))
        tempArr.sort()
        finalRank.append(tempArr[0:30])
    finalRank = finalRank[0]
    for i in range(len(finalRank)):
        finalRank[i] = finalRank[i][1]
    return finalRank


# In[22]:


urlFreq = {}
for word, urlDict in invertedIndex.items():
    urlFreq[word] = len(urlDict)
idfDict = {}
urlDen = {}
rankDict = {}
computeIdf(3000)
rankPages()


# In[23]:


# Flask application

app = Flask(__name__)


# In[14]:


@app.route('/')
def search():
    return render_template('search.html')


# In[62]:


finalRankings = []
@app.route('/', methods=['POST'])
def get_user_input():
    if request.form['submit_button'] == 'Search':
        user_query = request.form['inputQuery']
        #global finalRankings
        simList = []
        queryParse(user_query,simList, idfDict, urlDen, rankDict)
        finalRankings = assignRanks(simList, rankDict)
        print(finalRankings)
        return render_template('search.html', your_list = enumerate(finalRankings[:10]))

if __name__ == '__main__':
    app.run()
