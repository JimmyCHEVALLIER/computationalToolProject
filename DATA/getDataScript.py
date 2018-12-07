import wikipedia
from bs4 import BeautifulSoup
import re
import urllib.parse
import string
import json
import os

def getWikiArticle(strTitle):
    contentDict = {}
    htmlElements = None
    res = wikipedia.WikipediaPage(title=strTitle) # get HTML content of the article
    if "pageid" in res.__dict__.keys():
        soup = BeautifulSoup(res.html(), 'html.parser')
        htmlElements = soup.find("table", {"class": "infobox vevent"})
    if htmlElements:
        parsedTable = [[th.text, td.get_text("|")] for line in htmlElements.find_all('tr') for th in line('th') for td in line('td')]
        contentDict = {str(item[0]): list(filter(lambda x: x not in ['(', ' (', '', ' ', ')', ' )', ', '], re.sub(r" ?\([^)]+\)", "", re.sub(r" ?\[[^)]+\]", "", item[1:][0])).replace(r"\(.*\)", "").replace("\n", "").replace("\xa0", " ").split("|"))) for item in parsedTable}
        contentDict["Plot"] = res.section("Plot") # append plot
    return contentDict

def getWikiCategorie(strTitle):
    res = wikipedia.WikipediaPage(title=strTitle)  # get HTML content of the article
    soup = BeautifulSoup(res.html(), 'html.parser')
    tags = [tag('a') for tag in soup.find_all('li')]
    data = []
    for tag in tags:
        if len(tag) == 0:
            continue
        tag = tag[0]
        if "href" in tag.attrs and "title" in tag.attrs:
            if tag.attrs['title'].startswith("List") or tag.attrs['title'].startswith("Template") or tag.attrs['title'].startswith("Edit"):
                pass
            else:
                #print("movie ->", tag.attrs['title'],tag.attrs['href'].rsplit('/', 1)[-1])
                data.append(urllib.parse.unquote(tag.attrs['href'].rsplit('/', 1)[-1]))
    return data

count = 0
res = {}

#generate list of letter accordingly to twitter category A,B,U-W,numbers...
lists = list(string.ascii_lowercase[:9].upper())
lists.append(['J-K','N-O','Q-R','U-W','X-Z','numbers'])
lists.append(list(string.ascii_lowercase[11:13].upper()))
lists.append(list(string.ascii_lowercase[15].upper()))
lists.append(list(string.ascii_lowercase[18:20].upper()))
lists = [item for sublist in lists for item in sublist]
lists= sorted(lists)
print(lists)


for element in lists: #[13:] replace by this to get the second part to split into 2 computers
    category = 'List of films: '+ element
    if os.path.exists(os.path.dirname(os.path.abspath(__file__))+ "/../_" + element + ".txt"):
        statinfo = os.stat(os.path.dirname(os.path.abspath(__file__))+ "/../_" + element + ".txt")

        if statinfo.st_size != 0:
            f = open("../_" + str(element) + ".txt", 'r')
            res = json.load(f)
            for i in res.keys():
                count += 1
                print(count)
    else:
        os.mknod(os.path.dirname(os.path.abspath(__file__))+ "/../_" + element + ".txt")
        f = open("../_" + str(element) + ".txt", 'w')

    print("==========> Category -> _", element)
    for elem in getWikiCategorie(category):
        if elem not in res.keys() :
            page = getWikiArticle(elem)
            if page != {} :
                res[elem] = getWikiArticle(elem)
                count+=1
                print('count-> ', count, 'name->', elem)

                f = open('../_' + element + '.txt', 'r+')
                f.truncate(0) # need '0' when using r+
                f.close()

                with open('../_' + element + '.txt', 'w') as file:
                    file.write(json.dumps(res))  # use `json.loads` to do the reverse

print("done")
