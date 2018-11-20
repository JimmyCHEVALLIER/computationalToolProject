import wikipedia
from bs4 import BeautifulSoup
import re
import urllib.parse

def getWikiArticle(strTitle):
    res = wikipedia.WikipediaPage(title=strTitle) # get HTML content of the article
    soup = BeautifulSoup(res.html(), 'html.parser')
    parsedTable = [[th.text, td.get_text("|")] for line in soup.find("table", {"class": "infobox vevent"}).find_all('tr') for th in line('th') for td in line('td')]
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

#print(getWikiArticle('BABO'))
#res = wikipedia.WikipediaPage(title="Bitters_and_Blue_Ruin")  # get HTML content of the article

#print(getWikiCategorie('List of films: B'))

# create a big list of all name and store it

# create a file for each list with the data

count = 0
for elem in getWikiCategorie('List of films: B'):
    print(count)
    res = wikipedia.WikipediaPage(title=elem)
    count+=1

print("done")
