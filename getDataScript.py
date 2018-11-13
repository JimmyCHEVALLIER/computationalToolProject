import wikipedia
from bs4 import BeautifulSoup
import re

def getWikiArticle(strTitle):
    res = wikipedia.WikipediaPage(title=strTitle) # get HTML content of the article
    soup = BeautifulSoup(res.html(), 'html.parser')
    parsedTable = [[th.text, td.get_text("|")] for line in soup.find("table", {"class": "infobox vevent"}).find_all('tr') for th in line('th') for td in line('td')]
    contentDict = {str(item[0]): list(filter(lambda x: x not in ['(', ' (', '', ' ', ')', ' )', ', '], re.sub(r" ?\([^)]+\)", "", re.sub(r" ?\[[^)]+\]", "", item[1:][0])).replace(r"\(.*\)", "").replace("\n", "").replace("\xa0", " ").split("|"))) for item in parsedTable}
    contentDict["Plot"] = res.section("Plot") # append plot
    return contentDict


print(getWikiArticle('Atragon'))

