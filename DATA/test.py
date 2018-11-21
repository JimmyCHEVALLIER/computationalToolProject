import wikipedia
import os
import json
res = wikipedia.WikipediaPage(title="The Attacks of 26/11")  # get HTML content of the article
if "pageid" in res.__dict__.keys():
    print(res)
else:
    print("none")

#statinfo = os.stat('../MOVIES/_A.txt')
#print(statinfo.st_size)
f = open('../_A.txt')
import os

statinfo = os.stat(os.path.dirname(os.path.abspath(__file__)) +"/../_A.txt")
print(statinfo.st_size)
res = json.load(f)
print(len(res.keys()))
os.getcwd()
print("done")