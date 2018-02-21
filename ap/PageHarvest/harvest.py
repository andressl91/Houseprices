import requests
from bs4 import BeautifulSoup

r = requests.get("http://pythonhow.com/example.html")
r = r.content

#One way
soup = BeautifulSoup(r, "html.parser")
print(soup.prettify())

#Other way, inspect page and find tags of interest
#Finds all tags
fi = soup.find_all("div", {"class": "cities"}) 
#Finds first tag
fi2 = soup.find("div", {"class": "cities"}) 
print(fi)
print()
print(fi2)

#Can use find_all("p") for paragraph


#Further use find to pinpoint
fi3 = fi[0].find_all("h2")
print()
print(fi3)
print()
print(fi3[0].text)
