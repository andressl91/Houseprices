from bs4 import BeautifulSoup
import urllib.request
import csv
from datetime import datetime

from ap.harvester.harvester import ActivityABC

def get_soup():                                                
    with urllib.request.urlopen('https://www.finn.no/realestate/homes/search.html?location=0.20003') as html_code: 
        read_html = html_code.read()                                    

    soup = BeautifulSoup(read_html, "html.parser")    
    return soup

def get_specific_soup(finn_code: str):
    with urllib.request.urlopen(f"https://www.finn.no/realestate/homes/ad.html?finnkode={finn_code}") as html_code:
        read_html = html_code.read()

    soup = BeautifulSoup(read_html, "html.parser")
    return soup

def get_finn_ids():
    soup = get_soup()
    finn_ids = []
    # All realestates on one page
    all_realestate_boxes = soup.find_all("div", class_="unit flex align-items-stretch result-item")

    for i in all_realestate_boxes:
        # Adresses
        finn_id = i.find("a")['id']
        finn_ids.append(finn_id)

    return finn_ids

def finn():
    soup = get_soup()
    data = []
    # All realestates on one page
    all_realestate_boxes = soup.find_all("div", class_="unit flex align-items-stretch result-item")

    #TODO: Consider Object programming of a site with attributes adress, finn_id... in constructor f.eks
    #TODO: Consider aggreate methods, to aggregate all realastate according to some option, f.eks price range or sq_m > 50 ..

    for i in all_realestate_boxes:
        #Adresses
        adress = i.find("div", class_="licorice valign-middle").string
        finn_id =  i.find("a")['id']
        sq_m =  i.find("p", {"class":"t5 word-break mhn"}).get_text().split('\n')[1]
        price_nok = i.find("p", {"class":"t5 word-break mhn"}).get_text().split('\n')[2]
        print("Description: \n%s" % i.find("h3").string)

        print("Done one realestate unit \n")
        data.append((finn_id, adress, sq_m, price_nok))

    with open('index.csv', 'a') as csv_file:
        writer = csv.writer(csv_file)
        for finn_id, adress, sq_m, price_nok in data:
            writer.writerow([finn_id, adress, sq_m, price_nok])

    adresses = soup.find_all("div", class_="licorice valign-middle")
    # print(adresses[0].string)

    # Naive test
    print(len(all_realestate_boxes), len(adresses))

def finn2():
    finn_ids = get_finn_ids()
    print(finn_ids)
    for id in finn_ids:
        # soup = get_specific_soup(115039377)
        try:
            soup = get_specific_soup(id)
            t = soup.find_all("div", class_="bd word-break")
            print(t[0].find_all(['p'])[0])
        except:
            print(soup)

finn2()