from audioop import add
import csv
import requests
from bs4 import BeautifulSoup
from pprint import pprint
import time
import json

def get_websites():
    websites = {}
    websites_list = []
    with open('input_data.csv', newline='') as csvfile:
        readcsvfile = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in readcsvfile:
            url = row[0]
            websites_list.append(url)
            # websites[url] = {'company_name':'','address':'','sectors':'', 'url':url}

    return websites_list

def scrape_website(url):

    # grab the html
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    # find the messy areas we need
    elems = soup.find_all(class_='grid__item')
    raw_address = soup.find_all(name='address')
    raw_sectors = soup.find(class_='u-mb-30')

    # narrow it down.
    elem_company_name = elems[3]
    elem_company_address = raw_address[0]
    elem_sectors = raw_sectors

    # do the formatting for company name
    company_name = str(elem_company_name).split('\n')[1].strip('<h1>').strip('</').strip('\n')

    # do the formatting for the address
    address = str(elem_company_address).split('\n')[1].strip('</address>').strip().strip('\n')
    
    # do the formatting for the sectors
    sectors = []
    for sector in elem_sectors:
        if str(sector).startswith('<li>'):
            sector = str(sector).strip('<li>').strip('</').strip('\n')
            sectors.append(sector)

    website_data = {
        'url':url,
        'company_name':company_name,
        'address':address,
        'sectors': ','.join(sectors),
    }
    
    return website_data

def write_to_csv(data):
    csv_file = open("output_data.csv", "a", newline='')
    writer = csv.writer(csv_file)
    # for key, value in data.items():
        # writer.writerow([key, value])

    writer.writerow([data['url'], data['company_name'], data['address'], data['sectors']])
    csv_file.close()

def read_output_data():
    processed_websites = []
    output_urls = []
    with open('output_data.csv', newline='') as csvfile:
        readcsvfile = csv.reader(csvfile, delimiter=',', quotechar='"', skipinitialspace=True)
        for row in readcsvfile:
            processed_website = row[0:1][0]
            processed_websites.append(processed_website)
    
    return processed_websites        
            


def scrape_websites(websites, batchsize=20, restseconds=5, intervalseconds=0):

    # get all the previously processed websites
    processed_websites = read_output_data()

    count = 1
    total_count = 1
    for url in websites:
        
        # first check if it's already been scraped
        if url in processed_websites:
            # print(url, 'has already been processed.')
            pass
        else:
            print(total_count, ':', count, 'scraping', url)
            try:
                data = scrape_website(url)
                # pprint(data)
                # print('---------')
                write_to_csv(data)
            except:
                print('!!!! failed to scape', url, '!!!!!!!!!!!')
                continue

            count += 1
            total_count += 1

            # add an interval between scrapes
            if intervalseconds > 0:
                time.sleep(intervalseconds)

            # enforce batches with a rest between
            if not batchsize == 0:
                if count > batchsize:
                    count = 1
                    print('resting for', restseconds, 'seconds...')
                    time.sleep(restseconds)

scrape_websites(get_websites())
# read_output_data()