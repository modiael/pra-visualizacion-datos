import time
import random
import requests
from bs4 import BeautifulSoup

count = 500000
maximum = 500000

while count < maximum:
    try:
        r = requests.get(f'https://games.crossfit.com/athlete/{count}')
        if r.status_code == 429:
            print('Rate limit reached, sleeping for 1 minute')
            time.sleep(60)
            continue
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            # select by css selector
            name = soup.select('.c-heading-page-cover')[0].text
            name = " ".join(name.split())
            personal_data = soup.select('.infobar')[0].text
            personal_data = " ".join(personal_data.split())
            benchmarks = soup.select('.stats-container')[0].text
            benchmarks = " ".join(benchmarks.split())

            # append line to csv file
            with open('athletes.csv', 'a') as f:
                f.write(f'{count},{name},{personal_data},{benchmarks}\n')
    except Exception as e:
        with open('error.log', 'a') as f:
            f.write(f'{count},{e}\n')

    # always create a new file with the last count processed
    with open('last_count.txt', 'w') as f:
        f.write(f'{count}')
    # sleep random time between 0 and 1 seconds
    time.sleep(random.randint(0, 1))
    count += 1
