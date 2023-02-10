import requests
import csv
from time import sleep
import os

COMPANIES_URL = "https://arc.dev/_next/data/X_2l5AhHs8aPrzkuZrp7z/en/companies.json"
COMPANIES_DETAILS_URL = "https://arc.dev/_next/data/X_2l5AhHs8aPrzkuZrp7z/en/company/"

CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE")) or 2
TOTAL_PAGES = int(os.environ.get("TOTAL_PAGES")) or 100


FAIL = "\033[91m"
ENDC = "\033[0m"

empty_txt = "Empty"

fields = [
    "Company Name",
    "LinkedIn URL",
    "Location",
    "Address",
]


totalPagesChunks = [
    int((TOTAL_PAGES * (i + 1)) / CHUNK_SIZE) for i in range(CHUNK_SIZE)
]


def write_csv(chunk, chunk_order, i):
    csvfile = open(f"./csv/companies-{chunk_order}.csv", "a")
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(fields)
    for page in range(0 if i == 0 else totalPagesChunks[i - 1], chunk):
        try:
            pageNo = page + 1
            sleepVal = 10
            print(f"Chunk: {chunk_order} - Page: {pageNo}")
            print(f"Sleeping for {sleepVal} seconds...")
            sleep(sleepVal)
            resp = requests.get(f"{COMPANIES_URL}?page={pageNo}")
            companyDetailsData = resp.json()["pageProps"]["companies"]["companies"]
            for company in companyDetailsData:
                try:
                    resp = requests.get(
                        f'{COMPANIES_DETAILS_URL}{company["urlString"]}.json?companyUrlString={company["urlString"]}'
                    )
                    data = resp.json()["pageProps"]["companyData"]

                    print(f'Writing {company["name"]} - {company["urlString"]}')
                    csvwriter.writerows(
                        [
                            [
                                data["name"] or empty_txt,
                                data["linkedinUrl"] or empty_txt,
                                str(
                                    f'{data["headquarters"]["location"]["latitude"] or empty_txt}, {data["headquarters"]["location"]["longitude"] or empty_txt}'
                                ),
                                data["headquarters"]["name"] or empty_txt,
                            ]
                        ]
                    )
                except Exception as e:
                    print(
                        FAIL,
                        e.with_traceback(e.__traceback__),
                        f'{COMPANIES_DETAILS_URL}{company["urlString"]}.json?companyUrlString={company["urlString"]}',
                        ENDC,
                    )
                    continue

        except Exception as e:
            print(FAIL, e, f"{COMPANIES_URL}?page={pageNo}", ENDC)
            continue
    print(f"Closing chunk {chunk_order}...")
    csvfile.close()


for i, chunk in enumerate(totalPagesChunks):
    chunk_order = i + 1
    write_csv(chunk, chunk_order, i)
