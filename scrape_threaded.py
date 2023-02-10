import requests
import csv
from time import sleep
import multiprocessing as mp
import os


COMPANIES_URL = "https://arc.dev/_next/data/X_2l5AhHs8aPrzkuZrp7z/en/companies.json"
COMPANIES_DETAILS_URL = "https://arc.dev/_next/data/X_2l5AhHs8aPrzkuZrp7z/en/company/"

CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE")) or 2
TOTAL_PAGES = int(os.environ.get("TOTAL_PAGES")) or 100

FAIL = "\033[91m"
ENDC = "\033[0m"

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
                                data["name"] or "N/A",
                                data["linkedinUrl"] or "N/A",
                                str(
                                    f'{data["headquarters"]["location"]["latitude"] or "N/A"}, {data["headquarters"]["location"]["longitude"] or "N/A"}'
                                ),
                                data["headquarters"]["name"] or "N/A",
                            ]
                        ]
                    )
                except Exception as e:
                    print(
                        FAIL,
                        e,
                        f'{COMPANIES_DETAILS_URL}{company["urlString"]}.json?companyUrlString={company["urlString"]}',
                        ENDC,
                    )
                    continue

        except Exception as e:
            print(FAIL, e, f"{COMPANIES_URL}?page={pageNo}", ENDC)
            continue
    print(f"Closing chunk {chunk_order}...")
    csvfile.close()


processes = []
if __name__ == "__main__":
    for i, chunk in enumerate(totalPagesChunks):
        chunk_order = i + 1
        try:
            p = mp.Process(target=write_csv, args=(chunk, chunk_order, i))
            processes.append(p)
            p.start()
        except Exception as e:
            print(FAIL, e, ENDC)

[process.join() for process in processes]