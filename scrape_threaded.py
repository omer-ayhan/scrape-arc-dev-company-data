import requests

# import csv
from time import sleep
import multiprocessing as mp
import os
import pandas as pd
from constants import country_names

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


def sort_csv(file_name):
    df = pd.read_csv(file_name)
    rows = {
        "Company Name": df["Company Name"].to_list(),
        "LinkedIn URL": df["LinkedIn URL"].to_list(),
        "Location": df["Location"].to_list(),
        "Address": df["Address"].to_list(),
    }
    countries = {}

    for i, address in enumerate(rows["Address"]):
        address: str
        i: int

        if address == "Empty":
            continue

        parsed_name = address.split(",")[-1].strip()

        for country_name in country_names:
            if parsed_name not in country_names[country_name]:

                if (
                    "other" in countries
                    and rows["Company Name"][i] in countries["other"]["Company Name"]
                ):
                    continue

                out_name = "other"
            else:
                out_name = country_name

            if out_name not in countries:
                countries[out_name] = {
                    "Company Name": [],
                    "LinkedIn URL": [],
                    "Location": [],
                    "Address": [],
                }
            else:
                countries[out_name]["Company Name"].append(rows["Company Name"][i])
                countries[out_name]["LinkedIn URL"].append(rows["LinkedIn URL"][i])
                countries[out_name]["Location"].append(rows["Location"][i])
                countries[out_name]["Address"].append(rows["Address"][i])

    for country in countries:
        currRow = countries[country]
        if len(currRow["Company Name"]) < 1:
            continue
        pd.DataFrame(currRow).to_csv(f"./csv/countries/{country}.csv", index=False)


def write_csv(chunk, chunk_order, i):
    rows = {
        "Company Name": [],
        "LinkedIn URL": [],
        "Location": [],
        "Address": [],
    }
    for page in range(0 if i == 0 else totalPagesChunks[i - 1], chunk):
        try:
            pageNo = page + 1
            sleepVal = 2
            print(f"Chunk: {chunk_order} - Page: {pageNo}")
            print(f"Sleeping for {sleepVal} seconds...")
            sleep(sleepVal)
            resp = requests.get(f"{COMPANIES_URL}?page={pageNo}")
            companyDetailsData = resp.json()["pageProps"]["companies"]["companies"]
            for company in companyDetailsData:
                try:
                    sleep(1.5)
                    resp = requests.get(
                        f'{COMPANIES_DETAILS_URL}{company["urlString"]}.json?companyUrlString={company["urlString"]}'
                    )
                    companyDetails = resp.json()["pageProps"]["companyData"]

                    print(f'Writing {company["name"]} - {company["urlString"]}')
                    rows["Company Name"].append(companyDetails["name"] or empty_txt)
                    rows["LinkedIn URL"].append(
                        companyDetails["linkedinUrl"] or empty_txt
                    )
                    rows["Location"].append(
                        str(
                            f'{companyDetails["headquarters"]["location"]["latitude"] or empty_txt}, {companyDetails["headquarters"]["location"]["longitude"] or empty_txt}'
                        )
                    )
                    rows["Address"].append(
                        companyDetails["headquarters"]["name"] or empty_txt
                    )
                    pd.DataFrame(rows).to_csv(
                        f"./csv/companies-{chunk_order}.csv", index=False
                    )
                except Exception as e:
                    fail_detail_url = f'{COMPANIES_DETAILS_URL}{company["urlString"]}.json?companyUrlString={company["urlString"]} \n'
                    print(
                        FAIL,
                        e.with_traceback(e.__traceback__),
                        resp.status_code,
                        fail_detail_url,
                        ENDC,
                    )
                    with open("./failedDetails.txt", "a+") as failed:
                        failed.write(str(fail_detail_url))
                    continue
        except Exception as e:
            fail_url = f"{COMPANIES_URL}?page={pageNo} \n"
            print(FAIL, e, fail_url, ENDC)
            with open("./failedDetails.txt", "a+") as failed:
                failed.write(str(fail_url))
            continue
    print(f"Closing chunk {chunk_order}...")


def JSONWorker():
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


sort_csv("./csv/companies-merged.csv")

# data1 = pd.read_csv("./csv/companies-1.csv")
# data2 = pd.read_csv("./csv/companies-2.csv")

# merged = pd.concat([data1, data2])

# merged.to_csv("./csv/companies-merged.csv", index=False)
