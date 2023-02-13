import os
from time import sleep
import requests
import pandas as pd

COMPANIES_URL = os.environ.get("COMPANIES_URL")
COMPANIES_DETAILS_URL = os.environ.get("COMPANIES_DETAILS_URL")

FAIL = "\033[91m"
ENDC = "\033[0m"

empty_txt = "Empty"

fields = [
    "Company Name",
    "LinkedIn URL",
    "Location",
    "Address",
]


def custom_write_csv(start, end, file_name):
    rows = {
        "Company Name": [],
        "LinkedIn URL": [],
        "Location": [],
        "Address": [],
    }
    for page in range(start, end):
        try:
            pageNo = page + 1
            sleepVal = 1
            print(f"Page: {pageNo}")
            print(f"Sleeping for {sleepVal} seconds...")
            sleep(sleepVal)
            resp = requests.get(f"{COMPANIES_URL}?page={pageNo}")
            companyDetailsData = resp.json()["pageProps"]["companies"]["companies"]
            for company in companyDetailsData:
                try:
                    sleep(1)
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
                    pd.DataFrame(rows).to_csv(file_name, index=False)
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


custom_write_csv(0, 100, "./csv/companies-1.csv")
