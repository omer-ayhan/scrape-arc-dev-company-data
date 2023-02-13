import pandas as pd
from constants import country_names


def sort_csv(file_name):
    df = pd.read_csv(file_name)
    rows = {
        "Company Name": df["Company Name"].to_list(),
        "LinkedIn URL": df["LinkedIn URL"].to_list(),
        "Location": df["Location"].to_list(),
        "Address": df["Address"].to_list(),
    }
    countries = {}

    print("Sorting CSV...")
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
    print("Done sorting CSV")


sort_csv("./csv/companies-merged.csv")
