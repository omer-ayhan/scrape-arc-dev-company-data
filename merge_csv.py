import pandas as pd

all_data = [pd.read_csv(f"./csv/companies-{i}.csv") for i in range(1, 7)]

merged = pd.concat(all_data)

merged.to_csv("./csv/companies-merged.csv", index=False)
