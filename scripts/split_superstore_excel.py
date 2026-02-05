import pandas as pd

source_file = "data/raw/Global_Superstore_Data.xlsx"

sheets = {
    "Orders": "orders.csv",
    "Returns": "returns.csv",
    "People": "people.csv",
}

for sheet, output in sheets.items():
    df = pd.read_excel(source_file, sheet_name=sheet)
    df.to_csv(f"data/raw/{output}", index=False)

print("Sheets successfully separated and saved as CSV.")
