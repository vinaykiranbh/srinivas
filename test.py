import pandas as pd

def format_output(data):
    formatted_data = ""

    for _, row in data.iterrows():
        record_identifier = "PIC".ljust(3)
        ssn = row['TaxID'].ljust(9)
        first_name = row['Organization First Name'].ljust(16)[:16]
        middle_initial = (row['Organization Middle Name'][:1] if pd.notna(row['Organization Middle Name']) else '').ljust(1)
        last_name = row['Organization Last Name'].ljust(30)[:30]
        address = (row['Organization Street Line1 Address'] + " " + (row['Organization Street Line2 Address'] if pd.notna(row['Organization Street Line2 Address']) else '')).ljust(40)[:40]
        city = row['Organization City'].ljust(25)[:25]
        state = row['Organization State'].ljust(2)[:2]
        zip_code = row['Organization Zip Code'].split('-')[0].ljust(5)[:5]
        zip_extension = (row['Organization Zip Code'].split('-')[1] if '-' in row['Organization Zip Code'] else '').ljust(4)[:4]
        start_date = pd.to_datetime(row['Start Date of Contract']).strftime('%Y%m%d')
        amount = f"{int(float(row['Amount of Contract'].replace('$', '').replace(',', '')) * 100):011}"
        contract_exp = ' ' * 8
        ongoing_contract = ' '
        
        formatted_row = (record_identifier + ssn + first_name + middle_initial + last_name +
                         address + city + state + zip_code + zip_extension +
                         start_date + amount + contract_exp + ongoing_contract)
        
        formatted_data += formatted_row + '\n'

    return formatted_data

data = {
    'TaxID': ['123456789', '987654321'],
    'Organization First Name': ['John', 'Jane'],
    'Organization Middle Name': ['A', 'B'],
    'Organization Last Name': ['Doe', 'Smith'],
    'Organization Street Line1 Address': ['123 Elm St', '456 Oak St'],
    'Organization Street Line2 Address': ['', 'Apt 204'],
    'Organization City': ['Somewhere', 'Anywhere'],
    'Organization State': ['CA', 'NY'],
    'Organization Zip Code': ['12345-1234', '98765-9876'],
    'Start Date of Contract': ['2024-01-01', '2024-01-15'],
    'Amount of Contract': ['$600.00', '$1600.00']
}
df = pd.DataFrame(data)

formatted_text = format_output(df)

with open('output_format.txt', 'w') as f:
    f.write(formatted_text)
