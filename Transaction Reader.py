"""
This transaction reader was created with the intention of increasing Python proficency. The purpose of this program is to accept bank csv files that show the transactions for a particular Month/Year,
categorize each line description into a spending or earning category. It gives the option to print the lines processed to check for errors, see the categorizing and the totals. It also allows for the
unknown categories to be listed so the store and earning dictionaries can be expanded over time with personal spending habbits. 
Finally, the program uploads the processed information to a local SQL Server where the data is stored and used with a PowerBI Dashboard to provide an easy view at monthly transactions in a personalized
way. 
"""
# Import here
import pandas as pd
import os
import copy
import pyodbc
from datetime import datetime

# Initialize variables and df here
yearly_data = {}
payments_by_category = {}
delimiter = ','
file_name = ''

# Separate payments and purchases. Here are the different functions pre-built.
payments = []
purchases = []
total_spent_amounts = {}
total_paid_amounts = {}
spent_category_percentages = {}
paid_category_percentages = {}
paid_category_totals = {}
spent_category_totals = {}
payments_by_category = {}
spending_by_category = {}

# Customize based on stores and sources of income you frequent
Store_Category = {
    "Mart Super" : "Groceries",
    "Target" : "Groceries",
    "ATM" : "Hobbies",
    "AMZN" : "Amazon",
    "MUGS" : "Fast Food",
    "ADVANCED" : "Pets",
    "FAT" : "Fast Food",
    "UBER" : "Uber",
    "MICROSOFT" : "Entertainment",
    "HULU" : "Entertainment",
    "EMBRACE" : "Pets",
    "RILVALRY" : "Haircuts",
    "FORTCOLUTILITIES" : "Utilities",
    "LOAF" : "Gas",
    "SAFEWAY" : "Groceries",
    "MURPHY'S" : "Fast Food",
    "DOORDASH" : "Door-Dash",
    "FRONT" : "Education",
    "USAA" : "Insurance",
    "DAVES" : "Fast Food",
    "TMOBILE" : "Cell Phone",
    "WM" : "Groceries",
    "CHEWY" : "Pets",
    "STARBUCKS" : "Coffee",
    "Green" : "Hobbies",
    "COCA": "Snacks",
    "WAL-MART" : "Groceries",
    "WELLMART" : "Hobbies",
    "HOME"  : "Household",
    "STEAM" : "Hobbies",
    "Paid Check" : "Rent",
    "NORDSTROM" : "Clothing",
    "CSU CAM LOBBY" : "Clothing",
    "MAX": "Enterainment",
    "RIVALRY" : "Haircut",
    "AMAZON" : "Shopping", 

}

Source_Category = {
    "Job1": "Income",
    "Job2": "Income",
    "VENMO": "Transfer",
    "ATM Rebate": "Cashback ATM FEE",
    "Dividend": "Dividend",
    "VACP": "VA Benefits"
}

def process_transaction_file(file_path, yearly_data, total_spent_amounts, total_paid_amounts, year, month):
    df = None
    count = 0
    try:
        df = pd.read_csv(file_path, sep=',', parse_dates=[0])
        

        if not year or not month:
            print("Invalid input. Year and month are required.")
            return

        if year not in yearly_data:
            yearly_data[year] = {}
        if month not in yearly_data[year]:
            yearly_data[year][month] = []

        spent_category_totals = {}
        paid_category_totals = {}

        for index, row in df.iterrows():
            count += 1
            # Check if the date, description, and either debit or credit are not NaN
            if pd.notna(row['Date']) and pd.notna(row['Description']) and (pd.notna(row['Debit']) or pd.notna(row['Credit'])):
                # The Description must be a string, ensure it's not NaN before calling .lower()
                description = row["Description"]
                if isinstance(description, str):
                    description = description.lower()
                debit = row["Debit"] if pd.notna(row["Debit"]) else 0
                credit = row["Credit"] if pd.notna(row["Credit"]) else 0

                transaction_type = "Spending" if debit > 0 else "Income"

                category = "Unknown"
                for key, value in (Store_Category.items() if transaction_type == "Spending" else Source_Category.items()):
                    if key.lower() in description.lower():
                        category = value
                        break

                if transaction_type == "Spending":
                    spent_category_totals[category] = spent_category_totals.get(category, 0) + debit
                else:
                    paid_category_totals[category] = paid_category_totals.get(category, 0) + credit

                df.at[index, "Category"] = category
            else:
                print(f"Skipping row {count} due to missing data.")
                continue

        total_spent_amounts[year] = total_spent_amounts.get(year, 0) + sum(spent_category_totals.values())
        total_paid_amounts[year] = total_paid_amounts.get(year, 0) + sum(paid_category_totals.values())

        yearly_data[year][month].append(df)
        print(f"File processed successfully for {month} {year}. Processed {count} lines.")

    except Exception as e:
        print(f"Error processing the file at line {count}:", e)
        if df is not None:
            print("Partial data loaded. Please check your file for any issues.")


def print_transaction_data(yearly_data, year, month):
    print(f"Details for {month} {year}:")

    if year in yearly_data and month in yearly_data[year]:
        df = yearly_data[year][month][0]

        total_spent_month = 0
        total_paid_month = 0

        spending_data = df[df['Debit'] > 0]
        income_data = df[df['Credit'] > 0]

        print("\nSpending Data by Category:")
        for category, category_df in spending_data.groupby("Category"):
            print(f"Category: {category}")
            print(category_df[['Description', 'Debit']])
            total_spent_month += category_df["Debit"].sum()

        print("\nIncome Data by Category:")
        for category, category_df in income_data.groupby("Category"):
            print(f"Category: {category}")
            print(category_df[['Description', 'Credit']])
            total_paid_month += category_df["Credit"].sum()

        print("\nFinancial Report:")
        print(f"Total Spending: ${total_spent_month:.2f}")
        print(f"Total Income: ${total_paid_month:.2f}")

def print_unknown_category_payments(yearly_data, year, month):
    print("\nUnknown Category Payments:")
    unknown_category_payments = 0

    if year in yearly_data and month in yearly_data[year]:
        df = yearly_data[year][month][0]  # Assuming there's only one DataFrame per month

        # Filter for unknown category payments
        unknown_payments = df[df['Category'] == "Unknown"]

        if unknown_payments.empty:
            print("No payments in the Unknown category.")
        else:
            for _, row in unknown_payments.iterrows():
                date = row["Date"]
                description = row["Description"]
                amount = row["Debit"] if row["Debit"] > 0 else row["Credit"]
                print(f"{date}: {description}, Amount: {amount:.2f}")
                unknown_category_payments += amount

            print(f"Total Unknown Category Payments: {unknown_category_payments:.2f}")
    else:
        print("No data available for the specified month and year.")

def backup_to_database(yearly_data, server, database):
    try:
        # Connection string for trusted connection (Windows Authentication)
        conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Insert data into the table
        for year, months in yearly_data.items():
            for month, data in months.items():
                df = data[0]
                for _, row in df.iterrows():
                    converted_date = convert_datetime(row['Date'])
                    
                    # Ensure that Debit and Credit are floats or None
                    debit = float(row['Debit']) if pd.notna(row['Debit']) else None
                    credit = float(row['Credit']) if pd.notna(row['Credit']) else None
                    category = row['Category']

                    # Check if category exists
                    category_id_query = "SELECT CategoryID FROM Categories WHERE Name = ?"
                    cursor.execute(category_id_query, (category,))
                    category_id_result = cursor.fetchone()
                    category_id = category_id_result[0] if category_id_result else None

                    # If category does not exist, insert it
                    if category_id is None:
                        cursor.execute("INSERT INTO Categories (Name) VALUES (?)", (category,))
                        conn.commit()
                        cursor.execute(category_id_query, (category,))
                        category_id = cursor.fetchone()[0]

                    # Insert transaction
                    insert_query = '''
                        INSERT INTO Transactions (TransactionDate, Description, DebitAmount, CreditAmount, CategoryID)
                        VALUES (?, ?, ?, ?, ?)
                    '''
                    cursor.execute(insert_query, (converted_date, row['Description'], debit, credit, category_id))
        
        conn.commit()
        print("Backup completed successfully.")
    except Exception as e:
        print("Error during backup:", e)
    finally:
        conn.close()

#Currently upload issue due to format of Datetime #FIXME
def convert_datetime(date):
    # Check if date is None or NaT
    if pd.isnull(date) or date is pd.NaT:
        return None
    try:
        # If the date is already in datetime format, no need for strptime
        if isinstance(date, pd.Timestamp):
            # Convert datetime to date format SQL Server can accept
            return date.date().isoformat()
        elif isinstance(date, str):
            # If the date is a string, parse it assuming it is in 'YYYY-MM-DD HH:MM:SS' format
            converted_date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
            return converted_date.date().isoformat()
        else:
            # If it's not a string or a Timestamp, it's an unexpected datatype
            raise ValueError(f"Unrecognized date format: {date}")
    except ValueError as e:
        print(f"Date conversion error for date {date}: {e}")
        return None

# Main menu loop
while True:
    print("\nMain Menu:")
    print("1. Upload Transaction File")
    print("2. Print Transaction Data")
    print("3. Show Unknown Category Payments")
    print("4. Backup to Database")
    print("5. Quit")

    choice = input("Enter your choice (1-5): ")

    # Move df definition outside the try block
    df = None

    if choice == "1":
        year = input("Enter the year (e.g., '2023'): ")
        month = input("Enter the month (e.g., 'January', 'February'): ").capitalize()
        file_name = input("Enter the file name (with extension): ")
    
        try:
            # Construct the full path to the file
            script_dir = os.path.dirname(os.path.abspath(__file__)) #Gets the Absolute path for the script
            file_path = os.path.join(script_dir, file_name)

            # Read the CSV file
            df = pd.read_csv(file_path, sep=delimiter)

            # Process the transaction file
            process_transaction_file(file_path, yearly_data, total_spent_amounts, total_paid_amounts, year, month)

            # Update the data dictionary
            if year not in yearly_data:
                yearly_data[year] = {}
            if month not in yearly_data[year]:
                yearly_data[year][month] = []
            yearly_data[year][month].append(df)

            
        except Exception as e:
            print("Error processing the file:", e)
            if df is not None:
                print("Partial data loaded. Please check your file for any issues.")

    elif choice == "2":
        # Sub-menu to choose a specific year and month
        while True:
            print("\nChoose a Year:")
            for year in yearly_data.keys():
                print(year)

            print("0: Back to Main Menu")
            year_choice = input("Enter the year or '0' to go back: ")

            if year_choice == "0":
                break
            elif year_choice in yearly_data:
                while True:
                    print(f"\nChoose a Month for {year_choice}:")
                    for month in yearly_data[year_choice].keys():
                        print(month)

                    print("0: Back to Year Selection")
                    month_choice = input("Enter the month or '0' to go back: ")

                    if month_choice == "0":
                        break
                    elif month_choice in yearly_data[year_choice]:
                        # Call the function to print monthly transaction data
                        print_transaction_data(yearly_data, year_choice, month_choice)
                    else:
                        print("Invalid month. Please try again.")
            else:
                print("Invalid year. Please try again.")

    elif choice == "3":
        # Display a menu to choose a year
        print("\nChoose a Year:")
        for year in yearly_data.keys():
            print(year)

        selected_year = input("Enter the year: ")

        # Check if the selected year exists in the data
        if selected_year in yearly_data:
            # Display a menu to choose a month for the selected year
            print(f"\nChoose a Month for {selected_year}:")
            for month in yearly_data[selected_year].keys():
                print(month)

            selected_month = input("Enter the month: ")

            # Check if the selected month exists in the data
            if selected_month in yearly_data[selected_year]:
                print_unknown_category_payments(yearly_data, year, month)
            else:
                print("Invalid month selection.")
        else:
            print("Invalid year selection.")

    elif choice == "4":
        backup_to_database(yearly_data, 'DESKTOP-9R2QVKD', 'FinancialData')
        break
    elif choice == "5":
        print("Goodbye!")
        break
    else:
        print("Invalid choice. Please enter a valid option (1/2/3/4/5).")