import requests
import datetime as dt
import pandas as pd
from dotenv import load_dotenv
import os

def fetch_and_save_income_statement():
    # Load environment variables from .env file
    load_dotenv()

    # Retrieve the URL and API key from environment variables
    url = os.getenv('income_database_url')
    api_key = os.getenv('alpha_api_key')

    # Debugging: Print the values of the environment variables
    print(f"URL: {url}")
    print(f"API Key: {api_key}")

    # Check if the environment variables are loaded correctly
    if url is None or api_key is None:
        raise ValueError("Missing environment variables: 'income_statement_url' or 'alpha_api_key'")

    function = 'INCOME_STATEMENT'
    symbol = 'AMZN'

    # Construct the full API request URL
    params = {
        'function': function,
        'symbol': symbol,
        'apikey': api_key
    }

    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Extract the time series data
        income_statements = data.get('annualReports', [])
        
        if not income_statements:
            raise ValueError("No 'annualReports' data found in the response")

        # Convert the time series data to a pandas DataFrame
        df = pd.DataFrame(income_statements)
        
        # Print original columns for debugging
        print("Original columns:", df.columns)
        
        # Rename columns for clarity
        df = df.rename(columns={
            'fiscalDateEnding': 'Fiscal Date Ending',
            'reportedCurrency': 'Reported Currency',
            'grossProfit': 'Gross Profit',
            'totalRevenue': 'Total Revenue',
            'costOfRevenue': 'Cost of Revenue',
            'costofGoodsAndServicesSold': 'Cost of Goods and Services Sold',
            'operatingIncome': 'Operating Income',
            'sellingGeneralAndAdministrative': 'Selling, General and Administrative',
            'researchAndDevelopment': 'Research and Development',
            'operatingExpenses': 'Operating Expenses',
            'investmentIncomeNet': 'Investment Income Net',
            'netInterestIncome': 'Net Interest Income',
            'interestIncome': 'Interest Income',
            'interestExpense': 'Interest Expense',
            'nonInterestIncome': 'Non-Interest Income',
            'otherNonOperatingIncome': 'Other Non-Operating Income',
            'depreciation': 'Depreciation',
            'depreciationAndAmortization': 'Depreciation and Amortization',
            'incomeBeforeTax': 'Income Before Tax',
            'incomeTaxExpense': 'Income Tax Expense',
            'interestAndDebtExpense': 'Interest and Debt Expense',
            'netIncomeFromContinuingOperations': 'Net Income from Continuing Operations',
            'comprehensiveIncomeNetOfTax': 'Comprehensive Income Net of Tax',
            'ebit': 'EBIT',
            'ebitda': 'EBITDA',
            'netIncome': 'Net Income'
        })
        
        # Print renamed columns for debugging
        print("Renamed columns:", df.columns)
        
        # Convert numeric columns to float where applicable
        numeric_columns = [
            'Gross Profit', 'Total Revenue', 'Cost of Revenue', 'Cost of Goods and Services Sold',
            'Operating Income', 'Selling, General and Administrative', 'Research and Development',
            'Operating Expenses', 'Investment Income Net', 'Net Interest Income', 'Interest Income',
            'Interest Expense', 'Non-Interest Income', 'Other Non-Operating Income', 'Depreciation',
            'Depreciation and Amortization', 'Income Before Tax', 'Income Tax Expense',
            'Interest and Debt Expense', 'Net Income from Continuing Operations',
            'Comprehensive Income Net of Tax', 'EBIT', 'EBITDA', 'Net Income'
        ]
        
        # Ensure columns exist before conversion
        missing_columns = [col for col in numeric_columns if col not in df.columns]
        if missing_columns:
            print(f"Missing columns in the DataFrame: {missing_columns}")
        else:
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
        
        # Select the last 15 records
        df = df.head(15)
        
        # Save the DataFrame to a CSV file
        df.to_csv('income_statement_amazon.csv', index=False)
        
        return df
    else:
        print(f"Failed to retrieve data: {response.status_code}")
        return None

# Example usage
try:
    df = fetch_and_save_income_statement()
    if df is not None:
        print(df)
except ValueError as e:
    print(e)