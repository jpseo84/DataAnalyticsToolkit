import dask.dataframe as dd
import argparse

def process_bank_data(bank_input, bank_output):
    
    """
    This function is used to identify internal transactions.

    Most parts of this code are Pandas compatible, while it utilizes Dask for large file processings and further uses.
    This data processor is developed to identify internal transactions using timestamp and transaction amounts.
    Due to the nature of the procedure, it may generate inaccurate results, especially when a same amount was withdrawn in one account,
    and the same amount from different funding source was deposited, while both transactions are not related.
    
    Parameters:
    bank_input(str): The file name of the raw bank data.
    bank_output(str): The file name of the processed bank data.
    bank_input file must be in the following format:
        "TXD","TXT","DEPOSIT","WITHDRAW","BALANCE","DESC1","DESC2","BRANCH","LOCATION","BANK","BANKNO","COMPLETED"
    TXD is transaction date in YYYY-MM-DD format;
    TXT is transaction time in HH:MM:SS format;
    Deposit, Withdraw and Balance are numbers(integer) and only Deposit and Withdraw amounts are used for analysis; and
    DESC1, DESC2, Branch, Location, Bank, Bankno, Completed is all string - not used for analysis.
    """

    # Specify the data types for the columns.
    dtypes = {'BRANCH': 'object'}

    # Read the csv file with specified data types.
    df = dd.read_csv(bank_input, dtype=dtypes)
    """
    Following date-time convention only works under specified format.
    This timestamp convention is widely used in South Korea but in different countries, it may need to be changed.
    Your dataset's format should be in following format:
    [1] Dates are in YYYY-MM-DD format, at the first column of the dataset.
    [2] Times are in HH:MM:SS format, at the second column of the dataset.
    If a dataset is provided in different format, you need to process in different manners by changging following key-generating procedures with TXTs and TXDs.
    """

    # Use only hour information to allow some delays. If this is too loose, you can expand to str 4.
    df['TXT'] = df['TXT'].str[:2]

    # Generating key data field using TXD, TXT and the amount.
    df['deposit_key'] = df['TXD'].astype(str) + '_' + df['TXT'].astype(str) + '_' + df['DEPOSIT'].astype(str)
    df['withdraw_key'] = df['TXD'].astype(str) + '_' + df['TXT'].astype(str) + '_' + df['WITHDRAW'].astype(str)

    # Separate deposit and withdrawal.
    df_deposit = df[df['DEPOSIT'] > 0].copy()
    df_withdraw = df[df['WITHDRAW'] > 0].copy()

    # Performing matches.
    df_merge = dd.merge(df_deposit, df_withdraw, left_on='deposit_key', right_on='withdraw_key').compute()

    # Write to the destination file.
    df_merge.to_csv(bank_output, index=False)


def process_gl_data(gl_input, gl_output):
    # Read the csv file into a Dask DataFrame without specifying the datatypes yet.
    df = dd.read_csv(gl_input)

    # Display header fields to user.
    print("Here are the available fields:")
    for i, h in enumerate(df.columns, 1):
        print(f"[{i}] {h}")

    # Get user input for required fields.
    je_no_field = df.columns[int(input("Which field contains journal entry document number? ")) - 1]
    je_date_field = df.columns[int(input("Which field contains journal entry (or actual transaction) date? ")) - 1]
    account_no_field = df.columns[int(input("Which field contains account number or code? ")) - 1]
    debit_amount_field = df.columns[int(input("Which field contains debited amount? ")) - 1]
    credit_amount_field = df.columns[int(input("Which field contains credited amount? ")) - 1]

    # Re-read the csv file into a Dask DataFrame with the specified datatypes.
    
    dtype = {col: 'object' for col in df.columns}
    dtype[debit_amount_field] = 'int'
    dtype[credit_amount_field] = 'int'

    df = dd.read_csv(gl_input, dtype = dtype)

    # Manually convert 'GL_DATE' to datetime
    df[je_date_field] = dd.to_datetime(df[je_date_field], format="%Y%m%d", errors='coerce')
    df[debit_amount_field].astype('int')
    df[credit_amount_field].astype('int')

    # Get account prefixes to consider as cash accounts.
    cash_account_prefixes = input("Which account number prefixes should be considered as cash accounts? ").split(',')

    # Separate dataframe into two: debits and credits.
    df_debits = df[df[account_no_field].astype(str).str.startswith(tuple(cash_account_prefixes)) & (df[debit_amount_field] > 0)]
    df_credits = df[df[account_no_field].astype(str).str.startswith(tuple(cash_account_prefixes)) & (df[credit_amount_field] > 0)]

    # Perform a merge operation (similar to a SQL join) between the two dataframes on the account number and amount fields.
    df_matched = dd.merge(df_debits, df_credits, left_on=[account_no_field, debit_amount_field], right_on=[account_no_field, credit_amount_field])

    # Select only the necessary fields from the matched dataframe.
    df_matched = df_matched[[f'{je_no_field}_x', f'{je_date_field}_x', account_no_field, f'{debit_amount_field}_x', f'{credit_amount_field}_y']]

    # Convert Dask DataFrame to Pandas DataFrame, then save to the output file.
    df_matched.drop_duplicates().compute().to_csv(gl_output, index=False)

def comparison_bank_gl():
    """
    This function processes the formerly prepared results.
    """

    # Define filenames
    bank_output = output_directory + "bank_output.csv"
    gl_output = output_directory + "gl_output.csv"

    # Define data types for each fields
    dtype_bank_dict = {
        'DESC1_x': 'object',
        'DESC2_x': 'object',
        'BRANCH_x': 'object',
        'LOCATION_x': 'object',
        'BANK_x': 'object',
        'BANKNO_x': 'object',
        'DESC1_y': 'object',
        'DESC2_y': 'object',
        'BRANCH_y': 'object',
        'LOCATION_y': 'object',
        'BANK_y': 'object',
        'BANKNO_y': 'object'
    }

    # Load the data from the csv files.
    df_bank = dd.read_csv(bank_output, dtype=dtype_bank_dict)
    df_gl = dd.read_csv(gl_output)

    # Create the key in each dataframe.
    df_bank['key'] = df_bank['TXD_x'] + '_' + df_bank['DEPOSIT_x'].astype(str)
    df_gl['key'] = df_gl['GL_DATE_x'] + '_' + df_gl['ACCOUNTED_DR_x'].astype(str)

    # Convert Dask DataFrame to Pandas DataFrame for easy manipulation.
    df_bank = df_bank.compute()
    df_gl = df_gl.compute()

    # Find the transactions that are in the GL data but not in the bank data.
    gl_not_in_bank = df_gl[~df_gl['key'].isin(df_bank['key'])]

    # Find the transactions that are in the bank data but not in the GL data.
    bank_not_in_gl = df_bank[~df_bank['key'].isin(df_gl['key'])]

    # Find the transactions that are in both datasets.
    both_in_gl_and_bank = df_gl[df_gl['key'].isin(df_bank['key'])]

    # Write the results to separate files.
    gl_not_in_bank.to_csv(output_directory + "result_type1.csv", index=False)
    bank_not_in_gl.to_csv(output_directory + "result_type2.csv", index=False)
    both_in_gl_and_bank.to_csv(output_directory + "result_type3.csv", index=False)

if __name__ == "__main__":
    # Check the arguments of filenames, if not provided, exit.
    # Instantiate the parser
    parser = argparse.ArgumentParser(description='Process some integers.')

    # Define arguments
    parser.add_argument('bank_input', type=str, help='Bank input CSV file.')
    parser.add_argument('gl_input', type=str, help='GL input CSV file.')
    parser.add_argument('--output', type=str, default='./', help='Output directory.')

    # Parse arguments
    args = parser.parse_args()

    # Use the provided filenames.
    bank_input = args.bank_input
    gl_input = args.gl_input
    output_directory = args.output

    process_bank_data(bank_input, output_directory + "bank_output.csv")
    
    process_gl_data(gl_input, output_directory + "gl_output.csv")
    
    comparison_bank_gl()