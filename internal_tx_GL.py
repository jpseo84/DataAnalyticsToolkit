import sys
import dask.dataframe as dd

def process_bank_data(bank_record_input, output_file):

    # Specify the data types for the columns.
    dtypes = {'BRANCH': 'object'}

    # Read the csv file with specified data types.
    df = dd.read_csv(bank_record_input, dtype=dtypes)

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
    df_merge.to_csv(output_file, index=False)

if __name__ == "__main__":
    # Check the arguments of filenames, if not provided, exit.
    if len(sys.argv) != 3:
        print('Usage: python script_name.py bank_record_input.csv output_file.csv')
        sys.exit()

    # Use the provided filenames.
    bank_record_input = sys.argv[1]
    output_file = sys.argv[2]

    process_bank_data(bank_record_input, output_file)
