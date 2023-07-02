import sys
import dask.dataframe as dd

"""

"""

# Check the arguments of filenames, if not provided, exit. If the file is in a different directory, you can specify the full location.
if len(sys.argv) != 3:
    print('Usage: python program.py input_file.csv output_file.csv')
    sys.exit()

# Use the provided filenames.
input_file = sys.argv[1]
output_file = sys.argv[2]

# Specify the data types for the columns.
dtypes = {'BRANCH': 'object'}

# Read the csv file with specified data types.
df = dd.read_csv(input_file, dtype=dtypes)

# Use only hour information to allow some delays. If this is too loose, you can expand to str 4.
# TXD stands for Transaction Date & TXT stands for Transaction Time.
df['TXT'] = df['TXT'].str[:2]

# Generating key data field using TXD, TXT and the amount.
df['deposit_key'] = df['TXD'].astype(str) + '_' + df['TXT'].astype(str) + '_' + df['DEPOSIT'].astype(str)
df['withdraw_key'] = df['TXD'].astype(str) + '_' + df['TXT'].astype(str) + '_' + df['WITHDRAW'].astype(str)

# Separate the dataset into deposit and withdrawal parts.
df_deposit = df[df['DEPOSIT'] > 0].copy()
df_withdraw = df[df['WITHDRAW'] > 0].copy()

# Performing matches using generated keys.
df_merge = dd.merge(df_deposit, df_withdraw, left_on='deposit_key', right_on='withdraw_key').compute()

# Write to the destination file.
df_merge.to_csv(output_file, index=False)