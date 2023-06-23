import sys
import pandas as pd

# Check the argument of filename, if not provided, exit.
if len(sys.argv) != 2:
    print('Usage: python script_name.py your_file.csv')
    sys.exit()

# Use the provided filename.
input_file = sys.argv[1]
output_file = 'internal_transactions.csv'

# Read the csv file
df = pd.read_csv(input_file)

# Data cleansing - remove thousands separator commas then convert to float type.
df['DEPOSIT'] = df['DEPOSIT'].replace(',', '').astype(float).fillna(0)
df['WITHDRAW'] = df['WITHDRAW'].replace(',', '').astype(float).fillna(0)

# Use only hour information to allow some delays. If this is to loose, you can expand to str 4.
df['TXT'] = df['TXT'].str[:2]

# Generating key data field using TXD, TXT and the amount.
df['deposit_key'] = df['TXD'].astype(str) + '_' + df['TXT'].astype(str) + '_' + df['DEPOSIT'].astype(str)
df['withdraw_key'] = df['TXD'].astype(str) + '_' + df['TXT'].astype(str) + '_' + df['WITHDRAW'].astype(str)

# Separate deposit and withdrawal.
df_deposit = df[df['DEPOSIT'] > 0].copy()
df_withdraw = df[df['WITHDRAW'] > 0].copy()

# Performing matches.
df_merge = pd.merge(df_deposit, df_withdraw, left_on='deposit_key', right_on='withdraw_key')

# Write to the destination file.
df_merge.to_csv(output_file, index=False)
