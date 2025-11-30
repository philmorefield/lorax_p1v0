import os

import polars as pl


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
PROCESSED_FILES = os.path.join(BASE_FOLDER, 'inputs\\processed_files')

CBO_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\CBO')
CSV_FOLDER = '57059-2025-09-Demographic-Projections\\CSV files'
CSV_FILE = 'grossMigration_byYearAgeSexStatusFlow.csv'


def main():
    df = pl.read_csv(source=os.path.join(CBO_FOLDER, CSV_FOLDER, CSV_FILE),
                     infer_schema_length=10000)
    df = df.rename({df.columns[0]: 'YEAR',
                    df.columns[1]: 'AGE',
                    df.columns[2]: 'SEX',
                    df.columns[3]: 'STATUS',
                    df.columns[4]: 'TYPE',
                    df.columns[5]: 'FLOW'})
    df = df.drop('STATUS')

    # Clean AGE column: remove '+' and replace '-1' with '0', then convert to int
    df = df.with_columns([pl.col('AGE').str.replace(pattern='+', value='', literal=True).str.replace(pattern='-1', value='0',).cast(pl.Int32),
                          pl.col('SEX').str.to_uppercase(),
                          pl.col('TYPE').str.to_uppercase(),
                          pl.col('FLOW').cast(pl.Int32)])

    # Group by and aggregate
    df = df.group_by(['YEAR', 'AGE', 'SEX', 'TYPE']).agg(pl.col('FLOW').sum())

    # Pivot to wide format
    df = df.pivot(on='TYPE',
                  index=['YEAR', 'AGE', 'SEX'],
                  values='FLOW').fill_null(0)

    # Calculate net immigration
    df = df.with_columns((pl.col('IMMIGRATION') - pl.col('EMIGRATION')).alias('NET_IMMIGRATION'))

    # Write to CSV
    df.write_csv(os.path.join(PROCESSED_FILES, 'immigration', 'national_cbo_net_migration_by_year_age_sex.csv'))


if __name__ == '__main__':
    main()
