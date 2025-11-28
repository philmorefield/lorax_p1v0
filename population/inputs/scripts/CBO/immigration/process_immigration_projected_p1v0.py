import os

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'
DATABASES = os.path.join(BASE_FOLDER, 'inputs\\databases')

CBO_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\raw_files\\CBO'
CSV_FOLDER = '57059-2025-09-Demographic-Projections//CSV files'
CSV_FILE = 'grossMigration_byYearAgeSexStatusFlow.csv'


def main():
    df = pd.read_csv(filepath_or_buffer=os.path.join(CBO_FOLDER, CSV_FOLDER, CSV_FILE))
    df.columns = ['YEAR', 'AGE', 'SEX', 'STATUS', 'TYPE', 'FLOW']
    df.drop(columns=['STATUS'], inplace=True)
    df.AGE = df.AGE.str.replace('+', '').replace('-1', '0').astype(int)
    df.SEX = df.SEX.str.upper()
    df.TYPE = df.TYPE.str.upper()
    df.FLOW = df.FLOW.astype(int)

    # aggregate by age group and pivot
    df = df.groupby(by=['YEAR', 'AGE', 'SEX', 'TYPE'], as_index=False).sum()
    df = df.pivot(index=['YEAR', 'AGE', 'SEX'],
                  columns='TYPE',
                  values='FLOW').fillna(0).reset_index()
    df = df.eval('NET_IMMIGRATION = IMMIGRATION - EMIGRATION')
    df.to_csv(path_or_buf=os.path.join(DATABASES, 'cbo_national_net_migration_by_year_age_sex.csv'),
              index=False)


if __name__ == '__main__':
    main()
