import os

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
PROCESSED_FILES = os.path.join(BASE_FOLDER, 'inputs\\processed_files')
CBO_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\CBO')
CSV_FOLDER = '57059-2025-09-Demographic-Projections\\CSV files'
CSV_FILE = 'fertilityRates_byYearAgePlace.csv'


def get_cbo_population():
    cols = ['AGE',
            'TOTAL_POPULATION',
            'BLANK1',
            'MALE',
            'TOTAL_MALE_SINGLE',
            'TOTAL_MALE_MARRIED',
            'TOTAL_MALE_WIDOWED',
            'TOTAL_MALE_DIVORCED',
            'BLANK2',
            'FEMALE',
            'TOTAL_FEMALE_SINGLE',
            'TOTAL_FEMALE_MARRIED',
            'TOTAL_FEMALE_WIDOWED',
            'TOTAL_FEMALE_DIVORCED']

    csv_folder = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'CBO', '57059-2025-09-Demographic-Projections')
    csv_fn = '57059-2025-09-Demographic-Projections.xlsx'
    df = pd.read_excel(io=os.path.join(csv_folder, csv_fn),
                       sheet_name='2. Pop by age, sex, marital',
                       names=cols,
                       skiprows=9,
                       skipfooter=6).dropna(axis='columns', how='all').dropna()

    df = df[['AGE', 'MALE', 'FEMALE']].dropna()
    df = df.loc[df['AGE'] != 'Age']
    df['AGE'] = df['AGE'].replace('100+', 100).astype(int)
    df['MALE'] = df['MALE'].astype(int)
    df['FEMALE'] = df ['FEMALE'].astype(int)

    n = 101
    df_list = [df[i:i+n].copy() for i in range(0, df.shape[0], n)]

    for i, df_item in enumerate(df_list):
        df_item['YEAR'] = 2022 + i

    df = pd.concat(df_list, ignore_index=True)
    df = df.melt(id_vars=['YEAR', 'AGE'], var_name='SEX', value_name='POPULATION')

    return df.loc[(df.AGE >= 15) & (df.AGE <= 49) & (df.SEX == 'FEMALE'), ['YEAR', 'AGE', 'POPULATION']]


def main():
    sya_pop = get_cbo_population()

    df = pd.read_csv(filepath_or_buffer=os.path.join(CBO_FOLDER, CSV_FOLDER, CSV_FILE))
    df.columns = ['YEAR', 'AGE', 'PLACE', 'ASFR']
    df = df.query('PLACE == "all" & AGE >= 15').drop(columns='PLACE')

    # starting with 2026, caculate the % change from the base year ASFR
    df = df.query('YEAR >= 2025').pivot_table(index='AGE', columns='YEAR')
    df.columns.name = None
    df.columns = df.columns.droplevel(0)
    df.columns = [f'ASFR_{col}' for col in df.columns]
    df = df.reset_index()

    # bin rows by age group
    df['AGE_GROUP'] = '15-19'
    df.loc[df.AGE.between(45, 49), 'AGE_GROUP'] = '45-49'
    df.loc[df.AGE.between(40, 44), 'AGE_GROUP'] = '40-44'
    df.loc[df.AGE.between(35, 39), 'AGE_GROUP'] = '35-39'
    df.loc[df.AGE.between(30, 34), 'AGE_GROUP'] = '30-34'
    df.loc[df.AGE.between(25, 29), 'AGE_GROUP'] = '25-29'
    df.loc[df.AGE.between(20, 24), 'AGE_GROUP'] = '20-24'

    # calculated population-weighted ASFR for each age group
    for year in range(2025, 2099):
        df = df.merge(right=sya_pop.query(f'YEAR == {year}'),
                      on='AGE',
                      how='left')
        df[f'WEIGHTS_X_POP_{year}'] = (df[f'ASFR_{year}'] * df['POPULATION'])
        df['SUM_WEIGHTS_X_POP'] = df.groupby(by='AGE_GROUP') [f'WEIGHTS_X_POP_{year}'].transform('sum')
        df['SUM_POP'] = df.groupby(by='AGE_GROUP')['POPULATION'].transform('sum')
        df[f'ASFR_{year}'] = df.eval('SUM_WEIGHTS_X_POP / SUM_POP')
        df = df.drop(columns=['YEAR', f'WEIGHTS_X_POP_{year}', 'SUM_WEIGHTS_X_POP', 'SUM_POP', 'POPULATION'])

    # at this stage each age group should corrspond to five identical values (one per age)
    df = df.drop(columns='AGE').groupby(by='AGE_GROUP', as_index=False).mean()

    # calculate ASFR as a ratio of the base year
    for year in range(2098, 2024, -1):
        df[f'ASFR_{year}'] = df[f'ASFR_{year}'] / df['ASFR_2025']

    # write to CSV
    df.to_csv(path_or_buf=os.path.join(PROCESSED_FILES, 'fertility', 'national_cbo_fertility_p1v0.csv'),
              index=False)


if __name__ == '__main__':
    main()
