import os

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
PROCESSED_FILES = os.path.join(BASE_FOLDER, 'inputs\\processed_files')
CBO_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\CBO')
CSV_FOLDER = 'demographic_projections_2025_9\\CSV files'
CSV_FILE = 'fertilityRates_byYearAgePlace.csv'

AGE_GROUPS = ['0-4',
              '5-9',
              '10-14',
              '15-19',
              '20-24',
              '25-29',
              '30-34',
              '35-39',
              '40-44',
              '45-49',
              '50-54',
              '55-59',
              '60-64',
              '65-69',
              '70-74',
              '75-79',
              '80-84',
              '85-100']

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

    return df


def main():
    df = pd.read_csv(filepath_or_buffer=os.path.join(CBO_FOLDER, CSV_FOLDER, CSV_FILE))
    df.columns = ['YEAR', 'AGE', 'PLACE', 'ASFR']
    df = df.query('PLACE == "all" & AGE >= 14').drop(columns='PLACE')

    # bin rows by age group
    df['AGE_GROUP'] = '15-19'
    df.loc[df.AGE.between(45, 49), 'AGE_GROUP'] = '45-49'
    df.loc[df.AGE.between(40, 44), 'AGE_GROUP'] = '40-44'
    df.loc[df.AGE.between(35, 39), 'AGE_GROUP'] = '35-39'
    df.loc[df.AGE.between(30, 34), 'AGE_GROUP'] = '30-34'
    df.loc[df.AGE.between(25, 29), 'AGE_GROUP'] = '25-29'
    df.loc[df.AGE.between(20, 24), 'AGE_GROUP'] = '20-24'

    df = df.drop(columns='AGE')
    df = df.groupby(by=['YEAR', 'AGE_GROUP'], as_index=False).mean()

    # CBO ASFR starts at 2025; use that as the baseline average, i.e., the
    # change factor for 2025 will be 1.0
    df_asfr_base = df.loc[df.YEAR == 2025].drop(columns='YEAR')
    df_asfr_base = df_asfr_base.groupby(by='AGE_GROUP', as_index=False).mean()
    df_asfr_base = df_asfr_base.rename(columns={'ASFR': 'ASFR_BASE'})

    # starting with 2025, calculate the % change from the 2019-2023 average ASFR
    df = df.query('YEAR >= 2025').pivot_table(index='AGE_GROUP', columns='YEAR')
    df.columns.name = None
    df.columns = df.columns.droplevel(0)
    df.columns = [f'ASFR_{col}' for col in df.columns]
    df = df.merge(df_asfr_base, on='AGE_GROUP', how='left')

    for year in range(2025, 2099):
        df[f'ASFR_{year}'] = df[f'ASFR_{year}'] / df['ASFR_BASE']
    df = df.drop(columns='ASFR_BASE')

    # write to CSV
    df.to_csv(path_or_buf=os.path.join(PROCESSED_FILES, 'fertility', 'national_cbo_fertility_p1v0.csv'),
              index=False)


if __name__ == '__main__':
    main()
