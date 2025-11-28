'''
20251110 - p1v01 - Process as single year ages
'''

import os
# import sqlite3

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'

CBO_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\CBO')
CSV_FOLDER = '57059-2025-09-Demographic-Projections\\CSV files'
CSV_FILE = 'mortalityRates_byYearAgeSex.csv'
OUTPUT_DB = os.path.join(BASE_FOLDER, 'inputs\\databases\\cbo.sqlite')

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

    return df.query('AGE >= 85')


def main():
    sya_pop = get_cbo_population()

    df = pd.read_csv(filepath_or_buffer=os.path.join(CBO_FOLDER, CSV_FOLDER, CSV_FILE))
    df.columns = ['YEAR', 'AGE', 'SEX', 'ASMR']
    df.SEX = df.SEX.str.upper()

    # calculate the 2021-2024 average ASMR by age and sex
    df_asmr_base = df.loc[df.YEAR.isin([2021, 2022, 2023, 2024])].drop(columns='YEAR')
    df_asmr_base = df_asmr_base.groupby(['AGE', 'SEX'], as_index=False).mean()
    df_asmr_base = df_asmr_base.rename(columns={'ASMR': 'ASMR_BASE'})

    # starting with 2025, calculate the % change from the base year ASMR
    df = df.query('YEAR >= 2025').pivot_table(index=['AGE', 'SEX'], columns='YEAR')
    df.columns.name = None
    df.columns = df.columns.droplevel(0)
    df.columns = [f'ASMR_{col}' for col in df.columns]
    df = df.merge(df_asmr_base, on=['AGE', 'SEX'], how='left')

    # ideally this would be a population-weighted average for 100+, but there
    # are no population counts for ages for 100 so I'm just calculating the
    # simple average
    df.loc[df.AGE >= 100, 'AGE'] = 100
    df = df.groupby(by=['AGE', 'SEX'], as_index=False).mean()

    # calculate population-weighted ASMR for 85+
    sya_pop_year = sya_pop.query('YEAR == 2025').drop(columns='YEAR')
    df = df.merge(right=sya_pop_year,
                  how='left',
                  on=['AGE', 'SEX'])
    weighted_average_male = (df.loc[(df.AGE >= 85) & (df.SEX == 'MALE'), 'ASMR_BASE'] * df.loc[(df.AGE >= 85) & (df.SEX == 'MALE'), 'POPULATION']).sum() / df.loc[(df.AGE >= 85) & (df.SEX == 'MALE'), 'POPULATION'].sum()
    df.loc[(df.AGE >= 85) & (df.SEX == 'MALE'), 'ASMR_BASE'] = weighted_average_male

    weighted_average_female = (df.loc[(df.AGE >= 85) & (df.SEX == 'FEMALE'), 'ASMR_BASE'] * df.loc[(df.AGE >= 85) & (df.SEX == 'FEMALE'), 'POPULATION']).sum() / df.loc[(df.AGE >= 85) & (df.SEX == 'FEMALE'), 'POPULATION'].sum()
    df.loc[(df.AGE >= 85) & (df.SEX == 'FEMALE'), 'ASMR_BASE'] = weighted_average_female

    df = df.drop(columns='POPULATION')

    for year in range(2025, 2099):
        sya_pop_year = sya_pop.query(f'YEAR == {year}').drop(columns='YEAR')

        # calculate population-weighted ASMR for 85+
        df = df.merge(right=sya_pop_year,
                      how='left',
                      on=['AGE', 'SEX'])

        weighted_average_male = (df.loc[(df.AGE >= 85) & (df.SEX == 'MALE'), f'ASMR_{year}'] * df.loc[(df.AGE >= 85) & (df.SEX == 'MALE'), 'POPULATION']).sum() / df.loc[(df.AGE >= 85) & (df.SEX == 'MALE'), 'POPULATION'].sum()
        df.loc[(df.AGE >= 85) & (df.SEX == 'MALE'), f'ASMR_{year}'] = weighted_average_male

        weighted_average_female = (df.loc[(df.AGE >= 85) & (df.SEX == 'FEMALE'), f'ASMR_{year}'] * df.loc[(df.AGE >= 85) & (df.SEX == 'FEMALE'), 'POPULATION']).sum() / df.loc[(df.AGE >= 85) & (df.SEX == 'FEMALE'), 'POPULATION'].sum()
        df.loc[(df.AGE >= 85) & (df.SEX == 'FEMALE'), f'ASMR_{year}'] = weighted_average_female

        df = df.drop(columns='POPULATION')

    # combine age groups >= 85 into a single 85+ group
    df.loc[df.AGE >= 85, 'AGE'] = 85
    df = df.groupby(by=['AGE', 'SEX'], as_index=False).mean()

    # calculate future ASMR as a ratio of the base year ASMR
    for year in range(2025, 2099):
        df[f'ASMR_{year}'] = df[f'ASMR_{year}'] / df['ASMR_BASE']
    df = df.drop(columns='ASMR_BASE')
    df = df.sort_values(by=['AGE', 'SEX'])

    # con = sqlite3.connect(database=OUTPUT_DB)
    # df.to_sql(name='cbo_mortality',
    #           con=con,
    #           if_exists='replace',
    #           index=False)
    # con.close()

    df.to_csv(path_or_buf=os.path.join(os.path.dirname(OUTPUT_DB), 'cbo_mortality_p1v01.csv'),
              index=False)


if __name__ == '__main__':
    main()
