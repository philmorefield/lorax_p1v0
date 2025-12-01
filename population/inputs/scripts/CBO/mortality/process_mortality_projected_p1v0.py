import os

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
PROCESSED_FILES = os.path.join(BASE_FOLDER, 'inputs\\processed_files')
CBO_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\CBO')
CSV_FOLDER = '57059-2025-09-Demographic-Projections\\CSV files'
CSV_FILE = 'mortalityRates_byYearAgeSex.csv'


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

def add_age_group(df):
    df['AGE_GROUP'] = '85+'
    df.loc[df.AGE <= 84, 'AGE_GROUP'] = '80-84'
    df.loc[df.AGE <= 79, 'AGE_GROUP'] = '75-79'
    df.loc[df.AGE <= 74, 'AGE_GROUP'] = '70-74'
    df.loc[df.AGE <= 69, 'AGE_GROUP'] = '65-69'
    df.loc[df.AGE <= 64, 'AGE_GROUP'] = '60-64'
    df.loc[df.AGE <= 59, 'AGE_GROUP'] = '55-59'
    df.loc[df.AGE <= 54, 'AGE_GROUP'] = '50-54'
    df.loc[df.AGE <= 49, 'AGE_GROUP'] = '45-49'
    df.loc[df.AGE <= 44, 'AGE_GROUP'] = '40-44'
    df.loc[df.AGE <= 39, 'AGE_GROUP'] = '35-39'
    df.loc[df.AGE <= 34, 'AGE_GROUP'] = '30-34'
    df.loc[df.AGE <= 29, 'AGE_GROUP'] = '25-29'
    df.loc[df.AGE <= 24, 'AGE_GROUP'] = '20-24'
    df.loc[df.AGE <= 19, 'AGE_GROUP'] = '15-19'
    df.loc[df.AGE <= 14, 'AGE_GROUP'] = '10-14'
    df.loc[df.AGE <= 9, 'AGE_GROUP'] = '5-9'
    df.loc[df.AGE <= 4, 'AGE_GROUP'] = '0-4'

    return df


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

    # calculated population-weighted ASMR for each age group
    age_group_pop = add_age_group(sya_pop)
    for year in range(2025, 2099):
        df = df.merge(right=age_group_pop.query(f'YEAR == {year}'),
                      how='left')
        df[f'WEIGHTS_X_POP_{year}'] = (df[f'ASMR_{year}'] * df['POPULATION'])
        df['SUM_WEIGHTS_X_POP'] = df.groupby(by=['AGE_GROUP', 'SEX']) [f'WEIGHTS_X_POP_{year}'].transform('sum')
        df['SUM_POP'] = df.groupby(by=['AGE_GROUP', 'SEX'])['POPULATION'].transform('sum')
        df[f'ASMR_{year}'] = df.eval('SUM_WEIGHTS_X_POP / SUM_POP')
        df = df.drop(columns=['YEAR', f'WEIGHTS_X_POP_{year}', 'SUM_WEIGHTS_X_POP', 'SUM_POP', 'POPULATION'])

        if year == 2025:
            df = df.merge(right=age_group_pop.query(f'YEAR == {year}'),
                          how='left')
            df['WEIGHTS_X_POP_BASE'] = (df['ASMR_BASE'] * df['POPULATION'])
            df['SUM_WEIGHTS_X_POP'] = df.groupby(by=['AGE_GROUP', 'SEX']) ['WEIGHTS_X_POP_BASE'].transform('sum')
            df['SUM_POP'] = df.groupby(by=['AGE_GROUP', 'SEX'])['POPULATION'].transform('sum')
            df['ASMR_BASE'] = df.eval('SUM_WEIGHTS_X_POP / SUM_POP')
            df = df.drop(columns=['YEAR', 'WEIGHTS_X_POP_BASE', 'SUM_WEIGHTS_X_POP', 'SUM_POP', 'POPULATION'])
    df = df.drop(columns='AGE').groupby(by=['AGE_GROUP', 'SEX'], as_index=False).mean()

    # calculate ASMR as a ratio of the base year ASMR
    for year in range(2025, 2099):
        df[f'ASMR_{year}'] = df[f'ASMR_{year}'] / df['ASMR_BASE']
    df = df.drop(columns='ASMR_BASE')
    df = df.set_index(['AGE_GROUP', 'SEX']).reset_index()

    df.to_csv(path_or_buf=os.path.join(PROCESSED_FILES, 'mortality', 'cbo_mortality_p1v0.csv'),
              index=False)


if __name__ == '__main__':
    main()
