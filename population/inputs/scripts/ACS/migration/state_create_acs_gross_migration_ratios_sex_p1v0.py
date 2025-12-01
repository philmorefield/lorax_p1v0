import os

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
PROCESSED_FILES = os.path.join(BASE_FOLDER, 'inputs\\processed_files')
ACS_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\ACS')


def get_acs_2011_2015_migration():
    xl_filename = 'county-to-county-by-sex-2011-2015-current-residence-sort.xlsx'

    sex_map = {1: 'MALE', 2: 'FEMALE'}

    columns = ('D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'SEX', 'D_STATE', 'D_COUNTY', 'D_POP',
               'D_POP_MOE', 'D_NONMOVERS', 'D_NONMOVERS_MOE', 'D_MOVERS',
               'D_MOVERS_MOE', 'D_MOVERS_SAME_CY', 'D_MOVERS_SAME_CY_MOE',
               'D_MOVERS_FROM_DIFF_CY_SAME_ST',
               'D_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'D_MOVERS_FROM_DIFF_ST',
               'D_MOVERS_DIFF_ST_MOE', 'D_MOVERS_FROM_ABROAD',
               'D_MOVERS_FROM_ABROAD_MOE', 'O_STATE', 'O_COUNTY', 'ORIGIN_POPULATION',
               'O_POP_MOE', 'O_NONMOVERS', 'O_NOMMOVERS_MOE', 'O_MOVERS',
               'O_MOVERS_MOE', 'O_MOVERS_SAME_CY', 'O_MOVERS_SAME_CY_MOE',
               'O_MOVERS_FROM_DIFF_CY_SAME_ST',
               'O_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'O_MOVERS_FROM_DIFF_ST',
               'O_MOVERS_DIFF_ST_MOE', 'O_MOVERS_PUERTO_RICO',
               'O_MOVERS_PUERTO_RICO_MOE', 'FLOW', 'FLOW_MOE')

    xls = pd.ExcelFile(os.path.join(ACS_FOLDER, '2011_2015', 'migration', xl_filename))
    df = pd.concat([xls.parse(sheet_name=name, header=None, names=columns, skiprows=4, skipfooter=8) for name in xls.sheet_names if name != 'Puerto Rico'])

    df = df[~df.O_STFIPS.str.contains('XXX')]
    foreign = ('EUR', 'ASI', 'SAM', 'ISL', 'NAM', 'CAM', 'CAR', 'AFR', 'OCE')
    df = df.loc[~df.O_STFIPS.isin(foreign), ['D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'SEX', 'ORIGIN_POPULATION', 'FLOW']]

    df['DESTINATION_FIPS'] = df.D_STFIPS.astype(int).astype(str).str.zfill(2)
    df['ORIGIN_FIPS'] = df.O_STFIPS.astype(int).astype(str).str.zfill(2)

    df.SEX = df.SEX.replace(to_replace=sex_map)
    df = df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'SEX', 'FLOW']]
    df = df.query('ORIGIN_FIPS != DESTINATION_FIPS').groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'SEX'], as_index=False).sum()

    assert not df.isna().any().any(), "NaN values present after cleaning"

    return df


def calculate_flow_percentages(df):
    # ignoring migration to/from Puerto Rico for now
    df = df.loc[~df.ORIGIN_FIPS.str.startswith('72')]
    df = df.loc[~df.DESTINATION_FIPS.str.startswith('72')]

    df['TOTAL_GROSS_FLOW'] = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'], as_index=False)['FLOW'].transform('sum')
    df['MIGRATION_PROPORTION'] = df['FLOW'] / df['TOTAL_GROSS_FLOW']

    assert not df.isna().any().any(), "NaN values present after calculating migration rates"

    return df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'SEX', 'MIGRATION_PROPORTION']]


def main():
    migration = get_acs_2011_2015_migration()
    df = calculate_flow_percentages(migration)

    df.to_csv(os.path.join(PROCESSED_FILES, 'migration', 'state_acs_gross_migration_ratios_2011_2015_sex.csv'),
              index=False)


if __name__ == '__main__':
    main()
