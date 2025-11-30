import os
import sqlite3

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'
DATABASES = os.path.join(BASE_FOLDER, 'inputs\\databases')
ACS_DB = os.path.join(DATABASES, 'acs.sqlite')


SEX_MAP = {1: 'MALE', 2: 'FEMALE'}


def main():

    columns = ('D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'SEX', 'D_STATE', 'D_COUNTY', 'D_POP',
                'D_POP_MOE', 'D_NONMOVERS', 'D_NONMOVERS_MOE', 'D_MOVERS',
                'D_MOVERS_MOE', 'D_MOVERS_SAME_CY', 'D_MOVERS_SAME_CY_MOE',
                'D_MOVERS_FROM_DIFF_CY_SAME_ST',
                'D_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'D_MOVERS_FROM_DIFF_ST',
                'D_MOVERS_DIFF_ST_MOE', 'D_MOVERS_FROM_ABROAD',
                'D_MOVERS_FROM_ABROAD_MOE', 'O_STATE', 'O_COUNTY', 'O_POP',
                'O_POP_MOE', 'O_NONMOVERS', 'O_NOMMOVERS_MOE', 'O_MOVERS',
                'O_MOVERS_MOE', 'O_MOVERS_SAME_CY', 'O_MOVERS_SAME_CY_MOE',
                'O_MOVERS_FROM_DIFF_CY_SAME_ST',
                'O_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'O_MOVERS_FROM_DIFF_ST',
                'O_MOVERS_DIFF_ST_MOE', 'O_MOVERS_PUERTO_RICO',
                'O_MOVERS_PUERTO_RICO_MOE', 'TOTAL_FLOW', 'TOTAL_FLOW_MOE')

    xlsx_folder = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\ACS\\2011_2015\\migration')
    xlsx_file = 'county-to-county-by-sex-2011-2015-current-residence-sort.xlsx'
    xlsx = pd.ExcelFile(os.path.join(xlsx_folder, xlsx_file))
    df = pd.concat([xlsx.parse(sheet_name=name, header=None, names=columns, skiprows=4, skipfooter=8) for name in xlsx.sheet_names if name != 'Puerto Rico'])

    df = df[~df.O_STFIPS.str.contains('XXX')]

    foreign = ['EUR', 'ASI', 'SAM', 'ISL', 'NAM', 'CAM', 'CAR', 'AFR', 'OCE']
    df = df.loc[df.O_STFIPS.isin(foreign), ['D_STFIPS', 'D_COFIPS', 'SEX', 'TOTAL_FLOW']]

    df['D_STFIPS'] = df.D_STFIPS.astype(int).astype(str).str.zfill(2)
    df['D_COFIPS'] = df.D_COFIPS.astype(int).astype(str).str.zfill(3)
    df['DESTINATION_FIPS'] = df.D_STFIPS + df.D_COFIPS

    df.SEX = df.SEX.replace(to_replace=SEX_MAP)
    df = df[['DESTINATION_FIPS', 'SEX', 'TOTAL_FLOW']]

    assert not df.isnull().any().any()

    df = df.groupby(['DESTINATION_FIPS', 'SEX'], as_index=False).sum()
    df['SEX_SUM'] = df.groupby('SEX')['TOTAL_FLOW'].transform('sum')
    df['WEIGHT_x_10^6'] = (df['TOTAL_FLOW'] / df['SEX_SUM']) * 1000000
    df = df.pivot_table(index='DESTINATION_FIPS',
                        columns='SEX',
                        values='WEIGHT_x_10^6',
                        fill_value=0)
    df.reset_index(inplace=True)
    df.columns.name = None

    con = sqlite3.connect(ACS_DB)
    df.to_sql(name='acs_immigration_weights_sex_2011_2015',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

    df.to_csv(path_or_buf=os.path.join(DATABASES, 'acs_immigration_weights_sex_2011_2015.csv'),
              index=False)


if __name__ == '__main__':
    main()
