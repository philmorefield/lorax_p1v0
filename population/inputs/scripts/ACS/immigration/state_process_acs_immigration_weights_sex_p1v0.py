import os
# import sqlite3

import polars as pl


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
PROCESSED_FILES = os.path.join(BASE_FOLDER, 'inputs\\processed_files')

SEX_MAP = {1: 'MALE', 2: 'FEMALE'}


def main():

    columns = ['D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'SEX', 'D_STATE', 'D_COUNTY', 'D_POP',
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
                'O_MOVERS_PUERTO_RICO_MOE', 'TOTAL_FLOW', 'TOTAL_FLOW_MOE']

    xlsx_folder = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\ACS\\2011_2015\\migration')
    xlsx_file = 'county-to-county-by-sex-2011-2015-current-residence-sort.xlsx'

    # Read Excel file using Polars
    xlsx_path = os.path.join(xlsx_folder, xlsx_file)
    dfs = [df[:-6, :] for df in pl.read_excel(source=xlsx_path,
                                              sheet_id=0,
                                              drop_empty_rows=True,
                                              read_options={'header_row': None,
                                                            'skip_rows': 4}).values()]
    for df in dfs:
        df.columns = columns

    df = pl.concat(items=dfs, how='vertical_relaxed')

    # Filter out rows with 'XXX' in O_STFIPS
    df = df.filter(~pl.col('O_STFIPS').str.contains('XXX'))

    # Filter for foreign countries
    foreign = ['EUR', 'ASI', 'SAM', 'ISL', 'NAM', 'CAM', 'CAR', 'AFR', 'OCE']
    df = df.filter(pl.col('O_STFIPS').is_in(foreign)).select(['D_STFIPS', 'SEX', 'TOTAL_FLOW'])

    # Convert D_STFIPS to string and zero-fill
    df = df.with_columns([
        pl.col('D_STFIPS').cast(pl.Int32).cast(pl.Utf8).str.zfill(2)
    ])

    # Convert D_STFIPS to string and zero-fill
    df = df.with_columns(pl.col('D_STFIPS').cast(pl.Int8).cast(pl.String).str.zfill(2).alias('DESTINATION_FIPS'))

    # Replace SEX values using SEX_MAP
    df = df.with_columns(pl.col('SEX').cast(pl.Int8).cast(pl.String).alias('SEX'))
    df = df.with_columns(pl.col('SEX').replace(SEX_MAP).alias('SEX'))
    df = df.select(['DESTINATION_FIPS', 'SEX', 'TOTAL_FLOW'])

    # Check for nulls
    assert df.null_count().sum_horizontal().item() == 0

    # Group by and sum
    df = df.group_by(['DESTINATION_FIPS', 'SEX']).agg(pl.col('TOTAL_FLOW').sum())

    # Add sex sum column
    df = df.with_columns(pl.col('TOTAL_FLOW').sum().over('SEX').alias('SEX_SUM'))

    # Calculate weights
    df = df.with_columns(((pl.col('TOTAL_FLOW') / pl.col('SEX_SUM')) * 1000000).alias('WEIGHT_x_10^6'))

    # Pivot to wide format
    df = df.pivot(on='SEX',
                  index='DESTINATION_FIPS',
                  values='WEIGHT_x_10^6').fill_null(0)

    # con = sqlite3.connect(ACS_DB)
    # df.to_sql(name='acs_immigration_weights_sex_2011_2015',
    #           con=con,
    #           if_exists='replace',
    #           index=False)
    # con.close()

    # Write to CSV
    df.write_csv(os.path.join(PROCESSED_FILES, 'immigration', 'state_acs_immigration_weights_sex_2011_2015.csv'))


if __name__ == '__main__':
    main()
