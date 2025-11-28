import os
# import sqlite3

import polars as pl


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
PROCESSED_FILES = os.path.join(BASE_FOLDER, 'inputs\\processed_files')

AGE_MAP = {1: '1_to_4',
           2: '5_to_17',
           3: '18_to_19',
           4: '20_to_24',
           5: '25_to_29',
           6: '30_to_34',
           7: '35_to_39',
           8: '40_to_44',
           9: '45_to_49',
           10: '50_to_54',
           11: '55_to_59',
           12: '60_to_64',
           13: '65_to_69',
           14: '70_to_74',
           15: '75+'}


def parse_age_groups(s):

    if s == '1_to_4':
        return range(0, 5)
    elif '_to_' in s:
        s = s.split('_to_')
        return list(range(int(s[0]), int(s[1]) + 1))
    elif s == '75+':
        return range(75, 101)
    else:
        raise Exception


def main():
    columns = ['D_STFIPS', 'O_STFIPS', 'AGE', 'D_STATE', 'D_POP',
               'D_POP_MOE', 'D_NONMOVERS', 'D_NONMOVERS_MOE', 'D_MOVERS',
               'D_MOVERS_MOE', 'D_MOVERS_SAME_ST', 'D_MOVERS_SAME_ST_MOE',
               'D_MOVERS_FROM_DIFF_ST',
               'D_MOVERS_DIFF_ST_MOE', 'D_MOVERS_FROM_ABROAD',
               'D_MOVERS_FROM_ABROAD_MOE', 'O_STATE', 'O_POP',
               'O_POP_MOE', 'O_NONMOVERS', 'O_NOMMOVERS_MOE', 'O_MOVERS',
               'O_MOVERS_MOE', 'O_MOVERS_SAME_ST', 'O_MOVERS_SAME_ST_MOE',
               'O_MOVERS_FROM_DIFF_ST',
               'O_MOVERS_DIFF_ST_MOE', 'O_MOVERS_PUERTO_RICO',
               'O_MOVERS_PUERTO_RICO_MOE', 'TOTAL_FLOW', 'TOTAL_FLOW_MOE']

    xlsx_folder = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\ACS\\2011_2015\\migration')
    xlsx_file = 'county-to-county-by-age-2011-2015-current-residence-sort.xlsx'

    # Use pandas for Excel reading, then convert to Polars
    import pandas as pd
    xlsx = pd.ExcelFile(os.path.join(xlsx_folder, xlsx_file))
    df_pandas = pd.concat([xlsx.parse(sheet_name=name, header=None, names=columns,
                                     skiprows=4, skipfooter=8)
                          for name in xlsx.sheet_names if name != 'Puerto Rico'])

    # Convert pandas DataFrame to Polars
    df = pl.from_pandas(df_pandas)

    # Filter out rows with 'XXX' in O_STFIPS
    df = df.filter(~pl.col('O_STFIPS').str.contains('XXX'))

    # Filter for foreign countries
    foreign = ['EUR', 'ASI', 'SAM', 'ISL', 'NAM', 'CAM', 'CAR', 'AFR', 'OCE']
    df = df.filter(pl.col('O_STFIPS').is_in(foreign)).select(['D_STFIPS', 'AGE', 'TOTAL_FLOW'])

    # Convert D_STFIPS to string and zero-fill
    df = df.with_columns([
        pl.col('D_STFIPS').cast(pl.Int32).cast(pl.Utf8).str.zfill(2).alias('DESTINATION_FIPS')
    ])

    # Replace AGE values using AGE_MAP - create mapping expression
    age_mapping = pl.col('AGE').replace_strict(AGE_MAP, default=pl.col('AGE'), return_dtype=pl.Utf8)
    df = df.with_columns(age_mapping.alias('AGE'))
    df = df.select(['DESTINATION_FIPS', 'AGE', 'TOTAL_FLOW'])

    # Check for nulls
    assert df.null_count().sum_horizontal().item() == 0

    # Group by and sum
    df = df.group_by(['DESTINATION_FIPS', 'AGE']).agg(pl.col('TOTAL_FLOW').sum())

    # Add age sum column
    df = df.with_columns(
        pl.col('TOTAL_FLOW').sum().over('AGE').alias('AGE_SUM')
    )

    # Calculate weights
    df = df.with_columns(
        ((pl.col('TOTAL_FLOW') / pl.col('AGE_SUM')) * 1000000).alias('WEIGHT_x_10^6')
    )

    # Pivot to wide format using Polars syntax
    df = df.pivot(
        on='AGE',
        index='DESTINATION_FIPS',
        values='WEIGHT_x_10^6'
    ).fill_null(0)

    # Reorder columns to match original order
    column_order = ['DESTINATION_FIPS'] + list(AGE_MAP.values())
    available_columns = [col for col in column_order if col in df.columns]
    df = df.select(available_columns)

    # Expand age groups - convert to long format first
    df_long = df.unpivot(
        index='DESTINATION_FIPS',
        variable_name='AGE_GROUP',
        value_name='WEIGHT'
    )

    # Expand age groups
    expanded_rows = []
    for row in df_long.iter_rows(named=True):
        ages = parse_age_groups(row['AGE_GROUP'])
        for age in ages:
            expanded_rows.append({
                'DESTINATION_FIPS': row['DESTINATION_FIPS'],
                'AGE': age,
                'WEIGHT': row['WEIGHT']
            })

    # Create new DataFrame from expanded rows
    df_expanded = pl.DataFrame(expanded_rows)

    # Pivot back to wide format with individual ages as columns
    df = df_expanded.pivot(
        on='AGE',
        index='DESTINATION_FIPS',
        values='WEIGHT'
    ).fill_null(0)

    # Write to CSV
    df.write_csv(os.path.join(PROCESSED_FILES, 'state_acs_immigration_weights_age_2011_2015.csv'))
if __name__ == '__main__':
    main()
