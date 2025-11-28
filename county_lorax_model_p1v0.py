"""
Author:  Phil Morefield
Purpose: Create county-level population projections using the 2025 vintage
         Congressional Budget Office (CBO) projections
Created: November 10th, 2025

20251110 - p1v1: Use CBO 2025 population estimate as the launch population
"""
import os
import time

import polars as pl


BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'

INPUT_FOLDER = os.path.join(BASE_FOLDER, 'inputs')
CENSUS_CSV_FOLDER = os.path.join(INPUT_FOLDER, 'raw_files', 'Census')
DATABASE_FOLDER = os.path.join(INPUT_FOLDER, 'databases')
OUTPUT_FOLDER = os.path.join(BASE_FOLDER, 'outputs', 'CBO')
OUTPUT_DATABASE = os.path.join(OUTPUT_FOLDER, 'p1v1.sqlite')
OUTPUT_DATABASE_URI = f'sqlite:{OUTPUT_DATABASE}'


def make_fips_changes(df):
    csv_name = 'fips_or_name_changes.csv'
    df_fips = pl.read_csv(source=os.path.join(INPUT_FOLDER, csv_name))
    df_fips = df_fips.with_columns(pl.col('OLD_FIPS').cast(pl.Utf8).str.zfill(5))
    df_fips = df_fips.with_columns(pl.col('NEW_FIPS').cast(pl.Utf8).str.zfill(5))

    if {'GEOID', 'AGE', 'SEX'}.issubset(df.columns):
        df = df.join(other=df_fips,
                     how='left',
                     left_on='GEOID',
                     right_on='OLD_FIPS')
        df = df.with_columns(pl.when(pl.col('NEW_FIPS').is_not_null())
                             .then(pl.col('NEW_FIPS'))
                             .otherwise(pl.col('GEOID'))
                             .alias('GEOID'))

        df = df.drop(['NEW_FIPS', 'NEW_NAME', 'NEW_STUSPS'])
        df = df.group_by(['GEOID', 'AGE', 'SEX']).agg(pl.col('POPULATION').sum())
    else:
        Exception("DataFrame doesn't have required columns for FIPS changes")

    assert df.null_count().sum_horizontal()[0] == 0, "NaN values present after FIPS changes"

    return df


def get_cbo_population():
    cols = ['AGE',
            'TOTAL_POPULATION',
            'TOTAL_MALE',
            'TOTAL_MALE_SINGLE',
            'TOTAL_MALE_MARRIED',
            'TOTAL_MALE_WIDOWED',
            'TOTAL_MALE_DIVORCED',
            'TOTAL_FEMALE',
            'TOTAL_FEMALE_SINGLE',
            'TOTAL_FEMALE_MARRIED',
            'TOTAL_FEMALE_WIDOWED',
            'TOTAL_FEMALE_DIVORCED']

    csv_folder = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'CBO', '57059-2025-09-Demographic-Projections')
    csv_fn = '57059-2025-09-Demographic-Projections.xlsx'
    df = pl.read_excel(source=os.path.join(csv_folder, csv_fn),
                       sheet_name='2. Pop by age, sex, marital',
                       read_options={'skip_rows': 9})

    # Set column names and drop null columns/rows
    df.columns = cols
    df = df.drop_nulls()

    # Select and clean the required columns
    df = df.select(['AGE', 'TOTAL_MALE', 'TOTAL_FEMALE']).drop_nulls()
    # df = df.filter(pl.col('AGE') != 'Age')

    # Convert to appropriate types
    df = df.with_columns([pl.col('TOTAL_MALE').cast(pl.Int32),
                          pl.col('TOTAL_FEMALE').cast(pl.Int32)])

    # Split into chunks of 101 rows and add year
    n = 100
    df_list = []
    for i in range(0, df.height, n):
        chunk = df.slice(i, n).with_columns(pl.lit(2022 + i // n).alias('YEAR'))
        df_list.append(chunk)

    df = pl.concat(df_list)
    df = df.filter(pl.col('YEAR') == 2024).drop('YEAR').rename({'TOTAL_MALE': 'MALE', 'TOTAL_FEMALE': 'FEMALE'})
    df = df.unpivot(index='AGE', variable_name='SEX', value_name='POPULATION_CBO')
    df = df.with_columns(pl.when(pl.col('AGE') > 85)
                         .then(85)
                         .otherwise(pl.col('AGE').cast(pl.Int32)).alias('AGE'))
    df = df.group_by(['AGE', 'SEX']).agg(pl.col('POPULATION_CBO').sum())

    assert df.shape == (172, 3)

    return df


def set_launch_population():
    '''
    2024 launch population is taken from U.S. Census Intercensal Population
    Estimates.
    '''
    census_sya_input_folder = os.path.join(INPUT_FOLDER, 'raw_files', 'Census', '2024', 'intercensal', 'syasex')

    df_list = None
    for csv in os.listdir(census_sya_input_folder):
        if csv.endswith('.csv'):
            temp = pl.read_csv(source=os.path.join(census_sya_input_folder, csv),
                                  encoding='latin1').filter(pl.col('YEAR') == 6)
            temp = temp.with_columns((pl.col('STATE').cast(pl.Utf8).str.zfill(2) +
                        pl.col('COUNTY').cast(pl.Utf8).str.zfill(3))
                        .alias('GEOID')).rename({'TOT_MALE': 'MALE', 'TOT_FEMALE': 'FEMALE'})
            temp = temp.select(['GEOID', 'AGE', 'MALE', 'FEMALE'])
            temp = temp.unpivot(index=['GEOID', 'AGE'], variable_name='SEX', value_name='POPULATION')

            if df_list is None:
                df_list = [temp]
            else:
                df_list.append(temp)
    df = pl.concat(items=df_list, how='vertical')

    df = df.sort(['GEOID', 'AGE', 'SEX'])
    df = make_fips_changes(df)

    # Group by age and sex, calculate percentages
    df = df.with_columns((pl.col('POPULATION') / pl.col('POPULATION').sum().over(['AGE', 'SEX']) * 100)
                           .alias('POPULATION') )
    assert df.shape == (538016, 4)

    cbo_2024_pop = get_cbo_population()

    df = df.join(other=cbo_2024_pop,
                 on=['AGE', 'SEX'],
                 how='left',
                 coalesce=True)
    assert df.shape == (538016, 5)

    df = df.with_columns((pl.col('POPULATION_CBO') * (pl.col('POPULATION') / 100.0)).alias('POPULATION'))

    # calculate and save fractional population
    df = df.with_columns(pl.col('POPULATION').round().alias('POPULATION_ROUNDED'))
    df = df.with_columns((pl.col('POPULATION') - pl.col('POPULATION_ROUNDED')).alias('POPULATION_REMAINDER'))
    population_r = df.select(['GEOID', 'AGE', 'SEX', 'POPULATION_REMAINDER']).clone()
    df = df.with_columns(pl.col('POPULATION').round().alias('POPULATION'))
    df = df.select(['GEOID', 'AGE', 'SEX', 'POPULATION'])

    population_r.write_database(table_name='population_by_age_sex_CBO_r',
                                connection=OUTPUT_DATABASE_URI,
                                if_table_exists='replace',
                                engine='adbc')

    return df


def main(scenario, version, fert_calibr_pct, mort_calibr_pct):
    '''
    TODO: Add docstring
    '''
    model = Projector(scenario=scenario,
                      version=version,
                      fert_calibr=fert_calibr_pct,
                      mort_calibr=mort_calibr_pct)
    model.run()


class Projector():
    '''
    TODO: Add docstring
    '''
    def __init__(self, scenario, version, fert_calibr, mort_calibr):

        # time-related attributes
        self.launch_year = 2024
        self.current_projection_year = self.launch_year + 1

        # scenario-related attributes
        self.scenario = scenario
        self.version = version

        # population-related attributes
        self.current_pop = None
        self.population_time_series = None

        # immigration-related attributes
        self.immigrants = None

        # mortality-related attributes
        self.deaths = None
        self.mort_calibr = mort_calibr

        # migration-related attributes
        self.net_migration = None

        # fertility-related attributes
        self.births = None
        self.fert_calibr_pct = fert_calibr


    def run(self, final_projection_year=2098):
        '''
        TODO:
        '''
        self.current_pop = set_launch_population()

        while self.current_projection_year <= final_projection_year:
            print("##############")
            print("###        ###")
            print(f"###  {self.current_projection_year}  ###")
            print("###        ###")
            print("##############")
            print(f"{time.ctime()}")
            print(f"Total population (start): {int(self.current_pop.select('POPULATION').sum().item()):,}\n")

            ############
            ## DEATHS ##
            ############

            self.mortality()  # creates self.death
            self.current_pop = (self.current_pop.join(self.deaths,
                                                      on=['GEOID', 'AGE', 'SEX'],
                                                      how='left',
                                                      coalesce=True)
                                .with_columns(pl.col('POPULATION') - pl.col('DEATHS')
                                .alias('POPULATION'))
                                .drop('DEATHS'))

            # assert self.current_pop.shape == (675648, 5)
            # self.current_pop = self.current_pop.with_columns(pl.col('POPULATION').clip(lower_bound=0))
            assert sum(self.current_pop.null_count()).item() == 0
            assert self.current_pop.filter(pl.col('POPULATION') < 0).shape[0] == 0
            self.deaths = None

            #################
            ## IMMIGRATION ##
            #################

            # calculate net international immigration
            self.immigration()  # creates self.immigrants
            self.current_pop = (self.current_pop.join(self.immigrants,
                                                      on=['GEOID', 'AGE', 'SEX'],
                                                      how='left',
                                                      coalesce=True)
                                .with_columns(pl.when(pl.col('NET_IMMIGRATION').is_not_null()).then(pl.col('POPULATION') + pl.col('NET_IMMIGRATION'))
                                .otherwise(pl.col('POPULATION'))
                                .alias('POPULATION'))
                                .drop('NET_IMMIGRATION'))

            # assert self.current_pop.shape == (675648, 5)
            # self.current_pop = self.current_pop.with_columns(pl.col('POPULATION').clip(lower_bound=0))
            assert sum(self.current_pop.null_count()).item() == 0
            assert self.current_pop.filter(pl.col('POPULATION') < 0).shape[0] == 0
            self.immigrants = None

            ###############
            ## MIGRATION ##
            ###############

            # calculate domestic migration
            self.migration()  # creates self.net_migration
            self.current_pop = (self.current_pop.join(other=self.net_migration,
                                                      on=['GEOID', 'AGE', 'SEX'],
                                                      how='left',
                                                      coalesce=True)
                                .fill_null(0)
                                .with_columns((pl.col('POPULATION') + pl.col('NET_MIGRATION'))
                                .alias('POPULATION')))
            self.current_pop = self.current_pop.drop('NET_MIGRATION')

            # assert self.current_pop.shape == (675648, 5)
            # self.current_pop = self.current_pop.with_columns(pl.col('POPULATION').clip(lower_bound=0))
            assert sum(self.current_pop.null_count()).item() == 0
            assert self.current_pop.filter(pl.col('POPULATION') < 0).shape[0] == 0
            self.net_migration = None

            ############
            ## BIRTHS ##
            ############

            # calculate births
            self.fertility()  # create self.births

            # age everyone by one year
            self.current_pop = (self.current_pop.with_columns(pl.when(pl.col('AGE') <= 84)
                                                                .then(pl.col('AGE') + 1)
                                                                .otherwise(pl.col('AGE'))
                                                                .alias('AGE')))
            # add new 85 year olds to the 85+ group
            self.current_pop = self.current_pop.group_by(['GEOID', 'AGE', 'SEX']).agg(pl.col('POPULATION').sum())

            assert self.current_pop.shape == (531760, 4)

            # add births to self.current_pop
            self.births = self.births.rename(mapping={'BIRTHS': 'POPULATION'})
            self.births = self.births[:, self.current_pop.columns]
            self.current_pop = pl.concat(items=[self.current_pop, self.births],
                                         how='vertical_relaxed')

            assert self.current_pop.shape == (538016, 4)
            self.births = None

            self.current_pop = self.current_pop.sort(['GEOID', 'SEX', 'AGE'])

            # add cumulative remainders from previous time steps (or from
            # setting up launch population if this is the first time step)
            # if self.current_projection_year > self.launch_year + 1:
            query = f'SELECT * FROM population_by_age_sex_{self.scenario}_r'
            remainders = pl.read_database_uri(query=query, uri=OUTPUT_DATABASE_URI)
            self.current_pop = (self.current_pop.join(other=remainders,
                                                        on=['GEOID', 'AGE', 'SEX'],
                                                        how='left',
                                                        coalesce=True))
            self.current_pop = self.current_pop.with_columns(pl.when(pl.col('POPULATION_REMAINDER').is_not_null())
                                                                .then(pl.col('POPULATION') + pl.col('POPULATION_REMAINDER'))
                                                                .otherwise(pl.col('POPULATION'))
                                                                .alias('POPULATION'))

            # calculate and save fractional population
            self.current_pop = self.current_pop.with_columns(pl.col('POPULATION').round().alias('POPULATION_ROUNDED'))
            self.current_pop = self.current_pop.with_columns((pl.col('POPULATION') - pl.col('POPULATION_ROUNDED')).alias('POPULATION_REMAINDER'))
            population_r = self.current_pop.select(['GEOID', 'AGE', 'SEX', 'POPULATION_REMAINDER']).clone()
            self.current_pop = self.current_pop.with_columns(pl.col('POPULATION').round().alias('POPULATION'))
            self.current_pop = self.current_pop.select(['GEOID', 'AGE', 'SEX', 'POPULATION'])

            population_r.write_database(table_name=f'population_by_age_sex_{self.scenario}_r',
                                        connection=OUTPUT_DATABASE_URI,
                                        if_table_exists='replace',
                                        engine='adbc')

            if self.population_time_series is None:
                self.population_time_series = self.current_pop.clone()
            else:
                self.population_time_series = pl.concat(items=[self.population_time_series, self.current_pop], how='align')
            self.population_time_series = self.population_time_series.rename({'POPULATION': str(self.current_projection_year)})
            self.current_projection_year += 1

            print(f"Total population (end): {int(self.current_pop.select('POPULATION').sum().item()):,}\n")

            # save results to sqlite3 database
            temp = self.population_time_series.clone()
            temp = temp.sort(by=['GEOID', 'SEX', 'AGE'])
            temp.write_database(table_name=f'population_by_age_sex_{self.scenario}',
                                connection=OUTPUT_DATABASE_URI,
                                if_table_exists='replace',
                                engine='adbc')
            del temp


    def mortality(self):
        '''
        Placeholder
        '''

        print("Calculating mortality...", end='')

        # get CDC mortality rates by AGE, SEX, and COUNTY
        mort_rates = os.path.join(DATABASE_FOLDER, 'mortality_2019_2023_county.csv')
        county_mort_rates = (pl.read_csv(source=mort_rates)
                             .with_columns(pl.col('GEOID').cast(pl.String).str.zfill(5).alias('GEOID')))

        df = self.current_pop.clone()
        df = df.join(other=county_mort_rates,
                        on=['GEOID', 'AGE', 'SEX'],
                        how='left',
                        coalesce=True)

        assert sum(df.null_count()).item() == 0

        # get CBO mortality rate adjustments
        cbo_mort_csv = os.path.join(DATABASE_FOLDER, 'cbo_mortality_p1v1.csv')
        cbo_mort_multiply = pl.read_csv(source=cbo_mort_csv).with_columns(pl.col('AGE'))
        cbo_mort_multiply = cbo_mort_multiply.select(['AGE', 'SEX', f'ASMR_{self.current_projection_year}'])
        cbo_mort_multiply = cbo_mort_multiply.rename({f'ASMR_{self.current_projection_year}': 'MORT_MULTIPLY'})

        # join CBO mortality rate adjustments
        df = df.join(other=cbo_mort_multiply,
                     on=['AGE', 'SEX'],
                     how='left',
                     coalesce=True)

        assert sum(df.null_count()).item() == 0

        df = df.with_columns(((pl.col('MORTALITY_RATE_100K') * (1.0 + (0.01 * self.mort_calibr)) * pl.col('MORT_MULTIPLY')) / 100000.0).alias('MORT_PROJ'))

        # calculate deaths
        df = df.with_columns((pl.col('MORT_PROJ') * pl.col('POPULATION')).alias('DEATHS'))
        df = df.select(['GEOID', 'AGE', 'SEX', 'DEATHS'])
        assert sum(df.null_count()).item() == 0

        # store deaths
        self.deaths = df.clone()
        total_deaths_this_year = round(self.deaths.select(pl.col('DEATHS').sum()).item())

        # store time series of mortality in sqlite3
        if self.current_projection_year == self.launch_year + 1:
            deaths = self.deaths.rename({'DEATHS': str(self.current_projection_year)})
        else:
            query = f'SELECT * FROM deaths_by_age_sex_{self.scenario}'
            deaths = pl.read_database_uri(query=query, uri=OUTPUT_DATABASE_URI)
            current_deaths = self.deaths.clone()
            current_deaths = current_deaths.rename({'DEATHS': str(self.current_projection_year)})
            deaths = pl.concat(items=[deaths, current_deaths], how='align')
        deaths.sort(by=['GEOID', 'SEX', 'AGE'])
        # assert deaths.shape[0] == 675648
        assert sum(deaths.null_count()).item() == 0

        deaths.write_database(table_name=f'deaths_by_age_sex_{self.scenario}',
                              connection=OUTPUT_DATABASE_URI,
                              if_table_exists='replace',
                              engine='adbc')

        print(f"finished! ({total_deaths_this_year:,} deaths this year)")

    def immigration(self):
        '''
        Calculate net immigration
        '''
        print("Calculating net immigration...", end='')
        # get the County level age-sex proportions
        county_weights_csv = os.path.join(DATABASE_FOLDER, 'acs_immigration_age_sex_fractions_2011_2015.csv')
        county_weights = pl.read_csv(source=county_weights_csv)
        county_weights = county_weights.with_columns(pl.col('GEOID').cast(pl.String).str.zfill(5).alias('GEOID'))

        # this is the net migrants for each age-sex combination
        df_cbo = pl.read_csv(source=os.path.join(DATABASE_FOLDER, 'cbo_national_net_migration_by_year_age_sex.csv'))
        df_cbo = (df_cbo.filter(pl.col('YEAR') == self.current_projection_year)
                        .select(['AGE', 'SEX', 'NET_IMMIGRATION'])
                        .with_columns(pl.col('AGE').cast(pl.Int32)))
        df = (county_weights.join(other=df_cbo,
                                  on=['AGE', 'SEX'],
                                  how='left',
                                  coalesce=True)
                            .with_columns((pl.col('NET_IMMIGRATION') * pl.col('PERCENT_OF_AGE_SEX_COHORT'))
                            .alias('NET_IMMIGRATION'))
                            .drop('PERCENT_OF_AGE_SEX_COHORT'))

        assert sum(df.null_count()).item() == 0

        self.immigrants = df.clone()

        # store time series of immigration in sqlite3
        if self.current_projection_year == self.launch_year + 1:
            immigration = self.immigrants.rename({'NET_IMMIGRATION': str(self.current_projection_year)}).clone()
        else:
            query = f'SELECT * FROM immigration_by_age_sex_{self.scenario}'
            immigration = pl.read_database_uri(query=query, uri=OUTPUT_DATABASE_URI)
            current_immigration = self.immigrants.clone()
            current_immigration = current_immigration.rename({'NET_IMMIGRATION': str(self.current_projection_year)}).clone()
            immigration = pl.concat(items=[immigration, current_immigration], how='align')

        assert sum(immigration.null_count()).item() == 0

        immigration.write_database(table_name=f'immigration_by_age_sex_{self.scenario}',
                                   connection=OUTPUT_DATABASE_URI,
                                   if_table_exists='replace',
                                   engine='adbc')

        total_immigrants_this_year = round(immigration.select(f'{self.current_projection_year}').sum().item())
        print(f"finished! ({total_immigrants_this_year:,} net immigrants this year)")

    def migration(self):
        '''
        Calculate domestic migration
        '''
        print("Calculating domestic migration...")

        # get the age-sex migration rates specific to each ORIGIN-DESTINATION
        rates = pl.read_csv(os.path.join(DATABASE_FOLDER, 'acs_gross_migration_age_sex_fractions_2011_2015.csv'))
        rates = rates.with_columns([pl.col('ORIGIN_FIPS').cast(pl.String).str.zfill(5).alias('ORIGIN_FIPS'),
                                   pl.col('DESTINATION_FIPS').cast(pl.String).str.zfill(5).alias('DESTINATION_FIPS')])

        # we have migration rates to/from Puerto Rico, but not currently
        # modeling migration involving PR
        rates = rates.filter(~pl.col('ORIGIN_FIPS').str.starts_with('7'))
        rates = rates.filter(~pl.col('DESTINATION_FIPS').str.starts_with('7'))

        # compute all county to county migration flows
        # join current population with migration rates ORIGIN_FIPS
        migr = rates.join(other=self.current_pop.clone(),
                          left_on=['ORIGIN_FIPS', 'AGE', 'SEX'],
                          right_on=['GEOID', 'AGE', 'SEX'],
                          how='left',
                          coalesce=True).rename({'POPULATION': 'ORIGIN_POPULATION'})

        assert sum(migr.null_count()).item() == 0

        # calculate net migration flows
        migr = migr.with_columns((pl.col('MIGRATION_RATE') * pl.col('ORIGIN_POPULATION')).alias('FLOW'))
        inflows = (migr.with_columns(pl.col('FLOW').sum().over(['DESTINATION_FIPS', 'AGE', 'SEX'])
                                           .alias('INFLOWS'))
                                           .select(['DESTINATION_FIPS', 'AGE', 'SEX', 'INFLOWS'])
                                           .unique()
                                           .rename({'DESTINATION_FIPS': 'GEOID'}))
        outflows = (migr.with_columns(pl.col('FLOW').sum().over(['ORIGIN_FIPS', 'AGE', 'SEX'])
                                            .alias('OUTFLOWS'))
                                            .select(['ORIGIN_FIPS', 'AGE', 'SEX', 'OUTFLOWS'])
                                            .unique()
                                            .rename({'ORIGIN_FIPS': 'GEOID'}))

        net_migr = inflows.join(other=outflows,
                                on=['GEOID', 'AGE', 'SEX'],
                                how='full',
                                coalesce=True).fill_null(0)
        assert round(inflows.select(pl.col.INFLOWS).sum().item()) == round(outflows.select(pl.col.OUTFLOWS).sum().item())
        total_migrants_this_year = round(net_migr.select(pl.col('INFLOWS').sum()).item())

        self.net_migration = net_migr.with_columns((pl.col('INFLOWS') - pl.col('OUTFLOWS')).alias('NET_MIGRATION'))

        assert self.net_migration.shape[0] == 483572
        assert self.net_migration.null_count().sum_horizontal().item() == 0
        assert self.net_migration.filter(pl.col('NET_MIGRATION').is_nan()).shape[0] == 0

        # store time series of migration in sqlite3
        if self.current_projection_year == self.launch_year + 1:
            migration = self.net_migration.rename({'NET_MIGRATION': f'NETMIG{self.current_projection_year}',
                                                    'INFLOWS': f'INMIG{self.current_projection_year}',
                                                    'OUTFLOWS': f'OUTMIG{self.current_projection_year}'}).clone()
        else:
            query = f'SELECT * FROM migration_by_age_sex_{self.scenario}'
            migration = pl.read_database_uri(query=query, uri=OUTPUT_DATABASE_URI)
            current_migration = self.net_migration.clone().rename({'NET_MIGRATION': f'NETMIG{self.current_projection_year}',
                                                                   'INFLOWS': f'INMIG{self.current_projection_year}',
                                                                   'OUTFLOWS': f'OUTMIG{self.current_projection_year}'})
            migration = migration.join(current_migration,
                                       on=['GEOID', 'AGE', 'SEX'],
                                       how='left',
                                       coalesce=True).sort(by=['GEOID', 'AGE', 'SEX'])

        self.net_migration = self.net_migration.drop(['INFLOWS', 'OUTFLOWS'])
        migration.write_database(table_name=f'migration_by_age_sex_{self.scenario}',
                                 connection=OUTPUT_DATABASE_URI,
                                 if_table_exists='replace',
                                 engine='adbc')

        pct_migration = round(((total_migrants_this_year / self.current_pop.select('POPULATION').sum().item())) * 100.0, 1)
        print(f"...finished! ({total_migrants_this_year:,} total migrants this year; {pct_migration}% of the current population)")


    def fertility(self):
        '''
        Calculate births
        '''
        print("Calculating fertility...", end='')

        # get CDC fertility rates by AGE (15-44) and COUNTY
        county_fert_rates = (pl.read_csv(source=os.path.join(DATABASE_FOLDER, 'fertility_2020_2024_county.csv'))
                                         .with_columns(pl.col('GEOID').cast(pl.String).str.zfill(5).alias('GEOID')))

        df = self.current_pop.filter((pl.col('SEX') == 'FEMALE') & (pl.col('AGE').is_between(15, 44)))

        # get CBO fertility rate adjustments
        fert_multiply = (pl.read_csv(source=os.path.join(DATABASE_FOLDER, 'cbo_fertility_p1v1.csv'))
                         .select(['AGE', f'ASFR_{self.current_projection_year}'])
                         .rename({f'ASFR_{self.current_projection_year}': 'FERT_MULT'}))

        # adjust the county fertility rates using change factors from
        # CBO and then calculate births
        df = df.join(other=county_fert_rates,
                     on=['GEOID', 'AGE'],
                     how='left',
                     coalesce=True)

        df = df.join(other=fert_multiply,
                     on='AGE',
                     how='left',
                     coalesce=True)

        df = df.with_columns(((pl.col('FERTILITY') * (1.0 + (0.01 * self.fert_calibr_pct)) * pl.col('FERT_MULT') / 1000) * pl.col('POPULATION')).alias('TOTAL_BIRTHS'))
        df = df.with_columns((pl.col('TOTAL_BIRTHS') * 0.512195122).alias('MALE'))  # from Mathews, et al. (2005)
        df = df.with_columns((pl.col('TOTAL_BIRTHS') - pl.col('MALE')).alias('FEMALE'))
        df = (df.select(['GEOID', 'MALE', 'FEMALE'])
                .unpivot(index='GEOID', variable_name='SEX', value_name='BIRTHS')
                .group_by(['GEOID', 'SEX']).agg(pl.col('BIRTHS').sum()))
        df = df.with_columns(pl.lit(0).alias('AGE'))
        assert sum(df.null_count()).item() == 0

        # store births
        self.births = df.clone()
        total_births_this_year = round(self.births.select('BIRTHS').sum().item())

        # store time series of fertility in sqlite3
        if self.current_projection_year == self.launch_year + 1:
            births = self.births.rename({'BIRTHS': str(self.current_projection_year)})
        else:
            query = f'SELECT * FROM births_by_age_sex_{self.scenario}'
            births = pl.read_database_uri(query=query, uri=OUTPUT_DATABASE_URI)
            current_births = self.births.clone()
            current_births = current_births.rename({'BIRTHS': str(self.current_projection_year)}).clone()
            births = pl.concat(items=[births, current_births], how='align')
        births.sort(by=['GEOID', 'AGE'])
        assert births.shape[0] == 6256
        assert sum(births.null_count()).item() == 0
        births.write_database(table_name=f'births_by_age_sex_{self.scenario}',
                              connection=OUTPUT_DATABASE_URI,
                              if_table_exists='replace',
                              engine='adbc')

        print(f"finished! ({total_births_this_year:,} births this year)")


if __name__ == '__main__':
    print(time.ctime())
    main(scenario='CBO',
         version='p1v1',
         fert_calibr_pct=0.0,
         mort_calibr_pct=0.0)
    print(time.ctime())
