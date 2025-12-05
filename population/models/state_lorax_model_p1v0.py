"""
Author:  Phil Morefield
Purpose: Create county-level population projections using the 2025 vintage
         Congressional Budget Office (CBO) projections
Created: November 10th, 2025

20251110 - p1v0: Use CBO 2025 population estimate as the launch population
"""
import os
import time

import polars as pl


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'

INPUT_FOLDER = os.path.join(BASE_FOLDER, 'inputs')
CENSUS_CSV_FOLDER = os.path.join(INPUT_FOLDER, 'raw_files', 'Census')
PROCESSED_FILES = os.path.join(INPUT_FOLDER, 'processed_files')
OUTPUT_FOLDER = os.path.join(BASE_FOLDER, 'outputs')

FERT_MULT_PARAM = 1.0  # fertility multiplier parameter
MORT_MULT_PARAM = 1.21 # mortality multiplier parameter


# Define five-year age groups
AGE_GROUPS = ['0-4', '5-9', '10-14', '15-19', '20-24', '25-29', '30-34',
              '35-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-69',
              '70-74', '75-79', '80-84', '85+']

def age_to_age_group(age):
    """Convert single year age to five-year age group."""
    if age >= 85:
        return '85+'
    else:
        group_start = (age // 5) * 5
        return f"{group_start}-{group_start + 4}"


def make_fips_changes(df):
    csv_name = 'fips_or_name_changes.csv'
    df_fips = pl.read_csv(source=os.path.join(INPUT_FOLDER, csv_name))
    df_fips = df_fips.with_columns(pl.col('OLD_FIPS').cast(pl.Utf8).str.zfill(5))
    df_fips = df_fips.with_columns(pl.col('NEW_FIPS').cast(pl.Utf8).str.zfill(5))

    if {'GEOID', 'AGE_GROUP', 'SEX'}.issubset(df.columns):
        df = df.join(other=df_fips,
                     how='left',
                     left_on='GEOID',
                     right_on='OLD_FIPS')
        df = df.with_columns(pl.when(pl.col('NEW_FIPS').is_not_null())
                             .then(pl.col('NEW_FIPS'))
                             .otherwise(pl.col('GEOID'))
                             .alias('GEOID'))

        df = df.drop(['NEW_FIPS', 'NEW_NAME', 'NEW_STUSPS'])
        df = df.group_by(['GEOID', 'AGE_GROUP', 'SEX']).agg(pl.col('POPULATION').sum())
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

    # Convert ages to age groups
    df = df.with_columns(pl.col('AGE').map_elements(age_to_age_group).alias('AGE_GROUP'))
    df = df.group_by(['AGE_GROUP', 'SEX']).agg(pl.col('POPULATION_CBO').sum())

    assert df.shape == (36, 3)

    return df


def set_launch_population():
    '''
    2024 launch population is taken from U.S. Census Intercensal Population
    Estimates.
    '''
    census_sya_input_folder = os.path.join(INPUT_FOLDER, 'raw_files', 'Census', '2024', 'intercensal', 'syasex')

    df_list = []
    for csv in os.listdir(census_sya_input_folder):
        if csv.endswith('.csv'):
            temp = pl.read_csv(source=os.path.join(census_sya_input_folder, csv),
                                  encoding='latin1').filter(pl.col('YEAR') == 6)
            temp = temp.with_columns((pl.col('STATE').cast(pl.String).str.zfill(2)).alias('GEOID')).rename({'TOT_MALE': 'MALE', 'TOT_FEMALE': 'FEMALE'})
            temp = temp.select(['GEOID', 'AGE', 'MALE', 'FEMALE'])
            # Convert ages to age groups
            temp = temp.with_columns(pl.col('AGE').map_elements(age_to_age_group).alias('AGE_GROUP')).drop('AGE')
            temp = temp.unpivot(index=['GEOID', 'AGE_GROUP'], variable_name='SEX', value_name='POPULATION')
            # Aggregate by age groups
            temp = temp.group_by(['GEOID', 'AGE_GROUP', 'SEX']).agg(pl.col('POPULATION').sum())

            df_list.append(temp)
    df = pl.concat(items=df_list, how='vertical')

    df = df.sort(['GEOID', 'AGE_GROUP', 'SEX'])
    df = make_fips_changes(df)

    # Group by age group and sex, calculate percentages
    df = df.with_columns((pl.col('POPULATION') / pl.col('POPULATION').sum().over(['AGE_GROUP', 'SEX']) * 100)
                           .alias('POPULATION') )

    cbo_2024_pop = get_cbo_population()

    df = df.join(other=cbo_2024_pop,
                 on=['AGE_GROUP', 'SEX'],
                 how='left',
                 coalesce=True)

    df = df.with_columns((pl.col('POPULATION_CBO') * (pl.col('POPULATION') / 100.0)).alias('POPULATION'))

    # calculate and save fractional population
    df = df.with_columns(pl.col('POPULATION').round().alias('POPULATION_ROUNDED'))
    df = df.with_columns((pl.col('POPULATION') - pl.col('POPULATION_ROUNDED')).alias('POPULATION_REMAINDER'))
    population_r = df.select(['GEOID', 'AGE_GROUP', 'SEX', 'POPULATION_REMAINDER']).clone()
    df = df.with_columns(pl.col('POPULATION').round().alias('POPULATION'))
    df = df.select(['GEOID', 'AGE_GROUP', 'SEX', 'POPULATION'])

    population_r.write_csv(os.path.join(OUTPUT_FOLDER, 'population_by_age_group_sex_CBO_r'))

    return df


def main(scenario, version):
    '''
    TODO: Add docstring
    '''
    model = Projector(scenario=scenario,
                      version=version)
    model.run()


class Projector():
    '''
    TODO: Add docstring
    '''
    def __init__(self, scenario, version):

        # time-related attributes
        self.launch_year = 2024
        self.current_projection_year = self.launch_year + 5

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

        # migration-related attributes
        self.net_migration = None

        # fertility-related attributes
        self.births = None


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
                                                      on=['GEOID', 'AGE_GROUP', 'SEX'],
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
                                                      on=['GEOID', 'AGE_GROUP', 'SEX'],
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
                                                      on=['GEOID', 'AGE_GROUP', 'SEX'],
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

            # age everyone by five years (advance age groups)
            self.advance_age_groups()

            # add births to self.current_pop
            self.births = self.births.rename(mapping={'BIRTHS': 'POPULATION'})
            self.births = self.births[:, self.current_pop.columns]
            self.current_pop = pl.concat(items=[self.current_pop, self.births],
                                         how='vertical_relaxed')

            self.births = None

            self.current_pop = self.current_pop.sort(['GEOID', 'SEX', 'AGE_GROUP'])

            # add cumulative remainders from previous time steps (or from
            # setting up launch population if this is the first time step)
            if self.current_projection_year > self.launch_year + 5:
                remainders = (pl.read_csv(os.path.join(OUTPUT_FOLDER, f'population_by_age_group_sex_{self.scenario}_r'))
                              .with_columns(pl.col('GEOID').cast(pl.String).str.zfill(2)))
                self.current_pop = (self.current_pop.join(other=remainders,
                                                        on=['GEOID', 'AGE_GROUP', 'SEX'],
                                                        how='left',
                                                        coalesce=True))
                self.current_pop = self.current_pop.with_columns(pl.when(pl.col('POPULATION_REMAINDER').is_not_null())
                                                                .then(pl.col('POPULATION') + pl.col('POPULATION_REMAINDER'))
                                                                .otherwise(pl.col('POPULATION'))
                                                                .alias('POPULATION'))

            # calculate and save fractional population
            self.current_pop = self.current_pop.with_columns(pl.col('POPULATION').round().alias('POPULATION_ROUNDED'))
            self.current_pop = self.current_pop.with_columns((pl.col('POPULATION') - pl.col('POPULATION_ROUNDED')).alias('POPULATION_REMAINDER'))
            population_r = self.current_pop.select(['GEOID', 'AGE_GROUP', 'SEX', 'POPULATION_REMAINDER']).clone()
            self.current_pop = self.current_pop.with_columns(pl.col('POPULATION').round().alias('POPULATION'))
            self.current_pop = self.current_pop.select(['GEOID', 'AGE_GROUP', 'SEX', 'POPULATION'])

            population_r.write_csv(os.path.join(OUTPUT_FOLDER, f'population_by_age_group_sex_{self.scenario}_r.csv'))

            if self.population_time_series is None:
                self.population_time_series = self.current_pop.clone()
            else:
                self.population_time_series = pl.concat(items=[self.population_time_series, self.current_pop], how='align')
            self.population_time_series = self.population_time_series.rename({'POPULATION': str(self.current_projection_year)})
            self.current_projection_year += 5

            print(f"Total population (end): {int(self.current_pop.select('POPULATION').sum().item()):,}\n")

            # save results to sqlite3 database
            temp = self.population_time_series.clone()
            temp = temp.sort(by=['GEOID', 'SEX', 'AGE_GROUP'])
            temp.write_csv(os.path.join(OUTPUT_FOLDER, f'population_by_age_group_sex_{self.scenario}.csv'))

            del temp


    def advance_age_groups(self):
        """
        Advance population from one five-year age group to the next.
        This simulates aging over a 5-year period.
        """
        print("Advancing age groups by 5 years...", end='')

        # Define age group progression mapping
        age_progression = {
            '0-4': '5-9',
            '5-9': '10-14',
            '10-14': '15-19',
            '15-19': '20-24',
            '20-24': '25-29',
            '25-29': '30-34',
            '30-34': '35-39',
            '35-39': '40-44',
            '40-44': '45-49',
            '45-49': '50-54',
            '50-54': '55-59',
            '55-59': '60-64',
            '60-64': '65-69',
            '65-69': '70-74',
            '70-74': '75-79',
            '75-79': '80-84',
            '80-84': '85+',
            '85+': '85+'  # 85+ stays in 85+
        }

        # Apply age progression
        self.current_pop = self.current_pop.with_columns(
            pl.col('AGE_GROUP').map_elements(
                lambda x: age_progression.get(x, x),
                return_dtype=pl.Utf8
            ).alias('AGE_GROUP')
        )

        # Group by the new age groups to combine any populations that moved into 85+
        self.current_pop = self.current_pop.group_by(['GEOID', 'AGE_GROUP', 'SEX']).agg(
            pl.col('POPULATION').sum()
        )

        print("finished!")


    def mortality(self):
        '''
        Calculate mortality for five-year age groups
        '''

        print("Calculating mortality...", end='')

        # Read single-year mortality rates and convert to age groups
        mort_rates = os.path.join(PROCESSED_FILES, 'mortality', 'state_adjusted_cdc_mortality_2023_p1v0.csv')
        state_mort_rates = (pl.read_csv(source=mort_rates)
                             .with_columns(pl.col('GEOID').cast(pl.String).str.zfill(2).alias('GEOID')))

        df = self.current_pop.clone()
        df = df.join(other=state_mort_rates,
                        on=['GEOID', 'AGE_GROUP', 'SEX'],
                        how='left',
                        coalesce=True)

        # Read CBO mortality adjustments
        cbo_mort_csv = os.path.join(PROCESSED_FILES, 'mortality', 'cbo_mortality_p1v0.csv')
        cbo_mort_multiply = (pl.read_csv(source=cbo_mort_csv)
                               .select(['AGE_GROUP', 'SEX', f'ASMR_{self.current_projection_year}'])
                               .rename({f'ASMR_{self.current_projection_year}': 'MORT_MULTIPLY'}))

        # join CBO mortality rate adjustments
        df = df.join(other=cbo_mort_multiply,
                     on=['AGE_GROUP', 'SEX'],
                     how='left',
                     coalesce=True)

        df = df.with_columns(((pl.col('MORTALITY_RATE_100K') * pl.col('MORT_MULTIPLY')) / 100000.0).alias('MORT_PROJ'))

        # calculate deaths over 5-year period (multiply by 5)
        df = df.with_columns((pl.col('MORT_PROJ') * pl.col('POPULATION') * 5.0 * MORT_MULT_PARAM).alias('DEATHS'))
        df = df.select(['GEOID', 'AGE_GROUP', 'SEX', 'DEATHS'])

        # store deaths
        self.deaths = df.clone()
        total_deaths_this_year = round(self.deaths.select(pl.col('DEATHS').sum()).item())

        # store time series of mortality in sqlite3
        if self.current_projection_year == self.launch_year + 5:
            deaths = self.deaths.rename({'DEATHS': str(self.current_projection_year)})
        else:
            deaths = (pl.read_csv(os.path.join(OUTPUT_FOLDER, f'deaths_by_age_group_sex_{self.scenario}.csv'))
                      .with_columns(pl.col('GEOID').cast(pl.String).str.zfill(2)))
            current_deaths = self.deaths.clone()
            current_deaths = current_deaths.rename({'DEATHS': str(self.current_projection_year)})
            deaths = pl.concat(items=[deaths, current_deaths], how='align')
        deaths.sort(by=['GEOID', 'SEX', 'AGE_GROUP'])

        deaths.write_csv(os.path.join(OUTPUT_FOLDER, f'deaths_by_age_group_sex_{self.scenario}.csv'))

        print(f"finished! ({total_deaths_this_year:,} deaths this period)")

    def immigration(self):
        '''
        Calculate net immigration for five-year age groups
        '''
        print("Calculating net immigration...", end='')
        # get the County level age-sex proportions
        county_weights_csv = os.path.join(PROCESSED_FILES, 'immigration', 'state_acs_immigration_age_sex_fractions_2011_2015.csv')
        county_weights = pl.read_csv(source=county_weights_csv)
        county_weights = county_weights.with_columns(pl.col('GEOID').cast(pl.String).str.zfill(2).alias('GEOID'))

        # this is the net migrants for each age-sex combination
        time_step = f'{self.current_projection_year - 4}-{self.current_projection_year}'
        df_cbo = pl.read_csv(source=os.path.join(PROCESSED_FILES, 'immigration', 'national_cbo_net_migration_by_year_age_sex.csv'))
        df_cbo = (df_cbo.filter(pl.col('TIME_STEP') == time_step)
                        .select(['AGE_GROUP', 'SEX', 'NET_IMMIGRATION']))

        df_cbo = df_cbo.group_by(['AGE_GROUP', 'SEX']).agg(pl.col('NET_IMMIGRATION').sum())

        df = (county_weights.join(other=df_cbo,
                                  on=['AGE_GROUP', 'SEX'],
                                  how='left',
                                  coalesce=True)
                            .with_columns((pl.col('NET_IMMIGRATION') * pl.col('PERCENT_OF_AGE_SEX_COHORT'))
                            .alias('NET_IMMIGRATION'))
                            .drop('PERCENT_OF_AGE_SEX_COHORT'))

        self.immigrants = df.clone()

        # store time series of immigration in sqlite3
        if self.current_projection_year == self.launch_year + 5:
            immigration = self.immigrants.rename({'NET_IMMIGRATION': str(self.current_projection_year)}).clone()
        else:
            immigration = (pl.read_csv(os.path.join(OUTPUT_FOLDER, f'immigration_by_age_group_sex_{self.scenario}.csv'))
                           .with_columns(pl.col('GEOID').cast(pl.String).str.zfill(2)))
            current_immigration = self.immigrants.clone()
            current_immigration = current_immigration.rename({'NET_IMMIGRATION': str(self.current_projection_year)}).clone()
            immigration = pl.concat(items=[immigration, current_immigration], how='align')

        immigration.write_csv(file=os.path.join(OUTPUT_FOLDER, f'immigration_by_age_group_sex_{self.scenario}.csv'))

        total_immigrants_this_year = round(immigration.select(f'{self.current_projection_year}').sum().item())
        print(f"finished! ({total_immigrants_this_year:,} net immigrants this period)")

    def migration(self):
        '''
        Calculate domestic migration for five-year age groups
        '''
        print("Calculating domestic migration...")

        # get the age-sex migration rates specific to each ORIGIN-DESTINATION
        rates = pl.read_csv(os.path.join(PROCESSED_FILES, 'migration', 'state_adjusted_acs_gross_migration_age_sex_fractions_2011_2015.csv'))
        rates = rates.with_columns([pl.col('ORIGIN_FIPS').cast(pl.String).str.zfill(2).alias('ORIGIN_FIPS'),
                                   pl.col('DESTINATION_FIPS').cast(pl.String).str.zfill(2).alias('DESTINATION_FIPS')])

        assert set(rates['AGE_GROUP'].unique()) == set(self.current_pop['AGE_GROUP'].unique())

        # join current population with migration rates ORIGIN_FIPS
        migr = rates.join(other=self.current_pop.clone(),
                          left_on=['ORIGIN_FIPS', 'AGE_GROUP', 'SEX'],
                          right_on=['GEOID', 'AGE_GROUP', 'SEX'],
                          how='left',
                          coalesce=True).rename({'POPULATION': 'ORIGIN_POPULATION'})

        # calculate net migration flows and multiply by 5 for five-year time step
        migr = migr.with_columns((pl.col('MIGRATION_RATE') * pl.col('ORIGIN_POPULATION') * 5).alias('FLOW'))
        inflows = (migr.with_columns(pl.col('FLOW').sum().over(['DESTINATION_FIPS', 'AGE_GROUP', 'SEX'])
                                           .alias('INFLOWS'))
                                           .select(['DESTINATION_FIPS', 'AGE_GROUP', 'SEX', 'INFLOWS'])
                                           .unique()
                                           .rename({'DESTINATION_FIPS': 'GEOID'}))
        outflows = (migr.with_columns(pl.col('FLOW').sum().over(['ORIGIN_FIPS', 'AGE_GROUP', 'SEX'])
                                            .alias('OUTFLOWS'))
                                            .select(['ORIGIN_FIPS', 'AGE_GROUP', 'SEX', 'OUTFLOWS'])
                                            .unique()
                                            .rename({'ORIGIN_FIPS': 'GEOID'}))

        net_migr = inflows.join(other=outflows,
                                on=['GEOID', 'AGE_GROUP', 'SEX'],
                                how='full',
                                coalesce=True).fill_null(0)
        assert round(inflows.select(pl.col.INFLOWS).sum().item()) == round(outflows.select(pl.col.OUTFLOWS).sum().item())
        total_migrants_this_year = round(net_migr.select(pl.col('INFLOWS').sum()).item())

        self.net_migration = net_migr.with_columns((pl.col('INFLOWS') - pl.col('OUTFLOWS')).alias('NET_MIGRATION'))

        # store time series of migration in sqlite3
        if self.current_projection_year == self.launch_year + 5:
            migration = self.net_migration.rename({'NET_MIGRATION': f'NETMIG{self.current_projection_year}',
                                                   'INFLOWS': f'INMIG{self.current_projection_year}',
                                                   'OUTFLOWS': f'OUTMIG{self.current_projection_year}'}).clone()
        else:
            migration = (pl.read_csv(os.path.join(OUTPUT_FOLDER, f'migration_by_age_group_sex_{self.scenario}.csv'))
                         .with_columns([pl.col('GEOID').cast(pl.String).str.zfill(2)]))
            current_migration = self.net_migration.clone().rename({'NET_MIGRATION': f'NETMIG{self.current_projection_year}',
                                                                   'INFLOWS': f'INMIG{self.current_projection_year}',
                                                                   'OUTFLOWS': f'OUTMIG{self.current_projection_year}'})
            migration = migration.join(current_migration,
                                       on=['GEOID', 'AGE_GROUP', 'SEX'],
                                       how='left',
                                       coalesce=True).sort(by=['GEOID', 'AGE_GROUP', 'SEX'])

        self.net_migration = self.net_migration.drop(['INFLOWS', 'OUTFLOWS'])
        migration.write_csv(os.path.join(OUTPUT_FOLDER, f'migration_by_age_group_sex_{self.scenario}.csv'))

        pct_migration = round(((total_migrants_this_year / self.current_pop.select('POPULATION').sum().item())) * 100.0, 1)
        print(f"...finished! ({total_migrants_this_year:,} total migrants this period; {pct_migration}% of the current population)")


    def fertility(self):
        '''
        Calculate births for five-year age groups
        '''
        print("Calculating fertility...", end='')

        # Define fertile age groups (15-44 converted to five-year groups)
        fertile_age_groups = ['15-19', '20-24', '25-29', '30-34', '35-39', '40-44']

        # get CDC fertility rates by AGE and COUNTY, aggregate to age groups
        state_fert_rates = (pl.read_csv(source=os.path.join(PROCESSED_FILES, 'fertility', 'state_adjusted_cdc_fertility_2024_p1v0.csv'))
                                         .with_columns(pl.col('GEOID').cast(pl.String).str.zfill(2).alias('GEOID')))


        df = self.current_pop.filter((pl.col('SEX') == 'FEMALE') & (pl.col('AGE_GROUP').is_in(fertile_age_groups)))

        # get CBO fertility rate adjustments
        fert_multiply = (pl.read_csv(source=os.path.join(PROCESSED_FILES, 'fertility', 'national_cbo_fertility_p1v0.csv'))
                         .select(['AGE_GROUP', f'ASFR_{self.current_projection_year}'])
                         .rename({f'ASFR_{self.current_projection_year}': 'FERT_MULT'}))

        # adjust the county fertility rates using change factors from
        # CBO and then calculate births
        df = df.join(other=state_fert_rates,
                     on=['GEOID', 'AGE_GROUP'],
                     how='left',
                     coalesce=True)

        df = df.join(other=fert_multiply,
                     on='AGE_GROUP',
                     how='left',
                     coalesce=True)

        # Calculate births over 5-year period (multiply by 5)
        df = df.with_columns(((pl.col('FERTILITY') / 1000) * pl.col('FERT_MULT') * pl.col('POPULATION') * 5.0 * FERT_MULT_PARAM).alias('TOTAL_BIRTHS'))
        df = df.with_columns((pl.col('TOTAL_BIRTHS') * 0.512195122).alias('MALE'))  # from Mathews, et al. (2005)
        df = df.with_columns((pl.col('TOTAL_BIRTHS') - pl.col('MALE')).alias('FEMALE'))
        df = (df.select(['GEOID', 'MALE', 'FEMALE'])
                .unpivot(index='GEOID', variable_name='SEX', value_name='BIRTHS')
                .group_by(['GEOID', 'SEX']).agg(pl.col('BIRTHS').sum()))
        df = df.with_columns(pl.lit('0-4').alias('AGE_GROUP'))

        # store births
        self.births = df.clone()
        total_births_this_year = round(self.births.select('BIRTHS').sum().item())

        # store time series of fertility in sqlite3
        if self.current_projection_year == self.launch_year + 5:
            births = self.births.rename({'BIRTHS': str(self.current_projection_year)})
        else:
            births = (pl.read_csv(source=os.path.join(OUTPUT_FOLDER, f'births_by_age_group_sex_{self.scenario}.csv'))
                      .with_columns(pl.col('GEOID').cast(pl.String).str.zfill(2)))
            current_births = self.births.clone()
            current_births = current_births.rename({'BIRTHS': str(self.current_projection_year)}).clone()
            births = pl.concat(items=[births, current_births], how='align')
        births.sort(by=['GEOID', 'AGE_GROUP'])
        births.write_csv(file=os.path.join(OUTPUT_FOLDER, f'births_by_age_group_sex_{self.scenario}.csv'))

        print(f"finished! ({total_births_this_year:,} births this period)")


if __name__ == '__main__':
    print(time.ctime())
    main(scenario='CBO',
         version='p1v0')
    print(time.ctime())
