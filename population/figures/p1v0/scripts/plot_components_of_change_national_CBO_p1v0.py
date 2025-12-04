import datetime
import os

import pandas as pd
import seaborn as sns

from matplotlib import pyplot as plt

BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census')
PROJECTIONS_FOLDER = os.path.join(BASE_FOLDER, 'outputs')

SCENARIO = 'CBO'
YEAR_MIN = 2010
YEAR_MAX = 2050


def get_cbo_population():
    cols = ['AGE',
            'TOTAL_POPULATION',
            'BLANK1',
            'TOTAL_MALE',
            'TOTAL_MALE_SINGLE',
            'TOTAL_MALE_MARRIED',
            'TOTAL_MALE_WIDOWED',
            'TOTAL_MALE_DIVORCED',
            'BLANK2',
            'TOTAL_FEMALE',
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

    df = df[['AGE', 'TOTAL_POPULATION', 'TOTAL_MALE', 'TOTAL_FEMALE']].dropna()
    df = df.loc[df['AGE'] != 'Age']
    df['TOTAL_POPULATION'] = df['TOTAL_POPULATION'].astype(int)
    df['TOTAL_MALE'] = df['TOTAL_MALE'].astype(int)
    df['TOTAL_FEMALE'] = df ['TOTAL_FEMALE'].astype(int)

    n = 101
    df_list = [df[i:i+n] for i in range(0, df.shape[0], n)]

    for i, df_item in enumerate(df_list):
        df_item.loc[:, 'YEAR'] = 2022 + i

    df = pd.concat(df_list, ignore_index=True)

    return df


def main():

    cbo = get_cbo_population()

    fig = plt.figure(constrained_layout=True)
    gs = fig.add_gridspec(3, 2)

    ######################
    ## TOTAL POPULATION ##
    ######################

    ax_pop = fig.add_subplot(gs[0, :1])

    # historical population, 2010-2020
    csv_folder = os.path.join(CENSUS_CSV_PATH, '2020', 'intercensal')
    csv_fn = 'co-est2020int-pop.xlsx'
    df = pd.read_excel(os.path.join(csv_folder, csv_fn),
                       skipfooter=6,
                       skiprows=5,
                       names=['COUNTY_STATE', '2010_base'] + [year for year in range(2010, 2021)])
    pre2020pop = df.drop(columns=['COUNTY_STATE', '2010_base']).sum().T.reset_index()
    pre2020pop.columns = ['YEAR', 'POPULATION']
    pre2020pop['POPULATION'] = pre2020pop['POPULATION'] / 1000000

    # historical population, 2020-2024
    csv_folder = os.path.join(CENSUS_CSV_PATH, '2024', 'intercensal')
    csv_fn = 'co-est2024-alldata.csv'
    df = pd.read_csv(os.path.join(csv_folder, csv_fn), encoding='latin-1')
    columns = ['SUMLEV', 'ESTIMATESBASE2020'] + [f'POPESTIMATE{year}' for year in range(2021, 2025)]
    post2020pop = df[columns]
    post2020pop = post2020pop.rename(columns={'ESTIMATESBASE2020': 'POPESTIMATE2020'})
    post2020pop = post2020pop.loc[post2020pop.SUMLEV == 40]
    post2020pop = post2020pop.drop(columns='SUMLEV').sum().reset_index()
    post2020pop.columns = ['YEAR', 'POPULATION']
    post2020pop['YEAR'] = post2020pop['YEAR'].str[-4:].astype(int)
    post2020pop['POPULATION'] = post2020pop['POPULATION'] / 1000000

    histpop = pd.concat([pre2020pop, post2020pop], ignore_index=True)

    # future population
    proj_pop = pd.read_csv(os.path.join(PROJECTIONS_FOLDER, f'population_by_age_group_sex_{SCENARIO}.csv'))

    proj_pop = proj_pop.drop(columns=['GEOID', 'AGE_GROUP', 'SEX']).sum()
    proj_pop = proj_pop.reset_index()
    proj_pop.columns = ['YEAR', 'POPULATION']
    proj_pop['YEAR'] = proj_pop['YEAR'].astype(int)
    proj_pop['POPULATION'] = proj_pop['POPULATION'] / 1000000

    # CBO future population
    # cbo_pop = cbo.loc[cbo['YEAR'] >= 2025, ['TOTAL_POPULATION', 'YEAR']]
    cbo_pop = cbo[['TOTAL_POPULATION', 'YEAR']]
    cbo_pop = cbo_pop.groupby(by='YEAR', as_index=False).sum()
    cbo_pop = cbo_pop.rename(columns={'TOTAL_POPULATION': 'POPULATION'})
    cbo_pop['POPULATION'] = cbo_pop['POPULATION'] / 1000000

    sns.lineplot(x='YEAR', y='POPULATION', data=histpop, linewidth=2, color='gray', legend=False, ax=ax_pop, label='U.S. Census\n(intercensal estimate)')
    sns.lineplot(x='YEAR', y='POPULATION', data=cbo_pop, linewidth=2, color='purple', legend=False, ax=ax_pop, label='CBO projection')
    # sns.lineplot(x='YEAR', y='POPULATION', data=proj_pop, linewidth=2, color='orange', markers='o', legend=False, ax=ax_pop, label='p1v1 projection')
    sns.scatterplot(x='YEAR', y='POPULATION', data=proj_pop, color='orange', markers='o', legend=False, ax=ax_pop, label='p1v0 projection')

    plt.title('U.S. POPULATION')
    ax_pop.set_xticklabels([])
    plt.gca().set_xlabel("")
    plt.gca().set_ylabel("")
    plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)
    fig.legend(bbox_to_anchor=(0.925, 0.925))

    ############
    ## BIRTHS ##
    ############

    # historical births
    columns = ['SUMLEV'] + ['BIRTHS' + str(year) for year in range(2010, 2021)]
    ax_births = fig.add_subplot(gs[1, :1])
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\intercensal\\co-est2020-alldata.csv')
    hist_births = pd.read_csv(csv, encoding='latin-1')

    hist_births = hist_births[columns]
    hist_births = hist_births.query('SUMLEV == 50')
    hist_births = hist_births.drop(columns='SUMLEV').sum().reset_index()
    hist_births.columns = ['YEAR', 'BIRTHS']
    hist_births['YEAR'] = hist_births['YEAR'].str[-4:].astype(int)
    hist_births.loc[hist_births['YEAR'] == 2010, 'BIRTHS'] *= 4
    hist_births['BIRTHS'] = hist_births['BIRTHS'] / 1000000

    # historical births, 2020-2024
    columns = ['SUMLEV'] + ['BIRTHS' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\intercensal\\co-est2024-alldata.csv')
    post2020_births = pd.read_csv(csv, encoding='latin-1')
    post2020_births = post2020_births[columns]
    post2020_births = post2020_births.query('SUMLEV == 50')
    post2020_births = post2020_births.drop(columns='SUMLEV').sum().reset_index()
    post2020_births.columns = ['YEAR', 'BIRTHS']
    post2020_births['YEAR'] = post2020_births['YEAR'].str[-4:].astype(int)
    post2020_births.loc[post2020_births['YEAR'] == 2020, 'BIRTHS'] *= 4
    post2020_births['BIRTHS'] = post2020_births['BIRTHS'] / 1000000

    # future births
    proj_births = pd.read_csv(os.path.join(PROJECTIONS_FOLDER, f'births_by_age_group_sex_{SCENARIO}.csv'))
    proj_births = proj_births.drop(columns=['GEOID', 'SEX', 'AGE_GROUP']).sum().T.reset_index()
    proj_births.columns = ['YEAR', 'BIRTHS']
    proj_births['YEAR'] = proj_births['YEAR'].astype(int)
    proj_births['BIRTHS'] = proj_births['BIRTHS'] / 1000000 / 5

    # CBO future births
    fert_csv_folder = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'CBO', '57059-2025-09-Demographic-Projections', 'CSV files')
    fert_csv_fn = 'fertilityRates_byYearAgePlace.csv'
    fert_df = pd.read_csv(os.path.join(fert_csv_folder, fert_csv_fn))
    fert_df.columns = ['YEAR', 'AGE', 'PLACE', 'FERTILITY_RATE_PER_K']
    fert_df = fert_df.query('PLACE == "all"').drop(columns='PLACE')
    fert_df = fert_df[fert_df['YEAR'] >= 2025].set_index(['YEAR', 'AGE'])
    fert_df = fert_df.rename(columns={'FERTILITY_RATE_PER_K': 'VALUE'})

    cbo_female = cbo.loc[cbo['YEAR'] >= 2025, ['AGE', 'TOTAL_FEMALE', 'YEAR']]
    cbo_female = cbo_female.rename(columns={'TOTAL_FEMALE': 'VALUE'})
    cbo_female['AGE'] = cbo_female['AGE'].astype(str).str.replace('100+', '100').astype(int)
    cbo_female = cbo_female.query('YEAR >= 2025 & AGE >= 14 & AGE <= 49')
    cbo_female = cbo_female.set_index(['YEAR', 'AGE'])
    cbo_female = cbo_female.rename(columns={'POPULATION': 'VALUE'})

    cbo_births = cbo_female.mul(fert_df, axis=0).div(1000).reset_index().drop(columns='AGE')
    cbo_births = cbo_births.groupby(by='YEAR', as_index=False).sum()
    cbo_births = cbo_births.rename(columns={'VALUE': 'BIRTHS'})
    cbo_births['BIRTHS'] = cbo_births['BIRTHS'] / 1000000

    sns.lineplot(x='YEAR', y='BIRTHS', data=hist_births, linewidth=2, color='gray', legend=False, ax=ax_births)
    # sns.lineplot(x='YEAR', y='BIRTHS', data=proj_births, linewidth=2, color='orange', legend=False, ax=ax_births)
    sns.scatterplot(x='YEAR', y='BIRTHS', data=proj_births, color='orange', markers='o', legend=False, ax=ax_births, label='p1v0 projection')
    sns.lineplot(x='YEAR', y='BIRTHS', data=post2020_births, linewidth=2, color='gray', legend=False, ax=ax_births)
    sns.lineplot(x='YEAR', y='BIRTHS', data=cbo_births, linewidth=2, color='purple', legend=False, ax=ax_births)

    plt.title('BIRTHS')
    ax_births.set_xticklabels([])
    ax_births.set_xlabel("")
    ax_births.set_ylabel("Millions")
    plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)

    ############################
    ## NET DOMESTIC MIGRATION ##
    ############################

    # historical migration
    ax_migration = fig.add_subplot(gs[1, 1:])

    columns = ['SUMLEV'] + ['DOMESTICMIG' + str(year) for year in range(2010, 2021)]
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\intercensal\\co-est2020-alldata.csv')
    hist_migration = pd.read_csv(csv, encoding='latin-1')

    hist_migration = hist_migration[columns]
    hist_migration = hist_migration.query('SUMLEV == 50').clip(lower=0)
    hist_migration = hist_migration.drop(columns='SUMLEV').sum().reset_index()
    hist_migration.columns = ['YEAR', 'MIGRATION']
    hist_migration['YEAR'] = hist_migration['YEAR'].str[-4:].astype(int)
    hist_migration.loc[hist_migration['YEAR'] == 2010, 'MIGRATION'] *= 4
    hist_migration['MIGRATION'] = hist_migration['MIGRATION'] / 1000000

    # historical migration, 2020-2024
    columns = ['SUMLEV'] + ['DOMESTICMIG' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\intercensal\\co-est2024-alldata.csv')
    post2020_migration = pd.read_csv(csv, encoding='latin-1')
    post2020_migration = post2020_migration[columns]
    post2020_migration = post2020_migration.query('SUMLEV == 50').clip(lower=0)
    post2020_migration = post2020_migration.drop(columns='SUMLEV').sum().reset_index()
    post2020_migration.columns = ['YEAR', 'MIGRATION']
    post2020_migration['YEAR'] = post2020_migration['YEAR'].str[-4:].astype(int)
    post2020_migration.loc[post2020_migration['YEAR'] == 2020, 'MIGRATION'] *= 4
    post2020_migration['MIGRATION'] = post2020_migration['MIGRATION'] / 1000000

    # future migration
    columns = [f'NETMIG{year}' for year in range(2029, 2095, 5)]
    columns = ['GEOID', 'AGE_GROUP'] + columns
    proj_migration = pd.read_csv(os.path.join(PROJECTIONS_FOLDER, f'migration_by_age_group_sex_{SCENARIO}.csv'))
    proj_migration = proj_migration[columns]
    proj_migration.columns = [col.replace('NETMIG', '') for col in proj_migration.columns]
    proj_migration = proj_migration.drop(columns=['GEOID', 'AGE_GROUP'])
    proj_migration = proj_migration.clip(lower=0).sum().T.reset_index()
    proj_migration.columns = ['YEAR', 'MIGRATION']
    proj_migration['YEAR'] = proj_migration['YEAR'].astype(int)
    proj_migration['MIGRATION'] = (proj_migration['MIGRATION'] / 1000000)

    sns.lineplot(x='YEAR', y='MIGRATION', data=hist_migration, linewidth=2, color='gray', legend=False, ax=ax_migration)
    # sns.lineplot(x='YEAR', y='MIGRATION', data=proj_migration, linewidth=2, color='orange', legend=False, ax=ax_migration)
    sns.scatterplot(x='YEAR', y='MIGRATION', data=proj_migration, color='orange', markers='o', legend=False, ax=ax_migration, label='p1v0 projection')
    sns.lineplot(x='YEAR', y='MIGRATION', data=post2020_migration, linewidth=2, color='gray', legend=False, ax=ax_migration)

    plt.title('MIGRATION')
    ax_migration.set_xticklabels([])
    ax_migration.set_xlabel("")
    ax_migration.set_ylabel("")
    plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)

    ############
    ## DEATHS ##
    ############

    ax_deaths = fig.add_subplot(gs[2, :1])

    # historical deaths, 2010-2020
    columns = ['SUMLEV'] + ['DEATHS' + str(year) for year in range(2010, 2021)]
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\intercensal\\co-est2020-alldata.csv')
    hist_deaths = pd.read_csv(csv, encoding='latin-1')
    hist_deaths = hist_deaths[columns]
    hist_deaths = hist_deaths.query('SUMLEV == 50')
    hist_deaths = hist_deaths.drop(columns='SUMLEV').sum().reset_index()
    hist_deaths.columns = ['YEAR', 'DEATHS']
    hist_deaths['YEAR'] = hist_deaths['YEAR'].str[-4:].astype(int)
    hist_deaths.loc[hist_deaths['YEAR'] == 2010, 'DEATHS'] *= 4
    hist_deaths['DEATHS'] = hist_deaths['DEATHS'] / 1000000

    # historical deaths, 2020-2024
    columns = ['SUMLEV'] + ['DEATHS' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\intercensal\\co-est2024-alldata.csv')
    post2020_deaths = pd.read_csv(csv, encoding='latin-1')
    post2020_deaths = post2020_deaths[columns]
    post2020_deaths = post2020_deaths.query('SUMLEV == 50')
    post2020_deaths = post2020_deaths.drop(columns='SUMLEV').sum().reset_index()
    post2020_deaths.columns = ['YEAR', 'DEATHS']
    post2020_deaths['YEAR'] = post2020_deaths['YEAR'].str[-4:].astype(int)
    post2020_deaths.loc[post2020_deaths['YEAR'] == 2020, 'DEATHS'] *= 4
    post2020_deaths['DEATHS'] = post2020_deaths['DEATHS'] / 1000000

    # future deaths
    proj_deaths = pd.read_csv(os.path.join(PROJECTIONS_FOLDER, f'deaths_by_age_group_sex_{SCENARIO}.csv'))
    proj_deaths = proj_deaths.drop(columns=['GEOID', 'SEX', 'AGE_GROUP']).sum().T.reset_index()
    proj_deaths.columns = ['YEAR', 'DEATHS']
    proj_deaths['YEAR'] = proj_deaths['YEAR'].astype(int)
    proj_deaths['DEATHS'] = (proj_deaths['DEATHS'] / 1000000) / 5

    # CBO future deaths
    mort_csv_folder = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'CBO', '57059-2025-09-Demographic-Projections', 'CSV files')
    mort_csv_fn = 'mortalityRates_byYearAgeSex.csv'
    mort_df = pd.read_csv(os.path.join(mort_csv_folder, mort_csv_fn))
    mort_df.columns = ['YEAR', 'AGE', 'SEX', 'MORTALITY_RATE_PER_K']
    mort_df = mort_df[mort_df['YEAR'] >= 2025].set_index(['YEAR', 'AGE', 'SEX'])
    mort_df = mort_df.rename(columns={'MORTALITY_RATE_PER_K': 'VALUE'})

    cbo_pop = cbo.copy()
    cbo_pop['AGE'] = cbo_pop['AGE'].astype(str).str.replace('100+', '100').astype(int)
    cbo_pop = cbo_pop.rename(columns={'TOTAL_FEMALE': 'female',
                                      'TOTAL_MALE': 'male'})
    cbo_pop = cbo_pop[['YEAR', 'AGE', 'female', 'male']]
    cbo_pop = cbo_pop.melt(id_vars=['YEAR', 'AGE'],
                           value_vars=['female', 'male'],
                           value_name='VALUE',
                           var_name='SEX')

    cbo_pop = cbo_pop.query('YEAR >= 2025')
    cbo_pop = cbo_pop.set_index(['YEAR', 'AGE', 'SEX'])

    cbo_deaths = cbo_pop.mul(mort_df, axis=0).div(1000).reset_index().drop(columns=['AGE', 'SEX'])
    cbo_deaths = cbo_deaths[['YEAR', 'VALUE']].groupby(by='YEAR', as_index=False).sum()
    cbo_deaths = cbo_deaths.rename(columns={'VALUE': 'DEATHS'})
    cbo_deaths['DEATHS'] = cbo_deaths['DEATHS'] / 1000000

    sns.lineplot(x='YEAR', y='DEATHS', data=hist_deaths, linewidth=2, color='gray', legend=False, ax=ax_deaths)
    # sns.lineplot(x='YEAR', y='DEATHS', data=proj_deaths, linewidth=2, color='orange', legend=False, ax=ax_deaths)
    sns.scatterplot(x='YEAR', y='DEATHS', data=proj_deaths, color='orange', markers='o', legend=False, ax=ax_deaths, label='p1v0 projection')
    sns.lineplot(x='YEAR', y='DEATHS', data=post2020_deaths, linewidth=2, color='gray', legend=False, ax=ax_deaths)
    sns.lineplot(x='YEAR', y='DEATHS', data=cbo_deaths, linewidth=2, color='purple', legend=False, ax=ax_deaths)

    plt.title('DEATHS')
    ax_deaths.set_xlabel('')
    ax_deaths.set_ylabel('')
    plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)

    #####################
    ## NET IMMIGRATION ##
    #####################

    ax_immig = fig.add_subplot(gs[2, 1:])

    # historical immigration, 2010-2020
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\intercensal\\co-est2020-alldata.csv')
    hist_immig = pd.read_csv(csv, encoding='latin-1')
    columns = ['SUMLEV'] + ['INTERNATIONALMIG' + str(year) for year in range(2010, 2021)]
    hist_immig = hist_immig[columns]
    hist_immig = hist_immig.query('SUMLEV == 50')
    hist_immig = hist_immig.drop(columns='SUMLEV').sum().reset_index()
    hist_immig.columns = ['YEAR', 'IMMIGRATION']
    hist_immig['YEAR'] = hist_immig['YEAR'].str[-4:].astype(int)
    hist_immig.loc[hist_immig['YEAR'] == 2010, 'IMMIGRATION'] *= 4
    hist_immig['IMMIGRATION'] = hist_immig['IMMIGRATION'] / 1000000

    # historical immigration, 2020-2024
    columns = ['SUMLEV'] + ['INTERNATIONALMIG' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\intercensal\\co-est2024-alldata.csv')
    post2020_immig = pd.read_csv(csv, encoding='latin-1')
    post2020_immig = post2020_immig[columns]
    post2020_immig = post2020_immig.query('SUMLEV == 50')
    post2020_immig = post2020_immig.drop(columns='SUMLEV').sum().reset_index()
    post2020_immig.columns = ['YEAR', 'IMMIGRATION']
    post2020_immig['YEAR'] = post2020_immig['YEAR'].str[-4:].astype(int)
    post2020_immig.loc[post2020_immig['YEAR'] == 2020, 'IMMIGRATION'] *= 4
    post2020_immig['IMMIGRATION'] = post2020_immig['IMMIGRATION'] / 1000000

    # future immigration
    proj_immig = pd.read_csv(os.path.join(PROJECTIONS_FOLDER, f'immigration_by_age_group_sex_{SCENARIO}.csv'))
    proj_immig = proj_immig.drop(columns=['GEOID', 'SEX', 'AGE_GROUP']).sum().T.reset_index()
    proj_immig.columns = ['YEAR', 'IMMIGRATION']
    proj_immig['YEAR'] = proj_immig['YEAR'].astype(int)
    proj_immig['IMMIGRATION'] = proj_immig['IMMIGRATION'] / 1000000 / 5

    sns.lineplot(x='YEAR', y='IMMIGRATION', data=hist_immig, linewidth=2, color='gray', legend=False, ax=ax_immig)
    # sns.lineplot(x='YEAR', y='IMMIGRATION', data=proj_immig, linewidth=2, color='orange', legend=False, ax=ax_immig)
    sns.scatterplot(x='YEAR', y='IMMIGRATION', data=proj_immig, color='orange', markers='o', legend=False, ax=ax_immig, label='p1v0 projection')
    sns.lineplot(x='YEAR', y='IMMIGRATION', data=post2020_immig, linewidth=2, color='gray', legend=False, ax=ax_immig)

    plt.title('IMMIGRATION')
    ax_immig.set_xlabel("")
    ax_immig.set_ylabel("")
    plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)

    # plt.tight_layout()
    month = datetime.date.today().month
    day = datetime.date.today().day
    year = datetime.date.today().year
    plt.figtext(x=0.85, y=0.95, s=f'Created: {month}/{day}/{year}', size=5)
    plt.show()

    return


if __name__ == '__main__':
    main()
