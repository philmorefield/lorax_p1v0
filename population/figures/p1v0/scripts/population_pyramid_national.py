import os
import sqlite3

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census')
PROJECTIONS_FOLDER = os.path.join(BASE_FOLDER, 'outputs')

YEAR = 2024


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
    df = df.loc[df['YEAR'] == YEAR, ['AGE', 'TOTAL_MALE', 'TOTAL_FEMALE']]
    df = df.rename(columns={'TOTAL_MALE': 'Male', 'TOTAL_FEMALE': 'Female'})
    df = df.melt(id_vars='AGE', var_name='SEX', value_name='POPULATION')
    df = df.rename(columns={'POPULATION': 'Population',
                            'AGE': 'Age',
                            'SEX': 'Sex'})

    df['Age'] = df['Age'].replace({'100+': '100'}).astype(int)
    df.loc[df.Age >= 85, 'Age'] = 85
    df = df.groupby(by=['Age', 'Sex'], as_index=False).sum()

    df.loc[df.Sex == 'Male', 'Population'] *= -1
    # df = df.sort_values(by=['Age', 'Sex'], ascending=False)

    return df


def get_census_population():
    '''
    2024 launch population is taken from U.S. Census Intercensal Population
    Estimates.
    '''
    census_sya_input_folder = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'Census', '2024', 'intercensal', 'syasex')

    df_list = None
    for csv in os.listdir(census_sya_input_folder):
        if csv.endswith('.csv'):
            temp = pd.read_csv(filepath_or_buffer=os.path.join(census_sya_input_folder, csv),
                                  encoding='latin1')
            temp = temp.query('YEAR == 6')

            if df_list is None:
                df_list = [temp]
            else:
                df_list.append(temp)

    df = pd.concat(objs=df_list, ignore_index=True)
    df = df[['AGE', 'TOT_MALE', 'TOT_FEMALE']].groupby(by=['AGE'], as_index=False).sum()
    df = df.rename(columns={'TOT_MALE': 'Male', 'TOT_FEMALE': 'Female'})
    df = df.melt(id_vars='AGE', var_name='SEX', value_name='POPULATION')
    df = df.rename(columns={'POPULATION': 'Population',
                            'AGE': 'Age',
                            'SEX': 'Sex'})
    df.loc[df.Sex == 'Male', 'Population'] *= -1

    return df


def plot_pyramid_with_frame(cbo, census):
    census_male = census.loc[census.Sex == 'Male', 'Population'].values
    census_female = census.loc[census.Sex == 'Female', 'Population'].values

    # draw the population pyramid
    g = sns.barplot(data=cbo,
                    x='Population',
                    y='Age',
                    hue='Sex',
                    orient='h',
                    dodge=False,
                    legend=True)

    for p in g.patches:
        p.set_height(1.0)
        p.set_linewidth(0.25)
        p.set_edgecolor('white')

    g.tick_params(left=False)

    male_min = cbo.loc[cbo.Sex == 'Male', 'Population'].min()
    female_max = cbo.loc[cbo.Sex == 'Female', 'Population'].max()
    abs_max = max(abs(male_min), female_max)
    g.set_xlim(-abs_max * 1.2, abs_max * 1.2)

    g.set_xlabel('Population (millions)')

    plt.gcf().set_figheight(6.0)
    plt.gcf().set_figwidth(8.0)

    # plt.figtext(x=0.40, y=0.9, s='Male', fontsize='large')
    # plt.figtext(x=0.60, y=0.9, s='Female', fontsize='large')

    plt.tight_layout()
    sns.despine(fig=plt.gcf(), top=True, left=True, right=True)

    labels = g.get_xticklabels()
    for label in labels:
        old_text = label.get_text()
        label.set_text(old_text.replace("\N{MINUS SIGN}", ''))
    g.set_xticklabels(labels)

    census_vert = []
    census_horiz = []

    for i in range(census_male.shape[0] - 1, -1, -1):
        census_vert.append(i + 0.6)
        census_vert.append(i - 0.4)
        census_horiz.append(census_male[i])
        census_horiz.append(census_male[i])

    for i in range(0, census_male.shape[0]):
        census_vert.append(i - 0.4)
        census_vert.append(i + 0.6)
        census_horiz.append(census_female[i])
        census_horiz.append(census_female[i])
    census_vert.append(census_vert[0])
    census_horiz.append(census_horiz[0])

    plt.gca().plot(census_horiz, census_vert, color='black', label='Census 2024')

    plt.gca().set_ylim(-0.5, 86.5)
    plt.subplots_adjust(top=0.9)

    # clean up y-axis tick labels
    labels = plt.gca().get_yticklabels()
    labels[-1].set_text('85+')
    plt.gca().set_yticklabels(labels=labels, ha='left')
    for index, label in enumerate(plt.gca().get_yticklabels()):
        if index % 5 != 0:
            label.set_visible(False)

    plt.gcf().suptitle('2024 National Population Estimates: CBO vs Census', fontsize=16)

    plt.legend(loc='right')
    plt.show()
    plt.close()

    return

def main():

    cbo = get_cbo_population()
    census = get_census_population()
    plot_pyramid_with_frame(cbo, census)

if __name__ == '__main__':
    main()
