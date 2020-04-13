from functools import reduce
import pandas as pd

# pd.options.display.max_columns = None
# pd.options.display.max_rows = 100

# The database is comprised of the following main tables:
#
#   MASTER - Player names, DOB, and biographical info
#   Batting - batting statistics
#   Pitching - pitching statistics
#   Fielding - fielding statistics

batting = pd.read_csv('raw_data/Batting.csv')
teams = pd.read_csv('raw_data/Teams.csv')
people = pd.read_csv('raw_data/People.csv')

# print(batting)
# print(people)


# Show memory error - Chunking, etc.  (3 memory pools being loaded)
# team_join = pd.merge(batting, teams)
# full_table = pd.merge(batting_people, teams, on=['teamID'])


# bat_df = pd.DataFrame(batting, columns=['playerID', 'yearID', 'teamID'])
teams_df = pd.DataFrame(teams)
people_df = pd.DataFrame(people, columns=['playerID', 'nameFirst', 'nameLast'])

df = batting.merge(teams_df).merge(people_df, on='playerID')

# all_join = pd.merge(team_join, people_df, on='playerID')
df.drop(columns=['stint', 'lgID'], axis=1, inplace=True)

# print(df)

# Starts with something easy -- ALTERING THE DB
df['fullName'] = df['nameFirst'] + ' ' + df['nameLast']

# Let's build some sabermetrics.  I believe the Lahman Database already has data madeup for these figures, but hey this is a learning experience.
# The objective measure to baseball


# Quick Query
# df = df[(df['fullName'] == 'Jacoby Ellsbury') & (df['yearID'] > 2012)]


df['1B'] = df['H'] - (df['2B'] + df['3B'] + df['HR'])

# OBS - On Base Percentage
# Jacoby Ellsbury OBP = .355 2013

# Times On Base (TOB) / Plate Appearances (PA) - Bunts
df['OBP'] = ((df['H'] + df['BB'] + df['HBP']) / (df['AB'] + df['BB'] + df['HBP'] + df['SF'])).apply(
    lambda x: round(x, 3))

df['SLG'] = ((df['1B'] + 2 * df['2B'] + 3 * df['3B'] + 4 * df['HR']) / df['AB']).apply(lambda x: round(x, 3))


# THEN FIND OPS - on base plus slugging
def ops(row):
    ops_calc = row['OBP'] + row['SLG']
    return round(ops_calc, 3)


df['OPS'] = df.apply(lambda row: ops(row), axis=1)


# THEN MOB Modified on base percentage
# Jacoby Ellsbury OBP = .349 2013
# Another way to find this is subtracting the caught stealing number from a hundreth of the OPS percentage.  IF YOU LIKE FRACTIONS!!
def mob(row):
    TOB = row['H'] + row['BB'] + row['HBP']
    modOB = TOB - row['CS']
    # Plate Appearances will have to be estimated - subtle differences in stats
    PA = row['AB'] + row['BB'] + row['HBP'] + row['SH'] + row['SF']

    if PA == 0:
        return 0
    else:
        MOB = modOB / PA
        return round(MOB, 3)


df['MOB'] = df.apply(lambda row: mob(row), axis=1)

# Here you are taking SLG and adding stolen bases to the numerator for
# Theres many other ways to do this without repeating code, but I'm trying to show what is happening in the equation.
df['MSA'] = ((df['1B'] + 2 * df['2B'] + 3 * df['3B'] + 4 * df['HR'] + df['SB']) / df['AB']).apply(lambda x: round(x, 3))
df['MOPS'] = (df['MOB'] + df['MSA']).apply(lambda x: round(x, 3))


def woba(row):
    uBB = row['BB'] - row['IBB']
    return round((0.690 * uBB + 0.722 * row['HBP'] + 0.888 * row['1B'] + 1.271 * row['2B'] + 1.616 * row['3B'] + 2.101 *
                  row['HR']) / (row['AB'] + uBB + row['SF'] + row['HBP']), 3)


df['WOBA'] = df.apply(lambda row: mob(row), axis=1)


def twoba(row):
    est_weights = ((1.27 - .89) + ((1.62 - 1.27) * 2)) / 2
    wSB = est_weights * row['SB']
    uBB = row['BB'] - row['IBB']

    if (row['AB'] + uBB + row['SF'] + row['HBP'] - row['CS']) > 0:
        return round((0.690 * uBB + 0.722 * row['HBP'] + 0.888 * row['1B'] + 1.271 * row['2B'] + 1.616 * row[
            '3B'] + 2.101 * row['HR'] + wSB) / (row['AB'] + uBB + row['SF'] + row['HBP'] - row['CS']), 3)


df['TWOBA'] = df.apply(lambda row: twoba(row), axis=1)

# df.query('fullName == "Billy Hamilton" and franchise == "Cincinnati Reds"', inplace=True)
# df.query('fullName == "Mike Trout"', inplace=True)


# cols = ['playerID', 'yearID', 'teamID', 'nameFirst', 'nameLast', 'fullName', 'franchise', 'G', 'AB', 'R', 'H', '1B', '2B', '3B', 'HR', 'RBI', 'SB', 'CS', 'BB', 'SO', 'IBB', 'HBP', 'SH', 'SF', 'GIDP', 'OBP', 'SLG', 'OPS', 'MOPS', 'WOBA', 'TWOBA']
# df = df[cols]

# df.to_csv('EvanFrabell_MLBstats.csv', index=False)

# df.query('fullName == "Eugenio Suarez"', inplace=True)
# print(df)


# Show df1 first
df2 = df.copy()

cols2 = ['yearID', 'franchise', 'fullName', 'AB', 'R', 'H', '1B', '2B', '3B', 'HR', 'RBI', 'SB', 'CS', 'BB', 'SO',
         'IBB', 'HBP', 'SH', 'SF', 'GIDP']
df2 = df2[cols2]
# df2.reset_index()

df2.sort_values(['yearID', 'franchise'], inplace=True)

# df.groupby doesn't change df; it returns a new object. In this case you perform an aggregation operation, so you get a new DataFrame. You have to give a name to the result if you want to use it later:
# df3 = df2.groupby(['yearID', 'franchise'])[['AB']].sum()

# , as_index=False

df3 = df2.groupby(['yearID', 'franchise']).agg(
    {
        'AB': sum,
        'R': sum,
        'H': sum,
        '1B': sum,
        '2B': sum,
        '3B': sum,
        'HR': sum,
        'RBI': sum,
        'SB': sum,
        'CS': sum,
        'BB': sum,
        'SO': sum,
        'IBB': sum,
        'HBP': sum,
        'SH': sum,
        'SF': sum,
        'GIDP': sum,
    }
)

df3['OBP'] = ((df3['H'] + df3['BB'] + df3['HBP']) / (df3['AB'] + df3['BB'] + df3['HBP'] + df3['SF'])).apply(
    lambda x: round(x, 3))
df3['SLG'] = ((df3['1B'] + 2 * df3['2B'] + 3 * df3['3B'] + 4 * df3['HR']) / df3['AB']).apply(lambda x: round(x, 3))
df3['OPS'] = df3.apply(lambda row: ops(row), axis=1)
df3['MOB'] = df3.apply(lambda row: mob(row), axis=1)
df3['MSA'] = ((df3['1B'] + 2 * df3['2B'] + 3 * df3['3B'] + 4 * df3['HR'] + df3['SB']) / df3['AB']).apply(lambda x: round(x, 3))
df3['MOPS'] = (df3['MOB'] + df3['MSA']).apply(lambda x: round(x, 3))
df3['WOBA'] = df3.apply(lambda row: mob(row), axis=1)
df3['TWOBA'] = df3.apply(lambda row: twoba(row), axis=1)

df3.drop(columns=['MOB', 'MSA'], axis=1, inplace=True)


print(df3)

# , index=False
df3.to_csv('TeamStats.csv')

# GOING by the data and neglecting fielding altogether - we can see that even with stolen bases Billy HAmilton does not add in great value to an offensive lineup based on the MOPS stat
