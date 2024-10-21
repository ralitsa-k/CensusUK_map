import polars as pl

#%% Load and process data from CENSUS
df_econ = pl.read_excel(r'./data/raw/Highest level of qualification by economic activity status.xls')
df = df_econ.rename({x: y for x, y in zip(df_econ.columns, ['city', 'Auth', 'Qual-code', 'Qualification', 'econ-code', 'econ', 'observation'])})
#df_plot = df.filter(pl.col('city').str.starts_with("E"))
df_plot2_base = df.group_by(['city','Auth','Qualification', 'econ']).agg(pl.col('observation').mean()).sort(by='observation', descending=True)


# recode economic activity
econ_dict = df_plot2_base.select(pl.col('econ')).unique()
to_ = {'Economically inactive (excluding full-time students)':'Econ inactive, nonStudent',
       'Economically active and a full-time student: In employment':'Econ active, Student, Working',
       'Economically active (excluding full-time students): Unemployed: Seeking work or waiting to start a job already obtained: Available to start working within 2 weeks':'Econ active, nonStudent, Searching',
       'Does not apply':'NA',
       'Economically inactive and a full-time student':'Econ inactive, Student',
       'Economically active (excluding full-time students): In employment':'Econ active, nonStudent, Working',
       'Economically active and a full-time student: Unemployed: Seeking work or waiting to start a job already obtained: Available to start working within 2 weeks': 'Econ active, Student, Searching'}

to_econ = {'Economically inactive (excluding full-time students)':'Inactive',
       'Economically active and a full-time student: In employment':'Active',
       'Economically active (excluding full-time students): Unemployed: Seeking work or waiting to start a job already obtained: Available to start working within 2 weeks':'Active',
       'Does not apply':'NA',
       'Economically inactive and a full-time student':'NA',
       'Economically active (excluding full-time students): In employment':'Active',
       'Economically active and a full-time student: Unemployed: Seeking work or waiting to start a job already obtained: Available to start working within 2 weeks': 'Active'}

# add the recoded variables to the original data
df_plot2 = df_plot2_base.with_columns(pl.col('econ').replace(to_econ).alias('Categorical_econ'))
df_plot2 = df_plot2.with_columns(pl.col('econ').replace(to_))

df_plot_econ_split = df_plot2.filter(pl.col('Categorical_econ')=='Active').group_by('city').agg(pl.col('observation').sum().alias('Active'))
df_plot_econ_split_inactive = df_plot2.filter(pl.col('Categorical_econ')=='Inactive').group_by('city').agg(pl.col('observation').sum().alias('Inactive'))

# get active and inactive ratio for the color of the map 
ratio_df = df_plot_econ_split.join(df_plot_econ_split_inactive, on = 'city')
ratio_df = ratio_df.with_columns((pl.col('Active')/(pl.col('Active')+pl.col('Inactive'))).alias('ratio'))

df_plot2 = df_plot2.join(ratio_df, how = 'inner', on = 'city')

# recode Qualif activity
qual_dict = df_plot2.select(pl.col('Qualification')).unique()
to_q = {'Other: vocational or work-related qualifications, other qualifications achieved in England or Wales, qualifications achieved outside England or Wales (equivalent not stated or unknown)':'Other',
        'Level 1 and entry level qualifications: 1 to 4 GCSEs grade A* to C, Any GCSEs at other grades, O levels or CSEs (any grades), 1 AS level, NVQ level 1, Foundation GNVQ, Basic or Essential Skills':'Level 1',
        'Level 4 qualifications or above: degree (BA, BSc), higher degree (MA, PhD, PGCE), NVQ level 4 to 5, HNC, HND, RSA Higher Diploma, BTEC Higher level, professional qualifications (for example, teaching, nursing, accountancy)':'Level 4',
        'Level 3 qualifications: 2 or more A levels or VCEs, 4 or more AS levels, Higher School Certificate, Progression or Advanced Diploma, Welsh Baccalaureate Advance Diploma, NVQ level 3; Advanced GNVQ, City and Guilds Advanced Craft, ONC, OND, BTEC National, RSA Advanced Diploma': 'Level 3',
        'Level 2 qualifications: 5 or more GCSEs (A* to C or 9 to 4), O levels (passes), CSEs (grade 1), School Certification, 1 A level, 2 to 3 AS levels, VCEs, Intermediate or Higher Diploma, Welsh Baccalaureate Intermediate Diploma, NVQ level 2, Intermediate GNVQ, City and Guilds Craft, BTEC First or General Diploma, RSA Diploma':'Level 2',
        'Does not apply':'NA',
        }
to_q_col = {'No qualifications':0,
            'Level 1':1,
            'Level 2':2,
            'Apprenticeship':3,
            'Level 3':4,
            'Level 4':5
        }

df_plot2 = (
    df_plot2.with_columns(pl.col('Qualification').replace(to_q).alias('Qualifications'))
    .filter(~pl.col('Qualifications').is_in(['NA', 'Other']))
    .with_columns(pl.col('Qualifications')
                                 .replace(to_q_col).alias('Qual-code')
                                 .cast(pl.Float32))
    .filter(pl.col('econ').is_in(['Econ inactive, nonStudent', 'Econ active, nonStudent, Searching', 'Econ active, Student, Searching']))
)
# recode back with shorter descriptions of qualifications (maybe to use as a tooltip, currently not used)
to_q_long = {'Other':'Other: vocational or work-related qualifications, (equivalent not stated or unknown)',
        'Level 1':'Level 1 and entry level: 1 to 4 GCSEs grade A* to C, Any GCSEs at other grades, Basic or Essential Skills',
        'Level 4':'Level 4:  (BA, BSc), (MA, PhD, PGCE), NVQ level 4 to 5, (teaching, nursing, accountancy)',
        'Level 3':'Level 3: 2 or more A levels or VCEs, 4 or more AS levels, Higher School Certificate',
        'Level 2':'Level 2: 5 or more GCSEs (A* to C or 9 to 4), CSEs (grade 1), School Certification',
        'NA':'Does not apply',
        }

df_plot2 = (df_plot2.with_columns(pl.col('Qualifications').replace(to_q_long).alias('Qualification')))
df_plot2.write_csv('data/processed/data_for_app.csv')