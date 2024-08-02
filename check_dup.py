from pandas import read_csv


with open('./data/species_level_functional_traits.csv') as f:
    df = read_csv(f, keep_default_na=False)
    # count number of duplicates of first columns, and print out the duplicates
    print(df[df.duplicated(['species_name'], keep=False)])
    # export that to two csv files
    df[df.duplicated(['species_name'], keep=False)].to_csv(
        './species_level_functional_traits_duplicates.csv', index=False)
