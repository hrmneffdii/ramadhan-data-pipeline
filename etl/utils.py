# function to validate dataframe input
def validate_data(df):
    # check the dataframe is empty
    if df.empty:
        raise ValueError("DataFrame is empty")

    # giving a warning if the dataframe has null value
    if df.isnull().sum().sum() > 0:
        print("Warning: Missing values detected")
