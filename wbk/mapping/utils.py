import pandas as pd

def create_description(df: pd.DataFrame, template: str) -> pd.Series:
    """
    Generate a description column by formatting a template string
    using dataframe columns referenced as {column_name}.
    
    Example:
        df['description'] = create_description(df, 
            "School from the {district_column} district"
        )
    """
    # extract placeholder names from the template
    import re
    placeholders = re.findall(r"{(.*?)}", template)

    # ensure all placeholders exist in dataframe
    missing = [col for col in placeholders if col not in df.columns]
    if missing:
        raise KeyError(f"Columns not found in dataframe: {missing}")

    # format efficiently using pandas Series.map tuple trick
    return df.apply(lambda row: template.format(**row), axis=1)

def create_description_row(row: pd.Series, template: str) -> str:
    """
    Generate a description string by formatting a template
    using a pandas Series (row).
    
    Example:
        description = create_description(row, 
            "School from the {district_column} district"
        )
    """
    import re
    placeholders = re.findall(r"{(.*?)}", template)
    missing = [col for col in placeholders if col not in row.index]
    if missing:
        raise KeyError(f"Columns not found in row: {missing}")
    return template.format(**row)
