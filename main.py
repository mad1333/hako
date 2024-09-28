import pandas as pd
import re
from dateutil import parser

BLOCK_NAMES = 20

def clean_russian_letters(text: str) -> str:
    if isinstance(text, str):
        text = text.replace('\n', '').replace('\r', '')
        return re.sub(r'[^а-яА-ЯёЁ\s]', '', text)
    return text

def preprocess_column(df: pd.DataFrame, column: str, process_func) -> pd.DataFrame:
    df[column] = df[column].apply(process_func)
    return df

def replace_empty_strings(df: pd.DataFrame) -> pd.DataFrame:
    return df.applymap(lambda x: '' if isinstance(x, str) and x.strip() == '' else x)

def remove_similar_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, df.nunique() > 1]

def _birthdate_processing(birthdate: str) -> str:
    if pd.isna(birthdate) or not isinstance(birthdate, str) or birthdate.strip() == "":
        return None
    birthdate = re.sub(r'^-', '', birthdate)
    try:
        parsed_date = parser.parse(birthdate, dayfirst=True)
        return parsed_date.strftime('%Y-%m-%d')
    except ValueError:
        return None

def _combine_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates()

def preprocessing(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for column in columns:
        if column in ['first_name', 'middle_name', 'last_name']:
            preprocess_column(df, column, clean_russian_letters)
        elif column == 'phone':
            preprocess_column(df, column, lambda x: re.sub(r'[^0-9]', '', str(x))[:10])
        elif column == 'email':
            preprocess_column(df, column, lambda x: str(x).strip().lower() if pd.notna(x) else '')
        elif column == 'address':
            preprocess_column(df, column, _address_processing)
        elif column == 'birthdate':
            preprocess_column(df, column, _birthdate_processing)

    if all(col in df.columns for col in ['first_name', 'middle_name', 'last_name']):
        df['full_name'] = df['first_name'] + ' ' + df['middle_name'] + ' ' + df['last_name']
        df['full_name'] = df['full_name'].apply(clean_russian_letters)

    return df

if __name__ == "__main__":
    df1 = pd.read_csv('path/to/your/first_dataset.csv')
    df2 = pd.read_csv('path/to/your/second_dataset.csv')
    df3 = pd.read_csv('path/to/your/third_dataset.csv')

    columns_to_process_1 = ['full_name', 'email', 'address', 'sex', 'birthdate', 'phone']
    df1_processed = preprocessing(df1, columns_to_process_1)
    df1_processed = replace_empty_strings(df1_processed)
    df1_processed = remove_similar_columns(df1_processed)
    df1_processed = _combine_duplicates(df1_processed)

    columns_to_process_2 = ['first_name', 'middle_name', 'last_name', 'birthdate', 'phone', 'address']
    df2_processed = preprocessing(df2, columns_to_process_2)
    df2_processed = replace_empty_strings(df2_processed)
    df2_processed = remove_similar_columns(df2_processed)
    df2_processed = _combine_duplicates(df2_processed)

    columns_to_process_3 = ['name', 'email', 'birthdate', 'sex']
    df3_processed = preprocessing(df3, columns_to_process_3)
    df3_processed.rename(columns={'name': 'full_name'}, inplace=True)
    df3_processed['full_name'] = df3_processed['full_name'].apply(clean_russian_letters)
    df3_processed = replace_empty_strings(df3_processed)
    df3_processed = remove_similar_columns(df3_processed)
    df3_processed = _combine_duplicates(df3_processed)

    df1_processed.to_csv('path/to/your/cleaned_first_dataset.csv', index=False)
    df2_processed.to_csv('path/to/your/cleaned_second_dataset.csv', index=False)
    df3_processed.to_csv('path/to/your/cleaned_third_dataset.csv', index=False)


