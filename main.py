import pandas as pd
import re
from dateutil import parser  

BLOCK_NAMES = 20

def clean_russian_letters(text):
    if isinstance(text, str):  
        text = text.replace('\n', '').replace('\r', '')
        return re.sub(r'[^а-яА-ЯёЁ]', '', text)
    return text

def _combine_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Удаление дубликатов из DataFrame."""
    return df.drop_duplicates()

def _name_processing(name: str) -> str:
    """Обработка имен: удаление лишних символов, приведение к нижнему регистру."""
    name = re.sub(r'[^а-яА-ЯёЁ\s]', '', name)  
    return name.strip().lower()

def _phone_processing(text: str) -> str:
    """Обработка телефонов: удаление лишних символов и очистка номера."""
    if isinstance(text, str):
        cleaned_phone = re.sub(r'[^0-9]', '', text)
        while len(cleaned_phone) > 10:
            cleaned_phone = cleaned_phone[1:]  
        return cleaned_phone
    return text

def _email_processing(email: str) -> str:
    """Обработка email: удаление лишних символов и приведение к нижнему регистру."""
    if pd.isna(email):
        return ''
    
    email = str(email).strip().lower()  
    email = re.sub(r'[^\w\.-@]', '', email) 
    email = re.sub(r'\.{2,}', '.', email)  
    email = re.sub(r'@+', '@', email)  
    if '@' in email:
        local_part, domain = email.split('@', 1)
        local_part = re.sub(r'[^a-z0-9._-]', '', local_part)  
        email = f"{local_part}@{domain}"
    
    return email

def _birthdate_processing(birthdate):
    """Обработка даты рождения: приведение к нормальному формату."""
    if pd.isna(birthdate) or not isinstance(birthdate, str) or birthdate.strip() == "":
        return None  

    birthdate = re.sub(r'^-', '', birthdate)  

    try:
        parsed_date = parser.parse(birthdate, dayfirst=True)  
        return parsed_date.strftime('%Y-%m-%d')  
    except ValueError:
        return None 

def _address_processing(text: str) -> str:
    if isinstance(text, str):
        return re.sub(r'[^а-яА-ЯёЁ0-9\s,]', '', text).strip()
    return text

def block_data(df: pd.DataFrame, column: str) -> pd.DataFrame:   
    df[f'{column}_block'] = df[column].apply(lambda x: x[:BLOCK_NAMES])
    return df

def preprocessing(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    
    for column in columns:
        if column in ['first_name', 'middle_name', 'last_name']:
            df[column] = df[column].apply(_name_processing)
        if column == 'name':
            df[column] = df[column].apply(_name_processing)
        elif column == 'full_name':
            df[column] = df[column].apply(_name_processing)
        elif column == 'phone':
            df[column] = df[column].apply(_phone_processing)
        elif column == 'email':
            df[column] = df[column].apply(_email_processing)
        elif column == 'address':
            df[column] = df[column].apply(_address_processing)
        elif column == 'birthdate':
            df[column] = df[column].apply(_birthdate_processing)
    return df

def combine_full_name(df: pd.DataFrame) -> pd.DataFrame:
    """Создает столбец full_name из first_name, middle_name и last_name."""
    df['full_name'] = df['first_name'] + ' ' + df['middle_name'] + ' ' + df['last_name']
    return df
