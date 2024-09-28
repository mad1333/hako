# hako
Hako — это инструмент на Python для обработки данных, который упрощает очистку, преобразование и управление наборами данных. Проект фокусируется на улучшении качества данных для дальнейшего анализа и машинного обучения.

## Особенности

- **Очистка данных**: Удаление дубликатов и NaN значений, а также нежелательных символов.
- **Преобразование данных**: Конвертация и форматирование типов данных, таких как даты и строки.
- **Гибкие функции обработки**: Легко расширяемые функции для обработки конкретных столбцов (имена, электронные почты и т.д.).
- **Работа с CSV**: Загрузка и сохранение наборов данных в формате CSV.

## Установка
   ```bash
   git clone https://github.com/mad1333/hako.git
   cd hako```
   pip install -r requirements.txt
Использование
Загрузка CSV файла: Используйте функции для загрузки вашего набора данных в DataFrame.
Обработка данных: Примените функции очистки и преобразования.
Сохранение данных: Экспортируйте очищенный DataFrame обратно в CSV.
Пример
python
Копировать код
import pandas as pd
from hako import preprocessing, combine_full_name, _combine_duplicates

# Загрузка вашего набора данных
df = pd.read_csv('path/to/your/dataset.csv')

# Обработка данных
columns_to_process = ['first_name', 'middle_name', 'last_name', 'phone', 'email', 'address', 'birthdate']
df_processed = preprocessing(df, columns_to_process)
df_processed = combine_full_name(df_processed)
df_processed = _combine_duplicates(df_processed)

# Сохранение очищенного набора данных
df_processed.to_csv('path/to/your/cleaned_dataset.csv', index=False)
Вклад

  
