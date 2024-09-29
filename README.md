# Record Linkage Project

## Описание

Данный проект предназначен для обработки и нормализации данных из базы данных ClickHouse с последующим использованием моделей машинного обучения для поиска дубликатов и сопоставления данных. Проект включает два основных скрипта:

1. **data_reader.py** — скрипт для чтения данных из ClickHouse, их предварительной очистки и сохранения в формате CSV.
2. **data_proccessor.py** — скрипт для дальнейшей обработки очищенных данных, сопоставления записей и создания модели для нахождения совпадений.

![scheme](https://i.imgur.com/7FnO8Ux.png)

## Установка и Запуск

### 1. Клонирование репозитория

```bash
git clone https://github.com/MakSoS1/RecordLinkage.git
cd data-processing-project
```

## Структура данных
Ожидается, что данные в ClickHouse будут иметь следующие столбцы в таблицах:

table_dataset1: full_name, email, address, birthdate, phone \
table_dataset2: first_name, middle_name, last_name, birthdate, phone, address \
table_dataset3: name, email, birthdate, sex
## Описание полей
full_name_norm: нормализованное полное имя.\
email_norm: нормализованный email.\
address_norm: нормализованный адрес.\
birthdate_norm: нормализованная дата рождения.\
phone_norm: нормализованный номер телефона.
## Заметки
Если ClickHouse не готов к подключению, скрипт будет пытаться подключиться каждые 5 секунд.
При возникновении ошибок в процессе сохранения CSV, скрипт будет пытаться сохранять данные в меньших кусках.
Если проект предназначен для реального использования, замените случайные метки на реальные данные для обучения моделей.

## Использование данных
Данные очищаются и нормализуются с целью дальнейшего сопоставления и поиска дубликатов. Для этого используется методика машинного обучения с предварительной подготовкой данных.

## Авторы
1. Костромитин Максим
2. Фабзиев Ильшат
3. Архипов Сергей
4. Фалевский Юрий
5. Ярославцев Захар

Конечно, вот документация, полностью оформленная в Markdown формате:

---

# Документация к решению задачи Record Linkage

## Введение

Данное решение предназначено для идентификации и объединения записей, относящихся к одному и тому же клиенту из разных информационных систем (Record Linkage). Решение состоит из двух основных этапов:

1. **Предобработка данных**: очистка, нормализация и подготовка данных из CSV-файлов.
2. **Сопоставление записей**: использование алгоритмов машинного обучения и библиотеки `recordlinkage` для нахождения соответствий между записями из разных источников.

## Структура проекта

- **clean_and_prepare_csv_with_dask.py**: Скрипт для предобработки данных.
- **main.py**: Основной скрипт для сопоставления записей и формирования итогового результата.
- **datasets/**: Папка с исходными CSV-файлами (`main1.csv`, `main2.csv`, `main3.csv`).
- **results/**: Папка для сохранения очищенных данных и итогового результата (`final_results.csv`).

## Предобработка данных

### Файл: `clean_and_prepare_csv_with_dask.py`

### Описание

Скрипт предназначен для очистки, нормализации и подготовки CSV-файлов с клиентскими данными. Он обрабатывает большие файлы с минимальным использованием оперативной памяти за счёт чтения данных по частям.

### Используемые библиотеки

- `pandas`: Для обработки и манипуляции данными.
- `os`: Для работы с файловой системой.

### Функция: `clean_and_prepare_csv_with_dask`

#### Аргументы

- `input_file` (str): Путь к входному CSV-файлу.
- `output_clean_file` (str): Путь к выходному CSV-файлу после очистки.
- `columns_mapping` (dict): Словарь с маппингом старых и новых названий колонок для нормализации.

#### Описание работы функции

1. **Чтение данных по частям**: Используется `pd.read_csv` с параметром `chunksize` для чтения файла по частям (чанкам) размером 10,000 строк. Это позволяет обрабатывать большие файлы без перегрузки оперативной памяти.
2. **Обработка пропущенных значений**: Пропущенные значения в столбцах, указанных в `columns_mapping`, заполняются пустыми строками.
3. **Нормализация данных**:
   - Все символы переводятся в нижний регистр.
   - Удаляются начальные и конечные пробелы.
   - Множественные пробелы заменяются на один пробел.
   - Создаются новые столбцы с суффиксом `_norm`.
4. **Удаление старых колонок**: Исходные столбцы, указанные в `columns_mapping`, удаляются из DataFrame.
5. **Удаление явных дубликатов**: Дубликаты строк удаляются на основе всех столбцов.
6. **Сохранение очищенного файла**:
   - Если файл не удаётся сохранить из-за ограничений памяти, выполняется сохранение по частям с уменьшенным размером чанка (50,000 строк).

#### Пример вызова функции

```python
clean_and_prepare_csv_with_dask('main1.csv', 'main1_clean_dask.csv', {
    'full_name': 'full_name_norm',
    'email': 'email_norm',
    'address': 'address_norm',
    'birthdate': 'birthdate_norm',
    'phone': 'phone_norm'
})
```

### Обработка всех датасетов

Скрипт определяет список датасетов и применяет функцию `clean_and_prepare_csv_with_dask` к каждому из них.

```python
datasets = [
    {
        "input_file": "main1.csv",
        "output_clean_file": "main1_clean_dask.csv",
        "columns_mapping": { ... }
    },
    ...
]

for dataset in datasets:
    clean_and_prepare_csv_with_dask(dataset['input_file'], dataset['output_clean_file'], dataset['columns_mapping'])
```

## Сопоставление записей

### Файл: `main.py`

### Описание

Основной скрипт выполняет следующие задачи:

1. **Загрузка и предобработка очищенных данных**: Загрузка очищенных CSV-файлов и подготовка данных для сопоставления.
2. **Проверка наличия необходимых столбцов**: Убедиться, что все требуемые столбцы присутствуют в каждом DataFrame.
3. **Объединение имен**: Для датасета, где имя разбито на части, объединить их в один столбец `full_name_norm`.
4. **Создание индексаторов и компараторов**: Настройка правил сопоставления для каждой пары датасетов.
5. **Последовательная обработка данных**: Обработка данных по частям для экономии оперативной памяти.
6. **Обучение модели**: Использование алгоритма `RandomForestClassifier` для обучения модели сопоставления.
7. **Предсказание соответствий**: Применение обученной модели для нахождения соответствий между записями.
8. **Формирование итогового результата**: Создание итоговой таблицы результатов и сохранение её в CSV-файл.

### Используемые библиотеки

- `dask.dataframe`: Для работы с большими данными.
- `pandas`: Для обработки данных.
- `recordlinkage`: Для выполнения сопоставления записей.
- `sklearn`: Для машинного обучения и оценки модели.
- `numpy`: Для работы с массивами данных.
- `tqdm`: Для отображения прогресса обработки.

### Функции

#### `process_chunk`

- Обрабатывает отдельный чанк данных для сопоставления.
- Выполняет индексирование и вычисление признаков для пар кандидатов.

#### `load_and_preprocess_data`

- Загружает очищенные данные из файлов.
- Преобразует `dask` DataFrame в `pandas` DataFrame для дальнейшей обработки.

#### `check_required_columns`

- Проверяет наличие необходимых столбцов в каждом DataFrame.
- Выводит предупреждение, если каких-либо столбцов не хватает.

#### `combine_names`

- Объединяет столбцы `first_name_norm`, `middle_name_norm` и `last_name_norm` в один столбец `full_name_norm`.
- Удаляет исходные столбцы после объединения.

#### `create_indexer_and_comparator`

- Создает индексатор и компаратор для сопоставления записей.
- Настраивает правила сравнения для указанных столбцов.

#### `sequential_processing`

- Последовательно обрабатывает данные по частям для снижения нагрузки на память.
- Использует функцию `process_chunk` для обработки каждого чанка.

#### `train_model`

- Обучает модель машинного обучения на сгенерированных признаках.
- Использует случайные метки (в реальном сценарии необходимо использовать реальные метки).
- Выводит отчет о качестве модели.

#### `predict_matches`

- Предсказывает соответствия между записями на основе обученной модели.
- Формирует список результатов с идентификаторами записей из разных систем.

### Основной поток выполнения (`main`)

1. **Загрузка данных**:

   ```python
   file_paths = ['main1_clean_dask.csv', 'main2_clean_dask.csv', 'main3_clean_dask.csv']
   df_is1, df_is2, df_is3 = load_and_preprocess_data(file_paths)
   ```

2. **Проверка столбцов**:

   ```python
   required_columns = [
       ['uid', 'full_name_norm', 'birthdate_norm', 'email_norm', 'phone_norm', 'address_norm'],
       ['uid', 'full_name_norm', 'birthdate_norm', 'phone_norm', 'address_norm'],
       ['uid', 'full_name_norm', 'birthdate_norm', 'email_norm']
   ]
   check_required_columns([df_is1, df_is2, df_is3], required_columns)
   ```

3. **Объединение имен** (для `df_is2`):

   ```python
   df_is2 = combine_names(df_is2)
   ```

4. **Сопоставление `df_is1` и `df_is2`**:

   - Создание индексатора и компаратора:

     ```python
     indexer_1_2, compare_cl_1_2 = create_indexer_and_comparator(['birthdate_norm', 'full_name_norm', 'phone_norm', 'address_norm'])
     ```

   - Последовательная обработка данных:

     ```python
     features_1_2 = sequential_processing(df_is1, df_is2, indexer_1_2, compare_cl_1_2, 10000, 'df_is1 и df_is2')
     ```

   - Обучение модели:

     ```python
     classifier = train_model(features_1_2)
     ```

   - Предсказание соответствий:

     ```python
     results = predict_matches(classifier, features_1_2, df_is1, df_is2)
     ```

5. **Сопоставление `df_is1` и `df_is3`**:

   - Создание индексатора и компаратора:

     ```python
     indexer_1_3, compare_cl_

1_3 = create_indexer_and_comparator(['birthdate_norm', 'full_name_norm', 'email_norm'])
     ```

   - Последовательная обработка данных:

     ```python
     features_1_3 = sequential_processing(df_is1, df_is3, indexer_1_3, compare_cl_1_3, 10000, 'df_is1 и df_is3')
     ```

   - Предсказание соответствий:

     ```python
     results.extend(predict_matches(classifier, features_1_3, df_is1, df_is3))
     ```

6. **Формирование итогового результата**:

   ```python
   final_results = [[r['id_is1'], r['id_is2'], r['id_is3']] for r in results]
   final_df = pd.DataFrame(final_results, columns=['id_is1', 'id_is2', 'id_is3'])
   final_df.to_csv('final_results.csv', index=False)
   ```

7. **Завершение процесса**:

   ```python
   print("Процесс завершен успешно!")
   ```

### Важные замечания

- **Использование случайных меток**: В функции `train_model` для обучения модели используются случайные метки. В реальном сценарии необходимо использовать реальные метки соответствий для обучения модели.
  
  ```python
  features['match'] = np.random.randint(0, 2, size=features.shape[0])  # Замените на реальные метки
  ```

- **Ограничение по памяти**: Для обработки больших объемов данных используются `dask` и обработка по частям (чанкам) для снижения нагрузки на оперативную память.

- **Настройка параметров**: Параметры, такие как размер чанка, пороги сравнения и методы сравнения, могут быть настроены в соответствии с требованиями и свойствами данных.

## Потенциал масштабирования

Решение спроектировано с учетом возможности обработки больших объемов данных при ограниченных ресурсах:

- **Обработка по частям**: Чтение и обработка данных по чанкам позволяет работать с файлами, превышающими объем оперативной памяти.
- **Использование `dask`**: Библиотека `dask` предоставляет инструменты для параллельной обработки данных и эффективного использования ресурсов.
- **Масштабируемость**: При необходимости можно увеличить количество используемых ядер процессора и объем оперативной памяти для ускорения обработки.

## Универсальность решения

- **Гибкость**: Решение может быть адаптировано для работы с другими наборами данных путем изменения параметров и правил сопоставления.
- **Расширяемость**: Модульная структура кода позволяет добавлять новые функции и улучшать существующие без значительных изменений.
- **Применимость**: Подходы, используемые в данном решении, являются стандартными в задаче Record Linkage и могут быть применены в различных областях, где необходимо объединение данных из разных источников.


## Заключение

Представленное решение позволяет эффективно выполнять задачу Record Linkage на больших объемах данных с ограниченными ресурсами. Использование современных библиотек и методов обработки данных обеспечивает высокую производительность и точность сопоставления.

При дальнейшем развитии решения рекомендуется:

- **Собрать реальные метки соответствий** для обучения модели машинного обучения.
- **Оптимизировать параметры модели** и методов сравнения для повышения точности.
- **Добавить обработку дополнительных полей** или источников данных при необходимости.

--- 
