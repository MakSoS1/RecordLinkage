import dask.dataframe as dd
import pandas as pd
import recordlinkage
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import numpy as np
from multiprocessing import Pool, cpu_count
from functools import partial

# Шаг 1: Загрузка данных с помощью Dask
print("Шаг 1: Загрузка данных")
df_is1 = dd.read_csv('/app/main1_clean.csv')
df_is2 = dd.read_csv('/app/main2_clean.csv')
df_is3 = dd.read_csv('/app/main3_clean.csv')

# Шаг 2: Вычисление Dask DataFrame в Pandas DataFrame
print("Шаг 2: Преобразование Dask DataFrame в Pandas DataFrame")
df_is1 = df_is1.compute()
df_is2 = df_is2.compute()
df_is3 = df_is3.compute()

# Шаг 3: Сброс индексов для каждого DataFrame
print("Шаг 3: Сброс индексов и обработка дубликатов")
df_is1 = df_is1.reset_index(drop=True)
df_is2 = df_is2.reset_index(drop=True)
df_is3 = df_is3.reset_index(drop=True)

# Проверка наличия нужных столбцов в DataFrame
required_columns_is1 = ['uid', 'full_name_norm', 'birthdate_norm', 'email_norm', 'phone_norm', 'address_norm']
required_columns_is2 = ['uid', 'full_name_norm', 'birthdate_norm', 'phone_norm', 'address_norm']
required_columns_is3 = ['uid', 'full_name_norm', 'birthdate_norm', 'email_norm']

print("Проверка столбцов в df_is1")
print(df_is1.columns)
print("Проверка столбцов в df_is2")
print(df_is2.columns)
print("Проверка столбцов в df_is3")
print(df_is3.columns)

# Проверка наличия всех нужных столбцов
for df, required_columns, name in zip([df_is1, df_is2, df_is3], 
                                      [required_columns_is1, required_columns_is2, required_columns_is3],
                                      ['df_is1', 'df_is2', 'df_is3']):
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Предупреждение: В {name} отсутствуют столбцы: {missing_columns}")
    else:
        print(f"Все необходимые столбцы присутствуют в {name}")

# **Объединение имен в df_is2, если еще не объединено**
if 'full_name_norm' not in df_is2.columns and all(col in df_is2.columns for col in ['first_name_norm', 'middle_name_norm', 'last_name_norm']):
    print("Объединение имен в df_is2")
    df_is2['full_name_norm'] = df_is2['first_name_norm'] + ' ' + df_is2['middle_name_norm'] + ' ' + df_is2['last_name_norm']
    df_is2.drop(['first_name_norm', 'middle_name_norm', 'last_name_norm'], axis=1, inplace=True)

# Функция для обработки блока данных
def process_chunk(chunk, df_other, indexer, compare_cl, label):
    print(f"Начата обработка блока: {label}")
    # Создание пар кандидатов
    candidate_links = indexer.index(chunk, df_other)
    print(f"Обработка {label}: создано {len(candidate_links)} пар кандидатов")

    # Вычисление признаков
    features = compare_cl.compute(candidate_links, chunk, df_other)
    print(f"Обработка {label}: вычисление признаков завершено")
    return features

def parallel_process(start, df_main, df_other, indexer, compare_cl, chunk_size, label):
    chunk = df_main.iloc[start:start + chunk_size]
    return process_chunk(chunk, df_other, indexer, compare_cl, f'{label} (блок {start})')

# Инициализация списка для хранения результатов
results = []

# **Сопоставление df_is1 и df_is2 с параллельной обработкой**
print("Начало параллельной обработки df_is1 и df_is2")
chunk_size = 50000  # Размер блока для обработки
num_cores = cpu_count()

# Определение необходимых столбцов для сравнения df_is1 и df_is2
required_columns_1_2 = ['phone_norm', 'full_name_norm', 'address_norm', 'birthdate_norm']

# Создание индексатора и объекта сравнения для df_is1 и df_is2
indexer_1_2 = recordlinkage.Index()
indexer_1_2.sortedneighbourhood('birthdate_norm', window=3)
indexer_1_2.sortedneighbourhood('full_name_norm', window=3)

compare_cl_1_2 = recordlinkage.Compare()
compare_cl_1_2.exact('phone_norm', 'phone_norm', label='phone')
compare_cl_1_2.string('full_name_norm', 'full_name_norm', method='jarowinkler', threshold=0.85, label='full_name')
compare_cl_1_2.string('address_norm', 'address_norm', method='jarowinkler', threshold=0.8, label='address')

# Параллельная обработка для df_is1 и df_is2
pool = Pool(num_cores)
partial_process_1_2 = partial(parallel_process, df_main=df_is1, df_other=df_is2, indexer=indexer_1_2, 
                              compare_cl=compare_cl_1_2, chunk_size=chunk_size, label='df_is1 и df_is2')

features_list_1_2 = pool.map(partial_process_1_2, range(0, len(df_is1), chunk_size))
pool.close()
pool.join()

# Объединение результатов
print("Объединение результатов сопоставления df_is1 и df_is2")
features_1_2 = pd.concat(features_list_1_2)

# Присвоение случайных меток для демонстрации (замените на реальные метки для обучения)
features_1_2['match'] = np.random.randint(0, 2, size=features_1_2.shape[0])

# Подготовка данных для обучения и тестирования модели
print("Подготовка данных для обучения модели")
X = features_1_2.drop(columns='match')
y = features_1_2['match']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Обучение модели
print("Обучение модели на основе RandomForestClassifier")
classifier = RandomForestClassifier(n_estimators=100, random_state=42)
classifier.fit(X_train, y_train)

# Прогнозирование и оценка качества модели
print("Прогнозирование и оценка модели")
y_pred = classifier.predict(X_test)
print(classification_report(y_test, y_pred))

# Применение модели ко всем кандидатам для фильтрации совпадений
print("Фильтрация совпадений по результатам модели")
predictions = classifier.predict(X)
matched_indices = features_1_2[predictions == 1].index

# Добавление результатов в общий список
for index in matched_indices:
    id_is1 = df_is1.loc[index[0], 'uid']
    id_is2 = df_is2.loc[index[1], 'uid']
    results.append({'id_is1': [id_is1], 'id_is2': [id_is2], 'id_is3': []})

# **Сопоставление df_is1 и df_is3 с параллельной обработкой**
print("Начало параллельной обработки df_is1 и df_is3")
chunk_size = 50000  # Размер блока для обработки

# Определение необходимых столбцов для сравнения df_is1 и df_is3
required_columns_1_3 = ['email_norm', 'full_name_norm', 'birthdate_norm']

# Создание индексатора и объекта сравнения для df_is1 и df_is3
indexer_1_3 = recordlinkage.Index()
indexer_1_3.sortedneighbourhood('birthdate_norm', window=3)
indexer_1_3.sortedneighbourhood('full_name_norm', window=3)

compare_cl_1_3 = recordlinkage.Compare()
compare_cl_1_3.exact('email_norm', 'email_norm', label='email')
compare_cl_1_3.string('full_name_norm', 'full_name_norm', method='levenshtein', threshold=0.85, label='full_name')

# Параллельная обработка для df_is1 и df_is3
pool = Pool(num_cores)
partial_process_1_3 = partial(parallel_process, df_main=df_is1, df_other=df_is3, indexer=indexer_1_3, 
                              compare_cl=compare_cl_1_3, chunk_size=chunk_size, label='df_is1 и df_is3')

features_list_1_3 = pool.map(partial_process_1_3, range(0, len(df_is1), chunk_size))
pool.close()
pool.join()

# Объединение результатов
print("Объединение результатов сопоставления df_is1 и df_is3")
features_1_3 = pd.concat(features_list_1_3)

# Присвоение случайных меток для демонстрации (замените на реальные метки для обучения)
features_1_3['match'] = np.random.randint(0, 2, size=features_1_3.shape[0])

# Подготовка данных для прогнозирования
X = features_1_3.drop(columns='match')
y = features_1_3['match']

# Прогнозирование совпадений для df_is1 и df_is3
print("Прогнозирование совпадений для df_is1 и df_is3")
predictions_1_3 = classifier.predict(X)
matched_indices_1_3 = features_1_3[predictions_1_3 == 1].index

# Добавление результатов в общий список
for index in matched_indices_1_3:
    id_is1 = df_is1.loc[index[0], 'uid']
    id_is3 = df_is3.loc[index[1], 'uid']
    results.append({'id_is1': [id_is1], 'id_is2': [], 'id_is3': [id_is3]})


# Создание итоговой таблицы результатов
print("Создание итоговой таблицы результатов")
final_results = []
for result in results:
    final_results.append([
        result['id_is1'], 
        result['id_is2'], 
        result['id_is3']
    ])

# Преобразование в DataFrame и сохранение в CSV
print("Сохранение финальных результатов в CSV файл")
final_df = pd.DataFrame(final_results, columns=['id_is1', 'id_is2', 'id_is3'])
final_df.to_csv('/app/final_results.csv', index=False)

print("Процесс завершен успешно!")