import dask.dataframe as dd
import pandas as pd
import recordlinkage
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import numpy as np
from tqdm import tqdm

# Функция для обработки одного блока данных
def process_chunk(chunk, df_other, indexer, compare_cl, label):
    """
    Обрабатывает один блок данных, создавая пары кандидатов и вычисляя признаки.
    
    :param chunk: Часть основного DataFrame для обработки
    :param df_other: Второй DataFrame для сопоставления
    :param indexer: Объект индексатора recordlinkage
    :param compare_cl: Объект сравнения recordlinkage
    :param label: Метка для идентификации текущего блока
    :return: DataFrame с вычисленными признаками
    """
    print(f"Начата обработка блока: {label}")
    # Создание пар кандидатов для сопоставления
    candidate_links = indexer.index(chunk, df_other)
    print(f"Обработка {label}: создано {len(candidate_links)} пар кандидатов")
    # Вычисление признаков для пар кандидатов
    features = compare_cl.compute(candidate_links, chunk, df_other)
    print(f"Обработка {label}: вычисление признаков завершено")
    return features

# Функция для загрузки и предобработки данных
def load_and_preprocess_data(file_paths):
    """
    Загружает данные из CSV-файлов и выполняет базовую предобработку.
    
    :param file_paths: Список путей к CSV-файлам
    :return: Список обработанных DataFrame
    """
    dfs = []
    for file_path in file_paths:
        # Загрузка данных с использованием Dask для эффективной работы с большими файлами
        df = dd.read_csv(file_path).compute()
        # Сброс индекса для обеспечения последовательной нумерации
        df = df.reset_index(drop=True)
        dfs.append(df)
    return dfs

# Функция для проверки наличия необходимых столбцов
def check_required_columns(dfs, required_columns_list):
    """
    Проверяет наличие необходимых столбцов в каждом DataFrame.
    
    :param dfs: Список DataFrame для проверки
    :param required_columns_list: Список списков необходимых столбцов для каждого DataFrame
    """
    for df, required_columns, name in zip(dfs, required_columns_list, ['df_is1', 'df_is2', 'df_is3']):
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Предупреждение: В {name} отсутствуют столбцы: {missing_columns}")
        else:
            print(f"Все необходимые столбцы присутствуют в {name}")

# Функция для объединения столбцов имен
def combine_names(df):
    """
    Объединяет столбцы имени, если они существуют отдельно.
    
    :param df: DataFrame для обработки
    :return: Обработанный DataFrame
    """
    if 'full_name_norm' not in df.columns and all(col in df.columns for col in ['first_name_norm', 'middle_name_norm', 'last_name_norm']):
        print("Объединение имен в DataFrame")
        df['full_name_norm'] = df['first_name_norm'] + ' ' + df['middle_name_norm'] + ' ' + df['last_name_norm']
        df.drop(['first_name_norm', 'middle_name_norm', 'last_name_norm'], axis=1, inplace=True)
    return df

# Функция для создания индексатора и компаратора
def create_indexer_and_comparator(columns):
    """
    Создает объекты индексатора и компаратора для recordlinkage.
    
    :param columns: Список столбцов для использования в индексации и сравнении
    :return: Кортеж (индексатор, компаратор)
    """
    indexer = recordlinkage.Index()
    for column in columns:
        indexer.sortedneighbourhood(column, window=3)
    
    compare_cl = recordlinkage.Compare()
    for column in columns:
        if column.endswith('_norm'):
            compare_cl.string(column, column, method='jarowinkler', threshold=0.85, label=column.replace('_norm', ''))
        else:
            compare_cl.exact(column, column, label=column)
    
    return indexer, compare_cl

# Функция для последовательной обработки данных
def sequential_processing(df_main, df_other, indexer, compare_cl, chunk_size, label):
    """
    Выполняет последовательную обработку данных блоками.
    
    :param df_main: Основной DataFrame
    :param df_other: Второй DataFrame для сопоставления
    :param indexer: Объект индексатора
    :param compare_cl: Объект компаратора
    :param chunk_size: Размер блока для обработки
    :param label: Метка для идентификации процесса
    :return: DataFrame с результатами обработки
    """
    features_list = []
    for start in tqdm(range(0, len(df_main), chunk_size), desc=f"Обработка {label}"):
        chunk = df_main.iloc[start:start + chunk_size]
        features = process_chunk(chunk, df_other, indexer, compare_cl, f'{label} (блок {start})')
        features_list.append(features)
    return pd.concat(features_list)

# Функция для обучения модели
def train_model(features):
    """
    Обучает модель случайного леса на основе предоставленных признаков.
    
    :param features: DataFrame с признаками
    :return: Обученная модель
    """
    # ВНИМАНИЕ: Здесь используются случайные метки. В реальном сценарии нужно использовать настоящие метки
    features['match'] = np.random.randint(0, 2, size=features.shape[0])
    X = features.drop(columns='match')
    y = features['match']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    classifier = RandomForestClassifier(n_estimators=100, random_state=42)
    classifier.fit(X_train, y_train)
    
    y_pred = classifier.predict(X_test)
    print(classification_report(y_test, y_pred))
    
    return classifier

# Функция для предсказания совпадений
def predict_matches(classifier, features, df1, df2):
    """
    Предсказывает совпадения на основе обученной модели.
    
    :param classifier: Обученная модель
    :param features: DataFrame с признаками
    :param df1: Первый DataFrame
    :param df2: Второй DataFrame
    :return: Список словарей с идентификаторами совпадающих записей
    """
    X = features.drop(columns='match', errors='ignore')
    predictions = classifier.predict(X)
    matched_indices = features[predictions == 1].index
    
    results = []
    for index in matched_indices:
        id1 = df1.loc[index[0], 'uid']
        id2 = df2.loc[index[1], 'uid']
        results.append({'id_is1': [id1], 'id_is2': [id2], 'id_is3': []})
    
    return results

# Основная функция
def main():
    # Загрузка и предобработка данных
    file_paths = ['main1_clean_dask.csv', 'main2_clean_dask.csv', 'main3_clean_dask.csv']
    df_is1, df_is2, df_is3 = load_and_preprocess_data(file_paths)
    
    # Определение необходимых столбцов для каждого DataFrame
    required_columns = [
        ['uid', 'full_name_norm', 'birthdate_norm', 'email_norm', 'phone_norm', 'address_norm'],
        ['uid', 'full_name_norm', 'birthdate_norm', 'phone_norm', 'address_norm'],
        ['uid', 'full_name_norm', 'birthdate_norm', 'email_norm']
    ]
    
    # Проверка наличия необходимых столбцов
    check_required_columns([df_is1, df_is2, df_is3], required_columns)
    
    # Объединение столбцов имен в df_is2, если необходимо
    df_is2 = combine_names(df_is2)
    
    # Сопоставление df_is1 и df_is2
    indexer_1_2, compare_cl_1_2 = create_indexer_and_comparator(['birthdate_norm', 'full_name_norm', 'phone_norm', 'address_norm'])
    features_1_2 = sequential_processing(df_is1, df_is2, indexer_1_2, compare_cl_1_2, 10000, 'df_is1 и df_is2')
    
    # Обучение модели и предсказание совпадений для df_is1 и df_is2
    classifier = train_model(features_1_2)
    results = predict_matches(classifier, features_1_2, df_is1, df_is2)
    
    # Сопоставление df_is1 и df_is3
    indexer_1_3, compare_cl_1_3 = create_indexer_and_comparator(['birthdate_norm', 'full_name_norm', 'email_norm'])
    features_1_3 = sequential_processing(df_is1, df_is3, indexer_1_3, compare_cl_1_3, 10000, 'df_is1 и df_is3')
    
    # Предсказание совпадений для df_is1 и df_is3
    results.extend(predict_matches(classifier, features_1_3, df_is1, df_is3))
    
    # Создание итоговой таблицы результатов
    final_results = [[r['id_is1'], r['id_is2'], r['id_is3']] for r in results]
    final_df = pd.DataFrame(final_results, columns=['id_is1', 'id_is2', 'id_is3'])
    final_df.to_csv('final_results.csv', index=False)
    
    print("Процесс завершен успешно!")

# Точка входа в программу
if __name__ == '__main__':
    main()
