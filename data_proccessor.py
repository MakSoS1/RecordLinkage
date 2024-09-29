import dask.dataframe as dd
import pandas as pd
import recordlinkage
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import numpy as np
from tqdm import tqdm

def process_chunk(chunk, df_other, indexer, compare_cl, label):
    print(f"Начата обработка блока: {label}")
    candidate_links = indexer.index(chunk, df_other)
    print(f"Обработка {label}: создано {len(candidate_links)} пар кандидатов")
    features = compare_cl.compute(candidate_links, chunk, df_other)
    print(f"Обработка {label}: вычисление признаков завершено")
    return features

def load_and_preprocess_data(file_paths):
    dfs = []
    for file_path in file_paths:
        df = dd.read_csv(file_path).compute()
        df = df.reset_index(drop=True)
        dfs.append(df)
    return dfs

def check_required_columns(dfs, required_columns_list):
    for df, required_columns, name in zip(dfs, required_columns_list, ['df_is1', 'df_is2', 'df_is3']):
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Предупреждение: В {name} отсутствуют столбцы: {missing_columns}")
        else:
            print(f"Все необходимые столбцы присутствуют в {name}")

def combine_names(df):
    if 'full_name_norm' not in df.columns and all(col in df.columns for col in ['first_name_norm', 'middle_name_norm', 'last_name_norm']):
        print("Объединение имен в DataFrame")
        df['full_name_norm'] = df['first_name_norm'] + ' ' + df['middle_name_norm'] + ' ' + df['last_name_norm']
        df.drop(['first_name_norm', 'middle_name_norm', 'last_name_norm'], axis=1, inplace=True)
    return df

def create_indexer_and_comparator(columns):
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

def sequential_processing(df_main, df_other, indexer, compare_cl, chunk_size, label):
    features_list = []
    for start in tqdm(range(0, len(df_main), chunk_size), desc=f"Обработка {label}"):
        chunk = df_main.iloc[start:start + chunk_size]
        features = process_chunk(chunk, df_other, indexer, compare_cl, f'{label} (блок {start})')
        features_list.append(features)
    return pd.concat(features_list)

def train_model(features):
    features['match'] = np.random.randint(0, 2, size=features.shape[0])  # Замените на реальные метки
    X = features.drop(columns='match')
    y = features['match']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    classifier = RandomForestClassifier(n_estimators=100, random_state=42)
    classifier.fit(X_train, y_train)
    
    y_pred = classifier.predict(X_test)
    print(classification_report(y_test, y_pred))
    
    return classifier

def predict_matches(classifier, features, df1, df2):
    X = features.drop(columns='match', errors='ignore')
    predictions = classifier.predict(X)
    matched_indices = features[predictions == 1].index
    
    results = []
    for index in matched_indices:
        id1 = df1.loc[index[0], 'uid']
        id2 = df2.loc[index[1], 'uid']
        results.append({'id_is1': [id1], 'id_is2': [id2], 'id_is3': []})
    
    return results

def main():
    # Загрузка и предобработка данных
    file_paths = ['main1_clean_dask.csv', 'main2_clean_dask.csv', 'main3_clean_dask.csv']
    df_is1, df_is2, df_is3 = load_and_preprocess_data(file_paths)
    
    required_columns = [
        ['uid', 'full_name_norm', 'birthdate_norm', 'email_norm', 'phone_norm', 'address_norm'],
        ['uid', 'full_name_norm', 'birthdate_norm', 'phone_norm', 'address_norm'],
        ['uid', 'full_name_norm', 'birthdate_norm', 'email_norm']
    ]
    
    check_required_columns([df_is1, df_is2, df_is3], required_columns)
    
    df_is2 = combine_names(df_is2)
    
    # Сопоставление df_is1 и df_is2
    indexer_1_2, compare_cl_1_2 = create_indexer_and_comparator(['birthdate_norm', 'full_name_norm', 'phone_norm', 'address_norm'])
    features_1_2 = sequential_processing(df_is1, df_is2, indexer_1_2, compare_cl_1_2, 10000, 'df_is1 и df_is2')
    
    classifier = train_model(features_1_2)
    results = predict_matches(classifier, features_1_2, df_is1, df_is2)
    
    # Сопоставление df_is1 и df_is3
    indexer_1_3, compare_cl_1_3 = create_indexer_and_comparator(['birthdate_norm', 'full_name_norm', 'email_norm'])
    features_1_3 = sequential_processing(df_is1, df_is3, indexer_1_3, compare_cl_1_3, 10000, 'df_is1 и df_is3')
    
    results.extend(predict_matches(classifier, features_1_3, df_is1, df_is3))
    
    # Создание итоговой таблицы результатов
    final_results = [[r['id_is1'], r['id_is2'], r['id_is3']] for r in results]
    final_df = pd.DataFrame(final_results, columns=['id_is1', 'id_is2', 'id_is3'])
    final_df.to_csv('final_results.csv', index=False)
    
    print("Процесс завершен успешно!")

if __name__ == '__main__':
    main()
