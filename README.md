# Data Processing Project

## Описание

Данный проект предназначен для обработки и нормализации данных из базы данных ClickHouse с последующим использованием моделей машинного обучения для поиска дубликатов и сопоставления данных. Проект включает два основных скрипта:

1. **data_reader.py** — скрипт для чтения данных из ClickHouse, их предварительной очистки и сохранения в формате CSV.
2. **data_proccessor.py** — скрипт для дальнейшей обработки очищенных данных, сопоставления записей и создания модели для нахождения совпадений.

## Установка и Запуск

### 1. Клонирование репозитория

```bash
git clone https://github.com/your-username/data-processing-project.git
cd data-processing-project
```

## Структура данных
Ожидается, что данные в ClickHouse будут иметь следующие столбцы в таблицах:

table_dataset1: full_name, email, address, birthdate, phone
table_dataset2: first_name, middle_name, last_name, birthdate, phone, address
table_dataset3: name, email, birthdate, sex
## Описание полей
full_name_norm: нормализованное полное имя.
email_norm: нормализованный email.
address_norm: нормализованный адрес.
birthdate_norm: нормализованная дата рождения.
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
