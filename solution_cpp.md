## Документация к C++ решению для задачи Record Linkage

### Введение

Это решение на C++ реализует задачу Record Linkage, которая заключается в сопоставлении и объединении клиентских записей из нескольких источников данных (разных информационных систем) с использованием базы данных ClickHouse. Основная цель программы — объединить записи, относящиеся к одному и тому же клиенту, и записать результаты в таблицу `table_results`. Это решение предоставляет высокую производительность благодаря использованию C++, что делает его более быстрым по сравнению с реализацией на Python.

### Структура проекта

1. **`main.cpp`** — основной файл, который инициализирует соединение с ClickHouse и запускает процесс обработки данных.
2. **`data_reader.cpp`** — содержит функции для чтения и нормализации данных из различных таблиц в ClickHouse.
3. **`data_processor.cpp`** — отвечает за обработку и сопоставление данных из разных систем и запись результатов в ClickHouse.

### Описание работы

#### Файл: `main.cpp`

**Основной файл программы**. Он:
- Настраивает параметры подключения к базе данных ClickHouse.
- Вызывает функцию `processDataFromClickHouse` для загрузки данных, их обработки и записи результатов в таблицу.

Пример:
```cpp
#include "data_processor.h"
#include <clickhouse/client.h>

int main() {
    // Параметры подключения к ClickHouse
    clickhouse::ClientOptions options;
    options.SetHost("localhost");

    clickhouse::Client client(options);

    // Таблица для сохранения результатов
    std::string output_table = "table_results";

    // Процесс обработки данных из ClickHouse
    processDataFromClickHouse(client, output_table);

    std::cout << "Data processing completed and saved to " << output_table << std::endl;

    return 0;
}
```

#### Файл: `data_reader.cpp`

**Отвечает за чтение данных из таблиц различных систем**. 

1. **Нормализация данных**:
    - Строки приводятся к нижнему регистру.
    - Удаляются лишние пробелы и символы (например, дефисы).
  
2. **Загрузка данных**:
    - Используется SQL-запрос для получения данных из каждой системы (например, `system1_table`).
    - Результаты загружаются в структуру данных `std::vector<RecordSystem1>`, где каждая запись содержит такие поля, как `uid`, `full_name`, `email`, и др.

Пример кода:
```cpp
std::string normalizeString(const std::string& input) {
    std::string output;
    for (char c : input) {
        if (!std::isspace(c) && c != '-') {
            output += std::tolower(c, std::locale());
        }
    }
    return output;
}

// Загрузка данных из системы 1
std::vector<RecordSystem1> loadRecordsFromSystem1(clickhouse::Client& client) {
    std::vector<RecordSystem1> records;

    // SQL-запрос для получения данных из таблицы
    client.Select("SELECT uid, full_name, email, address, sex, birthdate, phone FROM system1_table", [&records](const clickhouse::Block& block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            RecordSystem1 record;
            record.uid = block[0]->As<clickhouse::ColumnString>()->At(i);
            record.full_name = normalizeString(block[1]->As<clickhouse::ColumnString>()->At(i));
            record.email = normalizeString(block[2]->As<clickhouse::ColumnString>()->At(i));
            record.address = normalizeString(block[3]->As<clickhouse::ColumnString>()->At(i));
            record.sex = block[4]->As<clickhouse::ColumnString>()->At(i);
            record.birthdate = block[5]->As<clickhouse::ColumnString>()->At(i);
            record.phone = normalizeString(block[6]->As<clickhouse::ColumnString>()->At(i));
            records.push_back(record);
        }
    });

    return records;
}
```

#### Файл: `data_processor.cpp`

**Обработка и сопоставление данных**. 

1. **Сопоставление записей**:
   - Загружаются данные из всех систем.
   - Выполняется простое сопоставление записей на основе индексов для каждого источника.
  
2. **Запись результатов**:
   - Результаты сопоставления записей сохраняются в таблицу `table_results` в ClickHouse.

Пример кода:
```cpp
void processDataFromClickHouse(clickhouse::Client& client, const std::string& output_table) {
    // Загрузка данных
    std::vector<RecordSystem1> records1 = loadRecordsFromSystem1(client);
    std::vector<RecordSystem2> records2 = loadRecordsFromSystem2(client);
    std::vector<RecordSystem3> records3 = loadRecordsFromSystem3(client);

    std::vector<std::vector<std::string>> results;

    // Пример простого сопоставления данных
    for (size_t i = 0; i < std::min({records1.size(), records2.size(), records3.size()}); ++i) {
        std::vector<std::string> result;
        result.push_back(records1[i].uid);
        result.push_back(records2[i].uid);
        result.push_back(records3[i].uid);
        results.push_back(result);
    }

    // Запись результата в таблицу
    writeResultsToClickHouse(client, results, output_table);
}

void writeResultsToClickHouse(
    clickhouse::Client& client, const std::vector<std::vector<std::string>>& results, const std::string& output_table) {
    
    clickhouse::Block block;
    
    auto col_uid1 = std::make_shared<clickhouse::ColumnString>();
    auto col_uid2 = std::make_shared<clickhouse::ColumnString>();
    auto col_uid3 = std::make_shared<clickhouse::ColumnString>();
    
    for (const auto& row : results) {
        col_uid1->Append(row[0]);
        col_uid2->Append(row[1]);
        col_uid3->Append(row[2]);
    }
    
    block.AppendColumn("uid_is1", col_uid1);
    block.AppendColumn("uid_is2", col_uid2);
    block.AppendColumn("uid_is3", col_uid3);
    
    client.Insert(output_table, block);
}
```

### Сравнение с решением на Python

Решение на C++ значительно быстрее, чем аналогичное решение на Python, благодаря следующим аспектам:
1. **Более низкий уровень абстракции**: C++ работает ближе к аппаратному обеспечению, что позволяет значительно быстрее выполнять вычисления и операции ввода-вывода.
2. **Статическая типизация**: C++ компилируется с проверкой типов во время компиляции, что исключает накладные расходы во время выполнения программы.
3. **Отсутствие необходимости в сборке мусора**: В отличие от Python, C++ не имеет сборщика мусора, что снижает накладные расходы на управление памятью и увеличивает скорость выполнения.
4. **Прямой доступ к ClickHouse**: Прямое использование ClickHouse API позволяет быстрее выполнять запросы и манипуляции с данными, в отличие от использования промежуточных библиотек, как в Python.

### Преимущества и особенности

- **Быстрота обработки данных**: C++ обладает высокой производительностью, что особенно важно при обработке больших объемов данных.
- **Оптимизация по памяти**: Использование C++ позволяет более точно управлять памятью и избегать чрезмерного потребления ресурсов.
- **Интеграция с ClickHouse**: Прямое взаимодействие с ClickHouse через API делает взаимодействие с базой данных более эффективным.

### Заключение

Решение на C++ идеально подходит для случаев, когда требуется высокая производительность и низкая задержка при обработке данных. Оно обеспечивает быстрый доступ к данным, их обработку и запись результатов, что делает его предпочтительным для сценариев с большими объёмами данных и ограниченным временем выполнения.
