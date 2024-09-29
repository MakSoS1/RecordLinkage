#include "data_processor.h"
#include <iostream>
#include <clickhouse/client.h>

// Запись результатов в таблицу ClickHouse
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

// Обработка данных из ClickHouse и запись результата
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
