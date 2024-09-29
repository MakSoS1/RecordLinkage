#include "data_reader.h"
#include <clickhouse/client.h>
#include <iostream>

// Вспомогательная функция для удаления лишних пробелов и символов
std::string normalizeString(const std::string& input) {
    std::string output;
    for (char c : input) {
        if (!std::isspace(c) && c != '-') {
            output += std::tolower(c, std::locale());
        }
    }
    return output;
}

// Загрузка данных из таблицы системы 1
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

// Загрузка данных из таблицы системы 2
std::vector<RecordSystem2> loadRecordsFromSystem2(clickhouse::Client& client) {
    std::vector<RecordSystem2> records;

    // SQL-запрос для получения данных из таблицы
    client.Select("SELECT uid, first_name, middle_name, last_name, birthdate, phone, address FROM system2_table", [&records](const clickhouse::Block& block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            RecordSystem2 record;
            record.uid = block[0]->As<clickhouse::ColumnString>()->At(i);
            record.first_name = normalizeString(block[1]->As<clickhouse::ColumnString>()->At(i));
            record.middle_name = normalizeString(block[2]->As<clickhouse::ColumnString>()->At(i));
            record.last_name = normalizeString(block[3]->As<clickhouse::ColumnString>()->At(i));
            record.birthdate = block[4]->As<clickhouse::ColumnString>()->At(i);
            record.phone = normalizeString(block[5]->As<clickhouse::ColumnString>()->At(i));
            record.address = normalizeString(block[6]->As<clickhouse::ColumnString>()->At(i));
            records.push_back(record);
        }
    });

    return records;
}

// Загрузка данных из таблицы системы 3
std::vector<RecordSystem3> loadRecordsFromSystem3(clickhouse::Client& client) {
    std::vector<RecordSystem3> records;

    // SQL-запрос для получения данных из таблицы
    client.Select("SELECT uid, name, email, birthdate, sex FROM system3_table", [&records](const clickhouse::Block& block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            RecordSystem3 record;
            record.uid = block[0]->As<clickhouse::ColumnString>()->At(i);
            record.name = normalizeString(block[1]->As<clickhouse::ColumnString>()->At(i));
            record.email = normalizeString(block[2]->As<clickhouse::ColumnString>()->At(i));
            record.birthdate = block[3]->As<clickhouse::ColumnString>()->At(i);
            record.sex = block[4]->As<clickhouse::ColumnString>()->At(i);
            records.push_back(record);
        }
    });

    return records;
}
