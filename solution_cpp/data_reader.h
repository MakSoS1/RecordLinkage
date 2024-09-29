#ifndef DATA_READER_H
#define DATA_READER_H

#include <clickhouse/client.h>
#include <vector>
#include <string>

// Структуры для хранения данных из каждой системы
struct RecordSystem1 {
    std::string uid;
    std::string full_name;
    std::string email;
    std::string address;
    std::string sex;
    std::string birthdate;
    std::string phone;
};

struct RecordSystem2 {
    std::string uid;
    std::string first_name;
    std::string middle_name;
    std::string last_name;
    std::string birthdate;
    std::string phone;
    std::string address;
};

struct RecordSystem3 {
    std::string uid;
    std::string name;
    std::string email;
    std::string birthdate;
    std::string sex;
};

// Функции для работы с ClickHouse
std::vector<RecordSystem1> loadRecordsFromSystem1(clickhouse::Client& client);
std::vector<RecordSystem2> loadRecordsFromSystem2(clickhouse::Client& client);
std::vector<RecordSystem3> loadRecordsFromSystem3(clickhouse::Client& client);

// Функция для нормализации данных
std::string normalizeString(const std::string& input);

#endif // DATA_READER_H
