#ifndef DATA_PROCESSOR_H
#define DATA_PROCESSOR_H

#include "data_reader.h"
#include <vector>
#include <string>
#include <clickhouse/client.h>

// Функции для обработки данных и записи результата в ClickHouse
void processDataFromClickHouse(
    clickhouse::Client& client, const std::string& output_table);

void writeResultsToClickHouse(
    clickhouse::Client& client, const std::vector<std::vector<std::string>>& results, const std::string& output_table);

#endif // DATA_PROCESSOR_H
