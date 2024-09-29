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
