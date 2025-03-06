/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/data_writer_interface.h"

#include <fmt/core.h>
namespace datalake {
std::ostream& operator<<(std::ostream& os, const writer_error& ev) {
    switch (ev) {
    case writer_error::ok:
        os << "Ok";
        break;
    case writer_error::parquet_conversion_error:
        os << "Parquet Conversion Error";
        break;
    case writer_error::file_io_error:
        os << "File IO Error";
        break;
    case writer_error::no_data:
        os << "No data";
        break;
    case writer_error::flush_error:
        os << "Flush failed";
        break;
    }
    return os;
}
std::string data_writer_error_category::message(int ev) const {
    return fmt::to_string(static_cast<writer_error>(ev));
}

} // namespace datalake
