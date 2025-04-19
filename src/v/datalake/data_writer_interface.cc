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
        return os << "Ok";
    case writer_error::parquet_conversion_error:
        return os << "Parquet Conversion Error";
    case writer_error::file_io_error:
        return os << "File IO Error";
    case writer_error::no_data:
        return os << "No data";
    case writer_error::flush_error:
        return os << "Flush failed";
    case writer_error::oom_error:
        return os << "Memory exhausted";
    case writer_error::time_limit_exceeded:
        return os << "Time limit exceeded";
    case writer_error::shutting_down:
        return os << "Shutting down";
    case writer_error::unknown_error:
        return os << "Unknown error";
    }
}
std::string data_writer_error_category::message(int ev) const {
    return fmt::to_string(static_cast<writer_error>(ev));
}

writer_error map_to_writer_error(reservation_error reservation_err) {
    switch (reservation_err) {
    case ok:
        return writer_error::ok;
    case shutting_down:
        return writer_error::shutting_down;
    case out_of_memory:
        return writer_error::oom_error;
    case time_quota_exceeded:
        return writer_error::time_limit_exceeded;
    case unknown:
        return writer_error::unknown_error;
    }
}

bool is_recoverable_error(datalake::writer_error err) {
    switch (err) {
    case datalake::writer_error::ok:
    case datalake::writer_error::oom_error:
    case datalake::writer_error::time_limit_exceeded:
        return true;
    case datalake::writer_error::parquet_conversion_error:
    case datalake::writer_error::file_io_error:
    case datalake::writer_error::no_data:
    case datalake::writer_error::flush_error:
    case datalake::writer_error::shutting_down:
    case datalake::writer_error::unknown_error:
        return false;
    }
}

} // namespace datalake
