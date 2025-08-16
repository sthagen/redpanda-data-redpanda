/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/time_transform_visitor.h"

#include "iceberg/values.h"

namespace iceberg {

namespace {

/**
 * Used to convert timestamp types
 */
template<typename DurationT>
int32_t micros_to_duration(int64_t micros) {
    if constexpr (std::is_same_v<DurationT, std::chrono::hours>) {
        return std::chrono::floor<std::chrono::hours>(
                 std::chrono::microseconds(micros))
          .count();
    } else {
        /**
         * This is a special case for days, months and years. Since if we go
         * directly from us to months the calculation is not the same as in Java
         * and Python reference implementations. In the implementation it is
         * simply a number of days/months/years since epoch, not a result of
         * duration from us to other duration.
         */

        return std::chrono::floor<DurationT>(
                 std::chrono::floor<std::chrono::days>(
                   std::chrono::microseconds(micros)))
          .count();
    }
}

/**
 * Used to convert date type being a number of days since epoch
 */
template<typename DurationT>
int32_t days_to_duration(int32_t days) {
    return std::chrono::floor<DurationT>(std::chrono::days(days)).count();
}
} // namespace

int32_t hour_transform_visitor::operator()(const primitive_value& v) {
    if (std::holds_alternative<timestamp_value>(v)) {
        return micros_to_duration<std::chrono::hours>(
          std::get<timestamp_value>(v).val);
    }
    if (std::holds_alternative<timestamptz_value>(v)) {
        return micros_to_duration<std::chrono::hours>(
          std::get<timestamptz_value>(v).val);
    }
    throw std::invalid_argument(
      fmt::format("hourly_visitor not implemented for primitive value {}", v));
}

template<typename DurationT>
int32_t
time_transform_visitor<DurationT>::operator()(const primitive_value& v) {
    if (std::holds_alternative<date_value>(v)) {
        return days_to_duration<DurationT>(std::get<date_value>(v).val);
    }

    if (std::holds_alternative<timestamp_value>(v)) {
        return micros_to_duration<DurationT>(std::get<timestamp_value>(v).val);
    }

    if (std::holds_alternative<timestamptz_value>(v)) {
        return micros_to_duration<DurationT>(
          std::get<timestamptz_value>(v).val);
    }

    throw std::invalid_argument(
      fmt::format(
        "day_transform_visitor not implemented for primitive value {}", v));
}
template int32_t
time_transform_visitor<std::chrono::days>::operator()(const primitive_value& v);
template int32_t time_transform_visitor<std::chrono::months>::operator()(
  const primitive_value& v);
template int32_t time_transform_visitor<std::chrono::years>::operator()(
  const primitive_value& v);

} // namespace iceberg
