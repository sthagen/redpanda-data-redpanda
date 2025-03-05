/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"

namespace datalake::translation {

struct translation_state
  : serde::
      envelope<translation_state, serde::version<1>, serde::compat_version<0>> {
    // highest offset that has been successfully translated (inclusive)
    // The translation process encompasses datalake format conversion, upload
    // to a cloud bucket and a subsequent notification to the datalake
    // coordinator of it's availablity.
    kafka::offset highest_translated_offset;
    // approximatie arrival time of the highest translated offset. it is the
    // responsibility of callers to ensure that this is an underestimate. that
    // is, this should be the system time at which some offset <= highest
    // translated offset became available for translation, from the perspective
    // of a translator.
    std::optional<model::timestamp> last_translated_timestamp;

    auto serde_fields() {
        return std::tie(highest_translated_offset, last_translated_timestamp);
    }
};

}; // namespace datalake::translation
