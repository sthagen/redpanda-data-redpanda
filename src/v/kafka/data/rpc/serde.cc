/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/data/rpc/serde.h"

namespace kafka::data::rpc {

kafka_topic_data::kafka_topic_data(
  model::topic_partition tp, model::record_batch b)
  : tp(std::move(tp)) {
    batches.reserve(1);
    batches.push_back(std::move(b));
}

kafka_topic_data::kafka_topic_data(
  model::topic_partition tp, ss::chunked_fifo<model::record_batch> b)
  : tp(std::move(tp))
  , batches(std::move(b)) {}

kafka_topic_data kafka_topic_data::share() {
    ss::chunked_fifo<model::record_batch> shared;
    shared.reserve(batches.size());
    for (auto& batch : batches) {
        shared.push_back(batch.share());
    }
    return {tp, std::move(shared)};
}

produce_request produce_request::share() {
    ss::chunked_fifo<kafka::data::rpc::kafka_topic_data> shared;
    shared.reserve(topic_data.size());
    for (auto& data : topic_data) {
        shared.push_back(data.share());
    }
    return {std::move(shared), timeout};
}

} // namespace kafka::data::rpc

auto fmt::formatter<kafka::data::rpc::produce_request>::format(
  const kafka::data::rpc::produce_request& req,
  format_context& ctx) const -> format_context::iterator {
    return fmt::format_to(
      ctx.out(),
      "{{ topic_data: {}, timeout: {} }}",
      fmt::join(req.topic_data, ", "),
      req.timeout);
}

auto fmt::formatter<kafka::data::rpc::kafka_topic_data>::format(
  const kafka::data::rpc::kafka_topic_data& data,
  format_context& ctx) const -> format_context::iterator {
    return fmt::format_to(
      ctx.out(),
      "{{ tp: {}, batches_size: {} }}",
      data.tp,
      data.batches.size());
}

auto fmt::formatter<kafka::data::rpc::produce_reply>::format(
  const kafka::data::rpc::produce_reply& reply,
  format_context& ctx) const -> format_context::iterator {
    return fmt::format_to(
      ctx.out(), "{{ results: {} }}", fmt::join(reply.results, ", "));
}

auto fmt::formatter<kafka::data::rpc::kafka_topic_data_result>::format(
  const kafka::data::rpc::kafka_topic_data_result& result,
  format_context& ctx) const -> format_context::iterator {
    return fmt::format_to(
      ctx.out(), "{{ errc: {}, tp: {} }}", result.err, result.tp);
}
