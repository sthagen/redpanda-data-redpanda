/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "net/dial.h"

#include "base/vlog.h"
#include "ssx/sformat.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/with_timeout.hh>

#include <system_error>

namespace {

class timed_out_error : public ss::timed_out_error {
public:
    explicit timed_out_error(ss::sstring msg)
      : _msg{std::move(msg)} {}
    const char* what() const noexcept override { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

} // namespace

namespace net::detail {

ss::future<ss::connected_socket> dial_single(
  const ss::socket_address& address,
  clock_type::time_point timeout,
  seastar::logger* log) {
    auto socket = ss::make_lw_shared<ss::socket>(ss::engine().net().socket());
    auto f = socket->connect(address).finally([socket] {});
    return ss::with_timeout(timeout, std::move(f))
      .handle_exception([socket, address, log](const std::exception_ptr& e) {
          try {
              std::rethrow_exception(e);
          } catch (const ss::timed_out_error& ex) {
              socket->shutdown();
              return ss::make_exception_future<ss::connected_socket>(
                timed_out_error(
                  ssx::sformat("connection to {} - {}", address, e)));
          } catch (const std::system_error& ex) {
              socket->shutdown();
              return ss::make_exception_future<ss::connected_socket>(
                std::system_error(
                  ex.code(), fmt::format("connection to {}", address)));
          } catch (...) {
              vlog(log->trace, "error connecting to {} - {}", address, e);
              socket->shutdown();
              return ss::make_exception_future<ss::connected_socket>(e);
          }
      });
}

std::exception_ptr dial_deadline_exceeded(const ss::socket_address& address) {
    return std::make_exception_ptr(timed_out_error(
      ssx::sformat("connection to {} - deadline exceeded", address)));
}

} // namespace net::detail
