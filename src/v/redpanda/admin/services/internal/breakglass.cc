
#include "redpanda/admin/services/internal/breakglass.h"

#include "base/vlog.h"
#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

#include <limits>

namespace proto {
using namespace proto::admin::internal;
}

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger logger{"admin_api_server/internal_breakglass_service"};
} // namespace

namespace admin::internal {

seastar::future<proto::controller_forced_reconfiguration_response>
breakglass_service_impl::controller_forced_reconfiguration(
  serde::pb::rpc::context,
  proto::controller_forced_reconfiguration_request request) {
    vlog(
      logger.info,
      "calling controller forced reconfiguration with nodes: {} and "
      "surviving_node_count: {}",
      request.get_dead_node_ids(),
      request.get_surviving_node_count());

    static constexpr auto check_err = [](cluster::error_info error_info) {
        if (error_info.err == std::error_code{cluster::errc::success}) {
            return;
        } else if (error_info.err == std::errc::operation_not_supported) {
            throw serde::pb::rpc::unimplemented_exception(
              std::move(error_info.message));
        }
        throw serde::pb::rpc::unknown_exception(std::move(error_info.message));
    };

    uint16_t surviving_node_count{0};
    { // proto doesn't support uint16, no cluster will ever reasonably be above
      // uint16_max in size
        const auto _surviving_node_count = request.get_surviving_node_count();
        if (_surviving_node_count > std::numeric_limits<uint16_t>::max()) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format(
                "provided surviving broker size is too large: {}",
                _surviving_node_count));
        }
        surviving_node_count = static_cast<uint16_t>(_surviving_node_count);
    }

    std::vector<model::node_id> dead_nodes{
      std::from_range,
      std::ranges::views::transform(
        std::move(request).get_dead_node_ids(),
        [](int32_t node_number) { return model::node_id{node_number}; })};

    auto error_info = co_await ss::smp::submit_to(
      0,
      [this, dead_nodes = std::move(dead_nodes), surviving_node_count] mutable {
          return _controller->initialize_controller_forced_reconfiguration(
            std::move(dead_nodes), surviving_node_count);
      });

    vlog(
      logger.info,
      "finished initializing controller forced reconfiguration with "
      "error_info: {}",
      error_info);

    check_err(std::move(error_info));

    co_return proto::controller_forced_reconfiguration_response{};
}
} // namespace admin::internal
