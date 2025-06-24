#include "metrics_registry.h"

#include "base/vassert.h"
#include "config/configuration.h"
#include "metrics/metrics.h"

#include <algorithm>

namespace {
const ss::sstring& label_name(const ss::metrics::label& l) { return l.name(); }

} // namespace

bool metrics_registry::group_info::should_aggregate_topic_label() const {
    return has_topic_label
           // Some metrics have little to no value when the partition label is
           // aggregated. For these metrics aggregating the topic label would
           // render them useless as well. Hence only aggregated the topic label
           // in metrics that are already aggregating the partition label.
           && partition_label_aggregated
           // If the topic label was already in the aggregate label set then
           // it'll always be aggregated regardless of topic count. Hence we
           // skip it here.
           && !topic_label_initially_aggregated;
}

const metrics_registry::group_info& metrics_registry::register_metric(
  const group_name_t& group_name,
  const metric_name_t& metric_name,
  const std::vector<ss::metrics::label>& non_aggregated_labels,
  const std::vector<ss::metrics::label>& aggregated_labels,
  bool has_topic_label) {
    auto& group = _registry[group_name];
    auto [metric_info_iter, inserted] = group.try_emplace(metric_name);
    auto& metric_info = metric_info_iter->second;

    if (inserted) {
        metric_info.has_topic_label = has_topic_label;
        metric_info.partition_label_aggregated = std::ranges::contains(
          aggregated_labels, metrics::partition_label.name(), label_name);
        metric_info.topic_label_initially_aggregated = std::ranges::contains(
          aggregated_labels, metrics::topic_label.name(), label_name);

        metric_info.non_aggregated_labels = non_aggregated_labels;
        metric_info.aggregated_labels = aggregated_labels;

        if (
          _aggregated_topic_label
          && metric_info.should_aggregate_topic_label()) {
            metric_info.aggregated_labels.emplace_back(metrics::topic_label);
        }
    }
#ifndef NDEBUG
    else {
        // seastar allows registering foo_bar{label="1"} and foo_bar{label="2"}
        // with different aggregation labels. However, the aggregation labels of
        // the later would be silently ignored.
        // Here we check that that isn't happening.
        auto label_comparer = [](const auto& lhs, const auto& rhs) {
            return lhs.name() == rhs.name();
        };

        vassert(
          std::equal(
            metric_info.non_aggregated_labels.begin(),
            metric_info.non_aggregated_labels.end(),
            non_aggregated_labels.begin(),
            non_aggregated_labels.end(),
            label_comparer),
          "Different non-aggregation labels specified for the same metric"
          "{} {}",
          group_name,
          metric_name);

        vassert(
          std::equal(
            metric_info.aggregated_labels.begin(),
            metric_info.aggregated_labels.end()
              // In cases where the topic_label has been dynamically aggregated
              // it'll be the last element. All elements before that should be
              // equal though.
              - ((_aggregated_topic_label && metric_info.should_aggregate_topic_label()) ? 1 : 0),
            aggregated_labels.begin(),
            aggregated_labels.end(),
            label_comparer),
          "Different aggregation labels specified for the same metric group"
          "{} {}",
          group_name,
          metric_name);
    }
#endif

    return metric_info;
}

void metrics_registry::update_aggregation_labels(bool aggregate_metrics) {
    for (const auto& [group_name, group] : _registry) {
        for (const auto& [metric_name, metric_info] : group) {
            ss::metrics::update_aggregate_labels(
              group_name,
              metric_name,
              aggregate_metrics ? metric_info.aggregated_labels
                                : metric_info.non_aggregated_labels);
        }
    }
}

void metrics_registry::enable_topic_label_aggregation() {
    if (_aggregated_topic_label) {
        return;
    }
    _aggregated_topic_label = true;
    auto aggregate_metrics = config::shard_local_cfg().aggregate_metrics();

    for (auto& [gn, metrics] : _registry) {
        for (auto& [mn, mi] : metrics) {
            if (mi.should_aggregate_topic_label()) {
                auto& agg_ls = mi.aggregated_labels;
                agg_ls.push_back(metrics::topic_label);

                if (aggregate_metrics) {
                    ss::metrics::update_aggregate_labels(gn, mn, agg_ls);
                }
            }
        }
    }
}

void metrics_registry::disable_topic_label_aggregation() {
    if (!_aggregated_topic_label) {
        return;
    }
    _aggregated_topic_label = false;
    auto aggregate_metrics = config::shard_local_cfg().aggregate_metrics();

    for (auto& [gn, metrics] : _registry) {
        for (auto& [mn, mi] : metrics) {
            if (mi.should_aggregate_topic_label()) {
                auto& agg_ls = mi.aggregated_labels;
#ifndef NDEBUG
                vassert(
                  agg_ls.back().name() == metrics::topic_label.name(),
                  "topic label should be the last element");
#endif
                agg_ls.pop_back();

                if (aggregate_metrics) {
                    ss::metrics::update_aggregate_labels(gn, mn, agg_ls);
                }
            }
        }
    }
}

thread_local metrics_registry metrics_registry::
  _local_instance; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
