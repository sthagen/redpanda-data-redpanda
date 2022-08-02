/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "bytes/iobuf_parser.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "reflection/adl.h"
#include "serde/serde.h"

#include <fmt/ostream.h>

#include <ostream>

namespace cluster {

struct ntp_leader : serde::envelope<ntp_leader, serde::version<0>> {
    model::ntp ntp;
    model::term_id term;
    std::optional<model::node_id> leader_id;

    ntp_leader() noexcept = default;

    ntp_leader(
      model::ntp ntp,
      model::term_id term,
      std::optional<model::node_id> leader_id)
      : ntp(std::move(ntp))
      , term(term)
      , leader_id(leader_id) {}

    friend std::ostream& operator<<(std::ostream& o, const ntp_leader& l) {
        fmt::print(
          o,
          "{{ntp: {}, term: {}, leader: {}}}",
          l.ntp,
          l.term,
          l.leader_id ? l.leader_id.value()() : -1);
        return o;
    }

    auto serde_fields() { return std::tie(ntp, term, leader_id); }
};

struct ntp_leader_revision
  : serde::envelope<ntp_leader_revision, serde::version<0>> {
    model::ntp ntp;
    model::term_id term;
    std::optional<model::node_id> leader_id;
    model::revision_id revision;

    ntp_leader_revision() noexcept = default;

    ntp_leader_revision(
      model::ntp ntp,
      model::term_id term,
      std::optional<model::node_id> leader_id,
      model::revision_id revision)
      : ntp(std::move(ntp))
      , term(term)
      , leader_id(leader_id)
      , revision(revision) {}

    friend std::ostream&
    operator<<(std::ostream& o, const ntp_leader_revision& r) {
        fmt::print(
          o,
          "{{ntp: {}, term: {}, leader: {}, revision: {}}}",
          r.ntp,
          r.term,
          r.leader_id,
          r.revision);
        return o;
    }

    auto serde_fields() { return std::tie(ntp, term, leader_id, revision); }
};

struct update_leadership_request
  : serde::envelope<update_leadership_request, serde::version<0>> {
    std::vector<ntp_leader> leaders;

    update_leadership_request() noexcept = default;

    explicit update_leadership_request(std::vector<ntp_leader> leaders)
      : leaders(std::move(leaders)) {}

    friend bool operator==(
      const update_leadership_request&, const update_leadership_request&)
      = default;

    auto serde_fields() { return std::tie(leaders); }

    friend std::ostream&
    operator<<(std::ostream& o, const update_leadership_request& r) {
        fmt::print(o, "leaders {}", r.leaders);
        return o;
    }
};

struct update_leadership_request_v2
  : serde::envelope<update_leadership_request_v2, serde::version<0>> {
    static constexpr int8_t version = 0;
    std::vector<ntp_leader_revision> leaders;

    update_leadership_request_v2() noexcept = default;

    explicit update_leadership_request_v2(
      std::vector<ntp_leader_revision> leaders)
      : leaders(std::move(leaders)) {}

    auto serde_fields() { return std::tie(leaders); }
};

struct update_leadership_reply
  : serde::envelope<update_leadership_reply, serde::version<0>> {
    update_leadership_reply() noexcept = default;

    auto serde_fields() { return std::tie(); }
};

struct get_leadership_request
  : serde::envelope<get_leadership_request, serde::version<0>> {
    get_leadership_request() noexcept = default;

    auto serde_fields() { return std::tie(); }
};

struct get_leadership_reply
  : serde::envelope<get_leadership_reply, serde::version<0>> {
    std::vector<ntp_leader> leaders;

    get_leadership_reply() noexcept = default;

    explicit get_leadership_reply(std::vector<ntp_leader> leaders)
      : leaders(std::move(leaders)) {}

    auto serde_fields() { return std::tie(leaders); }
};

} // namespace cluster

namespace reflection {
template<>
struct adl<cluster::get_leadership_reply> {
    void to(iobuf& out, cluster::get_leadership_reply&& r) {
        serialize(out, r.leaders);
    }
    cluster::get_leadership_reply from(iobuf_parser& in) {
        auto leaders = adl<std::vector<cluster::ntp_leader>>{}.from(in);
        return cluster::get_leadership_reply(std::move(leaders));
    }
};

template<>
struct adl<cluster::get_leadership_request> {
    void to(iobuf&, cluster::get_leadership_request&&) {}
    cluster::get_leadership_request from(iobuf_parser&) { return {}; }
};

template<>
struct adl<cluster::update_leadership_reply> {
    void to(iobuf&, cluster::update_leadership_reply&&) {}
    cluster::update_leadership_reply from(iobuf_parser&) { return {}; }
};

template<>
struct adl<cluster::update_leadership_request> {
    void to(iobuf& out, cluster::update_leadership_request&& r) {
        serialize(out, std::move(r.leaders));
    }
    cluster::update_leadership_request from(iobuf_parser& in) {
        auto leaders = adl<std::vector<cluster::ntp_leader>>{}.from(in);
        return cluster::update_leadership_request(std::move(leaders));
    }
};

template<>
struct adl<cluster::ntp_leader_revision> {
    void to(iobuf& out, cluster::ntp_leader_revision&& l) {
        serialize(out, std::move(l.ntp), l.term, l.leader_id, l.revision);
    }
    cluster::ntp_leader_revision from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto term = adl<model::term_id>{}.from(in);
        auto leader = adl<std::optional<model::node_id>>{}.from(in);
        auto revision = adl<model::revision_id>{}.from(in);
        return {std::move(ntp), term, leader, revision};
    }
};

template<>
struct adl<cluster::ntp_leader> {
    void to(iobuf& out, cluster::ntp_leader&& l) {
        serialize(out, std::move(l.ntp), l.term, l.leader_id);
    }
    cluster::ntp_leader from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto term = adl<model::term_id>{}.from(in);
        auto leader = adl<std::optional<model::node_id>>{}.from(in);
        return {std::move(ntp), term, leader};
    }
};

template<>
struct adl<cluster::update_leadership_request_v2> {
    void to(iobuf& out, cluster::update_leadership_request_v2&& req) {
        serialize(out, req.version, req.leaders);
    }
    cluster::update_leadership_request_v2 from(iobuf_parser& in) {
        // decode version
        adl<int8_t>{}.from(in);
        auto leaders = adl<std::vector<cluster::ntp_leader_revision>>{}.from(
          in);
        return cluster::update_leadership_request_v2(std::move(leaders));
    }
};
} // namespace reflection
