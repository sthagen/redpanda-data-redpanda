/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License included in
 * the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with the Business
 * Source License, use of this software will be governed by the Apache License,
 * Version 2.0
 *
 * Coverage
 * ========
 *
 * llvm-profdata merge -sparse default.profraw -o default.profdata
 *
 * llvm-cov show src/v/bytes/tests/fuzz_iobuf -instr-profile=default.profdata
 * -format=html ../src/v/bytes/iobuf.h ../src/v/bytes/iobuf.cc > cov.html
 */
#include "base/units.h"
#include "base/vassert.h"
#include "bytes/iobuf.h"
#include "bytes/scattered_message.h"

#include <deque>
#include <exception>
#include <ranges>
#include <stdexcept>
#include <string_view>

/*
 * All fuzz operations on an iobuf object are also applied to an object of this
 * reference type. The reference type's operations are considered correct and
 * therefore it is expected that the iobuf object ends up in a similar state as
 * the reference object if the iobuf operations are implemented correctly.
 */
using reference_t = std::deque<std::string_view>;

namespace {
/*
 * Compare contents of iobuf and reference container.
 */
void check_equals(const iobuf& buf, const reference_t& ref) {
    const auto ref_size = std::ranges::fold_left(
      ref, size_t(0), [](size_t acc, std::string_view sv) {
          return acc + sv.size();
      });

    if (buf.size_bytes() != ref_size) {
        throw std::runtime_error(
          fmt::format(
            "Iobuf size {} != reference size {}", buf.size_bytes(), ref_size));
    }

    auto buf_begin = iobuf::byte_iterator(buf.cbegin(), buf.cend());
    auto buf_end = iobuf::byte_iterator(buf.cend(), buf.cend());
    auto ref_range = ref | std::views::join;

    if (!std::equal(buf_begin, buf_end, ref_range.begin())) {
        throw std::runtime_error("buf != ref");
    }

    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
    if (buf.size_bytes() > 128_KiB) {
        // Skip the string_view operator checks if the iobuf is too large to
        // linearize.
        return;
    }

    auto s = buf.linearize_to_string();
    // coverage for iobuf::operator==(std:string_view) (equal)
    if (!(buf == std::string_view(s))) {
        throw std::runtime_error(
          "Iobuf contents are not identical for "
          "string_view comparison");
    }
    // coverage for iobuf::operator==(std:string_view) (not-equal)
    if (!s.empty()) {
        s.back()++;
        if (buf == std::string_view(s)) {
            throw std::runtime_error(
              "Iobuf contents are not identical for "
              "string_view comparison");
        }
        s.back()--;
    }
    // coverage for iobuf::operator!=(std:string_view) (size difference)
    s.append("a", 1);
    if (!(buf != std::string_view(s))) {
        throw std::runtime_error(
          "Iobuf contents are identical for string_view comparison");
    }
    return;
}

struct iobuf_and_ref {
    iobuf buf;
    reference_t ref;

    void ref_push_back(std::string_view sv) {
        if (!sv.empty()) {
            ref.push_back(sv);
        }
    }

    void check() const {
        try {
            ::check_equals(buf, ref);
        } catch (const std::exception& e) {
            throw std::runtime_error(
              fmt::format("iobuf_and_ref: {}", e.what()));
        }
    }
};

/*
 * A repeatable way of generating an iobuf with many fragments from a
 * string_view.
 */
iobuf_and_ref generate_many_frag_iobuf(std::string_view v) {
    iobuf_and_ref o;
    while (!v.empty()) {
        auto frag_size = std::min<size_t>(v[0], v.size() - 1);
        v.remove_prefix(1);

        auto frag = v.substr(0, frag_size);
        o.buf.append_fragments(iobuf::from(frag));
        o.ref_push_back(frag);
        v.remove_prefix(frag_size);
    }
    o.check();
    return o;
}

/*
 * Does an unsigned byte-wise lexicographical comparison between two reference
 * objects.
 */
std::strong_ordering
compare_reference(const reference_t& a, const reference_t& b) {
    auto to_unsigned_char = std::views::transform(
      [](char c) { return static_cast<unsigned char>(c); });
    auto a_chars = a | std::views::join | to_unsigned_char;
    auto b_chars = b | std::views::join | to_unsigned_char;

    return std::lexicographical_compare_three_way(
      a_chars.begin(), a_chars.end(), b_chars.begin(), b_chars.end());
}

/*
 * Concatenates reference_t fragments into a single string.
 */
std::string reference_to_string(const reference_t& ref) {
    std::string result;
    for (const auto& sv : ref) {
        result.append(sv);
    }
    return result;
}

/*
 * Does a signed byte-wise lexicographical comparison between a reference
 * object and a string_view (matches std::string_view::operator<=>).
 */
std::strong_ordering
compare_reference_to_sv(const reference_t& a, std::string_view b) {
    auto a_str = reference_to_string(a);
    return std::string_view(a_str) <=> b;
}

/*
 * Extract a subrange [pos, pos+len) from a reference_t.
 * Mirrors iobuf::share(pos, len) semantics.
 */
reference_t reference_subrange(const reference_t& ref, size_t pos, size_t len) {
    reference_t result;
    if (len == 0) {
        return result;
    }
    size_t remaining = len;
    for (const auto& sv : ref) {
        if (pos >= sv.size()) {
            pos -= sv.size();
            continue;
        }
        auto start = pos;
        pos = 0;
        auto take = std::min(sv.size() - start, remaining);
        if (take > 0) {
            result.push_back(sv.substr(start, take));
        }
        remaining -= take;
        if (remaining == 0) {
            break;
        }
    }
    return result;
}
} // namespace

/*
 * Holds an iobuf and a reference container. When an operation (e.g. append)
 * is applied it is performed equally to the iobuf and the reference. At any
 * time consistency between the iobuf and the reference can be verified with
 * check().
 */
class iobuf_ops {
    iobuf buf;
    reference_t ref;

    // Helper to push non-empty views to reference
    // iobuf drops empty fragments, so we want to
    // drop them on the oracle as well to keep it
    // in sync.
    void ref_push_back(std::string_view sv) {
        if (!sv.empty()) {
            ref.push_back(sv);
        }
    }
    void ref_push_front(std::string_view sv) {
        if (!sv.empty()) {
            ref.push_front(sv);
        }
    }

public:
    void moves() {
        auto tmp(std::move(buf)); // move constructor
        buf = std::move(tmp);     // move assignment
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-move"
        buf = std::move(buf); // self-move assignment
#pragma clang diagnostic pop
    }

    void reserve_memory(size_t size) {
        buf.reserve_memory(size);
        // no content change to reference container
    }

    void append_char_array(std::string_view payload) {
        buf.append(payload.data(), payload.size());
        ref_push_back(payload);
    }

    void append_uint8_array(std::string_view payload) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        auto uint8y_payload = reinterpret_cast<const uint8_t*>(payload.data());
        buf.append(uint8y_payload, payload.size());
        ref_push_back(payload);
    }

    void append_temporary_buffer(std::string_view payload) {
        buf.append(ss::temporary_buffer<char>(payload.data(), payload.size()));
        ref_push_back(payload);
    }

    void append_iobuf(std::string_view payload) {
        iobuf tmp;
        tmp.append(payload.data(), payload.size());
        buf.append(std::move(tmp));
        ref_push_back(payload);
    }

    void append_fragments(std::string_view payload, bool use_small_frags) {
        /*
         * footgun alert: if you append from an iobuf share then the backing
         * memory is also shared. so if you do something like this:
         *
         *    buf.append_fragments(buf.share(0, size));
         *
         * then it can end up that if you append to `buf` that you also
         * overwrite data at the front of the buffer. this same scenario can
         * probably be created when combining ops with iobuf::share.
         */
        if (!use_small_frags) {
            iobuf tmp;
            tmp.append(payload.data(), payload.size());
            buf.append_fragments(std::move(tmp));
            ref_push_back(payload);
        } else {
            auto g_buf = generate_many_frag_iobuf(payload);
            buf.append_fragments(std::move(g_buf.buf));
            for (auto v : g_buf.ref) {
                ref.push_back(v);
            }
        }
    }

    void prepend_temporary_buffer(std::string_view payload) {
        buf.prepend(ss::temporary_buffer<char>(payload.data(), payload.size()));
        ref_push_front(payload);
    }

    void prepend_iobuf(std::string_view payload) {
        iobuf tmp;
        tmp.append(payload.data(), payload.size());
        buf.prepend(std::move(tmp));
        ref_push_front(payload);
    }

    void compare_iobufs(std::string_view v, bool use_small_frags) {
        iobuf o_buf;
        reference_t o_ref;
        if (use_small_frags) {
            auto r = generate_many_frag_iobuf(v);
            o_buf = std::move(r.buf);
            o_ref = std::move(r.ref);
        } else {
            o_buf = iobuf::from(v);
            o_ref.push_back(v);
            check_equals(o_buf, o_ref);
        }

        // Test iobuf::operator<=>
        {
            auto iobuf_res = buf <=> o_buf;
            auto ref_res = compare_reference(ref, o_ref);
            if (iobuf_res != ref_res) {
                throw std::runtime_error("(buf <=> o_buf) != (ref <=> o_ref)");
            }

            iobuf_res = o_buf <=> buf;
            ref_res = compare_reference(o_ref, ref);
            if (iobuf_res != ref_res) {
                throw std::runtime_error("(o_buf <=> buf) != (o_ref <=> ref)");
            }
        }

        // Test iobuf::operator==
        {
            auto iobuf_res = buf == o_buf;
            auto ref_res = compare_reference(ref, o_ref)
                           == std::strong_ordering::equal;
            if (iobuf_res != ref_res) {
                throw std::runtime_error("(buf == o_buf) != (ref == o_ref)");
            }

            iobuf_res = o_buf == buf;
            ref_res = compare_reference(o_ref, ref)
                      == std::strong_ordering::equal;
            if (iobuf_res != ref_res) {
                throw std::runtime_error("(o_buf == buf) != (o_ref == ref)");
            }
        }

        // Test iobuf::operator<
        {
            auto iobuf_res = buf < o_buf;
            auto ref_res = compare_reference(ref, o_ref)
                           == std::strong_ordering::less;
            vassert(iobuf_res == ref_res, "(buf < o_buf) != (ref < o_ref)");

            iobuf_res = o_buf < buf;
            ref_res = compare_reference(o_ref, ref)
                      == std::strong_ordering::less;
            vassert(iobuf_res == ref_res, "(o_buf < buf) != (o_ref < ref)");
        }
    }

    void compare_string_view(std::string_view v) {
        auto iobuf_res = buf <=> v;
        auto ref_res = compare_reference_to_sv(ref, v);
        vassert(iobuf_res == ref_res, "(buf <=> sv) != (ref <=> sv)");
    }

    void trim_front(size_t size) {
        auto old_size = buf.size_bytes();
        buf.trim_front(size);
        size = std::min(size, old_size); // clamp to available
        ref = reference_subrange(ref, size, old_size - size);
    }

    void trim_back(size_t size) {
        auto old_size = buf.size_bytes();
        buf.trim_back(size);
        size = std::min(size, old_size); // clamp to available
        ref = reference_subrange(ref, 0, old_size - size);
    }

    void clear() {
        buf.clear();
        ref.clear();
    }

    void empty_check() {
        auto ref_empty = ref.empty();
        vassert(
          buf.empty() == ref_empty,
          "buf.empty()={} != ref.empty()={}",
          buf.empty(),
          ref_empty);
    }

    void memory_usage_check() {
        // Just exercise the method
        buf.memory_usage();
    }

    void share_full() {
        auto shared = buf.share();
        check_equals(shared, ref);
    }

    void share_range(uint32_t encoded) {
        // Lower 16 bits = pos, upper 16 bits = len
        size_t pos = encoded & 0xFFFF;
        size_t len = encoded >> 16;
        auto ref_size = buf.size_bytes();
        if (pos > ref_size) {
            return; // invalid range
        }
        len = std::min(len, ref_size - pos);
        auto shared = buf.share(pos, len);
        check_equals(shared, reference_subrange(ref, pos, len));
    }

    void append_str(std::string_view payload) {
        buf.append_str(payload);
        ref_push_back(payload);
    }

    void pop_front_op() {
        if (buf.empty()) {
            return;
        }
        // Get size of first fragment before removing
        auto frag_size = buf.begin()->size();
        buf.pop_front();
        // Remove same amount from reference
        size_t to_remove = frag_size;
        while (to_remove > 0 && !ref.empty()) {
            if (ref.front().size() <= to_remove) {
                to_remove -= ref.front().size();
                ref.pop_front();
            } else {
                ref.front().remove_prefix(to_remove);
                to_remove = 0;
            }
        }
    }

    void pop_back_op() {
        if (buf.empty()) {
            return;
        }
        // Get size of last fragment before removing
        auto frag_size = buf.rbegin()->size();
        buf.pop_back();
        // Remove same amount from reference
        size_t to_remove = frag_size;
        while (to_remove > 0 && !ref.empty()) {
            if (ref.back().size() <= to_remove) {
                to_remove -= ref.back().size();
                ref.pop_back();
            } else {
                ref.back().remove_suffix(to_remove);
                to_remove = 0;
            }
        }
    }

    void tail(size_t size) {
        if (size > buf.size_bytes()) {
            // tail() throws if size > buffer size, skip invalid ops
            return;
        }
        auto tail_buf = buf.tail(size);
        check_equals(
          tail_buf, reference_subrange(ref, buf.size_bytes() - size, size));
    }

    void hexdump(size_t size) {
        // clamp hexdump to 100 chars for speed
        auto limit = std::min(100uz, size);
        auto s = buf.hexdump(limit);
        vassert(
          s.size() <= limit * 6 + 80,
          "oversized: {} limit {} hex {}",
          s.size(),
          limit,
          s);
    }

    void print() {
        std::stringstream ss;
        ss << buf;
    }

    void iobuf_as_scattered() {
        auto s = ::iobuf_as_scattered(buf.share(0, buf.size_bytes()));
        auto p = std::move(s).release();
        iobuf tmp;
        for (auto& t : p.release()) {
            tmp.append(std::move(t));
        }
        if (tmp != buf) {
            throw std::runtime_error(
              "Iobuf as scattered message doesn't match original data");
        }
    }

    /*
     * covers
     *   iobuf::copy
     *   iobuf_copy (free function)
     *   iobuf::operator==(iobuf)
     *   iobuf::operator!=(iobuf)
     */
    void copy() const {
        auto iob0 = buf.copy();
        if (!(iob0 == buf)) {
            throw std::runtime_error(
              fmt::format(
                "Iobuf (sz={}) copy (sz={}) not identical (iobuf::copy)",
                buf.size_bytes(),
                iob0.size_bytes()));
        }

        auto it = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
        auto iob1 = iobuf_copy(it, buf.size_bytes());
        if (iob1 != buf) {
            throw std::runtime_error(
              fmt::format(
                "Iobuf (sz={}) copy (sz={}) not identical (iobuf_copy)",
                buf.size_bytes(),
                iob1.size_bytes()));
        }

        iob0.append("a", 1);
        if (iob0 == iob1) {
            throw std::runtime_error(
              "Iobufs compare equal with differing sizes");
        }

        iob1.append("b", 1);
        if (iob0 == iob1) {
            throw std::runtime_error(
              "Iobufs compare equal with differing final byte");
        }
    }

    /*
     * Check consistency of iobuf with reference.
     */
    void check() const {
        try {
            ::check_equals(buf, ref);
        } catch (const std::exception& e) {
            throw std::runtime_error(fmt::format("iobuf_ops: {}", e.what()));
        }
    }
};

enum class op_type : uint8_t {
    copy,
    moves,
    trim_front,
    trim_back,
    clear,
    print,
    iobuf_as_scattered,
    hexdump,
    reserve_memory,
    compare,
    compare_small_fragments,
    prepend_temporary_buffer,
    prepend_iobuf,
    append_iobuf,
    append_temporary_buffer,
    append_fragments,
    append_small_fragments,
    append_uint8_array,
    append_char_array,
    tail,
    empty_check,
    memory_usage,
    share_full,
    share_range,
    append_str,
    pop_front_op,
    pop_back_op,
    compare_string_view,
    max = compare_string_view,
};

template<>
struct fmt::formatter<op_type> : formatter<std::string_view> {
    auto format(op_type t, format_context& ctx) const {
        auto name = [](auto t) {
            switch (t) {
            case op_type::compare:
                return "compare";
            case op_type::compare_small_fragments:
                return "compare_small_fragments";
            case op_type::copy:
                return "copy";
            case op_type::append_fragments:
                return "append_fragments";
            case op_type::append_small_fragments:
                return "append_small_fragments";
            case op_type::moves:
                return "moves";
            case op_type::clear:
                return "clear";
            case op_type::print:
                return "print";
            case op_type::iobuf_as_scattered:
                return "iobuf_as_scattered";
            case op_type::hexdump:
                return "hexdump";
            case op_type::trim_front:
                return "trim_front";
            case op_type::trim_back:
                return "trim_back";
            case op_type::prepend_iobuf:
                return "prepend_iobuf";
            case op_type::reserve_memory:
                return "reserve_memory";
            case op_type::prepend_temporary_buffer:
                return "prepend_temporary_buffer";
            case op_type::append_iobuf:
                return "append_iobuf";
            case op_type::append_temporary_buffer:
                return "append_temporary_buffer";
            case op_type::append_uint8_array:
                return "append_uint8_array";
            case op_type::append_char_array:
                return "append_char_array";
            case op_type::tail:
                return "tail";
            case op_type::empty_check:
                return "empty_check";
            case op_type::memory_usage:
                return "memory_usage";
            case op_type::share_full:
                return "share_full";
            case op_type::share_range:
                return "share_range";
            case op_type::append_str:
                return "append_str";
            case op_type::pop_front_op:
                return "pop_front_op";
            case op_type::pop_back_op:
                return "pop_back_op";
            case op_type::compare_string_view:
                return "compare_string_view";
            }
        }(t);
        return formatter<std::string_view>::format(name, ctx);
    }
};

/*
 * a program is a series of bytes. the structure of a program is:
 *
 *   [{op_code[, operands...]}*]
 *
 * where the set of operands are dependent on the op_code. the program ends
 * when the end of the program is reached. if the operands for the final
 * op_code are truncated then the program should terminate normally.
 */
class driver {
public:
    explicit driver(std::string_view program)
      : program_(program)
      , pc_(program.cbegin()) {}

    void print_trace() const {
        for (auto op : trace_) {
            fmt::print("TRACED OP: {}\n", op);
        }
    }

    bool operator()() {
        const auto op = [this]() -> std::optional<op_spec> {
            try {
                return next();
            } catch (const end_of_program&) {
                return std::nullopt;
            }
        }();

        if (op.has_value()) {
            handle_op(op.value());
            return true;
        }

        return false;
    }

    void check() const { m_.check(); }

private:
    struct op_spec {
        op_type op;
        std::optional<std::string_view::difference_type> size;
        std::string_view data;

        explicit op_spec(op_type op)
          : op(op) {}

        friend std::ostream& operator<<(std::ostream& os, const op_spec& op) {
            fmt::print(
              os,
              "{}(size={}, data.size={})",
              op.op,
              (op.size.has_value() ? fmt::format("{}", *op.size) : "null"),
              op.data.size());
            return os;
        }
    };

    void handle_op(op_spec op) {
        switch (op.op) {
        case op_type::copy:
            m_.copy();
            return;

        case op_type::moves:
            m_.moves();
            return;

        case op_type::clear:
            m_.clear();
            return;

        case op_type::print:
            m_.print();
            return;

        case op_type::iobuf_as_scattered:
            m_.iobuf_as_scattered();
            return;

        case op_type::empty_check:
            m_.empty_check();
            return;

        case op_type::memory_usage:
            m_.memory_usage_check();
            return;

        case op_type::share_full:
            m_.share_full();
            return;

        case op_type::pop_front_op:
            m_.pop_front_op();
            return;

        case op_type::pop_back_op:
            m_.pop_back_op();
            return;

        default:
            break;
        }

        vassert(op.size.has_value(), "Op {} requires size operands", op.op);

        switch (op.op) {
        case op_type::compare:
            m_.compare_iobufs(op.data, false);
            return;

        case op_type::compare_small_fragments:
            m_.compare_iobufs(op.data, true);
            return;

        case op_type::append_fragments:
            m_.append_fragments(op.data, false);
            return;

        case op_type::append_small_fragments:
            m_.append_fragments(op.data, true);
            return;

        case op_type::trim_front:
            m_.trim_front(*op.size);
            return;

        case op_type::trim_back:
            m_.trim_back(*op.size);
            return;

        case op_type::hexdump:
            m_.hexdump(*op.size);
            return;

        case op_type::tail:
            m_.tail(*op.size);
            return;

        case op_type::prepend_iobuf:
            m_.prepend_iobuf(op.data);
            return;

        case op_type::reserve_memory:
            m_.reserve_memory(*op.size);
            return;

        case op_type::prepend_temporary_buffer:
            m_.prepend_temporary_buffer(op.data);
            return;

        case op_type::append_iobuf:
            m_.append_iobuf(op.data);
            return;

        case op_type::append_temporary_buffer:
            m_.append_temporary_buffer(op.data);
            return;

        case op_type::append_uint8_array:
            m_.append_uint8_array(op.data);
            return;

        case op_type::append_char_array:
            m_.append_char_array(op.data);
            return;

        case op_type::share_range:
            m_.share_range(static_cast<uint32_t>(*op.size));
            return;

        case op_type::append_str:
            m_.append_str(op.data);
            return;

        case op_type::compare_string_view:
            m_.compare_string_view(op.data);
            return;

        default:
            vunreachable("Caught wild op {}", op.op);
        }
    }

    // signal that the program should terminate normally
    class end_of_program : std::exception {};

    template<typename T>
    T read() {
        if (std::cmp_less(std::distance(pc_, program_.cend()), sizeof(T))) {
            throw end_of_program();
        }
        T ret;
        std::memcpy(&ret, pc_, sizeof(T));
        std::advance(pc_, sizeof(T));
        return ret;
    }

    op_spec next() {
        // next byte is the op code
        using underlying = std::underlying_type_t<op_type>;
        const auto max_op = static_cast<underlying>(op_type::max);
        auto& op = trace_.emplace_back(
          static_cast<op_type>(read<underlying>() % (max_op + 1)));

        // size operand
        switch (op.op) {
        case op_type::compare:
        case op_type::compare_small_fragments:
        case op_type::trim_front:
        case op_type::trim_back:
        case op_type::prepend_iobuf:
        case op_type::reserve_memory:
        case op_type::hexdump:
        case op_type::tail:
        case op_type::prepend_temporary_buffer:
        case op_type::append_fragments:
        case op_type::append_small_fragments:
        case op_type::append_iobuf:
        case op_type::append_temporary_buffer:
        case op_type::append_uint8_array:
        case op_type::append_char_array:
        case op_type::share_range:
        case op_type::append_str:
        case op_type::compare_string_view: {
            op.size = read<uint32_t>();
            break;
        }
        default:
            break;
        }

        // data operand
        switch (op.op) {
        case op_type::compare:
        case op_type::compare_small_fragments:
        case op_type::append_fragments:
        case op_type::append_small_fragments:
        case op_type::prepend_iobuf:
        case op_type::prepend_temporary_buffer:
        case op_type::append_iobuf:
        case op_type::append_temporary_buffer:
        case op_type::append_uint8_array:
        case op_type::append_char_array:
        case op_type::append_str:
        case op_type::compare_string_view:
            vassert(op.size.has_value(), "");
            op.data = {
              pc_,
              std::min<size_t>(*op.size, std::distance(pc_, program_.end()))};
            op.size = op.data.size();
            std::advance(pc_, op.data.size());
            break;

        default:
            break;
        }

        return op;
    }

    const std::string_view program_;
    std::string_view::iterator pc_;
    std::vector<op_spec> trace_;
    iobuf_ops m_;
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < sizeof(std::underlying_type_t<op_type>)) {
        return 0;
    }

    // NOLINTNEXTLINE
    std::string_view d(reinterpret_cast<const char*>(data), size);
    driver p(d);

    try {
        while (p()) {
            p.check();
        }
    } catch (...) {
        p.print_trace();
        throw;
    }

    return 0;
}
