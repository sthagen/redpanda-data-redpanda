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

#include "crypto/exceptions.h"
#include "crypto/ossl_context_service.h"
#include "ossl_context_service_test_base.h"

class ossl_context_test_no_env_framework
  : public ossl_context_base_test_framework {
public:
    ss::future<> SetUpAsync() override {
        if (char* conf = ::getenv("OPENSSL_CONF"); conf != nullptr) {
            orig_conf_file = conf;
        }
        std::cout << "orig_conf_file: " << orig_conf_file << std::endl;
        ::setenv("OPENSSL_CONF", "", 1);
        co_await ossl_context_base_test_framework::SetUpAsync();
    }

    ss::future<> TearDownAsync() override {
        co_await ossl_context_base_test_framework::TearDownAsync();
        if (orig_conf_file.empty()) {
            ::unsetenv("OPENSSL_CONF");
        } else {
            ::setenv("OPENSSL_CONF", orig_conf_file.c_str(), 1);
        }
    }

    ss::sstring module_dir() const {
        auto module_dir = test_utils::get_runfile_path("src/v/crypto/tests");
        return ss::sstring{module_dir};
    }

protected:
    std::string orig_conf_file;
};

TEST_F_CORO(ossl_context_test_no_env_framework, fips_mode) {
#ifndef FIPS_MODULE_REQUIRED
    auto fips_mod_present = co_await fips_module_present();
    if (!fips_mod_present) {
        GTEST_SKIP_CORO()
          << "Skipping FIPS failure test because module is not present";
    }
#endif
    ss::sharded<crypto::ossl_context_service> svc;
    co_await svc.start(
      std::ref(*thread_worker()),
      get_config_file_path(),
      module_dir(),
      crypto::is_fips_mode::yes);

    EXPECT_THROW(
      co_await svc.invoke_on_all(&crypto::ossl_context_service::start),
      crypto::exception);

    co_await svc.stop();
}

TEST_F_CORO(ossl_context_test_no_env_framework, non_fips_mode) {
    ss::sharded<crypto::ossl_context_service> svc;
    co_await svc.start(
      std::ref(*thread_worker()),
      get_config_file_path(),
      module_dir(),
      crypto::is_fips_mode::no);

    EXPECT_NO_THROW(
      co_await svc.invoke_on_all(&crypto::ossl_context_service::start));

    co_await svc.stop();
}
