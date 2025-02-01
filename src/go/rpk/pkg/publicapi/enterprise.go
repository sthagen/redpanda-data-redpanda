// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package publicapi

import (
	"net/http"
	"time"

	"buf.build/gen/go/redpandadata/gatekeeper/connectrpc/go/redpanda/api/gatekeeper/v1alpha1/gatekeeperv1alpha1connect"
	"connectrpc.com/connect"
)

// EnterpriseClientSet holds the respective service clients to interact with
// the enterprise endpoints of the Public API.
type EnterpriseClientSet struct {
	Gatekeeper gatekeeperv1alpha1connect.EnterpriseServiceClient
}

// NewEnterpriseClientSet creates a Public API client set with the service
// clients of each resource available to interact with this package.
func NewEnterpriseClientSet(host string, opts ...connect.ClientOption) *EnterpriseClientSet {
	if host == "" {
		host = ControlPlaneProdURL
	}
	opts = append([]connect.ClientOption{
		connect.WithInterceptors(
			newLoggerInterceptor(), // Add logs to every request.
		),
	}, opts...)

	httpCl := &http.Client{Timeout: 30 * time.Second}

	return &EnterpriseClientSet{
		Gatekeeper: gatekeeperv1alpha1connect.NewEnterpriseServiceClient(httpCl, host, opts...),
	}
}
