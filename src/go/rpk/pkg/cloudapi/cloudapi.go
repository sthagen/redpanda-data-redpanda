// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package cloudapi provides a client to talk to the Redpanda Cloud API.
package cloudapi

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
)

// ProdURL is the hostname of the Redpanda API.
const ProdURL = "https://cloud-api.prd.cloud.redpanda.com"

// Client talks to the cloud API.
type Client struct {
	cl *httpapi.Client
}

// NewClient initializes and returns a client for talking to the cloud API.
// If the host is empty, this defaults to the prod API host.
func NewClient(host, authToken string, hopts ...httpapi.Opt) *Client {
	if host == "" {
		host = ProdURL
	}
	opts := []httpapi.Opt{
		httpapi.Host(host),
		httpapi.BearerAuth(authToken),
	}
	opts = append(opts, hopts...)
	return &Client{cl: httpapi.NewClient(opts...)}
}

// NameID is a common type used in many endpoints / many structs.
type NameID struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}
