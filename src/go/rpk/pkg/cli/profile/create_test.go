package profile

import (
	"testing"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	"github.com/stretchr/testify/require"
)

func TestCombineClusterNames(t *testing.T) {
	tests := []struct {
		name string
		rgs  []*controlplanev1.ResourceGroup
		scs  []*controlplanev1.ServerlessCluster
		cs   []*controlplanev1.Cluster
		exp  namesAndClusters
	}{
		{
			name: "combine Serverless Clusters and Clusters",
			rgs: []*controlplanev1.ResourceGroup{
				{Id: "rg1", Name: "ResourceGroup1"},
				{Id: "rg2", Name: "ResourceGroup2"},
			},
			scs: []*controlplanev1.ServerlessCluster{
				{ResourceGroupId: "rg1", Name: "SC1", State: controlplanev1.ServerlessCluster_STATE_READY},
				{ResourceGroupId: "rg2", Name: "SC2", State: controlplanev1.ServerlessCluster_STATE_READY},
				{ResourceGroupId: "rg1", Name: "SC3", State: controlplanev1.ServerlessCluster_STATE_CREATING}, // should not appear if it's not ready.
			},
			cs: []*controlplanev1.Cluster{
				{ResourceGroupId: "rg1", Name: "C1", State: controlplanev1.Cluster_STATE_READY},
				{ResourceGroupId: "rg2", Name: "C2", State: controlplanev1.Cluster_STATE_DELETING}, // should not appear if it's not ready.
				{ResourceGroupId: "rg2", Name: "C3", State: controlplanev1.Cluster_STATE_READY},
			},
			exp: namesAndClusters{
				{name: "ResourceGroup1/SC1", sc: &controlplanev1.ServerlessCluster{ResourceGroupId: "rg1", Name: "SC1", State: controlplanev1.ServerlessCluster_STATE_READY}},
				{name: "ResourceGroup2/SC2", sc: &controlplanev1.ServerlessCluster{ResourceGroupId: "rg2", Name: "SC2", State: controlplanev1.ServerlessCluster_STATE_READY}},
				{name: "ResourceGroup1/C1", c: &controlplanev1.Cluster{ResourceGroupId: "rg1", Name: "C1", State: controlplanev1.Cluster_STATE_READY}},
				{name: "ResourceGroup2/C3", c: &controlplanev1.Cluster{ResourceGroupId: "rg2", Name: "C3", State: controlplanev1.Cluster_STATE_READY}},
			},
		},
		{
			name: "empty inputs",
			rgs:  []*controlplanev1.ResourceGroup{},
			scs:  []*controlplanev1.ServerlessCluster{},
			cs:   []*controlplanev1.Cluster{},
			exp:  nil,
		},
		{
			name: "nil inputs",
			rgs:  nil,
			scs:  nil,
			cs:   nil,
			exp:  nil,
		},
		{
			name: "Serverless Clusters only",
			rgs: []*controlplanev1.ResourceGroup{
				{Id: "rg1", Name: "ResourceGroup1"},
			},
			scs: []*controlplanev1.ServerlessCluster{
				{ResourceGroupId: "rg1", Name: "SC1", State: controlplanev1.ServerlessCluster_STATE_READY},
			},
			cs: []*controlplanev1.Cluster{},
			exp: namesAndClusters{
				{name: "ResourceGroup1/SC1", sc: &controlplanev1.ServerlessCluster{ResourceGroupId: "rg1", Name: "SC1", State: controlplanev1.ServerlessCluster_STATE_READY}},
			},
		},
		{
			name: "Clusters only",
			rgs: []*controlplanev1.ResourceGroup{
				{Id: "rg1", Name: "ResourceGroup1"},
			},
			scs: []*controlplanev1.ServerlessCluster{},
			cs: []*controlplanev1.Cluster{
				{ResourceGroupId: "rg1", Name: "C1", State: controlplanev1.Cluster_STATE_READY},
			},
			exp: namesAndClusters{
				{name: "ResourceGroup1/C1", c: &controlplanev1.Cluster{ResourceGroupId: "rg1", Name: "C1", State: controlplanev1.Cluster_STATE_READY}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := combineClusterNames(tt.rgs, tt.scs, tt.cs)
			require.Equal(t, tt.exp, result)
		})
	}
}
