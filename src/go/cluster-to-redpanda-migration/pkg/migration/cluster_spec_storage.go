// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package migration

import (
	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	"k8s.io/utils/pointer"
)

var defaultStorageSize = "100Gi"

func migrateStorage(oldStorage *vectorizedv1alpha1.StorageSpec) *v1alpha1.Storage {
	// since storage of the clusterSpec cannot be nil even though it can be empty, we must make assumption of contents

	var rpStorage *v1alpha1.Storage
	if !oldStorage.Capacity.IsZero() {

		rpStorage = &v1alpha1.Storage{}
		rpStorage.PersistentVolume = &v1alpha1.PersistentVolume{
			Enabled: pointer.Bool(true),
			Size:    pointer.String(oldStorage.Capacity.String()),
		}

		if oldStorage.StorageClassName != "" {
			rpStorage.PersistentVolume.StorageClass = pointer.String(oldStorage.StorageClassName)
		}

	} else {
		rpStorage = &v1alpha1.Storage{}
		rpStorage.PersistentVolume = &v1alpha1.PersistentVolume{
			Enabled: pointer.Bool(true),
			Size:    pointer.String(defaultStorageSize),
		}
	}

	return rpStorage
}

func migrateCloudStorage(oldCloudStorage *vectorizedv1alpha1.CloudStorageConfig, rp *v1alpha1.Redpanda) {
}
