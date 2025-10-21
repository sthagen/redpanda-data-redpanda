// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/common"
	"google.golang.org/protobuf/types/known/durationpb"
)

func shadowLinkConfigToProto(slCfg *ShadowLinkConfig) *adminv2.ShadowLink {
	if slCfg == nil {
		return nil
	}

	shadowLink := &adminv2.ShadowLink{
		Name: slCfg.Name,
		// uid and status are output-only fields, not included in create request
	}

	shadowLink.Configurations = &adminv2.ShadowLinkConfigurations{
		ClientOptions:             mapClientOptions(slCfg.ClientOptions),
		TopicMetadataSyncOptions:  mapTopicMetadataSyncOptions(slCfg.TopicMetadataSyncOptions),
		ConsumerOffsetSyncOptions: mapConsumerOffsetSyncOptions(slCfg.ConsumerOffsetSyncOptions),
		SecuritySyncOptions:       mapSecuritySyncOptions(slCfg.SecuritySyncOptions),
	}
	return shadowLink
}

func mapClientOptions(opts *ShadowLinkClientOptions) *adminv2.ShadowLinkClientOptions {
	if opts == nil {
		return nil
	}

	pbOpts := &adminv2.ShadowLinkClientOptions{
		BootstrapServers:    opts.BootstrapServers,
		SourceClusterId:     opts.SourceClusterID,
		MetadataMaxAgeMs:    opts.MetadataMaxAgeMs,
		ConnectionTimeoutMs: opts.ConnectionTimeoutMs,
		RetryBackoffMs:      opts.RetryBackoffMs,
		FetchWaitMaxMs:      opts.FetchWaitMaxMs,
		FetchMinBytes:       opts.FetchMinBytes,
		FetchMaxBytes:       opts.FetchMaxBytes,
		// client_id is output-only in the protobuf, so we don't set it.
	}

	if opts.TLSSettings != nil {
		pbOpts.TlsSettings = mapTLSSettings(opts.TLSSettings)
	}

	if opts.AuthenticationConfiguration != nil {
		pbOpts.AuthenticationConfiguration = mapAuthenticationConfiguration(opts.AuthenticationConfiguration)
	}

	return pbOpts
}

func mapTLSSettings(tls TLSSettings) *adminv2.TLSSettings {
	if tls == nil {
		return nil
	}

	pbTLS := &adminv2.TLSSettings{}

	switch t := tls.(type) {
	case *TLSFileSettings:
		pbTLS.Enabled = t.Enabled
		pbTLS.TlsSettings = &adminv2.TLSSettings_TlsFileSettings{
			TlsFileSettings: &adminv2.TLSFileSettings{
				CaPath:   t.CAPath,
				KeyPath:  t.KeyPath,
				CertPath: t.CertPath,
			},
		}
	case *TLSPEMSettings:
		pbTLS.Enabled = t.Enabled
		pbTLS.TlsSettings = &adminv2.TLSSettings_TlsPemSettings{
			TlsPemSettings: &adminv2.TLSPEMSettings{
				Ca:   t.CA,
				Key:  t.Key,
				Cert: t.Cert,
				// key_fingerprint is output-only
			},
		}
	}

	return pbTLS
}

func mapAuthenticationConfiguration(auth AuthenticationConfiguration) *adminv2.AuthenticationConfiguration {
	if auth == nil {
		return nil
	}

	pbAuth := &adminv2.AuthenticationConfiguration{}

	if a, ok := auth.(*ScramConfig); ok {
		pbAuth.Authentication = &adminv2.AuthenticationConfiguration_ScramConfiguration{
			ScramConfiguration: &adminv2.ScramConfig{
				Username:       a.Username,
				Password:       a.Password,
				ScramMechanism: mapScramMechanism(a.ScramMechanism),
				// password_set and password_set_at are output-only
			},
		}
	}

	return pbAuth
}

func mapScramMechanism(m ScramMechanism) adminv2.ScramMechanism {
	switch m {
	case ScramMechanismScramSha256:
		return adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_256
	case ScramMechanismScramSha512:
		return adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_512
	default:
		return adminv2.ScramMechanism_SCRAM_MECHANISM_UNSPECIFIED
	}
}

func mapTopicMetadataSyncOptions(opts *TopicMetadataSyncOptions) *adminv2.TopicMetadataSyncOptions {
	if opts == nil {
		return nil
	}

	pbOpts := &adminv2.TopicMetadataSyncOptions{
		SyncedShadowTopicProperties: opts.SyncedShadowTopicProperties,
		ExcludeDefault:              opts.ExcludeDefault,
	}

	if opts.Interval > 0 {
		pbOpts.Interval = durationpb.New(opts.Interval)
	}

	for _, filter := range opts.AutoCreateShadowTopicFilters {
		pbOpts.AutoCreateShadowTopicFilters = append(pbOpts.AutoCreateShadowTopicFilters, mapNameFilter(filter))
	}

	return pbOpts
}

func mapConsumerOffsetSyncOptions(opts *ConsumerOffsetSyncOptions) *adminv2.ConsumerOffsetSyncOptions {
	if opts == nil {
		return nil
	}

	pbOpts := &adminv2.ConsumerOffsetSyncOptions{
		Enabled: opts.Enabled,
	}

	if opts.Interval > 0 {
		pbOpts.Interval = durationpb.New(opts.Interval)
	}

	for _, filter := range opts.GroupFilters {
		pbOpts.GroupFilters = append(pbOpts.GroupFilters, mapNameFilter(filter))
	}

	return pbOpts
}

func mapSecuritySyncOptions(opts *SecuritySettingsSyncOptions) *adminv2.SecuritySettingsSyncOptions {
	if opts == nil {
		return nil
	}

	pbOpts := &adminv2.SecuritySettingsSyncOptions{
		Enabled: opts.Enabled,
	}

	if opts.Interval > 0 {
		pbOpts.Interval = durationpb.New(opts.Interval)
	}

	for _, filter := range opts.ACLFilters {
		pbOpts.AclFilters = append(pbOpts.AclFilters, mapACLFilter(filter))
	}

	return pbOpts
}

func mapNameFilter(filter *NameFilter) *adminv2.NameFilter {
	if filter == nil {
		return nil
	}

	return &adminv2.NameFilter{
		PatternType: mapPatternType(filter.PatternType),
		FilterType:  mapFilterType(filter.FilterType),
		Name:        filter.Name,
	}
}

func mapACLFilter(filter *ACLFilter) *adminv2.ACLFilter {
	if filter == nil {
		return nil
	}

	return &adminv2.ACLFilter{
		ResourceFilter: mapACLResourceFilter(filter.ResourceFilter),
		AccessFilter:   mapACLAccessFilter(filter.AccessFilter),
	}
}

func mapACLResourceFilter(filter *ACLResourceFilter) *adminv2.ACLResourceFilter {
	if filter == nil {
		return nil
	}

	return &adminv2.ACLResourceFilter{
		ResourceType: mapACLResource(filter.ResourceType),
		PatternType:  mapACLPattern(filter.PatternType),
		Name:         filter.Name,
	}
}

func mapACLAccessFilter(filter *ACLAccessFilter) *adminv2.ACLAccessFilter {
	if filter == nil {
		return nil
	}

	return &adminv2.ACLAccessFilter{
		Principal:      filter.Principal,
		Operation:      mapACLOperation(filter.Operation),
		PermissionType: mapACLPermissionType(filter.PermissionType),
		Host:           filter.Host,
	}
}

func mapPatternType(pt PatternType) adminv2.PatternType {
	switch pt {
	case PatternTypeLiteral:
		return adminv2.PatternType_PATTERN_TYPE_LITERAL
	case PatternTypePrefix:
		return adminv2.PatternType_PATTERN_TYPE_PREFIX
	default:
		return adminv2.PatternType_PATTERN_TYPE_UNSPECIFIED
	}
}

func mapFilterType(ft FilterType) adminv2.FilterType {
	switch ft {
	case FilterTypeInclude:
		return adminv2.FilterType_FILTER_TYPE_INCLUDE
	case FilterTypeExclude:
		return adminv2.FilterType_FILTER_TYPE_EXCLUDE
	default:
		return adminv2.FilterType_FILTER_TYPE_UNSPECIFIED
	}
}

func mapACLResource(resource ACLResource) common.ACLResource {
	switch resource {
	case ACLResourceAny:
		return common.ACLResource_ACL_RESOURCE_ANY
	case ACLResourceCluster:
		return common.ACLResource_ACL_RESOURCE_CLUSTER
	case ACLResourceGroup:
		return common.ACLResource_ACL_RESOURCE_GROUP
	case ACLResourceTopic:
		return common.ACLResource_ACL_RESOURCE_TOPIC
	case ACLResourceTXNID:
		return common.ACLResource_ACL_RESOURCE_TXN_ID
	case ACLResourceSRSubject:
		return common.ACLResource_ACL_RESOURCE_SR_SUBJECT
	case ACLResourceSRRegistry:
		return common.ACLResource_ACL_RESOURCE_SR_REGISTRY
	case ACLResourceSRAny:
		return common.ACLResource_ACL_RESOURCE_SR_ANY
	default:
		return common.ACLResource_ACL_RESOURCE_UNSPECIFIED
	}
}

func mapACLPattern(pattern ACLPattern) common.ACLPattern {
	switch pattern {
	case ACLPatternAny:
		return common.ACLPattern_ACL_PATTERN_ANY
	case ACLPatternLiteral:
		return common.ACLPattern_ACL_PATTERN_LITERAL
	case ACLPatternPrefixed:
		return common.ACLPattern_ACL_PATTERN_PREFIXED
	case ACLPatternMatch:
		return common.ACLPattern_ACL_PATTERN_MATCH
	default:
		return common.ACLPattern_ACL_PATTERN_UNSPECIFIED
	}
}

func mapACLOperation(operation ACLOperation) common.ACLOperation {
	switch operation {
	case ACLOperationAny:
		return common.ACLOperation_ACL_OPERATION_ANY
	case ACLOperationRead:
		return common.ACLOperation_ACL_OPERATION_READ
	case ACLOperationWrite:
		return common.ACLOperation_ACL_OPERATION_WRITE
	case ACLOperationCreate:
		return common.ACLOperation_ACL_OPERATION_CREATE
	case ACLOperationRemove:
		return common.ACLOperation_ACL_OPERATION_REMOVE
	case ACLOperationAlter:
		return common.ACLOperation_ACL_OPERATION_ALTER
	case ACLOperationDescribe:
		return common.ACLOperation_ACL_OPERATION_DESCRIBE
	case ACLOperationClusterAction:
		return common.ACLOperation_ACL_OPERATION_CLUSTER_ACTION
	case ACLOperationDescribeConfigs:
		return common.ACLOperation_ACL_OPERATION_DESCRIBE_CONFIGS
	case ACLOperationAlterConfigs:
		return common.ACLOperation_ACL_OPERATION_ALTER_CONFIGS
	case ACLOperationIdempotentWrite:
		return common.ACLOperation_ACL_OPERATION_IDEMPOTENT_WRITE
	default:
		return common.ACLOperation_ACL_OPERATION_UNSPECIFIED
	}
}

func mapACLPermissionType(permType ACLPermissionType) common.ACLPermissionType {
	switch permType {
	case ACLPermissionTypeAny:
		return common.ACLPermissionType_ACL_PERMISSION_TYPE_ANY
	case ACLPermissionTypeAllow:
		return common.ACLPermissionType_ACL_PERMISSION_TYPE_ALLOW
	case ACLPermissionTypeDeny:
		return common.ACLPermissionType_ACL_PERMISSION_TYPE_DENY
	default:
		return common.ACLPermissionType_ACL_PERMISSION_TYPE_UNSPECIFIED
	}
}

func shadowLinkToConfig(sl *adminv2.ShadowLink) *ShadowLinkConfig {
	if sl == nil {
		return nil
	}

	cfg := &ShadowLinkConfig{
		Name: sl.Name,
		// uid and status are output-only fields, not included in config
	}

	if sl.GetConfigurations() != nil {
		cfg.ClientOptions = adminClientOptsToCfg(sl.GetConfigurations().GetClientOptions())
		cfg.TopicMetadataSyncOptions = adminTopicMetadataSyncToCfg(sl.GetConfigurations().GetTopicMetadataSyncOptions())
		cfg.ConsumerOffsetSyncOptions = adminConsumerOffsetSyncToCfg(sl.GetConfigurations().GetConsumerOffsetSyncOptions())
		cfg.SecuritySyncOptions = adminSecuritySyncToCfg(sl.GetConfigurations().GetSecuritySyncOptions())
	}

	return cfg
}

func adminClientOptsToCfg(opts *adminv2.ShadowLinkClientOptions) *ShadowLinkClientOptions {
	if opts == nil {
		return nil
	}

	cfg := &ShadowLinkClientOptions{
		BootstrapServers:    opts.GetBootstrapServers(),
		SourceClusterID:     opts.GetSourceClusterId(),
		MetadataMaxAgeMs:    opts.GetMetadataMaxAgeMs(),
		ConnectionTimeoutMs: opts.GetConnectionTimeoutMs(),
		RetryBackoffMs:      opts.GetRetryBackoffMs(),
		FetchWaitMaxMs:      opts.GetFetchWaitMaxMs(),
		FetchMinBytes:       opts.GetFetchMinBytes(),
		FetchMaxBytes:       opts.GetFetchMaxBytes(),
	}

	if opts.GetTlsSettings() != nil {
		cfg.TLSSettings = adminTLSToCfg(opts.GetTlsSettings())
	}

	if opts.GetAuthenticationConfiguration() != nil {
		cfg.AuthenticationConfiguration = adminAuthToCfg(opts.GetAuthenticationConfiguration())
	}

	return cfg
}

func adminTLSToCfg(tls *adminv2.TLSSettings) TLSSettings {
	if tls == nil {
		return nil
	}

	switch t := tls.GetTlsSettings().(type) {
	case *adminv2.TLSSettings_TlsFileSettings:
		if t.TlsFileSettings == nil {
			return nil
		}
		return &TLSFileSettings{
			Enabled:  tls.GetEnabled(),
			CAPath:   t.TlsFileSettings.GetCaPath(),
			KeyPath:  t.TlsFileSettings.GetKeyPath(),
			CertPath: t.TlsFileSettings.GetCertPath(),
		}
	case *adminv2.TLSSettings_TlsPemSettings:
		if t.TlsPemSettings == nil {
			return nil
		}
		return &TLSPEMSettings{
			Enabled: tls.GetEnabled(),
			CA:      t.TlsPemSettings.GetCa(),
			Key:     t.TlsPemSettings.GetKey(),
			Cert:    t.TlsPemSettings.GetCert(),
		}
	}

	return nil
}

func adminAuthToCfg(auth *adminv2.AuthenticationConfiguration) AuthenticationConfiguration {
	if auth == nil {
		return nil
	}

	if a, ok := auth.GetAuthentication().(*adminv2.AuthenticationConfiguration_ScramConfiguration); ok {
		if a.ScramConfiguration == nil {
			return nil
		}
		return &ScramConfig{
			Username:       a.ScramConfiguration.GetUsername(),
			Password:       a.ScramConfiguration.GetPassword(),
			ScramMechanism: adminScramMechanismToCfg(a.ScramConfiguration.GetScramMechanism()),
		}
	}

	return nil
}

func adminScramMechanismToCfg(m adminv2.ScramMechanism) ScramMechanism {
	switch m {
	case adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_256:
		return ScramMechanismScramSha256
	case adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_512:
		return ScramMechanismScramSha512
	default:
		return ""
	}
}

func adminTopicMetadataSyncToCfg(opts *adminv2.TopicMetadataSyncOptions) *TopicMetadataSyncOptions {
	if opts == nil {
		return nil
	}

	cfg := &TopicMetadataSyncOptions{
		SyncedShadowTopicProperties: opts.GetSyncedShadowTopicProperties(),
		ExcludeDefault:              opts.GetExcludeDefault(),
	}

	if opts.GetInterval() != nil {
		cfg.Interval = opts.GetInterval().AsDuration()
	}

	for _, filter := range opts.GetAutoCreateShadowTopicFilters() {
		cfg.AutoCreateShadowTopicFilters = append(cfg.AutoCreateShadowTopicFilters, adminMapFilterToCfg(filter))
	}

	return cfg
}

func adminConsumerOffsetSyncToCfg(opts *adminv2.ConsumerOffsetSyncOptions) *ConsumerOffsetSyncOptions {
	if opts == nil {
		return nil
	}

	cfg := &ConsumerOffsetSyncOptions{
		Enabled: opts.GetEnabled(),
	}

	if opts.GetInterval() != nil {
		cfg.Interval = opts.GetInterval().AsDuration()
	}

	for _, filter := range opts.GetGroupFilters() {
		cfg.GroupFilters = append(cfg.GroupFilters, adminMapFilterToCfg(filter))
	}

	return cfg
}

func adminSecuritySyncToCfg(opts *adminv2.SecuritySettingsSyncOptions) *SecuritySettingsSyncOptions {
	if opts == nil {
		return nil
	}

	cfg := &SecuritySettingsSyncOptions{
		Enabled: opts.GetEnabled(),
	}

	if opts.GetInterval() != nil {
		cfg.Interval = opts.GetInterval().AsDuration()
	}

	for _, filter := range opts.GetAclFilters() {
		cfg.ACLFilters = append(cfg.ACLFilters, adminACLFilterToCfg(filter))
	}

	return cfg
}

func adminMapFilterToCfg(filter *adminv2.NameFilter) *NameFilter {
	if filter == nil {
		return nil
	}

	return &NameFilter{
		PatternType: adminPatternTypeToCfg(filter.GetPatternType()),
		FilterType:  adminFilterTypeToCfg(filter.GetFilterType()),
		Name:        filter.GetName(),
	}
}

func adminACLFilterToCfg(filter *adminv2.ACLFilter) *ACLFilter {
	if filter == nil {
		return nil
	}

	return &ACLFilter{
		ResourceFilter: adminACLResourceFilterToCfg(filter.GetResourceFilter()),
		AccessFilter:   adminACLAccessFilterToCfg(filter.GetAccessFilter()),
	}
}

func adminACLResourceFilterToCfg(filter *adminv2.ACLResourceFilter) *ACLResourceFilter {
	if filter == nil {
		return nil
	}

	return &ACLResourceFilter{
		ResourceType: adminACLResourceToCfg(filter.GetResourceType()),
		PatternType:  adminACLPatternToCfg(filter.GetPatternType()),
		Name:         filter.GetName(),
	}
}

func adminACLAccessFilterToCfg(filter *adminv2.ACLAccessFilter) *ACLAccessFilter {
	if filter == nil {
		return nil
	}

	return &ACLAccessFilter{
		Principal:      filter.GetPrincipal(),
		Operation:      adminACLOperationToCfg(filter.GetOperation()),
		PermissionType: adminPermissionTypeToCfg(filter.GetPermissionType()),
		Host:           filter.GetHost(),
	}
}

func adminPatternTypeToCfg(pt adminv2.PatternType) PatternType {
	switch pt {
	case adminv2.PatternType_PATTERN_TYPE_LITERAL:
		return PatternTypeLiteral
	case adminv2.PatternType_PATTERN_TYPE_PREFIX:
		return PatternTypePrefix
	default:
		return ""
	}
}

func adminFilterTypeToCfg(ft adminv2.FilterType) FilterType {
	switch ft {
	case adminv2.FilterType_FILTER_TYPE_INCLUDE:
		return FilterTypeInclude
	case adminv2.FilterType_FILTER_TYPE_EXCLUDE:
		return FilterTypeExclude
	default:
		return ""
	}
}

func adminACLResourceToCfg(resource common.ACLResource) ACLResource {
	switch resource {
	case common.ACLResource_ACL_RESOURCE_ANY:
		return ACLResourceAny
	case common.ACLResource_ACL_RESOURCE_CLUSTER:
		return ACLResourceCluster
	case common.ACLResource_ACL_RESOURCE_GROUP:
		return ACLResourceGroup
	case common.ACLResource_ACL_RESOURCE_TOPIC:
		return ACLResourceTopic
	case common.ACLResource_ACL_RESOURCE_TXN_ID:
		return ACLResourceTXNID
	case common.ACLResource_ACL_RESOURCE_SR_SUBJECT:
		return ACLResourceSRSubject
	case common.ACLResource_ACL_RESOURCE_SR_REGISTRY:
		return ACLResourceSRRegistry
	case common.ACLResource_ACL_RESOURCE_SR_ANY:
		return ACLResourceSRAny
	default:
		return ""
	}
}

func adminACLPatternToCfg(pattern common.ACLPattern) ACLPattern {
	switch pattern {
	case common.ACLPattern_ACL_PATTERN_ANY:
		return ACLPatternAny
	case common.ACLPattern_ACL_PATTERN_LITERAL:
		return ACLPatternLiteral
	case common.ACLPattern_ACL_PATTERN_PREFIXED:
		return ACLPatternPrefixed
	case common.ACLPattern_ACL_PATTERN_MATCH:
		return ACLPatternMatch
	default:
		return ""
	}
}

func adminACLOperationToCfg(operation common.ACLOperation) ACLOperation {
	switch operation {
	case common.ACLOperation_ACL_OPERATION_ANY:
		return ACLOperationAny
	case common.ACLOperation_ACL_OPERATION_READ:
		return ACLOperationRead
	case common.ACLOperation_ACL_OPERATION_WRITE:
		return ACLOperationWrite
	case common.ACLOperation_ACL_OPERATION_CREATE:
		return ACLOperationCreate
	case common.ACLOperation_ACL_OPERATION_REMOVE:
		return ACLOperationRemove
	case common.ACLOperation_ACL_OPERATION_ALTER:
		return ACLOperationAlter
	case common.ACLOperation_ACL_OPERATION_DESCRIBE:
		return ACLOperationDescribe
	case common.ACLOperation_ACL_OPERATION_CLUSTER_ACTION:
		return ACLOperationClusterAction
	case common.ACLOperation_ACL_OPERATION_DESCRIBE_CONFIGS:
		return ACLOperationDescribeConfigs
	case common.ACLOperation_ACL_OPERATION_ALTER_CONFIGS:
		return ACLOperationAlterConfigs
	case common.ACLOperation_ACL_OPERATION_IDEMPOTENT_WRITE:
		return ACLOperationIdempotentWrite
	default:
		return ""
	}
}

func adminPermissionTypeToCfg(permType common.ACLPermissionType) ACLPermissionType {
	switch permType {
	case common.ACLPermissionType_ACL_PERMISSION_TYPE_ANY:
		return ACLPermissionTypeAny
	case common.ACLPermissionType_ACL_PERMISSION_TYPE_ALLOW:
		return ACLPermissionTypeAllow
	case common.ACLPermissionType_ACL_PERMISSION_TYPE_DENY:
		return ACLPermissionTypeDeny
	default:
		return ""
	}
}
