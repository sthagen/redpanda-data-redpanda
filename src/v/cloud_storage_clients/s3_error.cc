/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/s3_error.h"

#include <boost/lexical_cast.hpp>

#include <map>

namespace cloud_storage_clients {

fmt::iterator format_to(s3_error_code code, fmt::iterator out) {
    switch (code) {
    case s3_error_code::access_denied:
        return fmt::format_to(out, "AccessDenied");
    case s3_error_code::account_problem:
        return fmt::format_to(out, "AccountProblem");
    case s3_error_code::all_access_disabled:
        return fmt::format_to(out, "AllAccessDisabled");
    case s3_error_code::ambiguous_grant_by_email_address:
        return fmt::format_to(out, "AmbiguousGrantByEmailAddress");
    case s3_error_code::authentication_required:
        return fmt::format_to(out, "AuthenticationRequired");
    case s3_error_code::authorization_header_malformed:
        return fmt::format_to(out, "AuthorizationHeaderMalformed");
    case s3_error_code::bad_digest:
        return fmt::format_to(out, "BadDigest");
    case s3_error_code::bucket_already_exists:
        return fmt::format_to(out, "BucketAlreadyExists");
    case s3_error_code::bucket_already_owned_by_you:
        return fmt::format_to(out, "BucketAlreadyOwnedByYou");
    case s3_error_code::bucket_not_empty:
        return fmt::format_to(out, "BucketNotEmpty");
    case s3_error_code::credentials_not_supported:
        return fmt::format_to(out, "CredentialsNotSupported");
    case s3_error_code::cross_location_logging_prohibited:
        return fmt::format_to(out, "CrossLocationLoggingProhibited");
    case s3_error_code::entity_too_small:
        return fmt::format_to(out, "EntityTooSmall");
    case s3_error_code::entity_too_large:
        return fmt::format_to(out, "EntityTooLarge");
    case s3_error_code::expired_token:
        return fmt::format_to(out, "ExpiredToken");
    case s3_error_code::illegal_location_constraint_exception:
        return fmt::format_to(out, "IllegalLocationConstraintException");
    case s3_error_code::illegal_versioning_configuration_exception:
        return fmt::format_to(out, "IllegalVersioningConfigurationException");
    case s3_error_code::incomplete_body:
        return fmt::format_to(out, "IncompleteBody");
    case s3_error_code::incorrect_number_of_files_in_post_request:
        return fmt::format_to(out, "IncorrectNumberOfFilesInPostRequest");
    case s3_error_code::inline_data_too_large:
        return fmt::format_to(out, "InlineDataTooLarge");
    case s3_error_code::internal_error:
        return fmt::format_to(out, "InternalError");
    case s3_error_code::invalid_access_key_id:
        return fmt::format_to(out, "InvalidAccessKeyId");
    case s3_error_code::invalid_access_point:
        return fmt::format_to(out, "InvalidAccessPoint");
    case s3_error_code::invalid_addressing_header:
        return fmt::format_to(out, "InvalidAddressingHeader");
    case s3_error_code::invalid_argument:
        return fmt::format_to(out, "InvalidArgument");
    case s3_error_code::invalid_bucket_name:
        return fmt::format_to(out, "InvalidBucketName");
    case s3_error_code::invalid_bucket_state:
        return fmt::format_to(out, "InvalidBucketState");
    case s3_error_code::invalid_digest:
        return fmt::format_to(out, "InvalidDigest");
    case s3_error_code::invalid_encryption_algorithm_error:
        return fmt::format_to(out, "InvalidEncryptionAlgorithmError");
    case s3_error_code::invalid_location_constraint:
        return fmt::format_to(out, "InvalidLocationConstraint");
    case s3_error_code::invalid_object_state:
        return fmt::format_to(out, "InvalidObjectState");
    case s3_error_code::invalid_part:
        return fmt::format_to(out, "InvalidPart");
    case s3_error_code::invalid_part_order:
        return fmt::format_to(out, "InvalidPartOrder");
    case s3_error_code::invalid_payer:
        return fmt::format_to(out, "InvalidPayer");
    case s3_error_code::invalid_policy_document:
        return fmt::format_to(out, "InvalidPolicyDocument");
    case s3_error_code::invalid_range:
        return fmt::format_to(out, "InvalidRange");
    case s3_error_code::invalid_request:
        return fmt::format_to(out, "InvalidRequest");
    case s3_error_code::invalid_security:
        return fmt::format_to(out, "InvalidSecurity");
    case s3_error_code::invalid_soaprequest:
        return fmt::format_to(out, "InvalidSOAPRequest");
    case s3_error_code::invalid_storage_class:
        return fmt::format_to(out, "InvalidStorageClass");
    case s3_error_code::invalid_target_bucket_for_logging:
        return fmt::format_to(out, "InvalidTargetBucketForLogging");
    case s3_error_code::invalid_token:
        return fmt::format_to(out, "InvalidToken");
    case s3_error_code::invalid_uri:
        return fmt::format_to(out, "InvalidURI");
    case s3_error_code::key_too_long_error:
        return fmt::format_to(out, "KeyTooLongError");
    case s3_error_code::malformed_aclerror:
        return fmt::format_to(out, "MalformedACLError");
    case s3_error_code::malformed_postrequest:
        return fmt::format_to(out, "MalformedPOSTRequest");
    case s3_error_code::malformed_xml:
        return fmt::format_to(out, "MalformedXML");
    case s3_error_code::max_message_length_exceeded:
        return fmt::format_to(out, "MaxMessageLengthExceeded");
    case s3_error_code::max_post_pre_data_length_exceeded_error:
        return fmt::format_to(out, "MaxPostPreDataLengthExceededError");
    case s3_error_code::metadata_too_large:
        return fmt::format_to(out, "MetadataTooLarge");
    case s3_error_code::method_not_allowed:
        return fmt::format_to(out, "MethodNotAllowed");
    case s3_error_code::missing_attachment:
        return fmt::format_to(out, "MissingAttachment");
    case s3_error_code::missing_content_length:
        return fmt::format_to(out, "MissingContentLength");
    case s3_error_code::missing_request_body_error:
        return fmt::format_to(out, "MissingRequestBodyError");
    case s3_error_code::missing_security_element:
        return fmt::format_to(out, "MissingSecurityElement");
    case s3_error_code::missing_security_header:
        return fmt::format_to(out, "MissingSecurityHeader");
    case s3_error_code::no_logging_status_for_key:
        return fmt::format_to(out, "NoLoggingStatusForKey");
    case s3_error_code::no_such_bucket:
        return fmt::format_to(out, "NoSuchBucket");
    case s3_error_code::no_such_bucket_policy:
        return fmt::format_to(out, "NoSuchBucketPolicy");
    case s3_error_code::no_such_key:
        return fmt::format_to(out, "NoSuchKey");
    case s3_error_code::no_such_lifecycle_configuration:
        return fmt::format_to(out, "NoSuchLifecycleConfiguration");
    case s3_error_code::no_such_tag_set:
        return fmt::format_to(out, "NoSuchTagSet");
    case s3_error_code::no_such_upload:
        return fmt::format_to(out, "NoSuchUpload");
    case s3_error_code::no_such_version:
        return fmt::format_to(out, "NoSuchVersion");
    case s3_error_code::not_implemented:
        return fmt::format_to(out, "NotImplemented");
    case s3_error_code::not_signed_up:
        return fmt::format_to(out, "NotSignedUp");
    case s3_error_code::operation_aborted:
        return fmt::format_to(out, "OperationAborted");
    case s3_error_code::permanent_redirect:
        return fmt::format_to(out, "PermanentRedirect");
    case s3_error_code::precondition_failed:
        return fmt::format_to(out, "PreconditionFailed");
    case s3_error_code::redirect:
        return fmt::format_to(out, "Redirect");
    case s3_error_code::request_header_section_too_large:
        return fmt::format_to(out, "RequestHeaderSectionTooLarge");
    case s3_error_code::request_is_not_multi_part_content:
        return fmt::format_to(out, "RequestIsNotMultiPartContent");
    case s3_error_code::request_timeout:
        return fmt::format_to(out, "RequestTimeout");
    case s3_error_code::request_time_too_skewed:
        return fmt::format_to(out, "RequestTimeTooSkewed");
    case s3_error_code::request_torrent_of_bucket_error:
        return fmt::format_to(out, "RequestTorrentOfBucketError");
    case s3_error_code::restore_already_in_progress:
        return fmt::format_to(out, "RestoreAlreadyInProgress");
    case s3_error_code::server_side_encryption_configuration_not_found_error:
        return fmt::format_to(
          out, "ServerSideEncryptionConfigurationNotFoundError");
    case s3_error_code::service_unavailable:
        return fmt::format_to(out, "ServiceUnavailable");
    case s3_error_code::signature_does_not_match:
        return fmt::format_to(out, "SignatureDoesNotMatch");
    case s3_error_code::slow_down:
        return fmt::format_to(out, "SlowDown");
    case s3_error_code::temporary_redirect:
        return fmt::format_to(out, "TemporaryRedirect");
    case s3_error_code::token_refresh_required:
        return fmt::format_to(out, "TokenRefreshRequired");
    case s3_error_code::too_many_access_points:
        return fmt::format_to(out, "TooManyAccessPoints");
    case s3_error_code::too_many_buckets:
        return fmt::format_to(out, "TooManyBuckets");
    case s3_error_code::unexpected_content:
        return fmt::format_to(out, "UnexpectedContent");
    case s3_error_code::unresolvable_grant_by_email_address:
        return fmt::format_to(out, "UnresolvableGrantByEmailAddress");
    case s3_error_code::user_key_must_be_specified:
        return fmt::format_to(out, "UserKeyMustBeSpecified");
    case s3_error_code::no_such_access_point:
        return fmt::format_to(out, "NoSuchAccessPoint");
    case s3_error_code::invalid_tag:
        return fmt::format_to(out, "InvalidTag");
    case s3_error_code::malformed_policy:
        return fmt::format_to(out, "MalformedPolicy");
    case s3_error_code::no_such_configuration:
        return fmt::format_to(out, "NoSuchConfiguration");
    case s3_error_code::authorization_query_parameters_error:
        return fmt::format_to(out, "AuthorizationQueryParametersError");
    case s3_error_code::access_point_already_owned_by_you:
        return fmt::format_to(out, "AccessPointAlreadyOwnedByYou");
    case s3_error_code::access_control_list_not_supported:
        return fmt::format_to(out, "AccessControlListNotSupported");
    case s3_error_code::endpoint_not_found:
        return fmt::format_to(out, "EndpointNotFound");
    case s3_error_code::device_not_active_error:
        return fmt::format_to(out, "DeviceNotActiveError");
    case s3_error_code::conditional_request_conflict:
        return fmt::format_to(out, "ConditionalRequestConflict");
    case s3_error_code::connection_closed_by_requester:
        return fmt::format_to(out, "ConnectionClosedByRequester");
    case s3_error_code::client_token_conflict:
        return fmt::format_to(out, "ClientTokenConflict");
    case s3_error_code::bucket_has_access_points_attached:
        return fmt::format_to(out, "BucketHasAccessPointsAttached");
    case s3_error_code::invalid_access_point_alias_error:
        return fmt::format_to(out, "InvalidAccessPointAliasError");
    case s3_error_code::incorrect_endpoint:
        return fmt::format_to(out, "IncorrectEndpoint");
    case s3_error_code::invalid_http_method:
        return fmt::format_to(out, "InvalidHttpMethod");
    case s3_error_code::invalid_host_header:
        return fmt::format_to(out, "InvalidHostHeader");
    case s3_error_code::invalid_bucket_owner_aws_account_id:
        return fmt::format_to(out, "InvalidBucketOwnerAWSAccountID");
    case s3_error_code::invalid_bucket_acl_with_object_ownership:
        return fmt::format_to(out, "InvalidBucketAclWithObjectOwnership");
    case s3_error_code::invalid_session_exception:
        return fmt::format_to(out, "InvalidSessionException");
    case s3_error_code::invalid_signature:
        return fmt::format_to(out, "InvalidSignature");
    case s3_error_code::kms_disabled_exception:
        return fmt::format_to(out, "KMS.DisabledException");
    case s3_error_code::kms_invalid_key_usage_exception:
        return fmt::format_to(out, "KMS.InvalidKeyUsageException");
    case s3_error_code::kms_invalid_state_exception:
        return fmt::format_to(out, "KMS.KMSInvalidStateException");
    case s3_error_code::kms_not_found_exception:
        return fmt::format_to(out, "KMS.NotFoundException");
    case s3_error_code::missing_authentication_token:
        return fmt::format_to(out, "MissingAuthenticationToken");
    case s3_error_code::no_such_async_request:
        return fmt::format_to(out, "NoSuchAsyncRequest");
    case s3_error_code::no_such_cors_configuration:
        return fmt::format_to(out, "NoSuchCORSConfiguration");
    case s3_error_code::no_such_multi_region_access_point:
        return fmt::format_to(out, "NoSuchMultiRegionAccessPoint");
    case s3_error_code::no_such_object_lock_configuration:
        return fmt::format_to(out, "NoSuchObjectLockConfiguration");
    case s3_error_code::no_such_website_configuration:
        return fmt::format_to(out, "NoSuchWebsiteConfiguration");
    case s3_error_code::not_modified:
        return fmt::format_to(out, "NotModified");
    case s3_error_code::not_device_owner_error:
        return fmt::format_to(out, "NotDeviceOwnerError");
    case s3_error_code::no_transformation_defined:
        return fmt::format_to(out, "NoTransformationDefined");
    case s3_error_code::object_lock_configuration_not_found_error:
        return fmt::format_to(out, "ObjectLockConfigurationNotFoundError");
    case s3_error_code::ownership_controls_not_found_error:
        return fmt::format_to(out, "OwnershipControlsNotFoundError");
    case s3_error_code::permanent_redirect_control_error:
        return fmt::format_to(out, "PermanentRedirectControlError");
    case s3_error_code::response_interrupted:
        return fmt::format_to(out, "ResponseInterrupted");
    case s3_error_code::token_code_invalid_error:
        return fmt::format_to(out, "TokenCodeInvalidError");
    case s3_error_code::too_many_multi_region_access_pointregions_error:
        return fmt::format_to(out, "TooManyMultiRegionAccessPointregionsError");
    case s3_error_code::too_many_multi_region_access_points:
        return fmt::format_to(out, "TooManyMultiRegionAccessPoints");
    case s3_error_code::unauthorized_access_error:
        return fmt::format_to(out, "UnauthorizedAccessError");
    case s3_error_code::unexpected_ip_error:
        return fmt::format_to(out, "UnexpectedIPError");
    case s3_error_code::unsupported_signature:
        return fmt::format_to(out, "UnsupportedSignature");
    case s3_error_code::unsupported_argument:
        return fmt::format_to(out, "UnsupportedArgument");
    case s3_error_code::_unknown:
        return fmt::format_to(out, "_unknown_error_code_");
    }
    return fmt::format_to(out, "_unknown_error_code_");
}

// NOLINTNEXTLINE
static const std::map<ss::sstring, s3_error_code> known_aws_error_codes = {
  {"AccessDenied", s3_error_code::access_denied},
  {"AccountProblem", s3_error_code::account_problem},
  {"AllAccessDisabled", s3_error_code::all_access_disabled},
  {"AmbiguousGrantByEmailAddress",
   s3_error_code::ambiguous_grant_by_email_address},
  {"AuthenticationRequired", s3_error_code::authentication_required},
  {"AuthorizationHeaderMalformed",
   s3_error_code::authorization_header_malformed},
  {"BadDigest", s3_error_code::bad_digest},
  {"BucketAlreadyExists", s3_error_code::bucket_already_exists},
  {"BucketAlreadyOwnedByYou", s3_error_code::bucket_already_owned_by_you},
  {"BucketNotEmpty", s3_error_code::bucket_not_empty},
  {"CredentialsNotSupported", s3_error_code::credentials_not_supported},
  {"CrossLocationLoggingProhibited",
   s3_error_code::cross_location_logging_prohibited},
  {"EntityTooSmall", s3_error_code::entity_too_small},
  {"EntityTooLarge", s3_error_code::entity_too_large},
  {"ExpiredToken", s3_error_code::expired_token},
  {"IllegalLocationConstraintException",
   s3_error_code::illegal_location_constraint_exception},
  {"IllegalVersioningConfigurationException",
   s3_error_code::illegal_versioning_configuration_exception},
  {"IncompleteBody", s3_error_code::incomplete_body},
  {"IncorrectNumberOfFilesInPostRequest",
   s3_error_code::incorrect_number_of_files_in_post_request},
  {"InlineDataTooLarge", s3_error_code::inline_data_too_large},
  {"InternalError", s3_error_code::internal_error},
  {"InvalidAccessKeyId", s3_error_code::invalid_access_key_id},
  {"InvalidAccessPoint", s3_error_code::invalid_access_point},
  {"InvalidAddressingHeader", s3_error_code::invalid_addressing_header},
  {"InvalidArgument", s3_error_code::invalid_argument},
  {"InvalidBucketName", s3_error_code::invalid_bucket_name},
  {"InvalidBucketState", s3_error_code::invalid_bucket_state},
  {"InvalidDigest", s3_error_code::invalid_digest},
  {"InvalidEncryptionAlgorithmError",
   s3_error_code::invalid_encryption_algorithm_error},
  {"InvalidLocationConstraint", s3_error_code::invalid_location_constraint},
  {"InvalidObjectState", s3_error_code::invalid_object_state},
  {"InvalidPart", s3_error_code::invalid_part},
  {"InvalidPartOrder", s3_error_code::invalid_part_order},
  {"InvalidPayer", s3_error_code::invalid_payer},
  {"InvalidPolicyDocument", s3_error_code::invalid_policy_document},
  {"InvalidRange", s3_error_code::invalid_range},
  {"InvalidRequest", s3_error_code::invalid_request},
  {"InvalidSecurity", s3_error_code::invalid_security},
  {"InvalidSOAPRequest", s3_error_code::invalid_soaprequest},
  {"InvalidStorageClass", s3_error_code::invalid_storage_class},
  {"InvalidTargetBucketForLogging",
   s3_error_code::invalid_target_bucket_for_logging},
  {"InvalidToken", s3_error_code::invalid_token},
  {"InvalidURI", s3_error_code::invalid_uri},
  {"KeyTooLongError", s3_error_code::key_too_long_error},
  {"MalformedACLError", s3_error_code::malformed_aclerror},
  {"MalformedPOSTRequest", s3_error_code::malformed_postrequest},
  {"MalformedXML", s3_error_code::malformed_xml},
  {"MaxMessageLengthExceeded", s3_error_code::max_message_length_exceeded},
  {"MaxPostPreDataLengthExceededError",
   s3_error_code::max_post_pre_data_length_exceeded_error},
  {"MetadataTooLarge", s3_error_code::metadata_too_large},
  {"MethodNotAllowed", s3_error_code::method_not_allowed},
  {"MissingAttachment", s3_error_code::missing_attachment},
  {"MissingContentLength", s3_error_code::missing_content_length},
  {"MissingRequestBodyError", s3_error_code::missing_request_body_error},
  {"MissingSecurityElement", s3_error_code::missing_security_element},
  {"MissingSecurityHeader", s3_error_code::missing_security_header},
  {"NoLoggingStatusForKey", s3_error_code::no_logging_status_for_key},
  {"NoSuchBucket", s3_error_code::no_such_bucket},
  {"NoSuchBucketPolicy", s3_error_code::no_such_bucket_policy},
  {"NoSuchKey", s3_error_code::no_such_key},
  {"NoSuchLifecycleConfiguration",
   s3_error_code::no_such_lifecycle_configuration},
  {"NoSuchTagSet", s3_error_code::no_such_tag_set},
  {"NoSuchUpload", s3_error_code::no_such_upload},
  {"NoSuchVersion", s3_error_code::no_such_version},
  {"NotImplemented", s3_error_code::not_implemented},
  {"NotSignedUp", s3_error_code::not_signed_up},
  {"OperationAborted", s3_error_code::operation_aborted},
  {"PermanentRedirect", s3_error_code::permanent_redirect},
  {"PreconditionFailed", s3_error_code::precondition_failed},
  {"Redirect", s3_error_code::redirect},
  {"RequestHeaderSectionTooLarge",
   s3_error_code::request_header_section_too_large},
  {"RequestIsNotMultiPartContent",
   s3_error_code::request_is_not_multi_part_content},
  {"RequestTimeout", s3_error_code::request_timeout},
  {"RequestTimeTooSkewed", s3_error_code::request_time_too_skewed},
  {"RequestTorrentOfBucketError",
   s3_error_code::request_torrent_of_bucket_error},
  {"RestoreAlreadyInProgress", s3_error_code::restore_already_in_progress},
  {"ServerSideEncryptionConfigurationNotFoundError",
   s3_error_code::server_side_encryption_configuration_not_found_error},
  {"ServiceUnavailable", s3_error_code::service_unavailable},
  {"SignatureDoesNotMatch", s3_error_code::signature_does_not_match},
  {"SlowDown", s3_error_code::slow_down},
  {"TemporaryRedirect", s3_error_code::temporary_redirect},
  {"TokenRefreshRequired", s3_error_code::token_refresh_required},
  {"TooManyAccessPoints", s3_error_code::too_many_access_points},
  {"TooManyBuckets", s3_error_code::too_many_buckets},
  {"UnexpectedContent", s3_error_code::unexpected_content},
  {"UnresolvableGrantByEmailAddress",
   s3_error_code::unresolvable_grant_by_email_address},
  {"UserKeyMustBeSpecified", s3_error_code::user_key_must_be_specified},
  {"NoSuchAccessPoint", s3_error_code::no_such_access_point},
  {"InvalidTag", s3_error_code::invalid_tag},
  {"MalformedPolicy", s3_error_code::malformed_policy},
  {"NoSuchConfiguration", s3_error_code::no_such_configuration},
  {"AuthorizationQueryParametersError",
   s3_error_code::authorization_query_parameters_error},
  {"AccessPointAlreadyOwnedByYou",
   s3_error_code::access_point_already_owned_by_you},
  {"AccessControlListNotSupported",
   s3_error_code::access_control_list_not_supported},
  {"EndpointNotFound", s3_error_code::endpoint_not_found},
  {"DeviceNotActiveError", s3_error_code::device_not_active_error},
  {"ConditionalRequestConflict", s3_error_code::conditional_request_conflict},
  {"ConnectionClosedByRequester",
   s3_error_code::connection_closed_by_requester},
  {"ClientTokenConflict", s3_error_code::client_token_conflict},
  {"BucketHasAccessPointsAttached",
   s3_error_code::bucket_has_access_points_attached},
  {"InvalidAccessPointAliasError",
   s3_error_code::invalid_access_point_alias_error},
  {"IncorrectEndpoint", s3_error_code::incorrect_endpoint},
  {"InvalidHttpMethod", s3_error_code::invalid_http_method},
  {"InvalidHostHeader", s3_error_code::invalid_host_header},
  {"InvalidBucketOwnerAWSAccountID",
   s3_error_code::invalid_bucket_owner_aws_account_id},
  {"InvalidBucketAclWithObjectOwnership",
   s3_error_code::invalid_bucket_acl_with_object_ownership},
  {"InvalidSessionException", s3_error_code::invalid_session_exception},
  {"InvalidSignature", s3_error_code::invalid_signature},
  {"KMS.DisabledException", s3_error_code::kms_disabled_exception},
  {"KMS.InvalidKeyUsageException",
   s3_error_code::kms_invalid_key_usage_exception},
  {"KMS.KMSInvalidStateException", s3_error_code::kms_invalid_state_exception},
  {"KMS.NotFoundException", s3_error_code::kms_not_found_exception},
  {"MissingAuthenticationToken", s3_error_code::missing_authentication_token},
  {"NoSuchAsyncRequest", s3_error_code::no_such_async_request},
  {"NoSuchCORSConfiguration", s3_error_code::no_such_cors_configuration},
  {"NoSuchMultiRegionAccessPoint",
   s3_error_code::no_such_multi_region_access_point},
  {"NoSuchObjectLockConfiguration",
   s3_error_code::no_such_object_lock_configuration},
  {"NoSuchWebsiteConfiguration", s3_error_code::no_such_website_configuration},
  {"NotModified", s3_error_code::not_modified},
  {"NotDeviceOwnerError", s3_error_code::not_device_owner_error},
  {"NoTransformationDefined", s3_error_code::no_transformation_defined},
  {"ObjectLockConfigurationNotFoundError",
   s3_error_code::object_lock_configuration_not_found_error},
  {"OwnershipControlsNotFoundError",
   s3_error_code::ownership_controls_not_found_error},
  {"PermanentRedirectControlError",
   s3_error_code::permanent_redirect_control_error},
  {"ResponseInterrupted", s3_error_code::response_interrupted},
  {"TokenCodeInvalidError", s3_error_code::token_code_invalid_error},
  {"TooManyMultiRegionAccessPointregionsError",
   s3_error_code::too_many_multi_region_access_pointregions_error},
  {"TooManyMultiRegionAccessPoints",
   s3_error_code::too_many_multi_region_access_points},
  {"UnauthorizedAccessError", s3_error_code::unauthorized_access_error},
  {"UnexpectedIPError", s3_error_code::unexpected_ip_error},
  {"UnsupportedSignature", s3_error_code::unsupported_signature},
  {"UnsupportedArgument", s3_error_code::unsupported_argument}};

std::istream& operator>>(std::istream& i, s3_error_code& code) {
    ss::sstring c;
    i >> c;
    auto it = known_aws_error_codes.find(c);
    if (it != known_aws_error_codes.end()) {
        code = it->second;
    } else {
        code = s3_error_code::_unknown;
    }
    return i;
}

rest_error_response::rest_error_response(
  std::string_view code,
  std::string_view message,
  std::string_view request_id,
  std::string_view resource)
  : _code(
      code.empty() ? s3_error_code::_unknown
                   : boost::lexical_cast<s3_error_code>(code))
  , _code_str(code)
  , _message(message)
  , _request_id(request_id)
  , _resource(resource) {}

const char* rest_error_response::what() const noexcept {
    return _message.c_str();
}
s3_error_code rest_error_response::code() const noexcept { return _code; }
std::string_view rest_error_response::code_string() const noexcept {
    return _code_str;
}
std::string_view rest_error_response::message() const noexcept {
    return _message;
}
std::string_view rest_error_response::request_id() const noexcept {
    return _request_id;
}
std::string_view rest_error_response::resource() const noexcept {
    return _resource;
}

} // namespace cloud_storage_clients
