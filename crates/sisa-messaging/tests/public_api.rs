use std::fs;
use std::path::Path;

// Import every crate-root export so missing or relocated names fail at compile time.
#[allow(unused_imports)]
use sisa_messaging::{
    ContentType, ConversationId, CorrelationMetadata, Delivery, DeliveryMetadata, Envelope,
    EnvelopeError, EnvelopeMapper, ErrorClassifier, ErrorSummary, FailureKind, FrameworkHeader,
    HeaderName, HeaderNameError, HeaderValue, HeaderValueError, Headers, HeadersError,
    IndividualCapability, IndividualDeliverySource, IndividualSettlement,
    IndividualSettlementError, IndividualSourceDescriptor, IndividualSourceDescriptorError,
    IndividualSourceOpenError, IndividualSourceRequirement, IndividualSourceRequirements,
    MAX_CUSTOM_HEADER_BYTES, MAX_CUSTOM_HEADER_COUNT, MAX_ERROR_SUMMARY_BYTES, Message, MessageId,
    MessageType, Metadata, MetadataValue, OrderingKey, PartitionAdvance,
    PartitionedLogDeliverySource, PartitionedLogReceive, PartitionedLogSettlement, Publisher,
    RequestId, RoutingMetadata, SerializedEnvelope, Serializer, TraceMetadata,
    UnsupportedIndividualRequirement, ValidationError,
};

#[cfg(feature = "json")]
#[allow(unused_imports)]
use sisa_messaging::{JsonSerializer, JsonSerializerError};

#[allow(dead_code)]
const _: usize = MAX_ERROR_SUMMARY_BYTES;
const _: usize = MAX_CUSTOM_HEADER_BYTES;
const _: usize = MAX_CUSTOM_HEADER_COUNT;

#[test]
fn crate_root_reexports_match_the_public_api_inventory() {
    let lib_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs");
    let source = fs::read_to_string(&lib_path).unwrap();
    let mut exports = Vec::new();
    let mut current = String::new();

    for line in source.lines() {
        let trimmed = line.trim();

        if let Some(start) = trimmed.strip_prefix("pub use ") {
            current.push_str(start);
        } else if !current.is_empty() {
            current.push_str(trimmed);
        } else {
            continue;
        }

        if current.ends_with(';') {
            current.pop();

            exports.push(
                current
                    .split_whitespace()
                    .collect::<String>()
                    .replace(",}", "}"),
            );

            current.clear();
        }
    }

    let actual = exports.join("\n");
    let expected = include_str!("fixtures/public-api.txt").trim_end();

    assert_eq!(actual, expected);
}
