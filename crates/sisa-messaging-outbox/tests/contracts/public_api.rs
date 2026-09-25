use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

use sisa_messaging::MessageType;
use sisa_messaging_outbox::{
    Claim, ClaimToken, ClaimedRecord, DeadLetterBatchError, DeadLetterCursor, DeadReason,
    DispatcherError, FailureAction, OutboxId, OutboxRunReport, PoisonReport,
};
use uuid::Uuid;

use super::support::SafeError;

#[test]
fn provider_owned_id_types_only_reconstruct_existing_values() {
    let row = Uuid::from_u128(1);
    let token = Uuid::from_u128(2);

    let claim = Claim {
        id: OutboxId::from_uuid(row),
        token: ClaimToken::from_uuid(token),
    };

    assert_eq!(claim.id.into_uuid(), row);
    assert_eq!(claim.token.into_uuid(), token);
}

#[cfg(feature = "serde")]
#[test]
fn provider_owned_ids_round_trip_with_the_core_uuid_json_representation() {
    let row = Uuid::from_u128(1);
    let token = Uuid::from_u128(2);
    let outbox_id = OutboxId::from_uuid(row);
    let claim_token = ClaimToken::from_uuid(token);

    let encoded_outbox = serde_json::to_string(&outbox_id)
        .unwrap_or_else(|error| panic!("outbox id serialization failed: {error}"));

    let encoded_uuid = serde_json::to_string(&row)
        .unwrap_or_else(|error| panic!("UUID serialization failed: {error}"));

    assert_eq!(encoded_outbox, encoded_uuid);

    assert_eq!(
        serde_json::from_str::<OutboxId>(&encoded_outbox)
            .unwrap_or_else(|error| panic!("outbox id deserialization failed: {error}")),
        outbox_id
    );

    let encoded_token = serde_json::to_string(&claim_token)
        .unwrap_or_else(|error| panic!("claim token serialization failed: {error}"));

    assert_eq!(
        serde_json::from_str::<ClaimToken>(&encoded_token)
            .unwrap_or_else(|error| panic!("claim token deserialization failed: {error}")),
        claim_token
    );
}

#[test]
fn crate_root_reexports_match_the_public_api_inventory() {
    let lib_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs");

    let source = fs::read_to_string(&lib_path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", lib_path.display()));

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
    let expected = include_str!("../fixtures/public-api.txt").trim_end();
    assert_eq!(actual, expected);
}

#[allow(dead_code)]
fn public_data_carriers_compile() {
    let _: Option<ClaimedRecord> = None;
    let _: Option<DeadLetterCursor> = None;
    let _: Option<DeadLetterBatchError> = None;
    let _: Option<DeadReason> = None;
    let _: Option<DispatcherError<SafeError>> = None;
    let _: Option<FailureAction> = None;
    let _: Option<OutboxRunReport> = None;
    let _: Option<PoisonReport> = None;
    let _: Option<MessageType> = None;
    let _: NonZeroUsize = NonZeroUsize::MIN;
}
