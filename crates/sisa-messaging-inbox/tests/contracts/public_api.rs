use std::fs;
use std::path::Path;

use sisa_messaging_inbox::{
    ClaimedReceipt, DeadLetterBatchError, DeadLetterCursor, DeadLetterRecord, DeadReason,
    InboxClaimOutcome, InboxFailure, InboxFailureOutcome, InboxId, InboxPurgeReport,
    InboxPurgeRequest, InboxRecord, InboxScope, InboxSettings, InboxStats,
};

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
fn public_contract_data_carriers_compile() {
    let _: Option<ClaimedReceipt> = None;
    let _: Option<DeadLetterBatchError> = None;
    let _: Option<DeadLetterCursor> = None;
    let _: Option<DeadLetterRecord> = None;
    let _: Option<DeadReason> = None;
    let _: Option<InboxClaimOutcome> = None;
    let _: Option<InboxFailure> = None;
    let _: Option<InboxFailureOutcome> = None;
    let _: Option<InboxId> = None;
    let _: Option<InboxPurgeReport> = None;
    let _: Option<InboxPurgeRequest> = None;
    let _: Option<InboxRecord> = None;
    let _: Option<InboxScope> = None;
    let _: Option<InboxSettings> = None;
    let _: Option<InboxStats> = None;
}
