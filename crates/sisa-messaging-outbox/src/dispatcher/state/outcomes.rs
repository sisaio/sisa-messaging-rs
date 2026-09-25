//! Resolved outcomes and persistence batch selection.

use sisa_messaging::{ErrorSummary, FailureKind};

use crate::{Claim, FailureAction, FailureRecord};

use super::{Phase, State};

#[derive(Debug)]
pub(crate) enum ResolvedOutcome {
    Complete,
    Failure {
        kind: FailureKind,

        summary: ErrorSummary,

        action: FailureAction,
    },
}

impl State {
    pub(crate) fn completion_batch(&self) -> Vec<Claim> {
        self.claims
            .iter()
            .filter_map(|(claim, owned)| {
                matches!(owned.phase, Phase::Resolved(ResolvedOutcome::Complete)).then_some(*claim)
            })
            .collect()
    }

    pub(crate) fn failure_batch(&self) -> Vec<FailureRecord> {
        self.claims
            .iter()
            .filter_map(|(claim, owned)| match &owned.phase {
                Phase::Resolved(ResolvedOutcome::Failure {
                    kind,
                    summary,
                    action,
                }) => Some(FailureRecord {
                    claim: *claim,
                    failure_kind: *kind,
                    error: summary.clone(),
                    action: *action,
                }),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn release_batch(&self) -> Vec<Claim> {
        self.claims
            .iter()
            .filter_map(|(claim, owned)| matches!(owned.phase, Phase::Release).then_some(*claim))
            .collect()
    }
}
