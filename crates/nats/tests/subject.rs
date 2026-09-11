//! Tenant derivation from the NATS subject, through the crate's public helpers.

use fusion_core::record::Record;
use fusion_nats::subject::{stamp_tenant, tenant_from_subject};

#[test]
fn tenant_is_the_second_subject_token() {
    assert_eq!(tenant_from_subject("logs.acme.syslog"), Some("acme"));
    assert_eq!(tenant_from_subject("logs.acme.app.web"), Some("acme"));
}

#[test]
fn subjects_without_a_tenant_token_yield_none() {
    assert_eq!(tenant_from_subject("logs"), None);
    assert_eq!(tenant_from_subject("logs."), None);
    assert_eq!(tenant_from_subject("logs..syslog"), None);
}

#[test]
fn tenant_is_stamped_when_the_record_has_none() {
    let mut record = Record::from_json(r#"{"id": 1, "body": "x"}"#).expect("record parses");

    stamp_tenant(&mut record, "acme");

    assert_eq!(record.tenant(), Some("acme"));
}

#[test]
fn an_existing_tenant_is_left_alone() {
    let mut record = Record::from_json(r#"{"id": 1, "resource": {"tenant.id": "globex"}}"#)
        .expect("record parses");

    stamp_tenant(&mut record, "acme");

    assert_eq!(record.tenant(), Some("globex"));
}

mod capture {
    use fusion_nats::subject::captures;

    #[test]
    fn literal_subjects_match_only_themselves() {
        assert!(captures("processed.logs", "processed.logs"));
        assert!(!captures("processed.logs", "processed.log"));
        assert!(!captures("processed.logs", "processed.logs.x"));
    }

    #[test]
    fn star_matches_exactly_one_token() {
        assert!(captures("processed.*", "processed.logs"));
        assert!(!captures("processed.*", "processed"));
        assert!(!captures("processed.*", "processed.logs.x"));
        assert!(captures("*.logs", "processed.logs"));
    }

    #[test]
    fn full_wildcard_matches_one_or_more_trailing_tokens() {
        assert!(captures("processed.>", "processed.logs"));
        assert!(captures("processed.>", "processed.logs.acme"));
        assert!(!captures("processed.>", "processed"));
        assert!(!captures("logs.>", "processed.logs"));
    }
}
