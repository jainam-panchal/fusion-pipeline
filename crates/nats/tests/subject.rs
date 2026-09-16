//! Tenant derivation from the NATS subject, through the crate's public helpers.

use fusion_nats::subject::tenant_from_subject;

#[test]
fn tenant_is_the_second_subject_token() {
    assert_eq!(
        tenant_from_subject("logs.acme.syslog", "logs"),
        Some("acme")
    );
    assert_eq!(
        tenant_from_subject("logs.acme.app.web", "logs"),
        Some("acme")
    );
}

#[test]
fn subjects_without_a_tenant_token_yield_none() {
    assert_eq!(tenant_from_subject("logs", "logs"), None);
    assert_eq!(tenant_from_subject("logs.", "logs"), None);
    assert_eq!(tenant_from_subject("logs..syslog", "logs"), None);
}

#[test]
fn a_two_token_subject_names_no_tenant() {
    assert_eq!(tenant_from_subject("processed.logs", "logs"), None);
    assert_eq!(tenant_from_subject("logs.acme", "logs"), None);
}

#[test]
fn only_a_subject_under_the_prefix_names_a_tenant() {
    assert_eq!(tenant_from_subject("processed.logs.v2", "logs"), None);
    assert_eq!(tenant_from_subject("logsx.acme.syslog", "logs"), None);
    assert_eq!(
        tenant_from_subject("ingest.acme.syslog", "ingest"),
        Some("acme")
    );
    assert_eq!(tenant_from_subject("logs.acme.syslog", "ingest"), None);
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
