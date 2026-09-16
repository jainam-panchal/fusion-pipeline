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

mod dead_letter {
    use fusion_nats::subject::{covers_every_tenant, dead_letter};

    #[test]
    fn a_tenant_is_the_token_after_the_prefix() {
        assert_eq!(dead_letter("dlq", "acme"), "dlq.acme");
        assert_eq!(dead_letter("dlq", "unknown"), "dlq.unknown");
        assert_eq!(dead_letter("dead", "acme-eu_1"), "dead.acme-eu_1");
    }

    #[test]
    fn a_tenant_that_is_not_one_token_is_escaped_into_one() {
        for (tenant, token) in [
            ("a.b", "a%2Eb"),
            ("a*", "a%2A"),
            ("a>", "a%3E"),
            ("a b", "a%20b"),
            ("a\tb", "a%09b"),
            ("é", "%C3%A9"),
            ("100%", "100%25"),
        ] {
            assert_eq!(
                dead_letter("dlq", tenant),
                format!("dlq.{token}"),
                "{tenant}"
            );
        }
    }

    #[test]
    fn two_tenants_never_share_a_subject() {
        assert_ne!(dead_letter("dlq", "a.b"), dead_letter("dlq", "a%2Eb"));
        assert_ne!(dead_letter("dlq", "a b"), dead_letter("dlq", "a%20b"));
    }

    #[test]
    fn a_stream_pattern_must_cover_every_tenant_under_the_prefix() {
        assert!(covers_every_tenant("dlq.*", "dlq"));
        assert!(covers_every_tenant("dlq.>", "dlq"));
        assert!(covers_every_tenant(">", "dlq"));
        assert!(covers_every_tenant("*.*", "dlq"));
        assert!(!covers_every_tenant("dlq.x", "dlq"), "one tenant only");
        assert!(!covers_every_tenant("dlq", "dlq"));
        assert!(!covers_every_tenant("dead.>", "dlq"));
    }
}
