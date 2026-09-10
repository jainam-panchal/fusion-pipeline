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
