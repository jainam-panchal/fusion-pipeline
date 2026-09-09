//! Engine classification and compile-error reporting through the public API.
#![allow(clippy::unwrap_used)]


use fusion_regex::{CompileError, Engine, Regex};

#[test]
fn linear_syntax_compiles_on_the_linear_engine() {
    let re = Regex::new(r"\w+").unwrap();
    assert_eq!(re.engine(), Engine::Linear);
}

#[test]
fn lookbehind_falls_back_to_the_backtracking_engine() {
    let re = Regex::new(r"(?<=x)y").unwrap();
    assert_eq!(re.engine(), Engine::Backtracking);
}

#[test]
fn backreference_falls_back_to_the_backtracking_engine() {
    let re = Regex::new(r"(a)\1").unwrap();
    assert_eq!(re.engine(), Engine::Backtracking);
}

#[test]
fn syntax_error_carries_pcre2_message_and_offset() {
    let err = Regex::new(r"ab(").unwrap_err();
    match err {
        CompileError::Syntax { offset, message, .. } => {
            assert_eq!(offset, 3);
            assert!(message.contains("missing closing parenthesis"), "{message}");
        }
        other => panic!("expected Syntax, got {other:?}"),
    }
}
