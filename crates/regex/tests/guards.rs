//! Compile-time guards and thread-safety. The no-JIT invariant is a unit test in `src/pcre2.rs`.
#![allow(clippy::unwrap_used)]

mod common;

use common::{BOTH_ENGINES, try_compile as with_limits};
use fusion_regex::{CompileError, EngineChoice, Limits, Regex};

#[test]
fn oversize_pattern_is_rejected_with_offset_on_both_engines() {
    let pattern = "a".repeat(100);
    for engine in BOTH_ENGINES {
        let err = with_limits(
            &pattern,
            engine,
            Limits {
                max_pattern_length: 64,
                ..Limits::default()
            },
        )
        .unwrap_err();
        match err {
            CompileError::PatternTooLong { len, limit, offset } => {
                assert_eq!((len, limit, offset), (100, 64, 64), "{engine:?}");
            }
            other => panic!("{engine:?}: expected PatternTooLong, got {other:?}"),
        }
    }
}

#[test]
fn deeply_nested_parens_are_rejected_with_offset_on_both_engines() {
    // Depth 4 opens at byte 3; classes and escapes do not count.
    let pattern = r"(((([\(]\(a))))";
    for engine in BOTH_ENGINES {
        let err = with_limits(
            pattern,
            engine,
            Limits {
                parens_nest_limit: 3,
                ..Limits::default()
            },
        )
        .unwrap_err();
        match err {
            CompileError::ParensTooDeep { limit, offset } => {
                assert_eq!((limit, offset), (3, 3), "{engine:?}");
            }
            other => panic!("{engine:?}: expected ParensTooDeep, got {other:?}"),
        }
        assert!(
            with_limits(
                pattern,
                engine,
                Limits {
                    parens_nest_limit: 4,
                    ..Limits::default()
                }
            )
            .is_ok()
        );
    }
}

#[test]
fn oversize_pattern_offset_lands_on_a_char_boundary() {
    // Byte 8 is inside the three-byte `€`, so the offset backs up to byte 7.
    let pattern = "abcdefg€hij";
    let err = with_limits(
        pattern,
        EngineChoice::Auto,
        Limits {
            max_pattern_length: 8,
            ..Limits::default()
        },
    )
    .unwrap_err();
    match err {
        CompileError::PatternTooLong { offset, .. } => {
            assert_eq!(offset, 7);
            assert!(pattern.is_char_boundary(offset));
        }
        other => panic!("expected PatternTooLong, got {other:?}"),
    }
}

#[test]
fn linear_only_choice_rejects_backtracking_syntax() {
    let err = with_limits(r"(?<=x)y", EngineChoice::Linear, Limits::default()).unwrap_err();
    assert!(matches!(err, CompileError::Syntax { .. }), "{err:?}");
}

#[test]
fn regex_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Regex>();
}

#[test]
fn compiled_pattern_is_usable_from_several_threads() {
    let re = std::sync::Arc::new(Regex::new(r"(?<=x)(?<n>\d+)").unwrap());
    let handles: Vec<_> = (0..8)
        .map(|i| {
            let re = std::sync::Arc::clone(&re);
            std::thread::spawn(move || {
                let hay = format!("x{i}");
                re.captures(&hay)
                    .unwrap()
                    .unwrap()
                    .name("n")
                    .unwrap()
                    .to_owned()
            })
        })
        .collect();
    for (i, h) in handles.into_iter().enumerate() {
        assert_eq!(h.join().unwrap(), i.to_string());
    }
}

#[test]
fn linear_pattern_too_big_to_compile_has_its_own_variant() {
    // The linear engine's compiled program for this bounded repetition of a large class
    // exceeds the crate's default size limit; PCRE2 compiles it without complaint.
    let err = with_limits(r"\pL{1000}", EngineChoice::Linear, Limits::default()).unwrap_err();
    assert!(
        matches!(err, CompileError::CompiledTooBig { .. }),
        "{err:?}"
    );
    assert!(with_limits(r"\pL{1000}", EngineChoice::Auto, Limits::default()).is_ok());
}
