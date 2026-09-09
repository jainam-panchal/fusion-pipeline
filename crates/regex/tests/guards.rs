//! Compile-time guards, thread-safety and the no-JIT invariant.
#![allow(clippy::unwrap_used)]

use fusion_regex::{CompileError, EngineChoice, Limits, Options, Regex};

fn with_limits(pattern: &str, engine: EngineChoice, limits: Limits) -> Result<Regex, CompileError> {
    Regex::with_options(pattern, &Options { limits, engine, ..Options::unchecked() })
}

#[test]
fn oversize_pattern_is_rejected_with_offset_on_both_engines() {
    let pattern = "a".repeat(100);
    for engine in [EngineChoice::Linear, EngineChoice::Backtracking] {
        let err = with_limits(&pattern, engine, Limits { max_pattern_length: 64, ..Limits::default() })
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
    for engine in [EngineChoice::Linear, EngineChoice::Backtracking] {
        let err = with_limits(pattern, engine, Limits { parens_nest_limit: 3, ..Limits::default() })
            .unwrap_err();
        match err {
            CompileError::ParensTooDeep { limit, offset } => {
                assert_eq!((limit, offset), (3, 3), "{engine:?}");
            }
            other => panic!("{engine:?}: expected ParensTooDeep, got {other:?}"),
        }
        assert!(with_limits(pattern, engine, Limits { parens_nest_limit: 4, ..Limits::default() }).is_ok());
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
                re.captures(&hay).unwrap().unwrap().name("n").unwrap().to_owned()
            })
        })
        .collect();
    for (i, h) in handles.into_iter().enumerate() {
        assert_eq!(h.join().unwrap(), i.to_string());
    }
}

#[test]
fn jit_is_never_referenced_by_the_wrapper() {
    let src_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src");
    let mut seen = 0;
    for entry in std::fs::read_dir(src_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "rs") {
            let text = std::fs::read_to_string(&path).unwrap();
            assert!(!text.contains("pcre2_jit"), "{} references the JIT", path.display());
            seen += 1;
        }
    }
    assert!(seen > 0);
}
