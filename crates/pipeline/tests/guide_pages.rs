//! The guide under `docs/guide/` keeps up with the code: every stage type the binary
//! registers has a page in the book, every file a page includes exists, and every example
//! file is shown on some page.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use fusion_pipeline::default_registry;

fn guide_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/guide")
        .canonicalize()
        .expect("docs/guide exists")
}

/// `path` relative to the guide folder, for messages.
fn shown(path: &Path) -> String {
    let guide = guide_dir();
    path.strip_prefix(&guide)
        .unwrap_or(path)
        .display()
        .to_string()
}

/// Every file under `dir` with extension `ext`, recursively.
fn files_with_extension(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|err| panic!("{} is readable: {err}", dir.display()));
        for entry in entries {
            let path = entry.expect("directory entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|e| e == ext) {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

/// The link targets of `SUMMARY.md`, as written: `[title](target)`.
fn summary_links() -> Vec<String> {
    let summary = std::fs::read_to_string(guide_dir().join("src/SUMMARY.md"))
        .expect("docs/guide/src/SUMMARY.md is readable");
    summary
        .split("](")
        .skip(1)
        .filter_map(|rest| rest.split_once(')').map(|(target, _)| target.to_owned()))
        .collect()
}

/// The paths of every `{{#include path}}` in `page`, resolved against the page's folder.
/// An anchor or line range after `:` is not part of the path.
fn includes(page: &Path) -> Vec<PathBuf> {
    let text = std::fs::read_to_string(page).expect("page is readable");
    let dir = page.parent().expect("page has a folder");
    text.split("{{#include ")
        .skip(1)
        .filter_map(|rest| rest.split_once("}}").map(|(arg, _)| arg.trim()))
        .map(|arg| normalize(&dir.join(arg.split(':').next().unwrap_or(arg))))
        .collect()
}

/// `path` with its `..` parts folded, without touching the file system, so a missing file
/// still has a readable name.
fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for part in path.components() {
        match part {
            std::path::Component::ParentDir => {
                out.pop();
            }
            std::path::Component::CurDir => {}
            other => out.push(other),
        }
    }
    out
}

/// A stage type's page is a `SUMMARY.md` entry under `stages/` whose file is named after the
/// type (`stages/filter.md`, `stages/regex/extract.md`) or sits in a folder named after it
/// (`stages/edit/README.md`).
#[test]
fn every_registered_stage_type_has_a_page_in_the_summary() {
    let pages: Vec<PathBuf> = summary_links()
        .iter()
        .filter(|target| target.starts_with("stages/"))
        .map(PathBuf::from)
        .collect();
    let missing: Vec<String> = default_registry()
        .stage_kinds()
        .filter(|kind| {
            !pages.iter().any(|page| {
                page.file_stem().is_some_and(|stem| stem == *kind)
                    || page.components().any(|part| part.as_os_str() == *kind)
            })
        })
        .map(str::to_owned)
        .collect();
    assert!(
        missing.is_empty(),
        "stage types with no page under stages/ in docs/guide/src/SUMMARY.md: {missing:?}"
    );
}

#[test]
fn every_included_file_exists() {
    let missing: Vec<String> = files_with_extension(&guide_dir().join("src"), "md")
        .iter()
        .flat_map(|page| {
            includes(page)
                .into_iter()
                .filter(|path| !path.is_file())
                .map(move |path| format!("{} includes {}", shown(page), shown(&path)))
        })
        .collect();
    assert!(missing.is_empty(), "\n{}", missing.join("\n"));
}

#[test]
fn every_example_is_shown_on_a_page() {
    let guide = guide_dir();
    let included: BTreeSet<PathBuf> = files_with_extension(&guide.join("src"), "md")
        .iter()
        .flat_map(|page| includes(page))
        .collect();
    let unshown: Vec<String> = files_with_extension(&guide.join("examples"), "yaml")
        .into_iter()
        .filter(|file| !included.contains(file))
        .map(|file| shown(&file))
        .collect();
    assert!(
        unshown.is_empty(),
        "example files no page includes:\n{}",
        unshown.join("\n")
    );
}
