//! `closed_set!`: one declaration for a closed set of named values.
//!
//! A closed set (a drop reason, a metric, a label value, a record kind, a path root) is an
//! enum whose values each have one name. Written by hand, the enum, its `ALL` list and its
//! `as_str` match are three lists, and the compiler checks only the match: a variant left out
//! of `ALL` compiles, and whatever iterates `ALL` (the OTLP recorder creating one instrument
//! per metric, a name lookup) silently misses it. Declared through [`closed_set!`], all of
//! them come from one list, so a variant cannot be left out of any.

/// Declares a closed set: the enum, and on it `ALL` (every value, in declaration order, each
/// at the index its discriminant says),
/// `as_str` (the value's name), `parse` (the value with a name, case-sensitive), `ONE_OF`
/// (every name as a message lists them, "`a`, `b` or `c`") and `Display` (the name). The
/// generated items take the enum's visibility. Two values with one name fail to compile.
///
/// ```ignore
/// closed_set! {
///     /// Why a record was dropped.
///     #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
///     pub enum DropReason {
///         /// A `filter` node dropped it.
///         Filter = "filter",
///     }
/// }
/// ```
///
/// Enum-level attributes pass through (derives, `#[non_exhaustive]`). A variant takes doc
/// comments only: an attribute such as `#[serde(alias)]` could give a value a spelling
/// `as_str` and `parse` do not know. For an enum that derives serde, start with `serde;` and
/// each variant is also `#[serde(rename = name)]`, so its JSON form is its name.
macro_rules! closed_set {
    (@one_of $only:literal) => {
        concat!("`", $only, "`")
    };
    (@one_of $first:literal, $last:literal) => {
        concat!("`", $first, "` or `", $last, "`")
    };
    (@one_of $first:literal, $($rest:literal),+) => {
        concat!("`", $first, "`, ", $crate::closed_set::closed_set!(@one_of $($rest),+))
    };
    (
        serde;
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $($(#[doc = $doc:literal])* $variant:ident = $wire:literal,)+
        }
    ) => {
        $(#[$meta])*
        $vis enum $name {
            $($(#[doc = $doc])* #[serde(rename = $wire)] $variant,)+
        }
        $crate::closed_set::closed_set!(@impl $vis $name; $($variant = $wire),+);
    };
    (@impl $vis:vis $name:ident; $($variant:ident = $wire:literal),+) => {
        impl $name {
            /// Every value, in declaration order.
            $vis const ALL: [Self; [$($wire),+].len()] = [$(Self::$variant),+];

            /// The value's name.
            #[must_use]
            $vis const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire,)+
                }
            }

            /// Every name, as a message lists them: "`a`, `b` or `c`".
            #[allow(dead_code, reason = "not every closed set lists its names in a message")]
            $vis const ONE_OF: &'static str =
                $crate::closed_set::closed_set!(@one_of $($wire),+);

            /// The value named `name`, the inverse of `as_str`. Case-sensitive.
            #[must_use]
            $vis fn parse(name: &str) -> ::core::option::Option<Self> {
                Self::ALL.into_iter().find(|value| value.as_str() == name)
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        const _: () = $crate::closed_set::assert_unique(&[$($wire),+]);

        // Every value is its own index into `ALL`, so a table built from `ALL` can be indexed
        // by the value.
        const _: () = {
            let mut i = 0;
            while i < $name::ALL.len() {
                assert!($name::ALL[i] as usize == i, "a closed set's ALL is in declaration order");
                i += 1;
            }
        };
    };
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $($(#[doc = $doc:literal])* $variant:ident = $wire:literal,)+
        }
    ) => {
        $(#[$meta])*
        $vis enum $name {
            $($(#[doc = $doc])* $variant,)+
        }
        $crate::closed_set::closed_set!(@impl $vis $name; $($variant = $wire),+);
    };
}

pub(crate) use closed_set;

/// Fails const evaluation, and so the build, when two of `names` are equal.
pub(crate) const fn assert_unique(names: &[&str]) {
    let mut i = 0;
    while i < names.len() {
        let mut j = i + 1;
        while j < names.len() {
            assert!(
                !same(names[i], names[j]),
                "two values of a closed set share a name"
            );
            j += 1;
        }
        i += 1;
    }
}

const fn same(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}
