//! Canonical Locks identifiers used by Paykit domain values.
//!
//! Grammar parsing and canonicalization are delegated exclusively to the pinned
//! `locks-core` package. These boundary parsers reject inputs that `locks-core`
//! accepts by normalizing them, then return opaque canonical domain values.

use std::{fmt, fmt::Display, str::FromStr};

use locks_core::ids::{
    BundleId as RawBundleId, CreatorPubky as RawCreatorPubky,
    PubkyLockResource as RawPubkyLockResource,
};
use thiserror::Error;

/// A canonical creator identity accepted at the Paykit domain boundary.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CreatorPubky(RawCreatorPubky);

impl fmt::Debug for CreatorPubky {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CreatorPubky(<redacted>)")
    }
}

impl Display for CreatorPubky {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A canonical reader identity accepted at the Paykit domain boundary.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ReaderPubky(RawCreatorPubky);

impl fmt::Debug for ReaderPubky {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReaderPubky(<redacted>)")
    }
}

impl Display for ReaderPubky {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A canonical Locks bundle identifier accepted at the Paykit domain boundary.
///
/// Its [`std::fmt::Debug`] implementation always redacts the bearer secret.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BundleId(RawBundleId);

impl fmt::Debug for BundleId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BundleId(<redacted>)")
    }
}

impl Display for BundleId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A canonical addressed Locks resource accepted at the Paykit domain boundary.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PubkyLockResource {
    raw: RawPubkyLockResource,
    creator: CreatorPubky,
}

impl fmt::Debug for PubkyLockResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PubkyLockResource(<redacted>)")
    }
}

impl PubkyLockResource {
    /// Returns the canonical creator addressed by this resource.
    pub fn creator(&self) -> &CreatorPubky {
        &self.creator
    }
}

impl Display for PubkyLockResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.raw.fmt(formatter)
    }
}

/// Parses an externally supplied creator identifier in its exact canonical form.
pub fn parse_creator(value: &str) -> Result<CreatorPubky, LocksIdentifierParseError> {
    parse_canonical("creator", value).map(CreatorPubky)
}

/// Parses an externally supplied reader identifier in its exact canonical form.
pub fn parse_reader(value: &str) -> Result<ReaderPubky, LocksIdentifierParseError> {
    parse_canonical("reader", value).map(ReaderPubky)
}

/// Parses an externally supplied bundle identifier in its exact canonical form.
pub fn parse_bundle_id(value: &str) -> Result<BundleId, LocksIdentifierParseError> {
    parse_canonical("bundle", value).map(BundleId)
}

/// Parses an externally supplied addressed lock resource in its exact canonical form.
pub fn parse_addressed_lock_resource(
    value: &str,
) -> Result<PubkyLockResource, LocksIdentifierParseError> {
    let raw: RawPubkyLockResource = parse_canonical("addressed lock resource", value)?;
    let creator = CreatorPubky(raw.creator().clone());

    Ok(PubkyLockResource { raw, creator })
}

fn parse_canonical<T>(kind: &'static str, value: &str) -> Result<T, LocksIdentifierParseError>
where
    T: FromStr + Display,
{
    let parsed = T::from_str(value).map_err(|_| LocksIdentifierParseError::Invalid { kind })?;
    if parsed.to_string() != value {
        return Err(LocksIdentifierParseError::NonCanonical { kind });
    }

    Ok(parsed)
}

/// Error returned while parsing an external Locks identifier at the domain boundary.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum LocksIdentifierParseError {
    /// The supplied value is not accepted by the `locks-core` identifier parser.
    #[error("invalid {kind} identifier")]
    Invalid { kind: &'static str },
    /// The supplied value is valid but differs from its `locks-core` canonical rendering.
    #[error("non-canonical {kind} identifier")]
    NonCanonical { kind: &'static str },
}
