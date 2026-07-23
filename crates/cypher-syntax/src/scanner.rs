use std::error::Error;
use std::fmt::{self, Display, Formatter};

use cypher_ast::CypherProfile;

const CYPHER_KEYWORD: &str = "CYPHER";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VersionScan<'query> {
    profile: CypherProfile,
    body: &'query str,
    body_offset: usize,
}

impl<'query> VersionScan<'query> {
    #[must_use]
    pub const fn profile(self) -> CypherProfile {
        self.profile
    }

    #[must_use]
    pub const fn body(self) -> &'query str {
        self.body
    }

    #[must_use]
    pub const fn body_offset(self) -> usize {
        self.body_offset
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionScanError {
    code: &'static str,
    offset: usize,
}

impl VersionScanError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    #[must_use]
    pub const fn offset(&self) -> usize {
        self.offset
    }
}

impl Display for VersionScanError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} at byte {}: only Cypher 25 is supported",
            self.code, self.offset
        )
    }
}

impl Error for VersionScanError {}

pub fn scan_version(query: &str) -> Result<VersionScan<'_>, VersionScanError> {
    let leading = query.len() - query.trim_start_matches(char::is_whitespace).len();
    let candidate = &query[leading..];
    let Some(after_keyword) = strip_ascii_keyword(candidate, CYPHER_KEYWORD) else {
        return Ok(VersionScan {
            profile: CypherProfile::default(),
            body: candidate,
            body_offset: leading,
        });
    };
    let version_leading =
        after_keyword.len() - after_keyword.trim_start_matches(char::is_whitespace).len();
    let version_text = &after_keyword[version_leading..];
    let version_end = version_text
        .find(char::is_whitespace)
        .unwrap_or(version_text.len());
    let version = &version_text[..version_end];
    let profile = match version {
        "25" => CypherProfile::cypher_25(),
        _ => {
            return Err(VersionScanError {
                code: "DTG-CYPHER-UNSUPPORTED-VERSION",
                offset: leading + CYPHER_KEYWORD.len() + version_leading,
            });
        }
    };
    let body_candidate = &version_text[version_end..];
    let body_leading =
        body_candidate.len() - body_candidate.trim_start_matches(char::is_whitespace).len();
    let body = &body_candidate[body_leading..];
    let body_offset = query.len() - body.len();
    Ok(VersionScan {
        profile,
        body,
        body_offset,
    })
}

fn strip_ascii_keyword<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
    let prefix = text.get(..keyword.len())?;
    if !prefix.eq_ignore_ascii_case(keyword) {
        return None;
    }
    let remainder = &text[keyword.len()..];
    if remainder
        .chars()
        .next()
        .is_some_and(|character| !character.is_whitespace())
    {
        return None;
    }
    Some(remainder)
}
