use crate::{
    LanguageError,
    token::{Token, TokenKind},
};

const MAX_SOURCE_BYTES: usize = 1_048_576;
const MAX_TOKENS: usize = 32_768;

pub(crate) fn lex(source: &str) -> Result<Vec<Token>, LanguageError> {
    if source.len() > MAX_SOURCE_BYTES {
        return Err(LanguageError::limit(
            "source exceeds the 1 MiB language limit",
            0,
            source.len(),
        ));
    }
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
            continue;
        }
        let start = cursor;
        let token = match bytes[cursor] {
            b'$' => {
                cursor += 1;
                let name_start = cursor;
                while cursor < bytes.len()
                    && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
                {
                    cursor += 1;
                }
                if name_start == cursor {
                    return Err(LanguageError::lex(
                        "parameter name expected after '$'",
                        start,
                        cursor,
                    ));
                }
                TokenKind::Parameter(source[name_start..cursor].to_owned())
            }
            b'0'..=b'9' | b'-'
                if cursor + 1 < bytes.len() && bytes[cursor + 1].is_ascii_digit()
                    || bytes[cursor].is_ascii_digit() =>
            {
                if bytes[cursor] == b'-' {
                    cursor += 1;
                }
                while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
                    cursor += 1;
                }
                let value = source[start..cursor].parse().map_err(|_| {
                    LanguageError::lex("integer literal is out of range", start, cursor)
                })?;
                TokenKind::Integer(value)
            }
            b'\'' | b'\"' => {
                let quote = bytes[cursor];
                cursor += 1;
                let mut value = String::new();
                let mut terminated = false;
                while cursor < bytes.len() {
                    if bytes[cursor] == quote {
                        cursor += 1;
                        terminated = true;
                        break;
                    }
                    if bytes[cursor] == b'\\' && cursor + 1 < bytes.len() {
                        cursor += 1;
                    }
                    value.push(bytes[cursor] as char);
                    cursor += 1;
                }
                if !terminated {
                    return Err(LanguageError::lex(
                        "unterminated string literal",
                        start,
                        cursor,
                    ));
                }
                TokenKind::String(value)
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                cursor += 1;
                while cursor < bytes.len()
                    && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
                {
                    cursor += 1;
                }
                TokenKind::Word(source[start..cursor].to_owned())
            }
            b'-' if cursor + 1 < bytes.len() && bytes[cursor + 1] == b'>' => {
                cursor += 2;
                TokenKind::ArrowRight
            }
            b'<' if cursor + 1 < bytes.len() && bytes[cursor + 1] == b'-' => {
                cursor += 2;
                TokenKind::ArrowLeft
            }
            b'(' | b')' | b'[' | b']' | b'{' | b'}' | b':' | b',' | b'.' | b'=' | b'*' | b'+'
            | b'-' | b'/' | b'!' => {
                cursor += 1;
                TokenKind::Symbol(bytes[start] as char)
            }
            _ => {
                return Err(LanguageError::lex(
                    "unsupported character",
                    start,
                    start + 1,
                ));
            }
        };
        tokens.push(Token {
            kind: token,
            start,
            end: cursor,
        });
        if tokens.len() > MAX_TOKENS {
            return Err(LanguageError::limit(
                "token count exceeds language limit",
                start,
                cursor,
            ));
        }
    }
    tokens.push(Token {
        kind: TokenKind::End,
        start: source.len(),
        end: source.len(),
    });
    Ok(tokens)
}
