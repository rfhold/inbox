use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tonic::Status;
use uuid::Uuid;
pub const MAX_RAW: usize = 25 * 1024 * 1024;
pub fn local(s: &str) -> Result<String, Status> {
    if s.is_empty()
        || s.len() > 64
        || s.starts_with('.')
        || s.ends_with('.')
        || s.contains("..")
        || !s
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-+".contains(&c))
    {
        return Err(Status::invalid_argument("invalid local part"));
    }
    Ok(s.to_ascii_lowercase())
}
pub fn domain(s: &str) -> Result<String, Status> {
    if s.is_empty()
        || s.len() > 253
        || !s.split('.').all(|l| {
            !l.is_empty()
                && !l.starts_with('-')
                && !l.ends_with('-')
                && l.len() <= 63
                && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
    {
        return Err(Status::invalid_argument("invalid domain"));
    }
    Ok(s.to_ascii_lowercase())
}
pub fn normalized_address(s: &str) -> Result<String, Status> {
    let (l, d) = s
        .split_once('@')
        .ok_or_else(|| Status::invalid_argument("invalid recipient"))?;
    Ok(format!("{}@{}", local(l)?, domain(d)?))
}
pub fn address(s: &str, domain: &str) -> Result<String, Status> {
    let domain = self::domain(domain)?;
    let (l, d) = s
        .split_once('@')
        .ok_or_else(|| Status::invalid_argument("invalid recipient"))?;
    if !d.eq_ignore_ascii_case(&domain) {
        return Err(Status::invalid_argument("only local recipients accepted"));
    }
    Ok(format!("{}@{}", local(l)?, domain))
}
pub fn header(s: &str) -> Result<(), Status> {
    if s.contains(['\r', '\n', '\0']) || s.len() > 8192 {
        Err(Status::invalid_argument("invalid header"))
    } else {
        Ok(())
    }
}
pub fn id(s: &str) -> Result<Uuid, Status> {
    s.parse()
        .map_err(|_| Status::invalid_argument("invalid identifier"))
}
pub fn page(n: i32) -> Result<i64, Status> {
    match n {
        0 => Ok(50),
        1..=100 => Ok(n.into()),
        _ => Err(Status::invalid_argument("invalid page size")),
    }
}
#[derive(Serialize, Deserialize)]
pub struct Cursor {
    pub query: String,
    pub at: DateTime<Utc>,
    pub id: Uuid,
}
pub fn decode(s: &str, q: &str) -> Result<Option<Cursor>, Status> {
    if s.is_empty() {
        return Ok(None);
    }
    if s.len() > 2048 {
        return Err(Status::invalid_argument("invalid page token"));
    }
    let c: Cursor = URL_SAFE_NO_PAD
        .decode(s)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .ok_or_else(|| Status::invalid_argument("invalid page token"))?;
    if c.query != q {
        return Err(Status::invalid_argument("page token query mismatch"));
    }
    Ok(Some(c))
}
pub fn encode(q: &str, at: DateTime<Utc>, id: Uuid) -> String {
    URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&Cursor {
            query: q.into(),
            at,
            id,
        })
        .expect("cursor serialization"),
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_addresses() {
        assert_eq!(
            address("Alice@INBOX.TEST", "inbox.test").unwrap(),
            "alice@inbox.test"
        );
        for s in ["a@else.test", "x\r\n@inbox.test", "x@@inbox.test"] {
            assert!(address(s, "inbox.test").is_err())
        }
    }
    #[test]
    fn validates_and_normalizes_domains() {
        assert_eq!(domain("Mail.EXAMPLE").unwrap(), "mail.example");
        assert_eq!(
            normalized_address("Alice@MAIL.Example").unwrap(),
            "alice@mail.example"
        );
        for s in [
            "",
            "*.test",
            "mail.test.",
            "mail.test:25",
            "é.test",
            "-a.test",
            "a-.test",
            "a..test",
            "a@b.test",
        ] {
            assert!(domain(s).is_err(), "{s}");
        }
        assert!(domain(&format!("{}.test", "a".repeat(64))).is_err());
        assert!(domain(&vec!["a".repeat(63); 5].join(".")).is_err());
    }
    #[test]
    fn pagination_is_bound() {
        let t = encode("a", Utc::now(), Uuid::new_v4());
        assert!(decode(&t, "a").unwrap().is_some());
        assert!(decode(&t, "b").is_err());
        assert!(page(-1).is_err());
        assert_eq!(page(0).unwrap(), 50)
    }
    #[test]
    fn blocks_injection() {
        assert!(header("ok\r\nBcc: bad").is_err());
        assert!(header("hello").is_ok())
    }
}
