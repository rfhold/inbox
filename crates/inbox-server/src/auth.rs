//! Authentication protocol state is the only direct-write runtime exception.
use axum::{
    Json, Router,
    extract::{Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tonic::Status;
use url::Url;
use uuid::Uuid;

pub const LOCAL_USER: Uuid = Uuid::from_u128(0x00000000000040008000000000000001);
const COOKIE: &str = "__Host-inbox-session";
const LOGIN_COOKIE: &str = "__Host-inbox-login";

#[derive(Clone, Serialize, Deserialize)]
pub struct Actor {
    pub user_id: Uuid,
    pub administrator: bool,
}
impl Actor {
    pub fn local() -> Self {
        Self {
            user_id: LOCAL_USER,
            administrator: true,
        }
    }
    pub fn require_admin(&self) -> Result<(), Status> {
        if self.administrator {
            Ok(())
        } else {
            Err(Status::permission_denied("administrator required"))
        }
    }
}

#[derive(Clone)]
pub struct Principal {
    pub actor: Actor,
    pub expires_at: DateTime<Utc>,
    session_hash: Option<String>,
}
impl Principal {
    pub fn delegated(actor: Actor, expires_at: DateTime<Utc>) -> Self {
        Self {
            actor,
            expires_at,
            session_hash: None,
        }
    }
    pub fn local() -> Self {
        Self {
            actor: Actor::local(),
            expires_at: DateTime::<Utc>::MAX_UTC,
            session_hash: None,
        }
    }
    pub async fn valid(&self, auth: &Auth) -> bool {
        if self.expires_at <= Utc::now() {
            return false;
        }
        match (&self.session_hash, &auth.inner) {
            (Some(hash), Some(a)) => sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM inbox_sessions WHERE token_hash=$1 AND owner_id=$2 AND issuer=$3 AND client_id=$4 AND expires_at>now())")
                .bind(hash).bind(self.actor.user_id).bind(&a.config.issuer).bind(&a.config.client_id)
                .fetch_one(&a.db).await.unwrap_or(false),
            _ => true,
        }
    }
    pub async fn ended(&self, auth: &Auth) {
        loop {
            let remaining = (self.expires_at - Utc::now()).to_std().unwrap_or_default();
            if !tokio::time::timeout(remaining.min(Duration::from_secs(1)), self.valid(auth))
                .await
                .unwrap_or(false)
            {
                return;
            }
            let remaining = (self.expires_at - Utc::now()).to_std().unwrap_or_default();
            tokio::time::sleep(Duration::from_secs(1).min(remaining)).await;
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub public_url: String,
    pub issuer: String,
    pub client_id: String,
    pub client_secret: String,
    pub api_audience: String,
    pub api_client_id: String,
    pub admin_group: String,
    pub scopes: Vec<String>,
    pub wrapping_keys_file: String,
}
impl Config {
    pub fn from_env() -> Result<Option<Self>, &'static str> {
        Self::from_values(std::env::vars().collect())
    }
    fn from_values(values: HashMap<String, String>) -> Result<Option<Self>, &'static str> {
        let mode = values
            .get("INBOX_AUTH_MODE")
            .map(String::as_str)
            .unwrap_or("disabled");
        if mode == "disabled" {
            if values.keys().any(|k| {
                k.starts_with("INBOX_OIDC_")
                    || k.starts_with("INBOX_OAUTH_")
                    || (k.starts_with("INBOX_AUTH_") && k != "INBOX_AUTH_MODE")
            }) {
                return Err("OIDC settings present with authentication disabled");
            }
            return Ok(None);
        }
        if mode != "oidc" {
            return Err("invalid authentication mode");
        }
        let required = |key| {
            values
                .get(key)
                .filter(|v| !v.is_empty() && v.trim() == *v && !v.chars().any(char::is_control))
                .cloned()
                .ok_or("missing or invalid authentication setting")
        };
        let c = Self {
            public_url: required("INBOX_PUBLIC_URL")?,
            issuer: required("INBOX_OIDC_ISSUER")?,
            client_id: required("INBOX_OIDC_CLIENT_ID")?,
            client_secret: required("INBOX_OIDC_CLIENT_SECRET")?,
            api_audience: required("INBOX_OIDC_API_AUDIENCE")?,
            api_client_id: required("INBOX_OIDC_API_CLIENT_ID")?,
            admin_group: required("INBOX_AUTH_ADMIN_GROUP")?,
            scopes: values
                .get("INBOX_OIDC_API_SCOPES")
                .map(String::as_str)
                .unwrap_or("inbox:api")
                .split_ascii_whitespace()
                .map(str::to_owned)
                .collect(),
            wrapping_keys_file: required("INBOX_OAUTH_WRAPPING_KEYS_FILE")?,
        };
        let public = secure_url(&c.public_url)?;
        secure_url(&c.issuer)?;
        if public.path() != "/" || c.public_url.ends_with('/') || c.scopes.is_empty() {
            return Err("public URL must be an HTTPS origin without trailing slash");
        }
        Ok(Some(c))
    }
}
fn secure_url(s: &str) -> Result<Url, &'static str> {
    let u = Url::parse(s).map_err(|_| "invalid HTTPS endpoint")?;
    if u.scheme() != "https"
        || u.host_str().is_none()
        || s.split("://")
            .nth(1)
            .and_then(|s| s.split('/').next())
            .is_some_and(|s| s.contains('@'))
        || !u.username().is_empty()
        || u.password().is_some()
        || u.query().is_some()
        || u.fragment().is_some()
    {
        return Err("invalid HTTPS endpoint");
    }
    Ok(u)
}

#[derive(Clone, Default)]
pub struct Auth {
    inner: Option<Arc<Enabled>>,
}
struct Enabled {
    config: Config,
    db: PgPool,
    client: reqwest::Client,
    provider: Provider,
    keys: tokio::sync::RwLock<(JwkSet, std::time::Instant)>,
    cursor_key: Vec<u8>,
    logins: tokio::sync::Semaphore,
}
#[derive(Deserialize)]
struct Provider {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
    code_challenge_methods_supported: Vec<String>,
    response_types_supported: Vec<String>,
    token_endpoint_auth_methods_supported: Vec<String>,
    id_token_signing_alg_values_supported: Vec<String>,
}
impl Auth {
    pub async fn initialize(db: PgPool, config: Option<Config>) -> Result<Self, &'static str> {
        let Some(config) = config else {
            return Ok(Self::default());
        };
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| "authentication HTTP client unavailable")?;
        let provider: Provider = bounded_json(
            client
                .get(format!(
                    "{}/.well-known/openid-configuration",
                    config.issuer.trim_end_matches('/')
                ))
                .send()
                .await
                .map_err(|_| "OIDC discovery unavailable")?,
        )
        .await?;
        if provider.issuer != config.issuer
            || !provider
                .response_types_supported
                .iter()
                .any(|v| v == "code")
            || !provider
                .token_endpoint_auth_methods_supported
                .iter()
                .any(|v| v == "client_secret_basic")
            || !provider
                .code_challenge_methods_supported
                .iter()
                .any(|v| v == "S256")
        {
            return Err("OIDC provider incompatible");
        }
        for endpoint in [
            &provider.authorization_endpoint,
            &provider.token_endpoint,
            &provider.jwks_uri,
        ] {
            secure_url(endpoint)?;
        }
        let keys = bounded_json(
            client
                .get(&provider.jwks_uri)
                .send()
                .await
                .map_err(|_| "OIDC keys unavailable")?,
        )
        .await?;
        let mut key = vec![0; 32];
        getrandom::fill(&mut key).map_err(|_| "entropy unavailable")?;
        sqlx::query("INSERT INTO inbox_auth_keys(purpose,secret) VALUES('cursor',$1) ON CONFLICT DO NOTHING").bind(&key).execute(&db).await.map_err(|_| "authentication persistence unavailable")?;
        let cursor_key =
            sqlx::query_scalar("SELECT secret FROM inbox_auth_keys WHERE purpose='cursor'")
                .fetch_one(&db)
                .await
                .map_err(|_| "authentication persistence unavailable")?;
        Ok(Self {
            inner: Some(Arc::new(Enabled {
                config,
                db,
                client,
                provider,
                keys: tokio::sync::RwLock::new((keys, std::time::Instant::now())),
                cursor_key,
                logins: tokio::sync::Semaphore::new(16),
            })),
        })
    }
    pub fn enabled(&self) -> bool {
        self.inner.is_some()
    }
    pub fn config(&self) -> Option<&Config> {
        self.inner.as_ref().map(|a| &a.config)
    }
    pub fn router(&self) -> Router {
        Router::new()
            .route("/auth/session", get(session))
            .route("/auth/login", get(login))
            .route("/auth/callback", get(callback))
            .route("/auth/logout", post(logout))
            .with_state(self.clone())
    }
    pub async fn browser(&self, headers: &HeaderMap) -> Result<Principal, Status> {
        let Some(a) = &self.inner else {
            return Ok(Principal::local());
        };
        let hash = crate::mutation::hash(cookie(headers, COOKIE).ok_or_else(unauth)?.as_bytes());
        let row = sqlx::query("SELECT s.owner_id,s.administrator AND u.administrator AS administrator,s.expires_at FROM inbox_sessions s JOIN inbox_users u ON u.id=s.owner_id WHERE token_hash=$1 AND s.issuer=$2 AND s.client_id=$3 AND expires_at>now()")
            .bind(&hash).bind(&a.config.issuer).bind(&a.config.client_id).fetch_optional(&a.db).await.map_err(crate::mutation::storage)?.ok_or_else(unauth)?;
        Ok(Principal {
            actor: Actor {
                user_id: row.get("owner_id"),
                administrator: row.get("administrator"),
            },
            expires_at: row.get("expires_at"),
            session_hash: Some(hash),
        })
    }
    async fn verify(
        &self,
        token: &str,
        audience: &str,
        nonce: Option<&str>,
    ) -> Result<Value, Status> {
        let a = self.inner.as_ref().ok_or_else(unauth)?;
        if token.len() > 32 * 1024 {
            return Err(unauth());
        }
        let header = decode_header(token).map_err(|_| unauth())?;
        if nonce.is_none() && header.typ.as_deref() != Some("JWT") {
            return Err(unauth());
        }
        if !matches!(
            header.alg,
            Algorithm::RS256
                | Algorithm::RS384
                | Algorithm::RS512
                | Algorithm::PS256
                | Algorithm::PS384
                | Algorithm::PS512
                | Algorithm::ES256
                | Algorithm::ES384
                | Algorithm::EdDSA
        ) {
            return Err(unauth());
        }
        let kid = header.kid.ok_or_else(unauth)?;
        if nonce.is_some()
            && !a
                .provider
                .id_token_signing_alg_values_supported
                .contains(&format!("{:?}", header.alg))
        {
            return Err(unauth());
        }
        let mut cache = a.keys.write().await;
        if cache.1.elapsed() > Duration::from_secs(300)
            || (cache.1.elapsed() > Duration::from_secs(30)
                && !cache
                    .0
                    .keys
                    .iter()
                    .any(|k| k.common.key_id.as_deref() == Some(&kid)))
        {
            cache.0 = bounded_json(
                a.client
                    .get(&a.provider.jwks_uri)
                    .send()
                    .await
                    .map_err(|_| unauth())?,
            )
            .await
            .map_err(|_| unauth())?;
            cache.1 = std::time::Instant::now();
        }
        let jwk = cache
            .0
            .keys
            .iter()
            .find(|k| k.common.key_id.as_deref() == Some(&kid))
            .ok_or_else(unauth)?;
        if cache
            .0
            .keys
            .iter()
            .filter(|k| k.common.key_id.as_deref() == Some(&kid))
            .count()
            != 1
            || jwk
                .common
                .public_key_use
                .as_ref()
                .is_some_and(|usage| *usage != jsonwebtoken::jwk::PublicKeyUse::Signature)
            || jwk
                .common
                .key_operations
                .as_ref()
                .is_some_and(|ops| !ops.contains(&jsonwebtoken::jwk::KeyOperations::Verify))
        {
            return Err(unauth());
        }
        if jwk
            .common
            .key_algorithm
            .is_some_and(|alg| alg.to_string() != format!("{:?}", header.alg))
        {
            return Err(unauth());
        }
        let key = DecodingKey::from_jwk(jwk).map_err(|_| unauth())?;
        let mut validation = Validation::new(header.alg);
        validation.leeway = 0;
        validation.validate_nbf = true;
        validation.set_issuer(&[&a.config.issuer]);
        validation.set_audience(&[audience]);
        validation.set_required_spec_claims(&["iss", "sub", "aud", "exp", "iat"]);
        let claims = decode::<Value>(token, &key, &validation)
            .map_err(|_| unauth())?
            .claims;
        if claims["sub"].as_str().is_none_or(str::is_empty)
            || claims["iat"]
                .as_i64()
                .is_none_or(|t| t > Utc::now().timestamp() + 60)
        {
            return Err(unauth());
        }
        if let Some(nonce) = nonce {
            if claims["nonce"].as_str() != Some(nonce)
                || claims["iat"]
                    .as_i64()
                    .is_none_or(|t| t < Utc::now().timestamp() - 600)
            {
                return Err(unauth());
            }
            if claims
                .get("azp")
                .is_some_and(|v| v.as_str() != Some(a.config.client_id.as_str()))
            {
                return Err(unauth());
            }
            if claims["aud"].as_array().is_some_and(|a| a.len() > 1)
                && claims["azp"].as_str() != Some(a.config.client_id.as_str())
            {
                return Err(unauth());
            }
        } else {
            // Authentik 2025.10.2 adds these only in IDToken.to_access_token().
            // Provider mappings must reserve uid/azp/scope, never add them to ID tokens.
            if claims["uid"]
                .as_str()
                .is_none_or(|v| v.is_empty() || v.len() > 256 || v.chars().any(char::is_control))
                || claims["azp"].as_str() != Some(a.config.api_client_id.as_str())
            {
                return Err(unauth());
            }
            if claims["aud"].as_str() != Some(audience)
                && !claims["aud"]
                    .as_array()
                    .is_some_and(|values| values.len() == 1 && values[0].as_str() == Some(audience))
            {
                return Err(unauth());
            }
            if claims
                .get("client_id")
                .is_some_and(|v| v.as_str() != Some(a.config.api_client_id.as_str()))
                || claims
                    .get("azp")
                    .is_some_and(|v| v.as_str() != Some(a.config.api_client_id.as_str()))
            {
                return Err(unauth());
            }
            if claims["azp"]
                .as_str()
                .or_else(|| claims["client_id"].as_str())
                != Some(a.config.api_client_id.as_str())
            {
                return Err(unauth());
            }
            let scopes: Vec<_> = claims["scope"]
                .as_str()
                .unwrap_or("")
                .split_ascii_whitespace()
                .collect();
            if a.config
                .scopes
                .iter()
                .any(|s| !scopes.contains(&s.as_str()))
            {
                return Err(Status::permission_denied("required scope missing"));
            }
        }
        Ok(claims)
    }
    pub async fn bearer(&self, headers: &HeaderMap) -> Result<Principal, Status> {
        let Some(a) = &self.inner else {
            return Ok(Principal::local());
        };
        if headers.get_all(header::AUTHORIZATION).iter().count() != 1 {
            return Err(unauth());
        }
        let token = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .filter(|v| !v.is_empty() && !v.contains(char::is_whitespace))
            .ok_or_else(unauth)?;
        let claims = self.verify(token, &a.config.api_audience, None).await?;
        let actor = self.identity(&claims).await?;
        Ok(Principal {
            actor,
            expires_at: DateTime::from_timestamp(claims["exp"].as_i64().ok_or_else(unauth)?, 0)
                .ok_or_else(unauth)?,
            session_hash: None,
        })
    }
    pub(crate) async fn identity(&self, claims: &Value) -> Result<Actor, Status> {
        let a = self.inner.as_ref().ok_or_else(unauth)?;
        if claims["iss"].as_str() != Some(a.config.issuer.as_str()) {
            return Err(unauth());
        }
        let subject = claims["sub"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(unauth)?;
        let administrator = administrator_claim(claims, &a.config.admin_group);
        let user_id=sqlx::query_scalar("INSERT INTO inbox_users(id,issuer,subject,administrator) VALUES($1,$2,$3,$4) ON CONFLICT(issuer,subject) DO UPDATE SET administrator=EXCLUDED.administrator RETURNING id")
            .bind(Uuid::new_v4()).bind(&a.config.issuer).bind(subject).bind(administrator).fetch_one(&a.db).await.map_err(crate::mutation::storage)?;
        Ok(Actor {
            user_id,
            administrator,
        })
    }
    pub fn csrf(&self, headers: &HeaderMap) -> bool {
        let Some(a) = &self.inner else { return true };
        headers.get_all(header::ORIGIN).iter().count() == 1
            && headers.get(header::ORIGIN).and_then(|v| v.to_str().ok())
                == Some(a.config.public_url.as_str())
            && headers
                .get("sec-fetch-site")
                .is_some_and(|v| v == "same-origin")
    }
    pub fn bind_cursor(&self, namespace: &str, actor: &Actor, kind: &str, raw: &str) -> String {
        let text = format!("{namespace}:{}:{kind}:{raw}", actor.user_id);
        let Some(a) = &self.inner else {
            return URL_SAFE_NO_PAD.encode(text);
        };
        let mut mac = Hmac::<Sha256>::new_from_slice(&a.cursor_key).expect("HMAC key");
        mac.update(text.as_bytes());
        URL_SAFE_NO_PAD.encode(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(raw),
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        ))
    }
    pub fn unbind_cursor(
        &self,
        namespace: &str,
        actor: &Actor,
        kind: &str,
        token: &str,
    ) -> Result<String, Status> {
        if token.is_empty() {
            return Ok(String::new());
        }
        let invalid = || Status::invalid_argument("invalid cursor");
        if token.len() > 4096 {
            return Err(invalid());
        }
        let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?;
        let text = std::str::from_utf8(&bytes).map_err(|_| invalid())?;
        let Some(a) = &self.inner else {
            return text
                .strip_prefix(&format!("{namespace}:{}:{kind}:", actor.user_id))
                .map(str::to_owned)
                .ok_or_else(invalid);
        };
        let (raw, sig) = text.split_once('.').ok_or_else(invalid)?;
        let raw = String::from_utf8(URL_SAFE_NO_PAD.decode(raw).map_err(|_| invalid())?)
            .map_err(|_| invalid())?;
        let sig = URL_SAFE_NO_PAD.decode(sig).map_err(|_| invalid())?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&a.cursor_key).expect("HMAC key");
        mac.update(format!("{namespace}:{}:{kind}:{raw}", actor.user_id).as_bytes());
        mac.verify_slice(&sig).map_err(|_| invalid())?;
        Ok(raw)
    }
}
pub fn unauth() -> Status {
    Status::unauthenticated("authentication required")
}
fn administrator_claim(claims: &Value, group: &str) -> bool {
    claims["groups"].as_array().is_some_and(|groups| {
        groups.iter().all(Value::is_string) && groups.iter().any(|g| g.as_str() == Some(group))
    })
}
pub(crate) fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut found = None;
    for h in headers.get_all(header::COOKIE) {
        for part in h.to_str().ok()?.split(';') {
            let Some((k, v)) = part.trim().split_once('=') else {
                continue;
            };
            if k == name {
                if found.is_some() || v.is_empty() || v.len() > 256 {
                    return None;
                }
                found = Some(v);
            }
        }
    }
    found
}
pub(crate) fn set_cookie(name: &str, value: &str, age: u64) -> String {
    format!("{name}={value}; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age={age}")
}
fn random() -> Result<String, Status> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| Status::unavailable("entropy unavailable"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}
async fn bounded_json<T: serde::de::DeserializeOwned>(
    mut response: reqwest::Response,
) -> Result<T, &'static str> {
    if !response.status().is_success() {
        return Err("OIDC endpoint failed");
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "OIDC response unavailable")?
    {
        if bytes.len() + chunk.len() > 64 * 1024 {
            return Err("OIDC response too large");
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid OIDC response")
}
async fn session(State(auth): State<Auth>, headers: HeaderMap) -> Response {
    match auth.browser(&headers).await {
        Ok(p)=>(StatusCode::OK,[(header::CACHE_CONTROL,"no-store")],Json(json!({"mode":if auth.enabled(){"oidc"}else{"disabled"},"authenticated":true,"user_id":p.actor.user_id,"administrator":p.actor.administrator,"expires_at":if auth.enabled(){Some(p.expires_at.to_rfc3339())}else{None}}))).into_response(),
        Err(_)=>(StatusCode::UNAUTHORIZED,[(header::CACHE_CONTROL,"no-store")],Json(json!({"mode":"oidc","authenticated":false}))).into_response()
    }
}
#[derive(Deserialize)]
struct Login {
    return_to: Option<String>,
}
async fn login(State(auth): State<Auth>, Query(query): Query<Login>) -> Response {
    match login_inner(&auth, query).await {
        Ok(r) => r,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
async fn login_inner(auth: &Auth, query: Login) -> Result<Response, Status> {
    let Some(a) = &auth.inner else {
        return Ok(Redirect::to("/").into_response());
    };
    let _permit = a
        .logins
        .try_acquire()
        .map_err(|_| Status::resource_exhausted("login busy"))?;
    let return_to = query.return_to.unwrap_or_else(|| "/".into());
    if !return_to.starts_with('/')
        || return_to.starts_with("//")
        || return_to.contains('\\')
        || return_to.chars().any(char::is_control)
        || return_to.len() > 4096
    {
        return Err(Status::invalid_argument("invalid return path"));
    }
    let state = random()?;
    let correlation = random()?;
    let verifier = random()?;
    let nonce = random()?;
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM inbox_login_attempts WHERE expires_at>now()")
            .fetch_one(&a.db)
            .await
            .map_err(crate::mutation::storage)?;
    if count >= 1000 {
        return Err(Status::resource_exhausted("login busy"));
    }
    sqlx::query(
        "INSERT INTO inbox_login_attempts VALUES($1,$2,$3,$4,$5,now()+interval '5 minutes')",
    )
    .bind(crate::mutation::hash(state.as_bytes()))
    .bind(crate::mutation::hash(correlation.as_bytes()))
    .bind(&verifier)
    .bind(&nonce)
    .bind(return_to)
    .execute(&a.db)
    .await
    .map_err(crate::mutation::storage)?;
    let mut url = Url::parse(&a.provider.authorization_endpoint).map_err(|_| unauth())?;
    url.query_pairs_mut().extend_pairs([
        ("response_type", "code"),
        ("client_id", &a.config.client_id),
        (
            "redirect_uri",
            &format!("{}/auth/callback", a.config.public_url),
        ),
        ("scope", "openid profile email"),
        ("state", &state),
        ("nonce", &nonce),
        (
            "code_challenge",
            &URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes())),
        ),
        ("code_challenge_method", "S256"),
    ]);
    Ok((
        [
            (
                header::SET_COOKIE,
                set_cookie(LOGIN_COOKIE, &correlation, 300),
            ),
            (header::CACHE_CONTROL, "no-store".into()),
        ],
        Redirect::to(url.as_str()),
    )
        .into_response())
}
#[derive(Deserialize)]
struct Callback {
    code: Option<String>,
    state: Option<String>,
}
async fn callback(
    State(auth): State<Auth>,
    headers: HeaderMap,
    Query(query): Query<Callback>,
) -> Response {
    match callback_inner(&auth, &headers, query).await {
        Ok(r) => r,
        Err(_) => (
            StatusCode::UNAUTHORIZED,
            [(header::CACHE_CONTROL, "no-store")],
            "login failed",
        )
            .into_response(),
    }
}
async fn callback_inner(
    auth: &Auth,
    headers: &HeaderMap,
    query: Callback,
) -> Result<Response, Status> {
    let a = auth.inner.as_ref().ok_or_else(unauth)?;
    let _permit = a.logins.try_acquire().map_err(|_| unauth())?;
    let state = query.state.filter(|s| s.len() <= 128).ok_or_else(unauth)?;
    let correlation = cookie(headers, LOGIN_COOKIE).ok_or_else(unauth)?;
    let row=sqlx::query("DELETE FROM inbox_login_attempts WHERE state_hash=$1 AND correlation_hash=$2 AND expires_at>now() RETURNING verifier,nonce,return_to")
        .bind(crate::mutation::hash(state.as_bytes())).bind(crate::mutation::hash(correlation.as_bytes())).fetch_optional(&a.db).await.map_err(crate::mutation::storage)?.ok_or_else(unauth)?;
    let code = query
        .code
        .filter(|s| !s.is_empty() && s.len() <= 4096)
        .ok_or_else(unauth)?;
    let response = a
        .client
        .post(&a.provider.token_endpoint)
        .basic_auth(&a.config.client_id, Some(&a.config.client_secret))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", &code),
            (
                "redirect_uri",
                &format!("{}/auth/callback", a.config.public_url),
            ),
            ("code_verifier", row.get::<&str, _>("verifier")),
        ])
        .send()
        .await
        .map_err(|_| unauth())?;
    let tokens: Value = bounded_json(response).await.map_err(|_| unauth())?;
    let claims = auth
        .verify(
            tokens["id_token"].as_str().ok_or_else(unauth)?,
            &a.config.client_id,
            Some(row.get("nonce")),
        )
        .await?;
    let actor = auth.identity(&claims).await?;
    let token = random()?;
    sqlx::query("INSERT INTO inbox_sessions VALUES($1,$2,$3,$4,$5,now()+interval '8 hours')")
        .bind(crate::mutation::hash(token.as_bytes()))
        .bind(actor.user_id)
        .bind(actor.administrator)
        .bind(&a.config.issuer)
        .bind(&a.config.client_id)
        .execute(&a.db)
        .await
        .map_err(crate::mutation::storage)?;
    let mut response = Redirect::to(row.get("return_to")).into_response();
    response.headers_mut().append(
        header::SET_COOKIE,
        set_cookie(COOKIE, &token, 28800)
            .parse()
            .map_err(|_| unauth())?,
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        set_cookie(LOGIN_COOKIE, "deleted", 0)
            .parse()
            .map_err(|_| unauth())?,
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().expect("header"));
    Ok(response)
}
async fn logout(State(auth): State<Auth>, headers: HeaderMap) -> Response {
    if !auth.csrf(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    if let Some(a) = &auth.inner {
        let Ok(p) = auth.browser(&headers).await else {
            return StatusCode::UNAUTHORIZED.into_response();
        };
        if sqlx::query("DELETE FROM inbox_sessions WHERE token_hash=$1")
            .bind(p.session_hash)
            .execute(&a.db)
            .await
            .is_err()
        {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    }
    (
        StatusCode::NO_CONTENT,
        [
            (header::SET_COOKIE, set_cookie(COOKIE, "deleted", 0)),
            (header::CACHE_CONTROL, "no-store".into()),
        ],
    )
        .into_response()
}
pub async fn protect(State(auth): State<Auth>, mut request: Request, next: Next) -> Response {
    let web = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/grpc-web"));
    let browser = web && !request.headers().contains_key(header::AUTHORIZATION);
    let principal = if browser {
        if !auth.csrf(request.headers()) {
            return StatusCode::FORBIDDEN.into_response();
        }
        auth.browser(request.headers()).await
    } else {
        auth.bearer(request.headers()).await
    };
    match principal {
        Ok(p) => {
            if (browser || !auth.enabled())
                && !expected_user_matches(request.headers(), &p.actor, browser && auth.enabled())
            {
                return StatusCode::UNAUTHORIZED.into_response();
            }
            request.extensions_mut().insert(p);
            next.run(request).await
        }
        Err(_) => StatusCode::UNAUTHORIZED.into_response(),
    }
}
fn expected_user_matches(headers: &HeaderMap, actor: &Actor, required: bool) -> bool {
    let mut values = headers.get_all("x-inbox-expected-user").iter();
    let Some(value) = values.next() else {
        return !required;
    };
    if values.next().is_some() {
        return false;
    }
    let Ok(text) = value.to_str() else {
        return false;
    };
    Uuid::parse_str(text).is_ok_and(|id| id == actor.user_id && id.to_string() == text)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn expected_user_is_one_canonical_uuid_not_an_identity_source() {
        let actor = Actor {
            user_id: Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap(),
            administrator: false,
        };
        let mut headers = HeaderMap::new();
        assert!(expected_user_matches(&headers, &actor, false));
        assert!(!expected_user_matches(&headers, &actor, true));
        for value in [
            "",
            "invalid",
            "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
            "{aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa}",
            "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa",
            " aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa,aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        ] {
            headers.insert("x-inbox-expected-user", value.parse().unwrap());
            assert!(!expected_user_matches(&headers, &actor, true));
            assert!(!expected_user_matches(&headers, &actor, false));
        }
        headers.insert(
            "x-inbox-expected-user",
            actor.user_id.to_string().parse().unwrap(),
        );
        assert!(expected_user_matches(&headers, &actor, true));
        headers.append(
            "x-inbox-expected-user",
            actor.user_id.to_string().parse().unwrap(),
        );
        assert!(!expected_user_matches(&headers, &actor, true));
        headers.insert(
            "x-inbox-expected-user",
            axum::http::HeaderValue::from_bytes(b"\xff").unwrap(),
        );
        assert!(!expected_user_matches(&headers, &actor, true));
    }

    #[tokio::test]
    async fn disabled_rpc_expected_user_is_optional_but_cannot_select_an_owner() {
        let app = Router::new()
            .route("/rpc", post(|| async { StatusCode::NO_CONTENT }))
            .layer(axum::middleware::from_fn_with_state(
                Auth::default(),
                protect,
            ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/rpc", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        for content_type in ["application/grpc-web+proto", "application/grpc"] {
            for expected in [
                None,
                Some(LOCAL_USER.to_string()),
                Some(Uuid::new_v4().to_string()),
                Some("invalid".into()),
            ] {
                let accepted = expected
                    .as_deref()
                    .is_none_or(|v| v == LOCAL_USER.to_string());
                let mut request = client.post(&endpoint).header("content-type", content_type);
                if let Some(expected) = expected {
                    request = request.header("x-inbox-expected-user", expected);
                }
                assert_eq!(
                    request.send().await.unwrap().status(),
                    if accepted {
                        StatusCode::NO_CONTENT
                    } else {
                        StatusCode::UNAUTHORIZED
                    }
                );
            }
        }
        server.abort();
    }

    fn mcp_read_probe(client: &reqwest::Client, endpoint: &str) -> reqwest::RequestBuilder {
        client.post(format!("{endpoint}/mcp"))
            .header("accept","application/json, text/event-stream")
            .header("mcp-protocol-version","2026-07-28")
            .header("mcp-method","resources/read")
            .header("mcp-name","waltr://mailboxes")
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"waltr://mailboxes","_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}))
    }
    async fn assert_mcp_unauthorized(response: reqwest::Response) {
        let status = response.status();
        let body = response.json::<Value>().await.unwrap_or(Value::Null);
        assert_eq!(
            status,
            reqwest::StatusCode::UNAUTHORIZED,
            "MCP JSON-RPC error code: {:?}; OAuth error code: {:?}",
            body["error"]["code"].as_i64(),
            body["error"].as_str()
        );
    }
    const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIN3Da5oiOodnnCZ1/FPyQBUxEh0SLAz7qw040s2r4uti\n-----END PRIVATE KEY-----";
    fn fixture() -> Auth {
        let config = Config {
            public_url: "https://inbox.example".into(),
            issuer: "https://id.example/application/o/inbox/".into(),
            client_id: "browser".into(),
            client_secret: "synthetic-secret".into(),
            api_audience: "inbox-api".into(),
            api_client_id: "native".into(),
            admin_group: "inbox-admins".into(),
            scopes: vec!["inbox:api".into()],
            wrapping_keys_file: "unused".into(),
        };
        Auth {inner:Some(Arc::new(Enabled {
            db:sqlx::postgres::PgPoolOptions::new().connect_lazy("postgres://unused:unused@localhost/unused").unwrap(),
            client:reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap(),
            provider:Provider {issuer:config.issuer.clone(),authorization_endpoint:"https://id.example/authorize".into(),token_endpoint:"https://id.example/token".into(),jwks_uri:"https://id.example/jwks".into(),code_challenge_methods_supported:vec!["S256".into()],response_types_supported:vec!["code".into()],token_endpoint_auth_methods_supported:vec!["client_secret_basic".into()],id_token_signing_alg_values_supported:vec!["EdDSA".into()]},
            config,keys:tokio::sync::RwLock::new((serde_json::from_value(json!({"keys":[{"kty":"OKP","crv":"Ed25519","kid":"test","use":"sig","alg":"EdDSA","x":"gRZWZKj1m8q-O4Bz9IBBXXKodWhnnXz8WHrUiFoRAsA"}]})).unwrap(),std::time::Instant::now())),
            cursor_key:vec![42;32],logins:tokio::sync::Semaphore::new(16),
        })) }
    }
    fn claims() -> Value {
        json!({"iss":"https://id.example/application/o/inbox/","sub":"user-one","aud":"inbox-api","azp":"native","uid":"synthetic-access-token-id","scope":"inbox:api","iat":Utc::now().timestamp(),"exp":Utc::now().timestamp()+300})
    }
    fn token(claims: &Value) -> String {
        let mut header = jsonwebtoken::Header::new(Algorithm::EdDSA);
        header.kid = Some("test".into());
        jsonwebtoken::encode(
            &header,
            claims,
            &jsonwebtoken::EncodingKey::from_ed_pem(TEST_KEY.as_bytes()).unwrap(),
        )
        .unwrap()
    }
    #[tokio::test]
    async fn mcp_negative_probes_use_valid_protocol_envelopes() {
        use mcp::{server::*, *};
        struct Probe;
        impl ServerHandler for Probe {
            fn server_info(&self) -> ServerInfo {
                ServerInfo::new("probe", "1")
            }
            fn capabilities(&self) -> ServerCapabilities {
                ServerCapabilities::new().resources()
            }
            fn read_resource(
                &self,
                uri: String,
                _: ServerContext,
            ) -> BoxFuture<ServerResult<McpResourceResult>> {
                Box::pin(async move {
                    Ok(McpResourceResult {
                        raw: json!({"contents":[{"uri":uri,"mimeType":"text/plain","text":"synthetic"}]}),
                    })
                })
            }
        }
        let authorization = StreamableHttpAuthorization::hosted(
            McpProtectedResourceMetadata::new(
                "https://inbox.example/mcp",
                vec!["https://inbox.example/oauth".to_owned()],
            ),
            |token, _| {
                Box::pin(async move {
                    if token.0 == "valid" {
                        McpHostedTokenValidation::Authorized(McpTokenAuthorization {
                            principal_id: McpPrincipalId::new("synthetic").unwrap(),
                            expires_at: None,
                            revocation: None,
                        })
                    } else {
                        McpHostedTokenValidation::Unauthorized {
                            error_description: None,
                        }
                    }
                })
            },
        )
        .unwrap();
        let app = streamable_http_router_with_options(
            Arc::new(Probe),
            StreamableHttpOptions::default().with_authorization(authorization),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::new();
        let accepted = mcp_read_probe(&client, &endpoint)
            .bearer_auth("valid")
            .send()
            .await
            .unwrap();
        let status = accepted.status();
        let body = accepted.text().await.unwrap();
        let error = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|value| value["error"]["code"].as_i64());
        assert_eq!(
            status,
            reqwest::StatusCode::OK,
            "MCP JSON-RPC error code: {error:?}"
        );
        assert!(body.contains("synthetic"));
        let malformed=mcp_read_probe(&client,&endpoint).bearer_auth("valid").json(&json!({"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"waltr://mailboxes"}})).send().await.unwrap();
        assert_eq!(malformed.status(), reqwest::StatusCode::BAD_REQUEST);
        let error = malformed.json::<Value>().await.unwrap();
        assert_eq!(
            error["error"]["code"],
            json!(-32602),
            "authenticated malformed probes fail metadata validation, not token admission"
        );
        assert_mcp_unauthorized(
            mcp_read_probe(&client, &endpoint)
                .bearer_auth("invalid")
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_mcp_unauthorized(
            mcp_read_probe(&client, &endpoint)
                .header("Cookie", "__Host-inbox-session=synthetic")
                .send()
                .await
                .unwrap(),
        )
        .await;
        server.abort();
    }
    #[tokio::test]
    async fn native_token_strict_issuer_audience_expiry_client_and_scopes() {
        let auth = fixture();
        assert!(
            auth.verify(&token(&claims()), "inbox-api", None)
                .await
                .is_ok()
        );
        for (field, value) in [
            ("iss", json!("https://other.example")),
            ("aud", json!("browser")),
            ("aud", json!(["inbox-api", "unrelated-service"])),
            ("exp", json!(Utc::now().timestamp() - 1)),
            ("azp", json!("other-client")),
            ("scope", json!("openid")),
            ("sub", json!("")),
            ("iat", json!(Utc::now().timestamp() + 120)),
        ] {
            let mut claims = claims();
            claims[field] = value;
            assert!(
                auth.verify(&token(&claims), "inbox-api", None)
                    .await
                    .is_err(),
                "{field}"
            );
        }
        let mut missing = claims();
        missing.as_object_mut().unwrap().remove("exp");
        assert!(
            auth.verify(&token(&missing), "inbox-api", None)
                .await
                .is_err()
        );
        let unsigned = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(Algorithm::HS256),
            &claims(),
            &jsonwebtoken::EncodingKey::from_secret(b"attacker"),
        )
        .unwrap();
        assert!(auth.verify(&unsigned, "inbox-api", None).await.is_err());
    }
    #[tokio::test]
    async fn native_admission_distinguishes_authentik_access_and_id_tokens() {
        // Both token types use typ=JWT. Pinned standard mappings reserve uid/azp/scope.
        let mut access_claims = claims();
        access_claims["aud"] = json!("native");
        access_claims["uid"] = json!("access-only-generated-id");
        let access = token(&access_claims);
        let mut id_claims = access_claims.clone();
        id_claims.as_object_mut().unwrap().remove("uid");
        let id = token(&id_claims);
        let mut auth = fixture();
        let enabled = Arc::get_mut(auth.inner.as_mut().unwrap()).unwrap();
        enabled.config.api_audience = "native".into();
        assert!(auth.verify(&access, "native", None).await.is_ok());
        assert!(
            auth.verify(&id, "native", None).await.is_err(),
            "matching ID-token aud/client/scope are insufficient without access-only uid"
        );
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("Bearer {id}").parse().unwrap());
        assert!(
            auth.bearer(&headers).await.is_err(),
            "reject before any identity persistence"
        );
        for typ in ["id_token", "dpop+id_token", "refresh_token"] {
            let mut header = jsonwebtoken::Header::new(Algorithm::EdDSA);
            header.kid = Some("test".into());
            header.typ = Some(typ.into());
            let id = jsonwebtoken::encode(
                &header,
                &access_claims,
                &jsonwebtoken::EncodingKey::from_ed_pem(TEST_KEY.as_bytes()).unwrap(),
            )
            .unwrap();
            assert!(auth.verify(&id, "native", None).await.is_err());
        }
        for field in ["azp", "uid", "scope"] {
            let mut c = access_claims.clone();
            c.as_object_mut().unwrap().remove(field);
            assert!(
                auth.verify(&token(&c), "native", None).await.is_err(),
                "{field}"
            );
        }
    }
    #[tokio::test]
    async fn browser_nonce_client_and_freshness_are_required() {
        let auth = fixture();
        let mut c = claims();
        c["aud"] = json!("browser");
        c["azp"] = json!("browser");
        c["nonce"] = json!("correlation");
        assert!(
            auth.verify(&token(&c), "browser", Some("correlation"))
                .await
                .is_ok()
        );
        assert!(
            auth.verify(&token(&c), "browser", Some("other"))
                .await
                .is_err()
        );
        c["iat"] = json!(Utc::now().timestamp() - 601);
        assert!(
            auth.verify(&token(&c), "browser", Some("correlation"))
                .await
                .is_err()
        );
        c["iat"] = json!(Utc::now().timestamp());
        c["aud"] = json!(["browser", "another"]);
        c.as_object_mut().unwrap().remove("azp");
        assert!(
            auth.verify(&token(&c), "browser", Some("correlation"))
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn csrf_requires_exact_origin_and_fetch_site() {
        let auth = fixture();
        let mut h = HeaderMap::new();
        assert!(!auth.csrf(&h));
        h.insert(header::ORIGIN, "https://inbox.example".parse().unwrap());
        assert!(!auth.csrf(&h));
        h.insert("sec-fetch-site", "same-origin".parse().unwrap());
        assert!(auth.csrf(&h));
        h.insert("sec-fetch-site", "same-site".parse().unwrap());
        assert!(!auth.csrf(&h));
        h.insert("sec-fetch-site", "same-origin".parse().unwrap());
        h.insert(
            header::ORIGIN,
            "https://inbox.example.attacker.test".parse().unwrap(),
        );
        assert!(!auth.csrf(&h));
        let mut cookies = HeaderMap::new();
        cookies.insert(
            header::COOKIE,
            "__Host-inbox-session=synthetic".parse().unwrap(),
        );
        assert!(
            auth.bearer(&cookies).await.is_err(),
            "native access never accepts browser cookies"
        );
    }
    #[tokio::test]
    async fn signed_cursors_cannot_cross_users_filters_or_be_modified() {
        let auth = fixture();
        let alice = Actor {
            user_id: Uuid::new_v4(),
            administrator: false,
        };
        let bob = Actor {
            user_id: Uuid::new_v4(),
            administrator: false,
        };
        let token = auth.bind_cursor("INBOX_PREVIEW", &alice, "arrival", "synthetic-position");
        assert_eq!(
            auth.unbind_cursor("INBOX_PREVIEW", &alice, "arrival", &token)
                .unwrap(),
            "synthetic-position"
        );
        assert!(
            auth.unbind_cursor("INBOX_PREVIEW", &bob, "arrival", &token)
                .is_err()
        );
        assert!(
            auth.unbind_cursor("INBOX_PREVIEW", &alice, "changes", &token)
                .is_err()
        );
        assert!(
            auth.unbind_cursor("INBOX_PROD", &alice, "arrival", &token)
                .is_err(),
            "copied PostgreSQL keys/users/generations must not permit cross-environment replay"
        );
        assert!(
            auth.unbind_cursor("INBOX_PREVIEW", &alice, "arrival", &format!("{token}x"))
                .is_err()
        );
        assert!(
            auth.unbind_cursor("INBOX_PREVIEW", &alice, "arrival", "synthetic-position")
                .is_err()
        );
        for kind in ["mailboxes", "messages", "domains", "changes", "arrival"] {
            for auth in [fixture(), Auth::default()] {
                let token =
                    auth.bind_cursor("INBOX_PREVIEW", &alice, kind, "cloned-database-position");
                assert_eq!(
                    auth.unbind_cursor("INBOX_PREVIEW", &alice, kind, &token)
                        .unwrap(),
                    "cloned-database-position"
                );
                assert!(
                    auth.unbind_cursor("INBOX_PROD", &alice, kind, &token)
                        .is_err()
                );
            }
        }
    }
    #[tokio::test]
    async fn credential_expiry_ends_lease() {
        let auth = fixture();
        let principal = Principal::delegated(
            Actor::local(),
            Utc::now() + chrono::Duration::milliseconds(20),
        );
        tokio::time::timeout(Duration::from_millis(200), principal.ended(&auth))
            .await
            .unwrap();
        assert!(!principal.valid(&auth).await);
    }
    #[tokio::test]
    #[ignore = "requires explicitly isolated INBOX_TEST_DATABASE_URL; creates a retained private schema"]
    async fn browser_pkce_callback_sessions_logout_and_shared_native_identity() {
        let url = std::env::var("INBOX_TEST_DATABASE_URL").expect("isolated test database");
        let admin = PgPool::connect(&url).await.unwrap();
        let schema = format!("auth_test_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        let db = sqlx::postgres::PgPoolOptions::new()
            .after_connect(move |connection, _| {
                let query = format!("SET search_path TO {schema}");
                Box::pin(async move {
                    sqlx::query(&query).execute(connection).await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .unwrap();
        sqlx::migrate!("../../migrations").run(&db).await.unwrap();
        type Correlation = Arc<std::sync::Mutex<(String, String)>>;
        async fn exchange(
            State(correlation): State<Correlation>,
            headers: HeaderMap,
            axum::Form(form): axum::Form<HashMap<String, String>>,
        ) -> Json<Value> {
            assert_eq!(
                headers["authorization"],
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode("browser:synthetic-secret")
                )
            );
            assert_eq!(form["grant_type"], "authorization_code");
            assert_eq!(form["redirect_uri"], "https://inbox.example/auth/callback");
            let (nonce, challenge) = correlation.lock().unwrap().clone();
            assert_eq!(
                URL_SAFE_NO_PAD.encode(Sha256::digest(form["code_verifier"].as_bytes())),
                challenge
            );
            let mut c = claims();
            c["aud"] = json!("browser");
            c["azp"] = json!("browser");
            c["nonce"] = json!(nonce);
            c["groups"] = json!(["inbox-admins"]);
            for field in ["uid", "azp", "scope"] {
                c.as_object_mut().unwrap().remove(field);
            }
            Json(
                json!({"id_token":token(&c),"access_token":"synthetic-unused","token_type":"Bearer"}),
            )
        }
        let correlation: Correlation = Default::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let idp = format!("http://{}", listener.local_addr().unwrap());
        let native_token = token(&claims());
        let provider = Router::new()
            .route("/token", post(exchange))
            .with_state(correlation.clone());
        let provider = tokio::spawn(async move { axum::serve(listener, provider).await.unwrap() });
        let mut auth = fixture();
        let enabled = Arc::get_mut(auth.inner.as_mut().unwrap()).unwrap();
        enabled.db = db.clone();
        // Only this in-module fixture bypasses production HTTPS discovery.
        enabled.provider.token_endpoint = format!("{idp}/token");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handler_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = handler_calls.clone();
        let rpc = Router::new()
            .route(
                "/rpc",
                post(move |request: Request| {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        axum::body::to_bytes(request.into_body(), 1024)
                            .await
                            .unwrap();
                        StatusCode::NO_CONTENT
                    }
                }),
            )
            .layer(axum::middleware::from_fn_with_state(auth.clone(), protect));
        let app = auth.router().merge(rpc);
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let login = client
            .get(format!("{endpoint}/auth/login?return_to=/mail"))
            .send()
            .await
            .unwrap();
        assert!(login.status().is_redirection());
        let cookie = login.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        assert!(
            login.headers()["set-cookie"]
                .to_str()
                .unwrap()
                .contains("Secure; HttpOnly; SameSite=Lax")
        );
        let destination = url::Url::parse(login.headers()["location"].to_str().unwrap()).unwrap();
        let params = destination
            .query_pairs()
            .into_owned()
            .collect::<HashMap<_, _>>();
        assert_eq!(params["code_challenge_method"], "S256");
        assert_eq!(params["scope"], "openid profile email");
        *correlation.lock().unwrap() = (params["nonce"].clone(), params["code_challenge"].clone());
        let callback = format!(
            "{endpoint}/auth/callback?state={}&code=synthetic-code",
            params["state"]
        );
        let no_correlation = client.get(&callback).send().await.unwrap();
        assert_eq!(no_correlation.status(), reqwest::StatusCode::UNAUTHORIZED);
        let response = client
            .get(&callback)
            .header("Cookie", &cookie)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_redirection());
        assert_eq!(response.headers()["location"], "/mail");
        let session_cookie = response.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let session = client
            .get(format!("{endpoint}/auth/session"))
            .header("Cookie", &session_cookie)
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        assert_eq!(session["authenticated"], true);
        assert_eq!(session["administrator"], true);
        assert_ne!(session["user_id"], json!(LOCAL_USER.to_string()));
        let second_id = Uuid::new_v4();
        sqlx::query("INSERT INTO inbox_users(id,issuer,subject) VALUES($1,$2,'second-account')")
            .bind(second_id)
            .bind(&auth.inner.as_ref().unwrap().config.issuer)
            .execute(&db)
            .await
            .unwrap();
        let second_token = random().unwrap();
        sqlx::query("INSERT INTO inbox_sessions(token_hash,owner_id,administrator,issuer,client_id,expires_at) VALUES($1,$2,false,$3,$4,$5)")
            .bind(crate::mutation::hash(second_token.as_bytes())).bind(second_id)
            .bind(&auth.inner.as_ref().unwrap().config.issuer)
            .bind(&auth.inner.as_ref().unwrap().config.client_id)
            .bind(Utc::now()+chrono::Duration::hours(1))
            .execute(&db).await.unwrap();
        let second_cookie = format!("__Host-inbox-session={second_token}");
        // A's successful preflight above cannot authorize the subsequently switched B cookie.
        let rpc_request = || {
            client
                .post(format!("{endpoint}/rpc"))
                .header("content-type", "application/grpc-web+proto")
                .header("origin", "https://inbox.example")
                .header("sec-fetch-site", "same-origin")
                .body("not a decodable RPC body")
        };
        for expected in [
            None,
            Some(session["user_id"].as_str().unwrap()),
            Some("invalid"),
        ] {
            let mut request = rpc_request().header("cookie", &second_cookie);
            if let Some(expected) = expected {
                request = request.header("x-inbox-expected-user", expected);
            }
            assert_eq!(
                request.send().await.unwrap().status(),
                StatusCode::UNAUTHORIZED
            );
        }
        assert_eq!(
            rpc_request()
                .header("cookie", &second_cookie)
                .header("x-inbox-expected-user", second_id.to_string())
                .header("x-inbox-expected-user", second_id.to_string())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            rpc_request()
                .header("x-inbox-expected-user", second_id.to_string())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(handler_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(
            rpc_request()
                .header("cookie", &second_cookie)
                .header("x-inbox-expected-user", second_id.to_string())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            client
                .post(format!("{endpoint}/rpc"))
                .header("content-type", "application/grpc")
                .bearer_auth(&native_token)
                .header("x-inbox-expected-user", "invalid")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(handler_calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(
            client
                .get(&callback)
                .header("Cookie", &cookie)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "callback is one-shot"
        );
        let mut native_headers = HeaderMap::new();
        native_headers.insert(
            "authorization",
            format!("Bearer {native_token}").parse().unwrap(),
        );
        let native = auth.bearer(&native_headers).await.unwrap();
        assert_eq!(
            session["user_id"],
            json!(native.actor.user_id.to_string()),
            "native/browser use the same verified issuer+subject"
        );
        let raw = session_cookie
            .strip_prefix("__Host-inbox-session=")
            .unwrap();
        let stored = sqlx::query_scalar::<_, String>("SELECT token_hash FROM inbox_sessions")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_ne!(stored, raw);
        assert_eq!(stored, crate::mutation::hash(raw.as_bytes()));
        assert_eq!(
            client
                .post(format!("{endpoint}/auth/logout"))
                .header("Cookie", &session_cookie)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::FORBIDDEN
        );
        let mut headers = HeaderMap::new();
        headers.insert("cookie", session_cookie.parse().unwrap());
        let lease = auth.browser(&headers).await.unwrap();
        let result = client
            .post(format!("{endpoint}/auth/logout"))
            .header("Cookie", &session_cookie)
            .header("Origin", "https://inbox.example")
            .header("Sec-Fetch-Site", "same-origin")
            .send()
            .await
            .unwrap();
        assert_eq!(result.status(), reqwest::StatusCode::NO_CONTENT);
        tokio::time::timeout(Duration::from_secs(2), lease.ended(&auth))
            .await
            .unwrap();
        assert_eq!(
            client
                .get(format!("{endpoint}/auth/session"))
                .header("Cookie", &session_cookie)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        server.abort();
        provider.abort();
        db.close().await;
        admin.close().await;
    }
    #[test]
    fn hosted_registration_requires_explicit_native_loopback_policy() {
        let mut request = json!({"client_name":"<script>untrusted</script>","redirect_uris":["http://127.0.0.1:39991/complete"],"grant_types":["authorization_code","refresh_token"],"response_types":["code"],"token_endpoint_auth_method":"none"});
        assert!(
            serde_json::from_value::<mcp::McpPublicClientRegistrationRequest>(request.clone())
                .is_err(),
            "application_type is required by the pinned registration schema"
        );
        request["application_type"] = json!("native");
        let native: mcp::McpPublicClientRegistrationRequest =
            serde_json::from_value(request.clone()).unwrap();
        assert!(
            native
                .clone()
                .validate_and_normalize(true, 16, 1024)
                .is_ok()
        );
        assert!(
            native.validate_and_normalize(false, 16, 1024).is_err(),
            "HTTP loopback remains opt-in"
        );
        request["application_type"] = json!("web");
        let web: mcp::McpPublicClientRegistrationRequest = serde_json::from_value(request).unwrap();
        assert!(
            web.validate_and_normalize(true, 16, 1024).is_err(),
            "web clients cannot claim native loopback redirects"
        );
    }
    #[tokio::test]
    #[ignore = "requires explicitly isolated INBOX_TEST_DATABASE_URL; creates retained private auth schema and generic OAuth protocol state"]
    async fn hosted_oauth_pkce_auto_approval_mcp_identity_and_revocation() {
        use crate::pb::{CreateMailboxRequest, CreateMailboxResponse};
        use mcp::{client::*, *};
        use prost::Message;
        let url = std::env::var("INBOX_TEST_DATABASE_URL").expect("isolated test database");
        let admin = PgPool::connect(&url).await.unwrap();
        let schema = format!("hosted_auth_test_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        let db = sqlx::postgres::PgPoolOptions::new()
            .max_connections(12)
            .after_connect(move |connection, _| {
                let query = format!("SET search_path TO {schema}");
                Box::pin(async move {
                    sqlx::query(&query).execute(connection).await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .unwrap();
        sqlx::migrate!("../../migrations").run(&db).await.unwrap();
        struct ProviderState {
            issuer: String,
            public: String,
            groups: std::sync::Mutex<Value>,
            attempts: std::sync::Mutex<HashMap<String, (String, String)>>,
        }
        async fn metadata(State(s): State<Arc<ProviderState>>) -> Json<Value> {
            Json(
                json!({"issuer":s.issuer,"authorization_endpoint":format!("{}/authorize",s.issuer),"token_endpoint":format!("{}/token",s.issuer),"jwks_uri":format!("{}/jwks",s.issuer),"response_types_supported":["code"],"grant_types_supported":["authorization_code"],"code_challenge_methods_supported":["S256"],"token_endpoint_auth_methods_supported":["client_secret_basic"],"id_token_signing_alg_values_supported":["EdDSA"]}),
            )
        }
        async fn authorize(
            State(s): State<Arc<ProviderState>>,
            headers: HeaderMap,
            Query(q): Query<HashMap<String, String>>,
        ) -> Response {
            assert!(
                !headers.contains_key("cookie"),
                "Inbox cookies must never be sent to the upstream provider"
            );
            assert_eq!(q["client_id"], "browser");
            assert_eq!(q["code_challenge_method"], "S256");
            assert!(
                q["redirect_uri"] == format!("{}/auth/callback", s.public)
                    || q["redirect_uri"] == format!("{}/oauth/oidc/callback", s.public)
            );
            let code = Uuid::new_v4().to_string();
            s.attempts.lock().unwrap().insert(
                code.clone(),
                (q["nonce"].clone(), q["code_challenge"].clone()),
            );
            let mut next = Url::parse(&q["redirect_uri"]).unwrap();
            next.query_pairs_mut()
                .extend_pairs([("code", code.as_str()), ("state", q["state"].as_str())]);
            Redirect::to(next.as_str()).into_response()
        }
        async fn exchange(
            State(s): State<Arc<ProviderState>>,
            headers: HeaderMap,
            axum::Form(q): axum::Form<HashMap<String, String>>,
        ) -> Json<Value> {
            assert_eq!(
                headers["authorization"],
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode("browser:synthetic-secret")
                )
            );
            assert_eq!(q["grant_type"], "authorization_code");
            let (nonce, challenge) = s.attempts.lock().unwrap().remove(&q["code"]).unwrap();
            assert_eq!(
                challenge,
                URL_SAFE_NO_PAD.encode(Sha256::digest(q["code_verifier"].as_bytes()))
            );
            let mut c = claims();
            c["iss"] = json!(s.issuer);
            c["aud"] = json!("browser");
            c["azp"] = json!("browser");
            c["nonce"] = json!(nonce);
            c["groups"] = s.groups.lock().unwrap().clone();
            for field in ["uid", "azp", "scope"] {
                c.as_object_mut().unwrap().remove(field);
            }
            let access = "synthetic-upstream-access";
            Json(
                json!({"id_token":token(&c),"access_token":access,"token_type":"Bearer","expires_in":300}),
            )
        }
        fn cookies(response: &reqwest::Response, jar: &mut HashMap<String, String>) {
            for value in response.headers().get_all("set-cookie") {
                let value = value.to_str().unwrap();
                let (name, value) = value.split(';').next().unwrap().split_once('=').unwrap();
                if value == "deleted" {
                    jar.remove(name);
                } else {
                    jar.insert(name.into(), value.into());
                }
            }
        }
        fn cookie_header(jar: &HashMap<String, String>) -> String {
            jar.iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("; ")
        }
        fn local_url(endpoint: &str, public: &str, location: &str) -> String {
            if let Some(path) = location.strip_prefix(public) {
                format!("{endpoint}{path}")
            } else if location.starts_with('/') {
                format!("{endpoint}{location}")
            } else {
                location.to_owned()
            }
        }
        let provider_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", provider_listener.local_addr().unwrap());
        let public = format!("https://auth-{}.example", Uuid::new_v4().simple());
        let provider_state = Arc::new(ProviderState {
            issuer: issuer.clone(),
            public: public.clone(),
            groups: std::sync::Mutex::new(json!(["inbox-admins"])),
            attempts: Default::default(),
        });
        let jwks =
            serde_json::to_value(&fixture().inner.as_ref().unwrap().keys.read().await.0).unwrap();
        let provider = Router::new()
            .route("/.well-known/openid-configuration", get(metadata))
            .route("/authorize", get(authorize))
            .route("/token", post(exchange))
            .route(
                "/jwks",
                get(move || {
                    let jwks = jwks.clone();
                    async move { Json(jwks) }
                }),
            )
            .with_state(provider_state.clone());
        let provider_task =
            tokio::spawn(async move { axum::serve(provider_listener, provider).await.unwrap() });
        let keyfile =
            std::env::temp_dir().join(format!("inbox-oauth-test-{}.json", Uuid::new_v4().simple()));
        std::fs::write(&keyfile,serde_json::to_vec(&json!({"schema_version":1,"active":"test","keys":[{"id":"test","key":URL_SAFE_NO_PAD.encode([31u8;32])}]})).unwrap()).unwrap();
        let mut auth = fixture();
        let enabled = Arc::get_mut(auth.inner.as_mut().unwrap()).unwrap();
        enabled.db = db.clone();
        enabled.config.issuer = issuer.clone();
        enabled.config.public_url = public.clone();
        enabled.config.wrapping_keys_file = keyfile.to_str().unwrap().into();
        enabled.provider.issuer = issuer.clone();
        enabled.provider.authorization_endpoint = format!("{issuer}/authorize");
        enabled.provider.token_endpoint = format!("{issuer}/token");
        enabled.provider.jwks_uri = format!("{issuer}/jwks");
        assert!(
            crate::oauth::Hosted::initialize(db.clone(), auth.clone())
                .await
                .is_err(),
            "production initialization must reject HTTP provider discovery"
        );
        let hosted = crate::oauth::Hosted::initialize_loopback_test(
            db.clone(),
            auth.clone(),
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .unwrap();
        let credentials =
            s3::creds::Credentials::new(Some("synthetic"), Some("synthetic"), None, None, None)
                .unwrap();
        let bucket = s3::Bucket::new(
            "synthetic",
            s3::Region::Custom {
                region: "local".into(),
                endpoint: "http://127.0.0.1:1".into(),
            },
            credentials,
        )
        .unwrap();
        let service = crate::service::MailService {
            auth: auth.clone(),
            principal: None,
            db: db.clone(),
            bucket: Arc::new(*bucket),
            domain: "inbox.test".into(),
            pipeline: None,
        };
        let stop = tokio_util::sync::CancellationToken::new();
        let app = auth.router().merge(hosted.router()).merge(
            crate::mcp::InboxMcp::new(service.clone(), stop.clone())
                .hosted_router(hosted.authorization().unwrap()),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server_stop = stop.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(server_stop.cancelled_owned())
                .await
                .unwrap()
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let mut jar = HashMap::new();
        let login = client
            .get(format!("{endpoint}/auth/login"))
            .send()
            .await
            .unwrap();
        cookies(&login, &mut jar);
        let upstream = client
            .get(login.headers()["location"].to_str().unwrap())
            .send()
            .await
            .unwrap();
        let callback = client
            .get(local_url(
                &endpoint,
                &public,
                upstream.headers()["location"].to_str().unwrap(),
            ))
            .header("Cookie", cookie_header(&jar))
            .send()
            .await
            .unwrap();
        cookies(&callback, &mut jar);
        assert!(callback.status().is_redirection());
        let session = client
            .get(format!("{endpoint}/auth/session"))
            .header("Cookie", cookie_header(&jar))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        assert_eq!(session["administrator"], true);
        let mut native_claims = claims();
        native_claims["iss"] = json!(issuer);
        let native_token = token(&native_claims);
        let mut native_headers = HeaderMap::new();
        native_headers.insert(
            "authorization",
            format!("Bearer {native_token}").parse().unwrap(),
        );
        let native = auth.bearer(&native_headers).await.unwrap();
        assert_eq!(session["user_id"], json!(native.actor.user_id.to_string()));
        assert!(
            !native.actor.administrator,
            "missing group claim is not authority"
        );
        *provider_state.groups.lock().unwrap() = json!(["inbox-admins", false]);
        let metadata = client
            .get(format!(
                "{endpoint}/.well-known/oauth-authorization-server/oauth"
            ))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        assert_eq!(metadata["issuer"], json!(hosted.issuer));
        let redirect = "http://127.0.0.1:39991/complete";
        let registered=client.post(format!("{endpoint}/oauth/register")).json(&json!({"application_type":"native","client_name":"<script>untrusted</script>","redirect_uris":[redirect],"grant_types":["authorization_code","refresh_token"],"response_types":["code"],"token_endpoint_auth_method":"none"})).send().await.unwrap();
        let registration_status = registered.status();
        let registered = registered.json::<Value>().await.unwrap();
        assert_eq!(
            registration_status,
            reqwest::StatusCode::CREATED,
            "DCR error code: {}",
            registered["error"].as_str().unwrap_or("none")
        );
        let client_id = registered["client_id"].as_str().unwrap();
        let verifier = "v".repeat(43);
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let mut authorize_url = Url::parse(&format!("{endpoint}/oauth/authorize")).unwrap();
        authorize_url.query_pairs_mut().extend_pairs([
            ("response_type", "code"),
            ("client_id", client_id),
            ("redirect_uri", redirect),
            ("resource", hosted.resource.as_str()),
            ("scope", "inbox:mcp"),
            ("state", "caller-state"),
            ("code_challenge_method", "S256"),
            ("code_challenge", challenge.as_str()),
        ]);
        let authorize = client
            .get(authorize_url)
            .header("Cookie", cookie_header(&jar))
            .send()
            .await
            .unwrap();
        let mut response = authorize;
        for _ in 0..6 {
            cookies(&response, &mut jar);
            if !response.status().is_redirection() {
                break;
            }
            if response.headers()["location"]
                .to_str()
                .unwrap()
                .starts_with(&format!("{redirect}?"))
            {
                break;
            }
            let next = local_url(
                &endpoint,
                &public,
                response.headers()["location"].to_str().unwrap(),
            );
            let mut request = client.get(&next);
            if next.starts_with(&endpoint) {
                request = request.header("Cookie", cookie_header(&jar));
            }
            response = request.send().await.unwrap();
        }
        assert_eq!(
            response.status(),
            reqwest::StatusCode::SEE_OTHER,
            "verified OIDC must return directly to the registered client"
        );
        let returned = Url::parse(response.headers()["location"].to_str().unwrap())
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect::<HashMap<_, _>>();
        assert_eq!(returned["state"], "caller-state");
        let token_form = [
            ("grant_type", "authorization_code"),
            ("client_id", client_id),
            ("redirect_uri", redirect),
            ("resource", hosted.resource.as_str()),
            ("code", returned["code"].as_str()),
            ("code_verifier", verifier.as_str()),
        ];
        let issued = client
            .post(format!("{endpoint}/oauth/token"))
            .form(&token_form)
            .send()
            .await
            .unwrap();
        assert_eq!(issued.status(), reqwest::StatusCode::OK);
        let issued = issued.json::<Value>().await.unwrap();
        let local_token = issued["access_token"].as_str().unwrap();
        assert!(issued["refresh_token"].is_string());
        let replay = client
            .post(format!("{endpoint}/oauth/token"))
            .form(&token_form)
            .send()
            .await
            .unwrap();
        assert!(replay.status().is_client_error(), "code is one-shot");
        assert!(
            !sqlx::query_scalar::<_, bool>("SELECT administrator FROM inbox_users WHERE id=$1")
                .bind(native.actor.user_id)
                .fetch_one(&db)
                .await
                .unwrap(),
            "malformed hosted groups are not authority"
        );
        let mut mailbox = None;
        for (receive, send) in [(false, false), (false, true), (true, false), (true, true)] {
            let domain = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO domains(id,name,receive_enabled,send_enabled) VALUES($1,$2,$3,$4)",
            )
            .bind(domain)
            .bind(format!("flags-{receive}-{send}.test"))
            .bind(receive)
            .bind(send)
            .execute(&db)
            .await
            .unwrap();
            let mut tx = db.begin().await.unwrap();
            let command = crate::mutation::Command::authorized(
                native.actor.clone(),
                crate::mutation::Command::Create(
                    CreateMailboxRequest {
                        domain_id: domain.to_string(),
                        local_part: "user".into(),
                        name: "Synthetic".into(),
                        request_id: Uuid::new_v4().to_string(),
                    }
                    .encode_to_vec(),
                ),
            );
            let result = service.apply(command, &mut tx, "").await;
            if !receive && !send {
                assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
                tx.rollback().await.unwrap();
            } else {
                let created = CreateMailboxResponse::decode(result.unwrap().as_slice())
                    .unwrap()
                    .mailbox
                    .unwrap();
                if receive && send {
                    mailbox = Some(created);
                    tx.commit().await.unwrap();
                } else {
                    tx.rollback().await.unwrap();
                }
            }
        }
        let mailbox = mailbox.unwrap();
        for (authorization, cookie) in [
            (Some(format!("Bearer {native_token}")), None),
            (None, Some(cookie_header(&jar))),
        ] {
            let mut request = mcp_read_probe(&client, &endpoint);
            if let Some(value) = authorization {
                request = request.header("Authorization", value);
            }
            if let Some(value) = cookie {
                request = request.header("Cookie", value);
            }
            assert_mcp_unauthorized(request.send().await.unwrap()).await;
        }
        struct Handler;
        impl McpClientHandler for Handler {}
        let cancel = tokio_util::sync::CancellationToken::new;
        let mcp = McpClient::connect(
            "hosted-auth-test",
            McpTransportConfig::StreamableHttp {
                endpoint: format!("{endpoint}/mcp"),
                bearer_token: Some(local_token.into()),
            },
            Arc::new(Handler),
            McpClientOptions::new("hosted-auth-test", "1", Duration::from_secs(5)).with_events(),
            cancel(),
        )
        .await
        .unwrap();
        let directory = mcp
            .read_resource("waltr://mailboxes", cancel())
            .await
            .unwrap();
        assert!(
            directory.raw.to_string().contains(&mailbox.address),
            "hosted principal resolves the same browser/native internal owner"
        );
        let mut events = mcp
            .stream_events(
                McpEventStreamRequest {
                    name: "mail.received".into(),
                    arguments: Some(
                        serde_json::from_value(
                            json!({"inbox":format!("waltr://mailboxes/{}/inbox",mailbox.address)}),
                        )
                        .unwrap(),
                    ),
                    cursor: None,
                    max_age_ms: None,
                },
                cancel(),
            )
            .await
            .unwrap();
        let active = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let McpEventMessage::Active {
            cursor: Some(cursor),
            ..
        } = active
        else {
            panic!("authorized Active frame")
        };
        assert!(
            auth.unbind_cursor("INBOX", &native.actor, "arrival", &cursor)
                .is_ok()
        );
        assert!(
            auth.unbind_cursor("INBOX_PROD", &native.actor, "arrival", &cursor)
                .is_err(),
            "actual OIDC arrival HMAC rejects cloned-database namespace replay"
        );
        let claims: Value = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(local_token.split('.').nth(1).unwrap())
                .unwrap(),
        )
        .unwrap();
        let expiry = claims["exp"].as_i64().unwrap();
        tokio::time::timeout(Duration::from_secs(35), async {
            loop {
                match events.recv().await {
                    Some(Ok(McpEventMessage::Heartbeat { .. })) => {}
                    None | Some(Err(_)) | Some(Ok(McpEventMessage::Terminated { .. })) => break,
                    _ => panic!("unexpected event while awaiting credential expiry"),
                }
            }
        })
        .await
        .unwrap();
        assert!(
            Utc::now().timestamp() >= expiry,
            "the stream must end at actual token expiry, not an earlier unrelated error"
        );
        events.shutdown().await;
        mcp.shutdown().await.unwrap();
        let expired = mcp_read_probe(&client, &endpoint)
            .bearer_auth(local_token)
            .send()
            .await
            .unwrap();
        assert_mcp_unauthorized(expired).await;
        let refreshed = client
            .post(format!("{endpoint}/oauth/token"))
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", client_id),
                ("resource", hosted.resource.as_str()),
                ("refresh_token", issued["refresh_token"].as_str().unwrap()),
            ])
            .send()
            .await
            .unwrap();
        assert_eq!(refreshed.status(), reqwest::StatusCode::OK);
        let refreshed = refreshed.json::<Value>().await.unwrap();
        let local_token = refreshed["access_token"].as_str().unwrap();
        let mcp = McpClient::connect(
            "hosted-auth-test",
            McpTransportConfig::StreamableHttp {
                endpoint: format!("{endpoint}/mcp"),
                bearer_token: Some(local_token.into()),
            },
            Arc::new(Handler),
            McpClientOptions::new("hosted-auth-test", "1", Duration::from_secs(5)).with_events(),
            cancel(),
        )
        .await
        .unwrap();
        let mut events = mcp
            .stream_events(
                McpEventStreamRequest {
                    name: "mail.received".into(),
                    arguments: Some(
                        serde_json::from_value(
                            json!({"inbox":format!("waltr://mailboxes/{}/inbox",mailbox.address)}),
                        )
                        .unwrap(),
                    ),
                    cursor: None,
                    max_age_ms: None,
                },
                cancel(),
            )
            .await
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), events.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            McpEventMessage::Active { .. }
        ));
        let revoked = client
            .post(format!("{endpoint}/oauth/revoke"))
            .form(&[
                ("client_id", client_id),
                ("token", local_token),
                ("token_type_hint", "access_token"),
            ])
            .send()
            .await
            .unwrap();
        assert_eq!(revoked.status(), reqwest::StatusCode::OK);
        let ended = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .unwrap();
        assert!(
            matches!(
                ended,
                None | Some(Err(_)) | Some(Ok(McpEventMessage::Terminated { .. }))
            ),
            "revocation must close the authorized Events stream, not merely later requests"
        );
        events.shutdown().await;
        mcp.shutdown().await.unwrap();
        let denied = mcp_read_probe(&client, &endpoint)
            .bearer_auth(local_token)
            .send()
            .await
            .unwrap();
        assert_mcp_unauthorized(denied).await;
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        provider_task.abort();
        std::fs::remove_file(keyfile).unwrap();
        db.close().await;
        admin.close().await;
    }
    #[tokio::test]
    async fn every_rpc_requires_a_server_authenticated_principal() {
        use crate::pb::{mail_service_server::MailService as _, *};
        use tonic::Request;
        let auth = fixture();
        let credentials =
            s3::creds::Credentials::new(Some("synthetic"), Some("synthetic"), None, None, None)
                .unwrap();
        let bucket = s3::Bucket::new(
            "synthetic",
            s3::Region::Custom {
                region: "local".into(),
                endpoint: "http://127.0.0.1:1".into(),
            },
            credentials,
        )
        .unwrap();
        let service = crate::service::MailService {
            auth: auth.clone(),
            principal: None,
            db: auth.inner.as_ref().unwrap().db.clone(),
            bucket: Arc::new(*bucket),
            domain: "inbox.test".into(),
            pipeline: None,
        };
        macro_rules! denied {
            ($method:ident,$request:expr) => {
                assert_eq!(
                    service
                        .$method(Request::new($request))
                        .await
                        .err()
                        .unwrap()
                        .code(),
                    tonic::Code::Unauthenticated,
                    stringify!($method)
                );
            };
        }
        denied!(create_domain, CreateDomainRequest::default());
        denied!(set_domain_policy, SetDomainPolicyRequest::default());
        denied!(list_domains, ListDomainsRequest::default());
        denied!(create_mailbox, CreateMailboxRequest::default());
        denied!(set_read_state, SetReadStateRequest::default());
        denied!(submit_message, SubmitMessageRequest::default());
        denied!(list_mailboxes, ListMailboxesRequest::default());
        denied!(list_messages, ListMessagesRequest::default());
        denied!(get_message, GetMessageRequest::default());
        denied!(get_raw_message, GetRawMessageRequest::default());
        denied!(get_submission, GetSubmissionRequest::default());
        denied!(watch_changes, WatchChangesRequest::default());
        let mut forged = Request::new(ListMailboxesRequest::default());
        forged
            .metadata_mut()
            .insert("x-inbox-owner", LOCAL_USER.to_string().parse().unwrap());
        forged
            .metadata_mut()
            .insert("x-inbox-administrator", "true".parse().unwrap());
        assert_eq!(
            service.list_mailboxes(forged).await.err().unwrap().code(),
            tonic::Code::Unauthenticated
        );
        let app = crate::mcp::InboxMcp::new(service, tokio_util::sync::CancellationToken::new())
            .hosted_router(
                mcp::server::StreamableHttpAuthorization::hosted(
                    mcp::McpProtectedResourceMetadata::new(
                        "https://inbox.example/mcp",
                        vec!["https://inbox.example/oauth".to_owned()],
                    ),
                    |_, _| {
                        Box::pin(async {
                            mcp::server::McpHostedTokenValidation::Unauthorized {
                                error_description: None,
                            }
                        })
                    },
                )
                .unwrap(),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let response = mcp_read_probe(&reqwest::Client::new(), &format!("http://{address}"))
            .header("Authorization", "Bearer upstream-provider-token")
            .header("Cookie", "__Host-inbox-session=synthetic")
            .header("x-inbox-owner", LOCAL_USER.to_string())
            .send()
            .await
            .unwrap();
        assert_mcp_unauthorized(response).await;
        server.abort();
    }
    #[test]
    fn partial_and_unknown_modes_fail_closed() {
        assert!(Config::from_values(HashMap::new()).unwrap().is_none());
        for (k, v) in [
            ("INBOX_AUTH_MODE", "auto"),
            ("INBOX_OIDC_ISSUER", "https://id.test"),
            ("INBOX_AUTH_MODE", "oidc"),
        ] {
            assert!(Config::from_values(HashMap::from([(k.into(), v.into())])).is_err());
        }
    }
    #[test]
    fn duplicate_cookies_fail_closed() {
        let mut h = HeaderMap::new();
        h.insert(
            header::COOKIE,
            "__Host-inbox-session=a; __Host-inbox-session=b"
                .parse()
                .unwrap(),
        );
        assert!(cookie(&h, COOKIE).is_none());
    }
    #[test]
    fn local_identity_is_not_an_oidc_identity() {
        assert_eq!(Actor::local().user_id, LOCAL_USER);
        assert_ne!(LOCAL_USER, Uuid::nil());
        assert!(
            Actor {
                user_id: Uuid::new_v4(),
                administrator: false
            }
            .require_admin()
            .is_err()
        );
    }
    #[test]
    fn administrator_authority_requires_the_verified_group_array() {
        for claims in [
            json!({}),
            json!({"groups":"inbox-admins"}),
            json!({"groups":["users"]}),
            json!({"groups":["inbox-admins",false]}),
            json!({"administrator":true}),
        ] {
            assert!(!administrator_claim(&claims, "inbox-admins"));
        }
        assert!(administrator_claim(
            &json!({"groups":["users","inbox-admins"]}),
            "inbox-admins"
        ));
    }
}
