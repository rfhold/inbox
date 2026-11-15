//! Generic hosted OAuth; only verified upstream issuer/subject creates an Inbox user.
use crate::auth::{Actor, Auth};
use axum::{
    Router,
    http::header,
    response::{Html, IntoResponse},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use mcp::{server::*, *};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::PgPool;
use std::result::Result;
use std::{sync::Arc, time::Duration};

pub struct Hosted {
    pub server: OAuthAuthorizationServer,
    pub oidc: OidcResourceOwnerAuthenticator,
    pub resource: String,
    pub issuer: String,
}
impl Hosted {
    pub async fn initialize(db: PgPool, auth: Auth) -> Result<Option<Self>, String> {
        Self::initialize_inner(
            db,
            auth,
            OidcEndpointPolicy::HttpsOnly,
            Duration::from_secs(600),
        )
        .await
    }
    #[cfg(test)]
    pub(crate) async fn initialize_loopback_test(
        db: PgPool,
        auth: Auth,
        access_token_lifetime: Duration,
    ) -> Result<Option<Self>, String> {
        Self::initialize_inner(
            db,
            auth,
            OidcEndpointPolicy::LoopbackTest,
            access_token_lifetime,
        )
        .await
    }
    async fn initialize_inner(
        db: PgPool,
        auth: Auth,
        endpoint_policy: OidcEndpointPolicy,
        access_token_lifetime: Duration,
    ) -> Result<Option<Self>, String> {
        let Some(config) = auth.config() else {
            return Ok(None);
        };
        let config = config.clone();
        let issuer = format!("{}/oauth", config.public_url);
        let resource = format!("{}/mcp", config.public_url);
        let store = Arc::new(
            PostgresOAuthAuthorizationStore::from_pool(db.clone())
                .await
                .map_err(|_| "OAuth persistence unavailable")?,
        );
        let oidc_store = Arc::new(
            PostgresOidcResourceOwnerStore::from_pool(db.clone())
                .await
                .map_err(|_| "OIDC persistence unavailable")?,
        );
        let entropy = Arc::new(Entropy);
        let oidc_config = OidcResourceOwnerConfig::new(
            config.issuer,
            config.client_id,
            Some(McpOAuthSecret::new(config.client_secret)),
            format!("{}/oauth/oidc/callback", config.public_url),
            format!("{issuer}/authorize"),
            vec!["openid".into(), "profile".into(), "email".into()],
            Duration::from_secs(300),
            endpoint_policy,
        )
        .map_err(|_| "invalid MCP OIDC configuration")?;
        let oidc = OidcResourceOwnerAuthenticator::discover(
            oidc_config,
            oidc_store,
            Arc::new(Mapper(auth.clone())),
            Arc::new(McpSystemOAuthClock),
            entropy.clone(),
            reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(10)),
        )
        .await
        .map_err(|_| "MCP OIDC discovery failed")?;
        let mut policy = OAuthAuthorizationServerConfig::new(
            &issuer,
            vec![OAuthResource {
                resource: resource.clone(),
                scopes: vec!["inbox:mcp".into()],
            }],
        );
        policy.registration_endpoint = Some(format!("{issuer}/register"));
        policy.access_token_lifetime = access_token_lifetime;
        policy.clock_skew = Duration::ZERO;
        policy.signing_verification_overlap = Duration::from_secs(660);
        let decision = policy.authorization_decision_endpoint.clone();
        let bytes =
            std::fs::read(&config.wrapping_keys_file).map_err(|_| "OAuth keyring unavailable")?;
        let file: Keyring = serde_json::from_slice(&bytes).map_err(|_| "invalid OAuth keyring")?;
        if file.schema_version != 1 {
            return Err("unsupported OAuth keyring".into());
        }
        let keys = file
            .keys
            .into_iter()
            .map(|k| URL_SAFE_NO_PAD.decode(k.key).map(|bytes| (k.id, bytes)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "invalid OAuth keyring")?;
        let keyring = Arc::new(
            VersionedOAuthWrappingKeyring::new(file.active, keys)
                .map_err(|_| "invalid OAuth keyring")?,
        );
        let server = OAuthAuthorizationServer::new(
            policy,
            store.clone(),
            Arc::new(oidc.clone()),
            Arc::new(Consent { auth, decision }),
            Arc::new(McpSystemOAuthClock),
            entropy,
            keyring,
        )
        .map_err(|_| "invalid OAuth policy")?
        .with_client_registration(OAuthClientRegistrationOptions {
            dynamic_registration: true,
            allow_loopback_redirects: true,
            ..Default::default()
        })
        .map_err(|_| "invalid OAuth registration")?;
        let mut tx = db
            .begin()
            .await
            .map_err(|_| "OAuth key initialization failed")?;
        sqlx::query("SELECT pg_advisory_xact_lock(873477278, 1)")
            .execute(&mut *tx)
            .await
            .map_err(|_| "OAuth key initialization failed")?;
        if store
            .active_signing_key(issuer.clone())
            .await
            .map_err(|_| "OAuth keys unavailable")?
            .is_none()
        {
            server
                .generate_signing_key_candidate(true)
                .await
                .map_err(|_| "OAuth key generation failed")?;
        }
        server
            .validate_signing_key_readiness()
            .await
            .map_err(|_| "OAuth signing key not ready")?;
        tx.commit()
            .await
            .map_err(|_| "OAuth key initialization failed")?;
        Ok(Some(Self {
            server,
            oidc,
            resource,
            issuer,
        }))
    }
    pub fn router(&self) -> Router {
        self.server.router().merge(self.oidc.router())
    }
    pub fn authorization(&self) -> mcp::Result<StreamableHttpAuthorization> {
        let server = self.server.clone();
        let metadata = McpProtectedResourceMetadata::new(&self.resource, vec![self.issuer.clone()])
            .with_scopes(["inbox:mcp"]);
        StreamableHttpAuthorization::hosted(metadata, move |token, context| {
            server.authorize_token(token, context)
        })
        .map(|a| a.with_required_scopes(["inbox:mcp"]))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Keyring {
    schema_version: u32,
    active: String,
    keys: Vec<Key>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Key {
    id: String,
    key: String,
}
struct Entropy;
impl McpOAuthEntropy for Entropy {
    fn fill_bytes(&self, bytes: &mut [u8]) -> mcp::Result<()> {
        getrandom::fill(bytes).map_err(|_| mcp::Error::protocol("entropy unavailable"))
    }
}
struct Mapper(Auth);
impl OidcPrincipalMapper for Mapper {
    fn map(&self, identity: OidcVerifiedIdentity) -> BoxFuture<OidcPrincipalMapping> {
        let auth = self.0.clone();
        Box::pin(async move {
            if auth.config().is_none_or(|c| c.issuer != identity.issuer) {
                return OidcPrincipalMapping::Denied;
            }
            let mut claims = Value::Object(identity.claims);
            claims["iss"] = json!(identity.issuer);
            claims["sub"] = json!(identity.subject);
            match auth.identity(&claims).await {
                Ok(actor) => McpPrincipalId::new(actor.user_id.to_string()).map_or(
                    OidcPrincipalMapping::Denied,
                    OidcPrincipalMapping::Principal,
                ),
                Err(_) => OidcPrincipalMapping::Unavailable,
            }
        })
    }
}
struct Consent {
    auth: Auth,
    decision: String,
}
fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
fn consent_cookie(tx: &str) -> String {
    format!(
        "__Host-inbox-consent-{}",
        &crate::mutation::hash(tx.as_bytes())[..24]
    )
}
impl OAuthConsentHandler for Consent {
    fn present(&self, model: OAuthConsentModel) -> BoxFuture<OAuthConsentPresentation> {
        let auth = self.auth.clone();
        let decision = self.decision.clone();
        Box::pin(async move {
            let csrf = auth.bind_cursor(
                &auth.config().expect("hosted auth").public_url,
                &Actor::local(),
                "consent",
                &model.transaction_id,
            );
            let body = format!(
                "<!doctype html><meta charset=utf-8><title>Authorize Inbox</title><h1>Authorize Inbox access</h1><p>Client: {}</p><p>Resource: {}</p><p>Scopes: {}</p><form method=post action=\"{}\"><input type=hidden name=transaction_id value=\"{}\"><input type=hidden name=csrf value=\"{}\"><button name=decision value=approve>Allow</button><button name=decision value=deny>Deny</button></form>",
                escape(&model.client_id),
                escape(&model.resource),
                escape(&model.requested_scopes.join(" ")),
                escape(&decision),
                escape(&model.transaction_id),
                escape(&csrf)
            );
            let cookie =
                crate::auth::set_cookie(&consent_cookie(&model.transaction_id), &csrf, 300);
            OAuthConsentPresentation::Response(([(header::SET_COOKIE,cookie),(header::CACHE_CONTROL,"no-store".into()),(header::CONTENT_SECURITY_POLICY,"default-src 'none'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'".into())],Html(body)).into_response())
        })
    }
    fn validate_decision(&self, e: OAuthConsentDecisionEvidence) -> BoxFuture<bool> {
        let auth = self.auth.clone();
        Box::pin(async move {
            auth.csrf(&e.headers)
                && crate::auth::cookie(&e.headers, &consent_cookie(&e.transaction_id))
                    == Some(e.csrf.expose_secret())
                && auth
                    .unbind_cursor(
                        &auth.config().expect("hosted auth").public_url,
                        &Actor::local(),
                        "consent",
                        e.csrf.expose_secret(),
                    )
                    .is_ok_and(|v| v == e.transaction_id)
        })
    }
}
