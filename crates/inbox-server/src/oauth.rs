//! Generic hosted OAuth; only verified upstream issuer/subject creates an Inbox user.
use crate::auth::Auth;
use axum::{Router, http::StatusCode, response::IntoResponse};
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
            Arc::new(AutoApproveConsent {
                resource: resource.clone(),
            }),
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
struct AutoApproveConsent {
    resource: String,
}
impl OAuthConsentHandler for AutoApproveConsent {
    fn present(&self, model: OAuthConsentModel) -> BoxFuture<OAuthConsentPresentation> {
        let approved =
            model.resource == self.resource && model.requested_scopes.as_slice() == ["inbox:mcp"];
        Box::pin(async move {
            if approved {
                OAuthConsentPresentation::Approved
            } else {
                OAuthConsentPresentation::Response(StatusCode::FORBIDDEN.into_response())
            }
        })
    }
    fn validate_decision(&self, _: OAuthConsentDecisionEvidence) -> BoxFuture<bool> {
        Box::pin(async { false })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn automatic_approval_requires_exact_resource_and_scope() {
        let policy = AutoApproveConsent {
            resource: "https://inbox.example/mcp".into(),
        };
        for (resource, scopes, allowed) in [
            ("https://inbox.example/mcp", vec!["inbox:mcp"], true),
            ("https://other.example/mcp", vec!["inbox:mcp"], false),
            ("https://inbox.example/mcp/", vec!["inbox:mcp"], false),
            ("https://inbox.example/mcp", vec![], false),
            ("https://inbox.example/mcp", vec!["inbox:api"], false),
            (
                "https://inbox.example/mcp",
                vec!["inbox:mcp", "inbox:api"],
                false,
            ),
            (
                "https://inbox.example/mcp",
                vec!["inbox:mcp", "inbox:mcp"],
                false,
            ),
        ] {
            let result = policy
                .present(OAuthConsentModel {
                    client_id: "untrusted-client".into(),
                    client_display_name: Some("<script>untrusted</script>".into()),
                    resource: resource.into(),
                    requested_scopes: scopes.into_iter().map(String::from).collect(),
                    already_granted_scopes: vec![],
                    transaction_id: "transaction".into(),
                })
                .await;
            match result {
                OAuthConsentPresentation::Approved => assert!(allowed),
                OAuthConsentPresentation::Response(response) => {
                    assert!(!allowed);
                    assert_eq!(response.status(), StatusCode::FORBIDDEN);
                }
            }
        }
        assert!(
            !policy
                .validate_decision(OAuthConsentDecisionEvidence {
                    transaction_id: "transaction".into(),
                    csrf: McpOAuthSecret::new("untrusted"),
                    headers: Default::default(),
                })
                .await
        );
    }
}
