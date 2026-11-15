use std::collections::HashMap;

#[derive(Clone)]
pub struct Config {
    namespace: String,
    replicas: usize,
    authentication: Authentication,
}

#[derive(Clone)]
enum Authentication {
    None,
    Password(String, String),
    Credentials(String),
}

impl Config {
    pub fn local() -> Self {
        Self {
            namespace: "INBOX".into(),
            replicas: 1,
            authentication: Authentication::None,
        }
    }

    pub fn from_env(oidc: bool) -> Result<Self, &'static str> {
        Self::from_values(std::env::vars().collect(), oidc)
    }

    pub fn from_values(values: HashMap<String, String>, oidc: bool) -> Result<Self, &'static str> {
        if oidc
            && (!values.contains_key("INBOX_NATS_NAMESPACE")
                || !values.contains_key("INBOX_NATS_REPLICAS"))
        {
            return Err("OIDC requires explicit NATS namespace and replicas");
        }
        let namespace = values
            .get("INBOX_NATS_NAMESPACE")
            .map(String::as_str)
            .unwrap_or("INBOX");
        if namespace.is_empty()
            || namespace.len() > 64
            || !namespace.as_bytes()[0].is_ascii_uppercase()
            || !namespace
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err("invalid NATS namespace");
        }
        let replicas = match values.get("INBOX_NATS_REPLICAS") {
            None => 1,
            Some(v) => match v.as_str() {
                "1" => 1,
                "3" => 3,
                "5" => 5,
                _ => return Err("invalid NATS replicas"),
            },
        };
        let authentication = match (
            values.get("INBOX_NATS_USER"),
            values.get("INBOX_NATS_PASSWORD"),
            values.get("INBOX_NATS_CREDENTIALS_FILE"),
        ) {
            (None, None, None) => Authentication::None,
            (Some(user), Some(password), None) if !user.is_empty() && !password.is_empty() => {
                Authentication::Password(user.clone(), password.clone())
            }
            (None, None, Some(path)) if !path.trim().is_empty() => {
                Authentication::Credentials(path.clone())
            }
            _ => return Err("invalid or ambiguous NATS authentication"),
        };
        Ok(Self {
            namespace: namespace.into(),
            replicas,
            authentication,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn replicas(&self) -> usize {
        self.replicas
    }

    pub fn resource(&self, suffix: &str) -> String {
        format!("{}_{suffix}", self.namespace)
    }

    pub fn subject(&self, suffix: &str) -> String {
        format!("{}.{suffix}", self.namespace)
    }

    pub(crate) fn outbox_subject(&self, subject: &str) -> Result<String, tonic::Status> {
        // Mutation producers persist logical local targets; resolve only these known routes.
        if subject == "INBOX.changes" || subject == self.subject("changes") {
            return Ok(self.subject("changes"));
        }
        if subject == "INBOX.commands.delivery" || subject == self.subject("commands.delivery") {
            return Ok(self.subject("commands.delivery"));
        }
        for prefix in ["INBOX.results.".into(), self.subject("results.")] {
            if let Some(id) = subject.strip_prefix(&prefix)
                && uuid::Uuid::parse_str(id).is_ok()
            {
                return Ok(self.subject(&format!("results.{id}")));
            }
        }
        Err(tonic::Status::failed_precondition("invalid outbox target"))
    }

    pub async fn connect(&self, url: &str) -> Result<async_nats::Client, tonic::Status> {
        // URL credentials would bypass paired/explicit authentication validation.
        let address: async_nats::ServerAddr = url
            .parse()
            .map_err(|_| tonic::Status::invalid_argument("invalid NATS address"))?;
        if address.has_user_pass() || address.password().is_some() {
            return Err(tonic::Status::invalid_argument(
                "NATS URL credentials are not allowed",
            ));
        }
        let options = async_nats::ConnectOptions::new()
            .connection_timeout(std::time::Duration::from_secs(3))
            .client_capacity(128);
        let options = match &self.authentication {
            Authentication::None => options,
            Authentication::Password(user, password) => {
                options.user_and_password(user.clone(), password.clone())
            }
            Authentication::Credentials(path) => options
                .credentials_file(path)
                .await
                .map_err(|_| tonic::Status::unavailable("NATS credentials unavailable"))?,
        };
        options
            .connect(address)
            .await
            .map_err(|_| tonic::Status::unavailable("NATS connection unavailable"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(values: &[(&str, &str)], oidc: bool) -> Result<Config, &'static str> {
        Config::from_values(
            values
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            oidc,
        )
    }

    #[test]
    fn namespace_is_canonical_and_explicit_when_hosted() {
        let local = config(&[], false).unwrap();
        assert_eq!(local.resource("COMMANDS"), "INBOX_COMMANDS");
        assert_eq!(local.replicas(), 1);
        for invalid in [
            "",
            "inbox",
            " INBOX",
            "INBOX ",
            "INBOX.*",
            "INBOX.PROD",
            "_INBOX",
            "INBOX-PR",
        ] {
            assert!(config(&[("INBOX_NATS_NAMESPACE", invalid)], false).is_err());
        }
        assert!(config(&[], true).is_err());
        let hosted = config(
            &[
                ("INBOX_NATS_NAMESPACE", "INBOX_PROD"),
                ("INBOX_NATS_REPLICAS", "3"),
            ],
            true,
        )
        .unwrap();
        assert_eq!(hosted.subject("changes"), "INBOX_PROD.changes");
        assert_eq!(hosted.replicas(), 3);
        for invalid in ["0", "2", "6", "03", " 3"] {
            assert!(config(&[("INBOX_NATS_REPLICAS", invalid)], false).is_err());
        }
    }

    #[test]
    fn partial_or_ambiguous_credentials_fail_closed() {
        for values in [
            vec![("INBOX_NATS_USER", "u")],
            vec![("INBOX_NATS_PASSWORD", "p")],
            vec![("INBOX_NATS_USER", ""), ("INBOX_NATS_PASSWORD", "p")],
            vec![("INBOX_NATS_CREDENTIALS_FILE", "")],
            vec![
                ("INBOX_NATS_USER", "u"),
                ("INBOX_NATS_PASSWORD", "p"),
                ("INBOX_NATS_CREDENTIALS_FILE", "/private/creds"),
            ],
        ] {
            assert!(config(&values, false).is_err());
        }
        assert!(
            config(
                &[("INBOX_NATS_USER", "u"), ("INBOX_NATS_PASSWORD", "p")],
                false
            )
            .is_ok()
        );
        assert!(config(&[("INBOX_NATS_CREDENTIALS_FILE", "/private/creds")], false).is_ok());
    }

    #[tokio::test]
    async fn url_credentials_cannot_bypass_explicit_configuration() {
        for url in [
            "nats://user:password@127.0.0.1:1",
            "nats://token@127.0.0.1:1",
            "nats://:password@127.0.0.1:1",
        ] {
            let error = match Config::local().connect(url).await {
                Ok(_) => panic!("URL credentials must fail before connecting"),
                Err(error) => error,
            };
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert!(!error.message().contains("password@"));
        }
    }

    #[test]
    fn outbox_resolution_never_accepts_foreign_environment() {
        let c = config(&[("INBOX_NATS_NAMESPACE", "INBOX_PREVIEW")], false).unwrap();
        assert_eq!(
            c.outbox_subject("INBOX.changes").unwrap(),
            "INBOX_PREVIEW.changes"
        );
        assert!(c.outbox_subject("INBOX_PROD.changes").is_err());
        assert!(c.outbox_subject("INBOX.results.not-a-uuid").is_err());
    }
}
