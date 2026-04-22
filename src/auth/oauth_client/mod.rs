mod import;
mod interactive;
mod resolve;
mod storage;
mod types;

#[cfg(test)]
mod tests;

pub use import::ImportedOAuthClient;
#[allow(unused_imports)]
pub use import::ImportedOAuthClientSourceKind;
#[cfg(test)]
pub(crate) use import::setup_guidance;
pub(crate) use import::{PreparedSetup, persist_prepared_google_desktop_client, prepare_setup};
pub use resolve::{
    OAuthClientError, OAuthClientSource, ResolvedOAuthClient, oauth_client_source, resolve,
};

pub const GOOGLE_AUTH_OVERVIEW_URL: &str = "https://console.cloud.google.com/auth/overview";
pub const GOOGLE_AUTH_CLIENTS_URL: &str = "https://console.cloud.google.com/auth/clients";
pub const GOOGLE_GMAIL_API_URL: &str =
    "https://console.cloud.google.com/apis/library/gmail.googleapis.com";
pub(crate) const GOOGLE_AUTH_CERTS_URL: &str = "https://www.googleapis.com/oauth2/v1/certs";
pub(crate) const DEFAULT_REDIRECT_URI: &str = "http://localhost";
pub(crate) const DEFAULT_AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
pub(crate) const DEFAULT_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
