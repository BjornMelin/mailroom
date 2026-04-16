use super::AuthError;
use crate::config::GmailConfig;
use anyhow::Result;
use oauth2::{AuthorizationCode, CsrfToken, RedirectUrl};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{Instant, timeout};
use url::Url;

const CALLBACK_TIMEOUT_SECS: u64 = 180;
const CALLBACK_PATH: &str = "/oauth2/callback";

#[derive(Debug)]
pub struct CallbackListener {
    listener: TcpListener,
    pub redirect_url: RedirectUrl,
}

impl CallbackListener {
    pub async fn bind(config: &GmailConfig) -> Result<Self> {
        let address = format!("{}:{}", config.listen_host, config.listen_port);
        let listener = TcpListener::bind(&address).await?;
        let local_addr = listener.local_addr()?;
        let redirect_url = redirect_url_for(local_addr)?;

        Ok(Self {
            listener,
            redirect_url,
        })
    }

    pub async fn wait_for_code(self, expected_state: &CsrfToken) -> Result<AuthorizationCode> {
        let deadline = Instant::now() + Duration::from_secs(CALLBACK_TIMEOUT_SECS);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AuthError::CallbackTimedOut.into());
            }

            let (mut stream, _) = timeout(remaining, self.listener.accept())
                .await
                .map_err(|_| AuthError::CallbackTimedOut)?
                .map_err(AuthError::CallbackIo)?;

            let mut buffer = vec![0_u8; 8 * 1024];
            let bytes_read = stream
                .read(&mut buffer)
                .await
                .map_err(AuthError::CallbackIo)?;
            let request = String::from_utf8_lossy(&buffer[..bytes_read]);
            let callback = match parse_callback_request(&request) {
                Ok(callback) => callback,
                Err(error) if is_malformed_callback_error(&error) => {
                    write_callback_response(
                        &mut stream,
                        "400 Bad Request",
                        "Mailroom is waiting for the Gmail OAuth callback on /oauth2/callback.",
                    )
                    .await?;
                    continue;
                }
                Err(error) => return Err(error),
            };

            let response = match callback {
                Ok(code) => {
                    if code.state != *expected_state.secret() {
                        write_callback_response(
                            &mut stream,
                            "400 Bad Request",
                            "OAuth state mismatch. You can close this tab and retry `mailroom auth login`.",
                        )
                        .await?;
                        return Err(AuthError::StateMismatch.into());
                    }

                    write_callback_response(
                        &mut stream,
                        "200 OK",
                        "Mailroom received the Gmail authorization response. You can close this tab.",
                    )
                    .await?;
                    return Ok(AuthorizationCode::new(code.code));
                }
                Err(error) => error,
            };

            write_callback_response(&mut stream, "400 Bad Request", &response).await?;
            return Err(AuthError::OAuthCallback(response).into());
        }
    }
}

pub fn open_browser_if_requested(url: &Url, enabled: bool) -> Result<bool> {
    if !enabled {
        return Ok(false);
    }

    webbrowser::open(url.as_str())
        .map(|_| true)
        .map_err(|error| AuthError::BrowserOpen(error.to_string()).into())
}

async fn write_callback_response(
    stream: &mut tokio::net::TcpStream,
    status: &str,
    body: &str,
) -> Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .map_err(AuthError::CallbackIo)?;
    Ok(())
}

fn redirect_url_for(local_addr: SocketAddr) -> Result<RedirectUrl> {
    RedirectUrl::new(format!("http://{local_addr}{CALLBACK_PATH}"))
        .map_err(|_| AuthError::InvalidRedirectUrl.into())
}

fn is_malformed_callback_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<AuthError>()
        .is_some_and(|error| matches!(error, AuthError::MalformedCallbackRequest))
}

fn parse_callback_request(request: &str) -> Result<Result<ParsedCallback, String>> {
    let request_line = request
        .lines()
        .next()
        .ok_or(AuthError::MalformedCallbackRequest)?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().ok_or(AuthError::MalformedCallbackRequest)?;
    let target = parts.next().ok_or(AuthError::MalformedCallbackRequest)?;

    if method != "GET" {
        return Err(AuthError::MalformedCallbackRequest.into());
    }

    let url = Url::parse(&format!("http://localhost{target}"))
        .map_err(|_| AuthError::MalformedCallbackRequest)?;
    if url.path() != CALLBACK_PATH {
        return Err(AuthError::MalformedCallbackRequest.into());
    }

    let mut code = None;
    let mut state = None;
    let mut oauth_error = None;
    let mut oauth_error_description = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "code" => code = Some(value.into_owned()),
            "state" => state = Some(value.into_owned()),
            "error" => oauth_error = Some(value.into_owned()),
            "error_description" => oauth_error_description = Some(value.into_owned()),
            _ => {}
        }
    }

    if let Some(error) = oauth_error {
        let description = oauth_error_description
            .unwrap_or_else(|| String::from("Google rejected the authorization request."));
        return Ok(Err(format!("{error}: {description}")));
    }

    let code = code.ok_or(AuthError::MissingAuthorizationCode)?;
    let state = state.ok_or(AuthError::StateMismatch)?;
    Ok(Ok(ParsedCallback { code, state }))
}

#[derive(Debug)]
struct ParsedCallback {
    code: String,
    state: String,
}

#[cfg(test)]
mod tests {
    use super::{CallbackListener, parse_callback_request, redirect_url_for};
    use crate::config::GmailConfig;
    use oauth2::CsrfToken;
    use std::net::SocketAddr;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };
    use url::Url;

    #[test]
    fn parses_successful_callback_request() {
        let callback = parse_callback_request(
            "GET /oauth2/callback?code=abc&state=def HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .unwrap()
        .unwrap();

        assert_eq!(callback.code, "abc");
        assert_eq!(callback.state, "def");
    }

    #[test]
    fn parses_oauth_error_response() {
        let response = parse_callback_request(
            "GET /oauth2/callback?error=access_denied&error_description=nope HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .unwrap();

        assert_eq!(response.unwrap_err(), "access_denied: nope");
    }

    #[test]
    fn redirect_url_brackets_ipv6_hosts() {
        let local_addr: SocketAddr = "[::1]:8181".parse().unwrap();

        assert_eq!(
            redirect_url_for(local_addr).unwrap().as_str(),
            "http://[::1]:8181/oauth2/callback"
        );
    }

    #[tokio::test]
    async fn wait_for_code_returns_oauth_callback_error() {
        let listener = CallbackListener::bind(&GmailConfig {
            client_id: Some(String::from("client-id")),
            client_secret: None,
            auth_url: String::from("https://accounts.google.com/o/oauth2/v2/auth"),
            token_url: String::from("https://oauth2.googleapis.com/token"),
            api_base_url: String::from("https://gmail.googleapis.com/gmail/v1"),
            listen_host: String::from("127.0.0.1"),
            listen_port: 0,
            open_browser: false,
            request_timeout_secs: 30,
            scopes: vec![String::from("https://www.googleapis.com/auth/gmail.modify")],
        })
        .await
        .unwrap();
        let callback_url = Url::parse(&listener.redirect_url.to_string()).unwrap();
        let callback_host = callback_url.host_str().unwrap();
        let callback_port = callback_url.port().unwrap();
        let wait_for_code = tokio::spawn(async move {
            listener
                .wait_for_code(&CsrfToken::new(String::from("expected-state")))
                .await
                .unwrap_err()
                .to_string()
        });

        let mut stream = TcpStream::connect((callback_host, callback_port))
            .await
            .unwrap();
        stream
            .write_all(
                b"GET /oauth2/callback?error=access_denied&error_description=nope HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();

        assert!(response.contains("400 Bad Request"));
        assert!(response.contains("access_denied: nope"));
        assert_eq!(
            wait_for_code.await.unwrap(),
            String::from("oauth callback returned an error: access_denied: nope")
        );
    }

    #[tokio::test]
    async fn wait_for_code_ignores_unrelated_requests_until_callback_arrives() {
        let listener = CallbackListener::bind(&GmailConfig {
            client_id: Some(String::from("client-id")),
            client_secret: None,
            auth_url: String::from("https://accounts.google.com/o/oauth2/v2/auth"),
            token_url: String::from("https://oauth2.googleapis.com/token"),
            api_base_url: String::from("https://gmail.googleapis.com/gmail/v1"),
            listen_host: String::from("127.0.0.1"),
            listen_port: 0,
            open_browser: false,
            request_timeout_secs: 30,
            scopes: vec![String::from("https://www.googleapis.com/auth/gmail.modify")],
        })
        .await
        .unwrap();
        let callback_url = Url::parse(&listener.redirect_url.to_string()).unwrap();
        let callback_host = callback_url.host_str().unwrap();
        let callback_port = callback_url.port().unwrap();
        let wait_for_code = tokio::spawn(async move {
            listener
                .wait_for_code(&CsrfToken::new(String::from("expected-state")))
                .await
                .unwrap()
                .secret()
                .to_owned()
        });

        let mut unrelated_stream = TcpStream::connect((callback_host, callback_port))
            .await
            .unwrap();
        unrelated_stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut unrelated_response = String::new();
        unrelated_stream
            .read_to_string(&mut unrelated_response)
            .await
            .unwrap();

        let mut callback_stream = TcpStream::connect((callback_host, callback_port))
            .await
            .unwrap();
        callback_stream
            .write_all(
                b"GET /oauth2/callback?code=real-code&state=expected-state HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
            .await
            .unwrap();
        let mut callback_response = String::new();
        callback_stream
            .read_to_string(&mut callback_response)
            .await
            .unwrap();

        assert!(unrelated_response.contains("400 Bad Request"));
        assert!(unrelated_response.contains("/oauth2/callback"));
        assert!(callback_response.contains("200 OK"));
        assert_eq!(wait_for_code.await.unwrap(), String::from("real-code"));
    }
}
