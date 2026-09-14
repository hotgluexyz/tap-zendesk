"""Zendesk Authentication."""

from __future__ import annotations

from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from typing_extensions import override

# Zendesk's token endpoint is scoped to the account's subdomain. The unformatted
# template is what the capability probe sees, when no config is available yet.
TOKEN_ENDPOINT = "https://{subdomain}.zendesk.com/oauth/tokens"


def token_endpoint_for(config: dict) -> str:
    """Return the Zendesk OAuth token endpoint for the configured subdomain.

    Args:
        config: The tap config.

    Returns:
        The fully qualified OAuth token endpoint.

    Raises:
        InvalidCredentialsError: If `subdomain` is missing from the config.
    """
    subdomain = config.get("subdomain")
    if not subdomain:
        msg = "Zendesk requires `subdomain` in config."
        raise InvalidCredentialsError(msg)
    return TOKEN_ENDPOINT.format(subdomain=subdomain)


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class ZendeskAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Zendesk."""

    @override
    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the Zendesk API.

        Returns:
            A dict with the request body

        Raises:
            InvalidCredentialsError: If `refresh_token` is missing from the config.
        """
        refresh_token = self.config.get("refresh_token")
        if not refresh_token:
            msg = "OAuth mode requires `refresh_token` in config."
            raise InvalidCredentialsError(msg)
        return {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": refresh_token,
        }
