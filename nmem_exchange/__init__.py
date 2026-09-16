"""nmem-exchange — pluggable, end-to-end-secured message bus for nmem agents."""
from nmem_exchange.crypto import (   # noqa: F401
    ExchangeCryptoError,
    Identity,
    NotAMember,
    Unauthorized,
    generate_identity,
    identity_from_secret,
    open_envelope,
    seal,
    seal_open,
    verify_sig,
)

__version__ = "0.2.0"
