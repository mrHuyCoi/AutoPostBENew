import base64
import hashlib
import secrets
import string


def generate_code_verifier(length: int = 43) -> str:
    """
    Generate a PKCE code_verifier string with allowed characters and length 43.
    Allowed set: ALPHA / DIGIT / "-" / "." / "_" / "~"
    """
    charset = string.ascii_letters + string.digits + "-._~"
    return "".join(secrets.choice(charset) for _ in range(length))


def code_challenge_s256(code_verifier: str) -> str:
    """
    Create a code_challenge from code_verifier using SHA-256 and base64url without padding.
    """
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
