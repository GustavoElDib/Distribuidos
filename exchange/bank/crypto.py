from __future__ import annotations

import base64
import logging
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
    load_pem_private_key,
    load_pem_public_key,
)

logger = logging.getLogger(__name__)


def generate_keypair() -> tuple[Ed25519PrivateKey, Ed25519PublicKey]:
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    return private_key, public_key


def private_key_to_pem(private_key: Ed25519PrivateKey) -> bytes:
    return private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )


def public_key_to_pem(public_key: Ed25519PublicKey) -> bytes:
    return public_key.public_bytes(
        encoding=Encoding.PEM,
        format=PublicFormat.SubjectPublicKeyInfo,
    )


def load_private_key_from_pem(pem_data: bytes) -> Ed25519PrivateKey:
    return load_pem_private_key(pem_data, password=None)  # type: ignore[return-value]


def load_public_key_from_pem(pem_data: bytes) -> Ed25519PublicKey:
    return load_pem_public_key(pem_data)  # type: ignore[return-value]


def sign_block(private_key: Ed25519PrivateKey, block_hash: str) -> str:
    signature_bytes = private_key.sign(block_hash.encode("utf-8"))
    return base64.b64encode(signature_bytes).decode("utf-8")


def verify_block(public_key: Ed25519PublicKey, block_hash: str, signature: str) -> bool:
    try:
        sig_bytes = base64.b64decode(signature)
        public_key.verify(sig_bytes, block_hash.encode("utf-8"))
        return True
    except Exception:
        return False


def load_peer_keys(keys_dir: Path) -> dict[str, Ed25519PublicKey]:
    peer_keys: dict[str, Ed25519PublicKey] = {}
    for pub_file in keys_dir.glob("*.pub"):
        bank_id = pub_file.stem
        pem_data = pub_file.read_bytes()
        peer_keys[bank_id] = load_public_key_from_pem(pem_data)
        logger.debug("loaded public key for %s", bank_id)
    return peer_keys


def save_keypair(keys_dir: Path, bank_id: str, private_key: Ed25519PrivateKey) -> None:
    public_key = private_key.public_key()
    priv_path = keys_dir / f"{bank_id}.priv"
    pub_path = keys_dir / f"{bank_id}.pub"
    priv_path.write_bytes(private_key_to_pem(private_key))
    pub_path.write_bytes(public_key_to_pem(public_key))
    logger.info("saved keypair for %s to %s", bank_id, keys_dir)


def load_private_key(keys_dir: Path, bank_id: str) -> Ed25519PrivateKey:
    priv_path = keys_dir / f"{bank_id}.priv"
    return load_private_key_from_pem(priv_path.read_bytes())
