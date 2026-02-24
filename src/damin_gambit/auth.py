from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Optional

import httpx
from fastapi import HTTPException, Request
from jose import jwt
from jose.exceptions import ExpiredSignatureError, JWTClaimsError, JWTError


@dataclass(frozen=True)
class Principal:
    sub: Optional[str]
    tenant_id: Optional[str]
    claims: dict[str, Any]


def _env_truthy(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() not in {"0", "false", "no", "off"}


def _claim_path(claims: dict[str, Any], path: str) -> Optional[Any]:
    cur: Any = claims
    for part in (path or "").split("."):
        part = part.strip()
        if not part:
            continue
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


_JWKS_CACHE: dict[str, Any] = {"exp": 0.0, "jwks": None}


async def _get_jwks(jwks_url: str) -> dict[str, Any]:
    now = time.time()
    if _JWKS_CACHE["jwks"] is not None and float(_JWKS_CACHE["exp"]) > now:
        return _JWKS_CACHE["jwks"]

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(jwks_url)
        r.raise_for_status()
        jwks = r.json()

    # cache for 10 minutes
    _JWKS_CACHE["jwks"] = jwks
    _JWKS_CACHE["exp"] = now + 600
    return jwks


async def require_principal(request: Request) -> Principal:
    """
    Validate bearer token (if required) and return user/tenant context.

    Env config:
    - DAMIN_GAMBIT_REQUIRE_AUTH=0/1
    - DAMIN_GAMBIT_JWT_ALG=HS256|RS256 (default RS256)
    - DAMIN_GAMBIT_JWT_SECRET=... (HS256)
    - DAMIN_GAMBIT_JWKS_URL=... (RS256)
    - DAMIN_GAMBIT_JWT_ISSUER=... (optional)
    - DAMIN_GAMBIT_JWT_AUDIENCE=... (optional)
    - DAMIN_GAMBIT_TENANT_CLAIM=tenant_id (default)
    - DAMIN_GAMBIT_USER_CLAIM=sub (default)
    """
    require_auth = _env_truthy("DAMIN_GAMBIT_REQUIRE_AUTH", "0")

    auth = (request.headers.get("authorization") or request.headers.get("Authorization") or "").strip()
    if not auth:
        if require_auth:
            raise HTTPException(status_code=401, detail="Missing Authorization: Bearer <token>")
        return Principal(sub=None, tenant_id=None, claims={})

    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Authorization header must be: Bearer <token>")
    token = auth.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Authorization header must be: Bearer <token>")

    alg = (os.getenv("DAMIN_GAMBIT_JWT_ALG") or "RS256").strip().upper()
    issuer = (os.getenv("DAMIN_GAMBIT_JWT_ISSUER") or "").strip() or None
    audience = (os.getenv("DAMIN_GAMBIT_JWT_AUDIENCE") or "").strip() or None
    tenant_claim = (os.getenv("DAMIN_GAMBIT_TENANT_CLAIM") or "tenant_id").strip()
    user_claim = (os.getenv("DAMIN_GAMBIT_USER_CLAIM") or "sub").strip()

    key: Any = None
    if alg == "HS256":
        secret = (os.getenv("DAMIN_GAMBIT_JWT_SECRET") or "").strip()
        if not secret:
            raise HTTPException(status_code=503, detail="Auth misconfigured: DAMIN_GAMBIT_JWT_SECRET is required for HS256")
        key = secret
    elif alg == "RS256":
        jwks_url = (os.getenv("DAMIN_GAMBIT_JWKS_URL") or "").strip()
        if not jwks_url:
            raise HTTPException(status_code=503, detail="Auth misconfigured: DAMIN_GAMBIT_JWKS_URL is required for RS256")
        jwks = await _get_jwks(jwks_url)
        key = jwks
    else:
        raise HTTPException(status_code=503, detail=f"Auth misconfigured: unsupported DAMIN_GAMBIT_JWT_ALG={alg}")

    options = {"verify_aud": audience is not None}
    try:
        claims = jwt.decode(token, key, algorithms=[alg], issuer=issuer, audience=audience, options=options)
    except ExpiredSignatureError as e:
        raise HTTPException(status_code=401, detail="Token expired") from e
    except JWTClaimsError as e:
        raise HTTPException(status_code=401, detail=f"Token claims error: {e}") from e
    except JWTError as e:
        raise HTTPException(status_code=401, detail="Invalid token") from e

    sub = _claim_path(claims, user_claim)
    tenant = _claim_path(claims, tenant_claim)
    return Principal(
        sub=str(sub) if sub is not None else None,
        tenant_id=str(tenant) if tenant is not None else None,
        claims=dict(claims),
    )

