//! JWT verification hook for website traffic (Part P.1, Phase-01 Step C2).
//! No real issuer exists yet in Phase-01 (the Website Backend arrives in a
//! later phase) — this is the verification PATH being made real and
//! testable now, ready to point at a real signing key when a real issuer
//! exists. HS256 with a shared secret for now (env var / K8s Secret, per
//! CLAUDE.md: "Secrets via Kubernetes Secrets until Vault arrives in
//! Phase-04"); swapping to RS256 + a public key later is a config change to
//! this file, not a redesign of the callers.

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;
use uuid::Uuid;

use super::{AuthError, Identity};

#[derive(Debug, Deserialize)]
struct Claims {
    org_id: Uuid,
    #[serde(default)]
    user_id: Option<Uuid>,
    #[serde(default)]
    project_id: Option<Uuid>,
    #[serde(default)]
    conversation_id: Option<Uuid>,
    // Required by `Validation`'s default (validate_exp: true), even though
    // nothing here reads it directly — decode() fails before Claims is ever
    // returned if it's missing or in the past.
    #[allow(dead_code)]
    exp: usize,
}

/// A bearer token shaped like a JWT (two '.' separators) vs. an opaque API
/// key (Phase-01's keys are a flat "oz_..." hex string, no dots) — how the
/// `Identity` extractor decides which verification path to take.
pub fn looks_like_jwt(token: &str) -> bool {
    token.matches('.').count() == 2
}

pub fn verify_jwt(token: &str, secret: &[u8]) -> Result<Identity, AuthError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;

    let data = decode::<Claims>(token, &DecodingKey::from_secret(secret), &validation).map_err(
        |e| match e.kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::Expired,
            _ => AuthError::InvalidCredential,
        },
    )?;

    Ok(Identity {
        org_id: data.claims.org_id,
        user_id: data.claims.user_id,
        project_id: data.claims.project_id,
        conversation_id: data.claims.conversation_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde::Serialize;

    #[derive(Serialize)]
    struct TestClaims {
        org_id: Uuid,
        user_id: Option<Uuid>,
        project_id: Option<Uuid>,
        conversation_id: Option<Uuid>,
        exp: usize,
    }

    fn mint(secret: &[u8], org_id: Uuid, exp_offset_secs: i64) -> String {
        let claims = TestClaims {
            org_id,
            user_id: None,
            project_id: None,
            conversation_id: None,
            exp: (Utc::now().timestamp() + exp_offset_secs) as usize,
        };
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    }

    #[test]
    fn valid_token_resolves_identity() {
        let secret = b"test-secret";
        let org_id = Uuid::new_v4();
        let token = mint(secret, org_id, 3600);
        let identity = verify_jwt(&token, secret).unwrap();
        assert_eq!(identity.org_id, org_id);
        assert_eq!(identity.user_id, None);
    }

    #[test]
    fn expired_token_is_rejected() {
        let secret = b"test-secret";
        let token = mint(secret, Uuid::new_v4(), -3600);
        let err = verify_jwt(&token, secret).unwrap_err();
        assert_eq!(err, AuthError::Expired);
    }

    #[test]
    fn wrong_secret_is_rejected() {
        let secret = b"test-secret";
        let token = mint(secret, Uuid::new_v4(), 3600);
        let err = verify_jwt(&token, b"wrong-secret-entirely").unwrap_err();
        assert_eq!(err, AuthError::InvalidCredential);
    }

    #[test]
    fn garbage_token_is_rejected() {
        let err = verify_jwt("not-a-jwt-at-all", b"test-secret").unwrap_err();
        assert_eq!(err, AuthError::InvalidCredential);
    }

    #[test]
    fn tampered_signature_is_rejected() {
        let secret = b"test-secret";
        let mut token = mint(secret, Uuid::new_v4(), 3600);
        token.push('x'); // corrupt the signature segment
        let err = verify_jwt(&token, secret).unwrap_err();
        assert_eq!(err, AuthError::InvalidCredential);
    }

    #[test]
    fn looks_like_jwt_detects_shape() {
        assert!(looks_like_jwt("aaa.bbb.ccc"));
        assert!(!looks_like_jwt("oz_test_deadbeef"));
        assert!(!looks_like_jwt(""));
    }
}
