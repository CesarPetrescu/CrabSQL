use crate::error::MiniError;
use crate::model::UserRecord;
use bitflags::bitflags;
use sha1::{Digest, Sha1};

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Priv: u64 {
        const SELECT = 1 << 0;
        const INSERT = 1 << 1;
        const UPDATE = 1 << 2;
        const DELETE = 1 << 3;
        const CREATE = 1 << 4;
        const DROP   = 1 << 5;
        const CREATE_USER = 1 << 6;
        const GRANT_OPTION = 1 << 7;
        const ALL = Self::SELECT.bits() | Self::INSERT.bits() | Self::UPDATE.bits() | Self::DELETE.bits()
                  | Self::CREATE.bits() | Self::DROP.bits() | Self::CREATE_USER.bits() | Self::GRANT_OPTION.bits();
    }
}

pub fn stage2_from_password(password: &[u8]) -> [u8; 20] {
    let stage1 = Sha1::digest(password);
    let stage2 = Sha1::digest(stage1);
    stage2.into()
}

/// Verify the mysql_native_password token (auth_data) against the stored stage2 hash.
///
/// Stored form is SHA1(SHA1(password)) (20 bytes).
pub fn verify_native_password_token(
    salt: &[u8],
    stored_stage2: &[u8; 20],
    auth_data: &[u8],
) -> bool {
    if auth_data.is_empty() {
        return false;
    }
    if auth_data.len() != 20 {
        return false;
    }

    // token = stage1 XOR SHA1(salt + stage2)
    // => stage1 = token XOR SHA1(salt + stage2)
    let mut hasher = Sha1::new();
    hasher.update(salt);
    hasher.update(stored_stage2);
    let salt_stage2_hash: [u8; 20] = hasher.finalize().into();

    let mut stage1 = [0u8; 20];
    for i in 0..20 {
        stage1[i] = auth_data[i] ^ salt_stage2_hash[i];
    }

    let stage2_check: [u8; 20] = Sha1::digest(stage1).into();
    stage2_check == *stored_stage2
}

pub fn verify_mysql_native_password(
    salt: &[u8],
    auth_data: &[u8],
    stored_stage2: Option<[u8; 20]>,
) -> bool {
    // Empty password can be represented by empty auth_data.
    if stored_stage2.is_none() {
        return auth_data.is_empty();
    }
    verify_native_password_token(salt, &stored_stage2.unwrap(), auth_data)
}

#[allow(dead_code)]
pub fn parse_priv_list(input: &str) -> Result<Priv, MiniError> {
    let s = input.trim();
    if s.eq_ignore_ascii_case("ALL") || s.eq_ignore_ascii_case("ALL PRIVILEGES") {
        return Ok(Priv::ALL);
    }
    let mut acc = Priv::empty();
    for part in s.split(',') {
        let p = part.trim();
        let bit = if p.eq_ignore_ascii_case("SELECT") {
            Priv::SELECT
        } else if p.eq_ignore_ascii_case("INSERT") {
            Priv::INSERT
        } else if p.eq_ignore_ascii_case("UPDATE") {
            Priv::UPDATE
        } else if p.eq_ignore_ascii_case("DELETE") {
            Priv::DELETE
        } else if p.eq_ignore_ascii_case("CREATE") {
            Priv::CREATE
        } else if p.eq_ignore_ascii_case("DROP") {
            Priv::DROP
        } else if p.eq_ignore_ascii_case("CREATE USER") || p.eq_ignore_ascii_case("CREATE_USER") {
            Priv::CREATE_USER
        } else if p.eq_ignore_ascii_case("GRANT OPTION") || p.eq_ignore_ascii_case("GRANT_OPTION") {
            Priv::GRANT_OPTION
        } else {
            return Err(MiniError::Parse(format!("unknown privilege: {p}")));
        };
        acc |= bit;
    }
    Ok(acc)
}

pub fn has_priv(user: &UserRecord, db: Option<&str>, needed: Priv) -> bool {
    let global = Priv::from_bits_truncate(user.global_privs);
    if global.contains(needed) {
        return true;
    }
    if let Some(db) = db {
        if let Some(bits) = user.db_privs.get(db) {
            let dbp = Priv::from_bits_truncate(*bits);
            return dbp.contains(needed);
        }
    }
    false
}
