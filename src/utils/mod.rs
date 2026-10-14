use argon2::password_hash::rand_core::{OsRng, RngCore};

const LOGIN_CODE_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const LOGIN_CODE_LEN: usize = 10;

/// Login codes are typed by hand, so the alphabet drops the characters that are easy to
/// confuse (0/O, 1/I). 10 chars over 32 symbols is ~50 bits, enough that guessing is
/// hopeless even before the rate limit kicks in.
pub(crate) fn generate_login_code() -> String {
    let mut bytes = [0u8; LOGIN_CODE_LEN];
    OsRng.fill_bytes(&mut bytes);
    bytes
        .iter()
        .map(|byte| LOGIN_CODE_ALPHABET[*byte as usize % LOGIN_CODE_ALPHABET.len()] as char)
        .collect()
}

pub(crate) fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(db) if db.is_unique_violation())
}
