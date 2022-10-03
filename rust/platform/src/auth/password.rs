use crate::error::Result;


use argon2::Argon2;
use password_hash::PasswordHash;

pub fn make_password_hash(password: &str) -> Result<String> {
    let salt = password_hash::SaltString::generate(rand::thread_rng());
    let hash = password_hash::PasswordHash::generate(
        argon2::Argon2::new(
            argon2::Algorithm::Argon2d,
            argon2::Version::V0x10,
            argon2::Params::default(),
        ),
        password,
        salt.as_str(),
    )?;

    Ok(hash.to_string())
}

pub fn verify_password(password: impl AsRef<[u8]>, password_hash: PasswordHash) -> Result<()> {
    Ok(password_hash.verify_password(&[&Argon2::default()], password)?)
}