use argon2::Argon2;
use password_hash::PasswordHash;

use crate::error::Result;
use crate::PlatformError;

pub fn make_password_hash(password: &str) -> Result<String> {
    let salt = password_hash::SaltString::generate(rand::thread_rng());
    let phf = Argon2::new(
        argon2::Algorithm::Argon2d,
        argon2::Version::V0x10,
        argon2::Params::default(),
    );
    let hash = PasswordHash::generate(phf, password, &salt)?;

    Ok(hash.to_string())
}

pub fn verify_password(password: impl AsRef<[u8]>, password_hash: PasswordHash) -> Result<()> {
    Ok(password_hash.verify_password(&[&Argon2::default()], password)?)
}

pub fn check_password_complexity(password: &str, user_inputs: &[&str]) -> Result<()> {
    match zxcvbn::zxcvbn(password, user_inputs) {
        Ok(ent) if ent.score() < 3 => {
            return Err(PlatformError::invalid_field(
                "password",
                "password is too simple",
            ));
        }
        Err(err) => return Err(PlatformError::invalid_field("password", err.to_string())),
        _ => {}
    }

    Ok(())
}
