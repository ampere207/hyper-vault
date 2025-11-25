use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Key, Nonce,
};
use argon2::{Argon2, Params, Algorithm, Version};
use pbkdf2::pbkdf2_hmac;
use sha2::{Sha256, Digest};
use std::io::{Error, ErrorKind};

pub const SALT_LENGTH: usize = 16;
pub const NONCE_LENGTH: usize = 12; // 96 bits for GCM

/// Encryption key derived from password
#[derive(Debug, Clone)]
pub struct EncryptionKey {
    key: Key<Aes256Gcm>,
}

impl EncryptionKey {
    /// Derive encryption key from password using PBKDF2 (faster than Argon2 for encryption keys)
    /// This is called once and the key is cached for performance
    /// Uses a deterministic salt derived from password for key caching
    pub fn from_password(password: &str, salt: &[u8]) -> Result<Self, Error> {
        let mut key_bytes = [0u8; 32]; // 256 bits for AES-256
        
        // Use PBKDF2 with 100,000 iterations - fast enough for encryption keys
        // Much faster than Argon2 while still being secure
        pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, 100_000, &mut key_bytes);
        
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        Ok(EncryptionKey { key: *key })
    }
    
    /// Derive encryption key from password using a deterministic salt (for caching)
    /// This allows us to cache the key and reuse it for all operations
    pub fn from_password_cached(password: &str) -> Result<Self, Error> {
        // Use a deterministic salt derived from password hash
        // This allows key caching while maintaining security
        let mut hasher = Sha256::new();
        hasher.update(b"hyper_vault_salt_");
        hasher.update(password.as_bytes());
        let deterministic_salt = hasher.finalize();
        
        Self::from_password(password, &deterministic_salt[..16])
    }
    
    /// Derive key using Argon2 (slower but more secure - use for initial password verification)
    #[allow(dead_code)]
    pub fn from_password_argon2(password: &str, salt: &[u8]) -> Result<Self, Error> {
        let mut key_bytes = [0u8; 32];
        
        // Use lighter Argon2 parameters
        let argon2 = Argon2::new(
            Algorithm::Argon2id,
            Version::V0x13,
            Params::new(16384, 2, 1, Some(32)).map_err(|e| {
                Error::new(ErrorKind::InvalidData, format!("Argon2 params error: {}", e))
            })?,
        );
        
        argon2
            .hash_password_into(password.as_bytes(), salt, &mut key_bytes)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Key derivation error: {}", e)))?;
        
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        Ok(EncryptionKey { key: *key })
    }
    
    /// Generate a random salt
    #[allow(dead_code)]
    pub fn generate_salt() -> [u8; SALT_LENGTH] {
        let mut salt = [0u8; SALT_LENGTH];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut salt);
        salt
    }
    
    /// Create encryption key directly from bytes (for cached keys)
    #[allow(dead_code)]
    pub fn from_bytes(key_bytes: &[u8; 32]) -> Self {
        let key = Key::<Aes256Gcm>::from_slice(key_bytes);
        EncryptionKey { key: *key }
    }
    
    /// Get key bytes (for caching)
    #[allow(dead_code)]
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.key.as_slice().try_into().unwrap()
    }
}

/// Encrypt data using AES-256-GCM
pub fn encrypt_data(data: &[u8], key: &EncryptionKey) -> Result<Vec<u8>, Error> {
    let cipher = Aes256Gcm::new(&key.key);
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    
    let ciphertext = cipher
        .encrypt(&nonce, data)
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Encryption error: {}", e)))?;
    
    // Prepend nonce to ciphertext: [nonce (12 bytes)][ciphertext]
    let mut result = Vec::with_capacity(NONCE_LENGTH + ciphertext.len());
    result.extend_from_slice(nonce.as_slice());
    result.extend_from_slice(&ciphertext);
    
    Ok(result)
}

/// Decrypt data using AES-256-GCM
pub fn decrypt_data(encrypted_data: &[u8], key: &EncryptionKey) -> Result<Vec<u8>, Error> {
    if encrypted_data.len() < NONCE_LENGTH {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "Encrypted data too short to contain nonce",
        ));
    }
    
    // Extract nonce and ciphertext
    let nonce = Nonce::from_slice(&encrypted_data[..NONCE_LENGTH]);
    let ciphertext = &encrypted_data[NONCE_LENGTH..];
    
    let cipher = Aes256Gcm::new(&key.key);
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Decryption error: {}", e)))?;
    
    Ok(plaintext)
}

/// Encrypted storage format: [salt (16 bytes)][nonce (12 bytes)][ciphertext]
/// Uses cached key to avoid expensive Argon2 derivation on every save
/// Note: Salt is stored but not used for key derivation when using cached key
pub fn encrypt_with_salt(data: &[u8], cached_key: &EncryptionKey) -> Result<Vec<u8>, Error> {
    let encrypted = encrypt_data(data, cached_key)?;
    
    // Store a placeholder salt for format compatibility
    // The actual key derivation was done once and cached
    let mut result = Vec::with_capacity(SALT_LENGTH + encrypted.len());
    result.extend_from_slice(&[0u8; SALT_LENGTH]); // Placeholder salt
    result.extend_from_slice(&encrypted);
    
    Ok(result)
}

/// Encrypt with password (derives key using PBKDF2 - use only once, then cache)
#[allow(dead_code)]
pub fn encrypt_with_password(data: &[u8], password: &str) -> Result<(Vec<u8>, EncryptionKey), Error> {
    let salt = EncryptionKey::generate_salt();
    let key = EncryptionKey::from_password(password, &salt)?;
    let encrypted = encrypt_data(data, &key)?;
    
    // Prepend salt: [salt (16 bytes)][nonce (12 bytes)][ciphertext]
    let mut result = Vec::with_capacity(SALT_LENGTH + encrypted.len());
    result.extend_from_slice(&salt);
    result.extend_from_slice(&encrypted);
    
    Ok((result, key))
}

/// Decrypt data with salt (derives key - use only once, then cache)
pub fn decrypt_with_salt(encrypted_data: &[u8], password: &str) -> Result<(Vec<u8>, EncryptionKey), Error> {
    if encrypted_data.len() < SALT_LENGTH + NONCE_LENGTH {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "Encrypted data too short",
        ));
    }
    
    // Extract salt
    let salt = &encrypted_data[..SALT_LENGTH];
    let encrypted = &encrypted_data[SALT_LENGTH..];
    
    let key = EncryptionKey::from_password(password, salt)?;
    let decrypted = decrypt_data(encrypted, &key)?;
    Ok((decrypted, key))
}

/// Decrypt with cached key (fast - skips key derivation)
pub fn decrypt_with_key(encrypted_data: &[u8], cached_key: &EncryptionKey) -> Result<Vec<u8>, Error> {
    if encrypted_data.len() < SALT_LENGTH + NONCE_LENGTH {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "Encrypted data too short",
        ));
    }
    
    // Skip salt (first SALT_LENGTH bytes) and decrypt using cached key
    let encrypted = &encrypted_data[SALT_LENGTH..];
    decrypt_data(encrypted, cached_key)
}

