//! # Tls module
//!
//! This module p certificate: (), certificate_chain: (), key: (), versions: ()  certificate: (), certificate_chain: (), key: (), versions: () provides traits and structures to handle tls. It provides a unified
//! certificate resolver for rustls.
use std::{
    borrow::ToOwned,
    collections::{HashMap, HashSet},
    convert::From,
    io::BufReader,
    sync::{Arc, Mutex},
};

use once_cell::sync::Lazy;
use ::rustls::{
    sign::{self, CertifiedKey, RsaSigningKey},
    Certificate, PrivateKey,
};
use sha2::{Digest, Sha256};
use x509_parser::pem::{parse_x509_pem, Pem};

use sozu_command::{
    certificate::{get_cn_and_san_attributes, CertificateError, Fingerprint},
    proto::command::{AddCertificate, CertificateAndKey, ReplaceCertificate, TlsVersion},
};

use crate::router::trie::{Key, KeyValue, TrieNode};

pub mod openssl;
pub mod rustls;

// -----------------------------------------------------------------------------
// Default ParsedCertificateAndKey

static DEFAULT_CERTIFICATE: Lazy<ParsedCertificateAndKey> = Lazy::new(|| {
    let certificate_and_key = CertificateAndKey {
        certificate: include_str!("../../assets/certificate.pem").to_string(),
        certificate_chain: vec![include_str!("../../assets/certificate_chain.pem").to_string()],
        key: include_str!("../../assets/key.pem").to_string(),
        versions: vec![],
        names: vec![],
    };
    GenericCertificateResolver::parse(&certificate_and_key).unwrap()
});

// -----------------------------------------------------------------------------
// CertificateResolver trait

pub trait CertificateResolver {
    type Error;

    // `get_certificate` returns the certificate, its chain and private key
    fn get_certificate(&self, fingerprint: &Fingerprint) -> Option<ParsedCertificateAndKey>;

    // `add_certificate` to the certificate resolver, ensures that it is valid and check if it can
    // replace another certificate
    fn add_certificate(&mut self, opts: &AddCertificate) -> Result<Fingerprint, Self::Error>;

    // `remove_certificate` from the certificate resolver, may fail if there is no alternative for
    // a domain name
    fn remove_certificate(&mut self, opts: &Fingerprint) -> Result<(), Self::Error>;

    // `replace_certificate` by a new one, this method is a short-hand for `add_certificate` and
    // then `remove_certificate`. It is possible that the certificate will not be replaced, if the
    // new certificate does not match `add_certificate` rules.
    fn replace_certificate(
        &mut self,
        opts: &ReplaceCertificate,
    ) -> Result<Fingerprint, Self::Error> {
        let fingerprint = self.add_certificate(&AddCertificate {
            address: opts.address.to_owned(),
            certificate: opts.new_certificate.to_owned(),
            expired_at: opts.new_expired_at.to_owned(),
        })?;

        match hex::decode(&opts.old_fingerprint) {
            Ok(old_fingerprint) => self.remove_certificate(&Fingerprint(old_fingerprint))?,
            Err(err) => {
                error!("failed to parse fingerprint, {}", err);
            }
        }

        Ok(fingerprint)
    }
}

// -----------------------------------------------------------------------------
// CertificateResolverHelper trait

pub trait CertificateResolverHelper {
    type Error;

    // `find_certificates_by_names` returns all fingerprints that are available to provide at least
    // one name
    fn find_certificates_by_names(
        &self,
        names: &HashSet<String>,
    ) -> Result<HashSet<Fingerprint>, Self::Error>;

    // `certificate_names` returns the hashset of subjects that the certificate is able to handle by
    // parsing the pem file and scrapping the information
    fn certificate_names(&self, pem: &Pem) -> Result<HashSet<String>, Self::Error>;

    // `fingerprint` returns the computed fingerprint for the given certificate
    fn fingerprint(certificate: &Pem) -> Fingerprint;

    // `parse` ensures that a certificate, its chain and its private key are valid by parsing them
    // and check that the signature algorithm is supported and return them
    fn parse(
        certificate_and_key: &CertificateAndKey,
    ) -> Result<ParsedCertificateAndKey, Self::Error>;
}

// -----------------------------------------------------------------------------
// CertificateOverride struct

#[derive(Clone, Debug)]
pub struct CertificateOverride {
    pub names: Option<HashSet<String>>,
    pub expiration: Option<i64>,
}

impl From<&AddCertificate> for CertificateOverride {
    fn from(opts: &AddCertificate) -> Self {
        let mut names = None;
        if !opts.certificate.names.is_empty() {
            names = Some(opts.certificate.names.iter().cloned().collect())
        }

        Self {
            names,
            expiration: opts.expired_at.to_owned(),
        }
    }
}

// -----------------------------------------------------------------------------
// ParsedCertificateAndKey struct

#[derive(Debug)]
pub struct ParsedCertificateAndKey {
    pub certificate: Pem,
    pub chain: Vec<Pem>,
    pub key: String,
    pub versions: Vec<TlsVersion>,
}

impl Clone for ParsedCertificateAndKey {
    fn clone(&self) -> Self {
        let certificate = Pem {
            label: self.certificate.label.to_owned(),
            contents: self.certificate.contents.to_owned(),
        };

        Self {
            certificate,
            chain: self
                .chain
                .iter()
                .map(|chain| Pem {
                    label: chain.label.to_owned(),
                    contents: chain.contents.to_owned(),
                })
                .collect(),
            key: self.key.to_owned(),
            versions: self.versions.to_owned(),
        }
    }
}

// -----------------------------------------------------------------------------
// GenericCertificateResolverError enum

#[derive(thiserror::Error, Clone, Debug)]
pub enum GenericCertificateResolverError {
    #[error("failed to get common name and subject alternate names from pem, {0}")]
    InvalidCommonNameAndSubjectAlternateNames(CertificateError),
    #[error("failed to parse pem certificate, {0}")]
    InvalidPem(String),
    #[error("failed to parse der certificate, {0}")]
    InvalidDer(String),
    #[error("certificate, chain or private key is invalid")]
    InvalidPrivateKey,
    #[error("certificate is still in use")]
    IsStillInUse,
}

// -----------------------------------------------------------------------------
// GenericCertificateResolver struct

#[derive(Debug)]
pub struct GenericCertificateResolver {
    pub domains: TrieNode<Fingerprint>,
    certificates: HashMap<Fingerprint, ParsedCertificateAndKey>,
    name_fingerprint_idx: HashMap<String, HashSet<Fingerprint>>,
    overrides: HashMap<Fingerprint, CertificateOverride>,
}

impl CertificateResolver for GenericCertificateResolver {
    type Error = GenericCertificateResolverError;

    fn get_certificate(&self, fingerprint: &Fingerprint) -> Option<ParsedCertificateAndKey> {
        self.certificates.get(fingerprint).map(ToOwned::to_owned)
    }

    fn add_certificate(&mut self, opts: &AddCertificate) -> Result<Fingerprint, Self::Error> {
        // Check if we could parse the certificate, chain and private key, if not just throw an
        // error.
        let parsed_certificate_and_key = Self::parse(&opts.certificate)?;
        let fingerprint = Self::fingerprint(&parsed_certificate_and_key.certificate);
        if !opts.certificate.names.is_empty() || opts.expired_at.is_some() {
            self.overrides
                .insert(fingerprint.to_owned(), CertificateOverride::from(opts));
        } else {
            self.overrides.remove(&fingerprint);
        }

        let (ok, certificates_to_remove) =
            self.should_insert(&fingerprint, &parsed_certificate_and_key)?;
        if !ok {
            // if we do not need to insert the fingerprint just return the fingerprint
            return Ok(fingerprint);
        }

        let new_names = match self.get_names_override(&fingerprint) {
            Some(names) => names,
            None => self.certificate_names(&parsed_certificate_and_key.certificate)?,
        };

        self.certificates
            .insert(fingerprint.to_owned(), parsed_certificate_and_key);
        for name in new_names {
            self.domains
                .insert(name.to_owned().into_bytes(), fingerprint.to_owned());

            self.name_fingerprint_idx
                .entry(name)
                .or_insert_with(HashSet::new)
                .insert(fingerprint.to_owned());
        }

        for (fingerprint, names) in &certificates_to_remove {
            for name in names {
                if let Some(fingerprints) = self.name_fingerprint_idx.get_mut(name) {
                    fingerprints.remove(fingerprint);
                }
            }

            self.certificates.remove(fingerprint);
        }

        Ok(fingerprint.to_owned())
    }

    fn remove_certificate(&mut self, fingerprint: &Fingerprint) -> Result<(), Self::Error> {
        if let Some(certificate_and_key) = self.get_certificate(fingerprint) {
            let names = match self.get_names_override(fingerprint) {
                Some(names) => names,
                None => self.certificate_names(&certificate_and_key.certificate)?,
            };

            if self.is_required_for_domain(&names, fingerprint) {
                return Err(GenericCertificateResolverError::IsStillInUse);
            }

            for name in &names {
                if let Some(fingerprints) = self.name_fingerprint_idx.get_mut(name) {
                    fingerprints.remove(fingerprint);

                    if fingerprints.is_empty() {
                        self.domains.domain_remove(&name.to_owned().into_bytes());
                    }
                }
            }

            self.certificates.remove(fingerprint);
        }

        Ok(())
    }
}

impl CertificateResolverHelper for GenericCertificateResolver {
    type Error = GenericCertificateResolverError;

    fn find_certificates_by_names(
        &self,
        names: &HashSet<String>,
    ) -> Result<HashSet<Fingerprint>, Self::Error> {
        let mut fingerprints = HashSet::new();
        for name in names {
            if let Some(fprints) = self.name_fingerprint_idx.get(name) {
                fprints.iter().for_each(|fingerprint| {
                    fingerprints.insert(fingerprint.to_owned());
                });
            }
        }

        Ok(fingerprints)
    }

    fn certificate_names(&self, pem: &Pem) -> Result<HashSet<String>, Self::Error> {
        let fingerprint = Self::fingerprint(pem);
        if let Some(certificate_override) = self.overrides.get(&fingerprint) {
            if let Some(names) = &certificate_override.names {
                return Ok(names.to_owned());
            }
        }

        get_cn_and_san_attributes(pem)
            .map_err(GenericCertificateResolverError::InvalidCommonNameAndSubjectAlternateNames)
    }

    fn fingerprint(pem: &Pem) -> Fingerprint {
        Fingerprint(Sha256::digest(&pem.contents).iter().cloned().collect())
    }

    fn parse(
        certificate_and_key: &CertificateAndKey,
    ) -> Result<ParsedCertificateAndKey, Self::Error> {
        let certificate =
            sozu_command::certificate::parse(certificate_and_key.certificate.as_bytes())
                .map_err(|err| GenericCertificateResolverError::InvalidPem(err.to_string()))?;

        let mut chains = vec![];
        for chain in &certificate_and_key.certificate_chain {
            let (_, chain) = parse_x509_pem(chain.as_bytes())
                .map_err(|err| GenericCertificateResolverError::InvalidPem(err.to_string()))?;

            chains.push(chain);
        }
        // try to parse key as rsa private key
        let mut key_reader = BufReader::new(certificate_and_key.key.as_bytes());
        let parsed_keys = rustls_pemfile::rsa_private_keys(&mut key_reader);
        if let Ok(mut keys) = parsed_keys {
            if !keys.is_empty() {
                let key = PrivateKey(keys.swap_remove(0));
                if RsaSigningKey::new(&key).is_ok() {
                    let versions = certificate_and_key
                        .versions
                        .iter()
                        .filter_map(|v| TlsVersion::try_from(*v).ok())
                        .collect();
                    return Ok(ParsedCertificateAndKey {
                        certificate,
                        chain: chains,
                        key: certificate_and_key.key.to_owned(),
                        versions,
                    });
                }
            }
        }

        // try to parse key as pkcs8-encoded private
        let mut key_reader = BufReader::new(certificate_and_key.key.as_bytes());
        let parsed_keys = rustls_pemfile::pkcs8_private_keys(&mut key_reader);
        if let Ok(mut keys) = parsed_keys {
            if !keys.is_empty() {
                let key = PrivateKey(keys.swap_remove(0));

                // try to read as rsa private key
                if RsaSigningKey::new(&key).is_ok() {
                    let versions = certificate_and_key
                        .versions
                        .iter()
                        .filter_map(|v| TlsVersion::try_from(*v).ok())
                        .collect();
                    return Ok(ParsedCertificateAndKey {
                        certificate,
                        chain: chains,
                        key: certificate_and_key.key.to_owned(),
                        versions,
                    });
                }

                // try to read as ecdsa private key
                if sign::any_ecdsa_type(&key).is_ok() {
                    let versions = certificate_and_key
                        .versions
                        .iter()
                        .filter_map(|v| TlsVersion::try_from(*v).ok())
                        .collect();

                    return Ok(ParsedCertificateAndKey {
                        certificate,
                        chain: chains,
                        key: certificate_and_key.key.to_owned(),
                        versions,
                    });
                }
            }
        }

        Err(GenericCertificateResolverError::InvalidPrivateKey)
    }
}

impl Default for GenericCertificateResolver {
    fn default() -> Self {
        Self {
            domains: TrieNode::root(),
            certificates: Default::default(),
            name_fingerprint_idx: Default::default(),
            overrides: Default::default(),
        }
    }
}

impl GenericCertificateResolver {
    pub fn new() -> Self {
        Self::default()
    }

    fn is_required_for_domain(&self, names: &HashSet<String>, fingerprint: &Fingerprint) -> bool {
        for name in names {
            if let Some(fingerprints) = self.name_fingerprint_idx.get(name) {
                if 1 == fingerprints.len() && fingerprints.get(fingerprint).is_some() {
                    return true;
                }
            }
        }

        false
    }

    fn should_insert(
        &self,
        fingerprint: &Fingerprint,
        parsed_certificate_and_key: &ParsedCertificateAndKey,
    ) -> Result<(bool, HashMap<Fingerprint, HashSet<String>>), GenericCertificateResolverError>
    {
        let x509 = parsed_certificate_and_key
            .certificate
            .parse_x509()
            .map_err(|err| GenericCertificateResolverError::InvalidDer(err.to_string()))?;

        // We need to know if the new certificate can replace an already existing one.
        let new_names = match self.get_names_override(fingerprint) {
            Some(names) => names,
            None => self.certificate_names(&parsed_certificate_and_key.certificate)?,
        };

        let expiration = self
            .get_expiration_override(fingerprint)
            .unwrap_or_else(|| x509.validity().not_after.timestamp());

        let fingerprints = self.find_certificates_by_names(&new_names)?;
        let mut certificates = HashMap::new();
        for fingerprint in &fingerprints {
            if let Some(certificate_and_key) = self.get_certificate(fingerprint) {
                certificates.insert(fingerprint, certificate_and_key);
            }
        }

        let mut should_insert = false;
        let mut certificates_to_remove = HashMap::new();
        let mut certificates_names = HashSet::new();
        for (fingerprint, certificate_and_key) in certificates {
            let certificate = certificate_and_key
                .certificate
                .parse_x509()
                .map_err(|err| GenericCertificateResolverError::InvalidDer(err.to_string()))?;

            let certificate_names = match self.get_names_override(fingerprint) {
                Some(names) => names,
                None => self.certificate_names(&parsed_certificate_and_key.certificate)?,
            };

            let certificate_expiration = self
                .get_expiration_override(fingerprint)
                .unwrap_or_else(|| certificate.validity().not_after.timestamp());

            let extra_names = certificate_names
                .difference(&new_names)
                .collect::<HashSet<_>>();

            // if the certificate has at least the same name or less and the expiration date
            // is closer than the new one. We could remove it and allow the new insertion.
            if extra_names.is_empty() && certificate_expiration < expiration {
                certificates_to_remove.insert(fingerprint.to_owned(), certificate_names.to_owned());
                should_insert = true;
            }

            // We keep a track of all name of certificates that match our query to
            // check, if the new certificate provide an extra domain which is not
            // already exposed
            for name in certificate_names {
                certificates_names.insert(name);
            }
        }

        // In the case where we do not insert the certificate, because there is
        // no additional value, we have to check whether it provides an extra domain
        // name not registered yet.
        let diff: HashSet<&String> = new_names.difference(&certificates_names).collect();
        if !should_insert && diff.is_empty() {
            // We already have all domain names registered and there is no update
            // for expiration date of certificate. So, skipping the update.
            return Ok((false, certificates_to_remove));
        }

        Ok((true, certificates_to_remove))
    }

    fn get_expiration_override(&self, fingerprint: &Fingerprint) -> Option<i64> {
        self.overrides.get(fingerprint).and_then(|co| co.expiration)
    }

    fn get_names_override(&self, fingerprint: &Fingerprint) -> Option<HashSet<String>> {
        self.overrides
            .get(fingerprint)
            .and_then(|co| co.names.to_owned())
    }

    pub fn domain_lookup(
        &self,
        domain: &[u8],
        accept_wildcard: bool,
    ) -> Option<&KeyValue<Key, Fingerprint>> {
        self.domains.domain_lookup(domain, accept_wildcard)
    }
}

// -----------------------------------------------------------------------------
// MutexWrappedCertificateResolver struct

#[derive(Debug)]
pub struct MutexWrappedCertificateResolver(pub Mutex<GenericCertificateResolver>);

impl Default for MutexWrappedCertificateResolver {
    fn default() -> Self {
        Self(Mutex::new(GenericCertificateResolver::default()))
    }
}

impl MutexWrappedCertificateResolver {
    pub fn new() -> Self {
        Self::default()
    }

    fn generate_certified_key(
        certificate_and_key: &ParsedCertificateAndKey,
    ) -> Option<CertifiedKey> {
        let mut chains = vec![Certificate(
            certificate_and_key.certificate.contents.to_owned(),
        )];
        for certificate in &certificate_and_key.chain {
            chains.push(Certificate(certificate.contents.to_owned()));
        }

        let mut key_reader = BufReader::new(certificate_and_key.key.as_bytes());
        let parsed_key = rustls_pemfile::rsa_private_keys(&mut key_reader);

        if let Ok(mut keys) = parsed_key {
            if !keys.is_empty() {
                let key = PrivateKey(keys.swap_remove(0));

                if let Ok(signing_key) = RsaSigningKey::new(&key) {
                    let certified = CertifiedKey::new(chains, Arc::new(signing_key));
                    return Some(certified);
                }
            } else {
                let mut key_reader = BufReader::new(certificate_and_key.key.as_bytes());
                let parsed_key = rustls_pemfile::pkcs8_private_keys(&mut key_reader);
                if let Ok(mut keys) = parsed_key {
                    if !keys.is_empty() {
                        let key = PrivateKey(keys.swap_remove(0));
                        if let Ok(signing_key) = RsaSigningKey::new(&key) {
                            let certified = CertifiedKey::new(chains, Arc::new(signing_key));
                            return Some(certified);
                        } else if let Ok(k) = sign::any_ecdsa_type(&key) {
                            let certified = CertifiedKey::new(chains, k);
                            return Some(certified);
                        } else {
                            error!("could not decode signing key (tried RSA and ECDSA)");
                        }
                    }
                }
            }
        } else {
            error!("could not parse private key: {:?}", parsed_key);
        }

        None
    }
}

// -----------------------------------------------------------------------------
// Unit tests

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        error::Error,
        time::{Duration, SystemTime},
    };

    use super::{
        CertificateResolver, CertificateResolverHelper, GenericCertificateResolver,
        GenericCertificateResolverError,
    };

    use rand::{seq::SliceRandom, thread_rng};
    use sozu_command::proto::command::{AddCertificate, CertificateAndKey};
    use x509_parser::pem::parse_x509_pem;

    #[test]
    fn lifecycle() -> Result<(), Box<dyn Error + Send + Sync>> {
        let address = "127.0.0.1:8080".to_string();
        let mut resolver = GenericCertificateResolver::new();
        let certificate_and_key = CertificateAndKey {
            certificate: String::from(include_str!("../../assets/certificate.pem")),
            key: String::from(include_str!("../../assets/key.pem")),
            ..Default::default()
        };

        let (_, pem) = parse_x509_pem(certificate_and_key.certificate.as_bytes())
            .map_err(|err| GenericCertificateResolverError::InvalidPem(err.to_string()))?;

        let fingerprint = resolver.add_certificate(&AddCertificate {
            address: address,
            certificate: certificate_and_key,
            expired_at: None,
        })?;

        if resolver.get_certificate(&fingerprint).is_none() {
            return Err("failed to retrieve certificate".into());
        }

        if let Err(err) = resolver.remove_certificate(&fingerprint) {
            match err {
                GenericCertificateResolverError::IsStillInUse => {}
                _ => {
                    return Err(format!("the certificate must not been removed, {err}").into());
                }
            }
        }

        let names = resolver.certificate_names(&pem)?;
        if resolver.find_certificates_by_names(&names)?.is_empty()
            || resolver.get_certificate(&fingerprint).is_none()
        {
            return Err(
                "failed to retrieve certificate that we had the command to delete, but mandatory"
                    .into(),
            );
        }

        Ok(())
    }

    #[test]
    fn name_override() -> Result<(), Box<dyn Error + Send + Sync>> {
        let address = "127.0.0.1:8080".to_string();
        let mut resolver = GenericCertificateResolver::new();
        let certificate_and_key = CertificateAndKey {
            certificate: String::from(include_str!("../../assets/certificate.pem")),
            key: String::from(include_str!("../../assets/key.pem")),
            names: vec!["localhost".into(), "lolcatho.st".into()],
            ..Default::default()
        };

        let (_, pem) = parse_x509_pem(certificate_and_key.certificate.as_bytes())
            .map_err(|err| GenericCertificateResolverError::InvalidPem(err.to_string()))?;

        let fingerprint = resolver.add_certificate(&AddCertificate {
            address: address,
            certificate: certificate_and_key,
            expired_at: None,
        })?;

        if resolver.get_certificate(&fingerprint).is_none() {
            return Err("failed to retrieve certificate".into());
        }

        if let Err(err) = resolver.remove_certificate(&fingerprint) {
            match err {
                GenericCertificateResolverError::IsStillInUse => {}
                _ => {
                    return Err(format!("the certificate must not been removed, {err}").into());
                }
            }
        }

        let names = resolver.certificate_names(&pem)?;
        if resolver.find_certificates_by_names(&names)?.is_empty()
            || resolver.get_certificate(&fingerprint).is_none()
        {
            return Err(
                "failed to retrieve certificate that we had the command to delete, but mandatory"
                    .into(),
            );
        }

        let mut lolcat = HashSet::new();
        lolcat.insert(String::from("lolcatho.st"));
        if resolver.find_certificates_by_names(&lolcat)?.is_empty()
            || resolver.get_certificate(&fingerprint).is_none()
        {
            return Err(
                "failed to retrieve certificate that we had the command to delete, but mandatory"
                    .into(),
            );
        }

        Ok(())
    }

    #[test]
    fn replacement() -> Result<(), Box<dyn Error + Send + Sync>> {
        let address = "127.0.0.1:8080".to_string();
        let mut resolver = GenericCertificateResolver::new();

        // ---------------------------------------------------------------------
        // load first certificate
        let certificate_and_key_1y = CertificateAndKey {
            certificate: String::from(include_str!("../../assets/tests/certificate-1y.pem")),
            key: String::from(include_str!("../../assets/tests/key-1y.pem")),
            ..Default::default()
        };

        let (_, pem) = parse_x509_pem(certificate_and_key_1y.certificate.as_bytes())
            .map_err(|err| GenericCertificateResolverError::InvalidPem(err.to_string()))?;

        let names_1y = resolver.certificate_names(&pem)?;
        let fingerprint_1y = resolver.add_certificate(&AddCertificate {
            address: address.clone(),
            certificate: certificate_and_key_1y,
            expired_at: None,
        })?;

        if resolver.get_certificate(&fingerprint_1y).is_none() {
            return Err("failed to retrieve certificate".into());
        }

        // ---------------------------------------------------------------------
        // load second certificate
        let certificate_and_key_2y = CertificateAndKey {
            certificate: String::from(include_str!("../../assets/tests/certificate-2y.pem")),
            key: String::from(include_str!("../../assets/tests/key-2y.pem")),
            ..Default::default()
        };

        let fingerprint_2y = resolver.add_certificate(&AddCertificate {
            address,
            certificate: certificate_and_key_2y,
            expired_at: None,
        })?;

        if resolver.get_certificate(&fingerprint_2y).is_none() {
            return Err("failed to retrieve certificate".into());
        }

        // ---------------------------------------------------------------------
        // Check if the fist certificate has been successfully replaced
        if resolver.get_certificate(&fingerprint_1y).is_some() {
            return Err("certificate must be replaced by the 2y expiration one".into());
        }

        if resolver.get_certificate(&fingerprint_2y).is_none() {
            return Err("certificate must be added instead of the 1y expiration one".into());
        }

        let fingerprints = resolver.find_certificates_by_names(&names_1y)?;
        if fingerprints.get(&fingerprint_1y).is_some() {
            return Err("index must not reference the 1y expiration certificate".into());
        }

        if fingerprints.get(&fingerprint_2y).is_none() {
            return Err("index have to reference the 2y expiration certificate".into());
        }

        Ok(())
    }

    #[test]
    fn expiration_override() -> Result<(), Box<dyn Error + Send + Sync>> {
        let address = "127.0.0.1:8080".to_string();
        let mut resolver = GenericCertificateResolver::new();

        // ---------------------------------------------------------------------
        // load first certificate
        let certificate_and_key_1y = CertificateAndKey {
            certificate: String::from(include_str!("../../assets/tests/certificate-1y.pem")),
            key: String::from(include_str!("../../assets/tests/key-1y.pem")),
            ..Default::default()
        };

        let (_, pem) = parse_x509_pem(certificate_and_key_1y.certificate.as_bytes())
            .map_err(|err| GenericCertificateResolverError::InvalidPem(err.to_string()))?;

        let names_1y = resolver.certificate_names(&pem)?;
        let fingerprint_1y = resolver.add_certificate(&AddCertificate {
            address: address.clone(),
            certificate: certificate_and_key_1y,
            expired_at: Some(
                (SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?
                    + Duration::from_secs(3 * 365 * 24 * 3600))
                .as_secs() as i64,
            ),
        })?;

        if resolver.get_certificate(&fingerprint_1y).is_none() {
            return Err("failed to retrieve certificate".into());
        }

        // ---------------------------------------------------------------------
        // load second certificate
        let certificate_and_key_2y = CertificateAndKey {
            certificate: String::from(include_str!("../../assets/tests/certificate-2y.pem")),
            key: String::from(include_str!("../../assets/tests/key-2y.pem")),
            ..Default::default()
        };

        let fingerprint_2y = resolver.add_certificate(&AddCertificate {
            address,
            certificate: certificate_and_key_2y,
            expired_at: None,
        })?;

        if resolver.get_certificate(&fingerprint_2y).is_some() {
            return Err("certificate should not be loaded".into());
        }

        // ---------------------------------------------------------------------
        // Check if the fist certificate has been successfully not replaced
        if resolver.get_certificate(&fingerprint_1y).is_none() {
            return Err("certificate must not be replaced by the 2y expiration one".into());
        }

        if resolver.get_certificate(&fingerprint_2y).is_some() {
            return Err("certificate must not be added instead of the 1y expiration one".into());
        }

        let fingerprints = resolver.find_certificates_by_names(&names_1y)?;
        if fingerprints.get(&fingerprint_1y).is_none() {
            return Err("index must reference the 1y expiration certificate".into());
        }

        if fingerprints.get(&fingerprint_2y).is_some() {
            return Err("index must not reference the 2y expiration certificate".into());
        }

        Ok(())
    }

    #[test]
    fn random() -> Result<(), Box<dyn Error + Send + Sync>> {
        // ---------------------------------------------------------------------
        // load certificates
        let mut certificates = vec![
            CertificateAndKey {
                certificate: include_str!("../../assets/tests/certificate-1.pem").to_string(),
                key: include_str!("../../assets/tests/key.pem").to_string(),
                ..Default::default()
            },
            CertificateAndKey {
                certificate: include_str!("../../assets/tests/certificate-2.pem").to_string(),
                key: include_str!("../../assets/tests/key.pem").to_string(),
                ..Default::default()
            },
            CertificateAndKey {
                certificate: include_str!("../../assets/tests/certificate-3.pem").to_string(),
                key: include_str!("../../assets/tests/key.pem").to_string(),
                ..Default::default()
            },
            CertificateAndKey {
                certificate: include_str!("../../assets/tests/certificate-4.pem").to_string(),
                key: include_str!("../../assets/tests/key.pem").to_string(),
                ..Default::default()
            },
            CertificateAndKey {
                certificate: include_str!("../../assets/tests/certificate-5.pem").to_string(),
                key: include_str!("../../assets/tests/key.pem").to_string(),
                ..Default::default()
            },
            CertificateAndKey {
                certificate: include_str!("../../assets/tests/certificate-6.pem").to_string(),
                key: include_str!("../../assets/tests/key.pem").to_string(),
                ..Default::default()
            },
        ];

        let mut fingerprints = vec![];
        for certificate in &certificates {
            let (_, pem) = parse_x509_pem(certificate.certificate.as_bytes())
                .map_err(|err| GenericCertificateResolverError::InvalidPem(err.to_string()))?;

            fingerprints.push(GenericCertificateResolver::fingerprint(&pem));
        }

        // randomize entries
        certificates.shuffle(&mut thread_rng());

        // ---------------------------------------------------------------------
        // load certificates in resolver
        let address = "127.0.0.1:8080".to_string();
        let mut resolver = GenericCertificateResolver::default();

        for certificate in &certificates {
            resolver.add_certificate(&AddCertificate {
                address: address.clone(),
                certificate: certificate.to_owned(),
                expired_at: None,
            })?;
        }

        let mut names = HashSet::new();
        names.insert("example.org".to_string());

        let fprints = resolver.find_certificates_by_names(&names)?;
        if 1 != fprints.len() && !fprints.contains(&fingerprints[1]) {
            return Err("domain 'example.org' resolve to the wrong certificate".into());
        }

        let mut names = HashSet::new();
        names.insert("*.example.org".to_string());

        let fprints = resolver.find_certificates_by_names(&names)?;
        if 1 != fprints.len() && !fprints.contains(&fingerprints[2]) {
            return Err("domain '*.example.org' resolve to the wrong certificate".into());
        }

        let mut names = HashSet::new();
        names.insert("clever-cloud.com".to_string());

        let fprints = resolver.find_certificates_by_names(&names)?;
        if 1 != fprints.len() && !fprints.contains(&fingerprints[4]) {
            return Err("domain 'clever-cloud.com' resolve to the wrong certificate".into());
        }

        let mut names = HashSet::new();
        names.insert("*.clever-cloud.com".to_string());

        let fprints = resolver.find_certificates_by_names(&names)?;
        if 1 != fprints.len() && !fprints.contains(&fingerprints[5]) {
            return Err("domain '*.clever-cloud.com' resolve to the wrong certificate".into());
        }

        Ok(())
    }
}
