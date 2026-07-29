use crate::{BackendClass, ControlError, ReplicaBinding};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBindingRecord {
    binding: ReplicaBinding,
    backend_class: BackendClass,
}

impl ReplicaBindingRecord {
    pub fn new(binding: ReplicaBinding, backend_class: BackendClass) -> Result<Self, ControlError> {
        if !is_external_reference(binding.credential_ref()) {
            return Err(ControlError::PlaintextCredential);
        }
        if binding.backend_class_digest() != backend_class.digest()
            || binding.provider_kind() != backend_class.provider_kind()
            || binding.contract_version() != backend_class.contract_version()
            || binding.layout_version() != backend_class.layout_version()
            || binding.capability_digest() != backend_class.required_capabilities().digest()
        {
            return Err(ControlError::BindingBackendClassMismatch);
        }
        Ok(Self {
            binding,
            backend_class,
        })
    }

    pub const fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    pub const fn backend_class(&self) -> &BackendClass {
        &self.backend_class
    }
}

fn is_external_reference(value: &str) -> bool {
    let Some((scheme, target)) = value.split_once("://") else {
        return false;
    };
    !scheme.is_empty()
        && !target.is_empty()
        && scheme.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'+' | b'-' | b'.')
        })
        && !target.chars().any(char::is_whitespace)
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct RetentionPin(String);

impl RetentionPin {
    pub fn new(value: String) -> Result<Self, ControlError> {
        if value.is_empty() || value.len() > 255 || value.chars().any(char::is_whitespace) {
            return Err(ControlError::InvalidPlacement("invalid retention pin"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}
