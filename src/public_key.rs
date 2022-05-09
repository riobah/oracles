use serde::{
    de::{self, Deserializer},
    ser::Serializer,
    Deserialize, Serialize,
};
use sqlx::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef, Postgres},
    types::Type,
};
use std::{ops::Deref, str::FromStr};

#[derive(Debug)]
pub struct PublicKey(helium_crypto::PublicKey);

impl Deref for PublicKey {
    type Target = helium_crypto::PublicKey;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Type<Postgres> for PublicKey {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("text")
    }
}

impl Encode<'_, Postgres> for PublicKey {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> IsNull {
        let address = self.to_string();
        Encode::<Postgres>::encode(&address, buf)
    }

    fn size_hint(&self) -> usize {
        25
    }
}

impl<'r> Decode<'r, Postgres> for PublicKey {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <&str as Decode<Postgres>>::decode(value)?;
        let key = helium_crypto::PublicKey::from_str(value)?;
        Ok(Self(key))
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match helium_crypto::PublicKey::from_str(&s) {
            Ok(v) => Ok(Self(v)),
            Err(_) => Err(de::Error::custom("invalid uuid")),
        }
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
