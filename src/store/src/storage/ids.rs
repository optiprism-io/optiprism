use std::borrow::Borrow;
use std::ops::Deref;
use std::time::SystemTime;

use bincode::de::BorrowDecoder;
use bincode::de::Decoder;
use bincode::enc::Encoder;
use bincode::error::DecodeError;
use bincode::error::EncodeError;
use bincode::BorrowDecode;
use bincode::Decode;
use bincode::Encode;
use derive_more::Display;
use derive_more::From;
use derive_more::FromStr;
use derive_more::Into;
use rand::Rng;
use serde::Deserialize;
use serde::Serialize;
use uuid::fmt::Hyphenated;
use uuid::v1::Timestamp;
use uuid::Uuid;

pub struct Uid(pub Uuid);

pub type UidFromStrError = uuid::Error;

// pub type UidStrHyphenated = InlinedStr<{ Hyphenated::LENGTH }>;

impl Uid {
    /// Generates a [`Uid`] based on counter, time and random from rng.
    #[must_use]
    pub fn new(counter: u16, time: SystemTime, mut rng: impl Rng) -> Self {
        let timestamp = Timestamp::wrapping_from(time, counter);
        let rand: [u8; 6] = rng.gen();
        let uuid = Uuid::new_v1(timestamp, &rand);
        Self(uuid)
    }

    /// The slightly more efficient string representation of [`Uuid`].
    /// It avoids heap allocations and virtual calls from [`std::fmt`] framework.
    //#[must_use]
    //pub fn to_str(&self) -> UidStrHyphenated {
    //    let mut inner = [0; Hyphenated::LENGTH];
    //    let _: &mut str = self.0.hyphenated().encode_lower(&mut inner[..]);
    //    unsafe { UidStrHyphenated::from_utf8_unchecked(inner) }
    //}

    /// Creates a UUID using the supplied bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// The nil/zero `Uid`. NOT A VALID UID, only to be used in ranges.
    #[must_use]
    pub const fn nil() -> Self {
        Self(Uuid::nil())
    }
}

impl Deref for Uid {
    type Target = [u8; 16];

    fn deref(&self) -> &Self::Target {
        self.0.as_bytes()
    }
}

impl AsRef<[u8]> for Uid {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl AsRef<[u8; 16]> for Uid {
    fn as_ref(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Borrow<[u8]> for Uid {
    fn borrow(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Borrow<[u8; 16]> for Uid {
    fn borrow(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Encode for Uid {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        let bytes: &[u8; 16] = self.as_ref();
        bincode::Encode::encode(bytes, encoder)?;
        Ok(())
    }
}

impl Decode for Uid {
    fn decode<D: Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        let bytes: [u8; 16] = bincode::Decode::decode(decoder)?;
        Ok(Self::from_bytes(bytes))
    }
}

impl<'a> BorrowDecode<'a> for Uid {
    fn borrow_decode<D: BorrowDecoder<'a>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let bytes: [u8; 16] = bincode::Decode::decode(decoder)?;
        Ok(Self::from_bytes(bytes))
    }
}

macro_rules! define_id {
    ( $( #[$attr:meta] )* $name:ident($ty:ty) ) => {
        $( #[$attr] )*
        $[derive(
            Clone,
            Copy,
            Debug,
            Decode,
            Deserialize,
            Display,
            Encode,
            Eq,
            From,
            FromStr,
            Hash,
            Into,
            Ord,
            PartialEq,
            PartialOrd,
            Serialize,
        )]
        pub struct $name(pub $ty);
    }
}

define_id!(TableId(Uid));

define_id!(EventId(Uid));
