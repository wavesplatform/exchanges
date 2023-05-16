//! Waves-specific stuff: hashes, addresses etc.

pub use address::Address;

mod waves_hash {
    fn keccak256(message: &[u8]) -> [u8; 32] {
        use sha3::{Digest, Keccak256};

        let mut hasher = Keccak256::new();
        hasher.update(message);
        hasher.finalize().into()
    }

    fn blake2b256(message: &[u8]) -> [u8; 32] {
        use blake2::digest::{Update, VariableOutput};
        use blake2::Blake2bVar;

        let mut hasher = Blake2bVar::new(32).unwrap();

        hasher.update(message);

        let mut arr = [0u8; 32];

        hasher.finalize_variable(&mut arr).unwrap();

        arr
    }

    pub(super) fn hash(message: &[u8]) -> [u8; 32] {
        keccak256(&blake2b256(message))
    }
}

mod address {
    use super::waves_hash::hash;
    use bytes::{BufMut, BytesMut};

    pub struct Address(String);

    impl Address {
        pub fn new(addr: &[u8]) -> Self {
            Address(bs58::encode(addr).into_string())
        }

        pub fn from_public_key(public_key: &[u8], chain_id: u8) -> Self {
            let pkh = hash(public_key);
            Self::from_public_key_hash(&pkh, chain_id)
        }

        pub fn from_public_key_hash(pkh: &[u8], chain_id: u8) -> Self {
            let mut addr = BytesMut::with_capacity(26); // VERSION + CHAIN_ID + PKH + checksum

            addr.put_u8(1); // address version is always 1
            addr.put_u8(chain_id);
            addr.put_slice(&pkh[..20]);

            let wh = hash(&addr[..22]);
            let chks = &wh[..4];

            addr.put_slice(chks);

            Address(bs58::encode(addr).into_string())
        }

        pub fn into_string(self) -> String {
            self.0
        }
    }
}
