#![allow(unused_imports, dead_code, unused_macros)]

pub use core::cmp::Ordering;
pub use core::ops::*;
pub use typenum::*;

pub type U0 = UTerm;
pub type U1 = UInt<U0, B1>;
pub type U2 = UInt<U1, B0>;
pub type U3 = UInt<U1, B1>;
pub type U4 = UInt<U2, B0>;
pub type U6 = UInt<U3, B0>;

macro_rules! assert_binary_op {
    ($name:ident, $trait:ident, $a:ty, $b:ty, $expected:ty) => {
        #[test]
        fn $name() {
            type Result = <<$a as $trait<$b>>::Output as Same<$expected>>::Output;
            assert_eq!(
                <Result as Unsigned>::to_u64(),
                <$expected as Unsigned>::to_u64()
            );
        }
    };
}

pub(crate) use assert_binary_op;
