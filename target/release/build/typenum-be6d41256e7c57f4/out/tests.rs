use typenum::*;
use core::ops::*;
use core::cmp::Ordering;

// Rebuild the minimal set of type level numbers we need for the tests.
type U0 = UTerm;
type U1 = UInt<U0, B1>;
type U2 = UInt<U1, B0>;
type U3 = UInt<U1, B1>;
type U4 = UInt<U2, B0>;
type U6 = UInt<U3, B0>;

macro_rules! assert_binary_op {
    ($name:ident, $trait:ident, $a:ty, $b:ty, $expected:ty) => {
        #[test]
        fn $name() {
            type Result = <<$a as $trait<$b>>::Output as Same<$expected>>::Output;
            assert_eq!(<Result as Unsigned>::to_u64(), <$expected as Unsigned>::to_u64());
        }
    };
}

mod bit_ops {
    use super::*;
    assert_binary_op!(bitand_0_1_is_0, BitAnd, U0, U1, U0);
    assert_binary_op!(bitor_0_1_is_1, BitOr, U0, U1, U1);
    assert_binary_op!(bitxor_0_1_is_1, BitXor, U0, U1, U1);
}

mod shift_ops {
    use super::*;
    assert_binary_op!(shl_1_by_1_is_2, Shl, U1, U1, U2);
    assert_binary_op!(shr_2_by_1_is_1, Shr, U2, U1, U1);
}

mod arithmetic_ops {
    use super::*;
    assert_binary_op!(add_1_1_is_2, Add, U1, U1, U2);
    assert_binary_op!(mul_2_2_is_4, Mul, U2, U2, U4);
    assert_binary_op!(pow_2_2_is_4, Pow, U2, U2, U4);
    assert_binary_op!(sub_2_1_is_1, Sub, U2, U1, U1);
    assert_binary_op!(min_2_1_is_1, Min, U2, U1, U1);
    assert_binary_op!(max_2_1_is_2, Max, U2, U1, U2);
    assert_binary_op!(gcd_6_4_is_2, Gcd, U6, U4, U2);
}

mod comparison_ops {
    use super::*;
    #[test]
    fn cmp_1_2_is_less() {
        type Cmp = <U1 as Cmp<U2>>::Output;
        assert_eq!(<Cmp as Ord>::to_ordering(), Ordering::Less);
    }
}

