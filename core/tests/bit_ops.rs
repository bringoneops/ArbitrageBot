mod util;
use util::{assert_binary_op, *};

assert_binary_op!(bitand_0_1_is_0, BitAnd, U0, U1, U0);
assert_binary_op!(bitor_0_1_is_1, BitOr, U0, U1, U1);
assert_binary_op!(bitxor_0_1_is_1, BitXor, U0, U1, U1);
