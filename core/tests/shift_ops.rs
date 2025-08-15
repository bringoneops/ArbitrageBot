mod util;
use util::{assert_binary_op, *};

assert_binary_op!(shl_1_by_1_is_2, Shl, U1, U1, U2);
assert_binary_op!(shr_2_by_1_is_1, Shr, U2, U1, U1);
