mod util;
use util::{assert_binary_op, *};

assert_binary_op!(add_1_1_is_2, Add, U1, U1, U2);
assert_binary_op!(mul_2_2_is_4, Mul, U2, U2, U4);
assert_binary_op!(pow_2_2_is_4, Pow, U2, U2, U4);
assert_binary_op!(sub_2_1_is_1, Sub, U2, U1, U1);
assert_binary_op!(min_2_1_is_1, Min, U2, U1, U1);
assert_binary_op!(max_2_1_is_2, Max, U2, U1, U2);
assert_binary_op!(gcd_6_4_is_2, Gcd, U6, U4, U2);
