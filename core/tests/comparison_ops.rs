mod util;
use util::*;

#[test]
fn cmp_1_2_is_less() {
    type CmpResult = <U1 as typenum::Cmp<U2>>::Output;
    assert_eq!(<CmpResult as Ord>::to_ordering(), Ordering::Less);
}
