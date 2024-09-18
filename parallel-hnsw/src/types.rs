use std::fmt::Debug;

#[derive(PartialEq, Eq, Debug, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct VectorId(pub usize);
#[derive(PartialEq, Eq, Debug, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct NodeId(pub usize);

impl VectorId {
    pub const MAX: Self = Self(!0);
}
impl NodeId {
    pub const MAX: Self = Self(!0);
}

pub trait EmptyValue {
    fn is_empty(&self) -> bool;
    fn empty() -> Self;
}

impl EmptyValue for NodeId {
    fn is_empty(&self) -> bool {
        self.0 == !0
    }

    fn empty() -> Self {
        Self::MAX
    }
}

impl EmptyValue for VectorId {
    fn is_empty(&self) -> bool {
        self.0 == !0
    }

    fn empty() -> Self {
        Self::MAX
    }
}

pub enum AbstractVector<'a, T: ?Sized> {
    Stored(VectorId),
    Unstored(&'a T),
}

impl<'a> AbstractVector<'a, Vec<f32>> {
    pub fn convert_to_array<const N: usize>(&'a self) -> AbstractVector<'a, [f32; N]> {
        match self {
            AbstractVector::Stored(id) => AbstractVector::Stored(*id),
            AbstractVector::Unstored(v) => {
                assert_eq!(v.len(), N);
                let slice = &v[..];
                let ptr = slice.as_ptr();
                let array_borrow: &[f32; N] = unsafe { (ptr as *const [f32; N]).as_ref().unwrap() };
                AbstractVector::Unstored(array_borrow)
            }
        }
    }
}

impl<'a, T: ?Sized> AbstractVector<'a, T> {
    pub fn convert_into<T2>(&self) -> AbstractVector<'a, T2>
    where
        &'a T: Into<&'a T2>,
    {
        match self {
            AbstractVector::Stored(id) => AbstractVector::Stored(*id),
            AbstractVector::Unstored(v) => AbstractVector::Unstored((*v).into()),
        }
    }
}

impl<'a, T> Debug for AbstractVector<'a, T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stored(arg0) => f.debug_tuple("Stored").field(arg0).finish(),
            Self::Unstored(arg0) => f.debug_tuple("Unstored").field(arg0).finish(),
        }
    }
}

impl<'a, T: ?Sized> Clone for AbstractVector<'a, T> {
    fn clone(&self) -> Self {
        match self {
            Self::Stored(arg0) => Self::Stored(*arg0),
            Self::Unstored(arg0) => Self::Unstored(arg0),
        }
    }
}

#[derive(PartialEq, PartialOrd, Clone, Copy, Debug)]
pub struct OrderedFloat(pub f32);

impl Eq for OrderedFloat {}

#[allow(clippy::derive_ord_xor_partial_ord)]
impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let res = self.partial_cmp(other);
        if res.is_none() {
            eprintln!("incomparable: {self:?} <> {other:?}");
            panic!();
        };
        res.unwrap()
    }
}
