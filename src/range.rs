use std::ops::*;

pub trait RangeOverlap<T: PartialOrd, O: RangeBounds<T> + ?Sized>: RangeBounds<T> {
    fn overlaps(&self, rhs: &O) -> bool;
}

// A..B
impl<T: PartialOrd, O: RangeBounds<T> + ?Sized> RangeOverlap<T, O> for Range<T> {
    fn overlaps(&self, rhs: &O) -> bool {
        (match rhs.start_bound() {
            Bound::Included(start) => self.contains(start),
            Bound::Excluded(start_ex) => *start_ex < self.end,
            Bound::Unbounded => {
                return rhs.contains(&self.start);
            }
        } || match rhs.end_bound() {
            Bound::Included(end) => self.contains(end),
            Bound::Excluded(end_ex) => *end_ex > self.start,
            Bound::Unbounded => rhs.contains(&self.start),
        })
    }
}

// A..
impl<T: PartialOrd, O: RangeBounds<T> + ?Sized> RangeOverlap<T, O> for RangeFrom<T> {
    fn overlaps(&self, rhs: &O) -> bool {
        // self.start is contained in the other range, or
        // either bound of the other range is contained in self
        match rhs.start_bound() {
            Bound::Included(start) => self.contains(start),
            Bound::Excluded(start_ex) if *start_ex >= self.start => true,
            Bound::Excluded(_) | Bound::Unbounded => match rhs.end_bound() {
                Bound::Included(end) => self.contains(end),
                Bound::Excluded(end_ex) => *end_ex > self.start,
                Bound::Unbounded => true,
            },
        }
    }
}
