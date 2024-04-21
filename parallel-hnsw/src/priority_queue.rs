use crate::types::{EmptyValue, OrderedFloat};

#[derive(Debug)]
pub enum VecOrSlice<'a, T> {
    Vec(Vec<T>),
    Slice(&'a mut [T]),
}

impl<T> std::ops::Deref for VecOrSlice<'_, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        match self {
            VecOrSlice::Vec(it) => it,
            VecOrSlice::Slice(it) => it,
        }
    }
}

impl<T> std::ops::DerefMut for VecOrSlice<'_, T> {
    fn deref_mut(&mut self) -> &mut [T] {
        match self {
            VecOrSlice::Vec(it) => it,
            VecOrSlice::Slice(it) => it,
        }
    }
}

pub struct PriorityQueue<'a, Id: Clone> {
    pub data: VecOrSlice<'a, Id>,
    pub priorities: VecOrSlice<'a, f32>,
}

impl<'a, Id: PartialOrd + PartialEq + Copy + EmptyValue> PriorityQueue<'a, Id> {
    pub fn is_empty(&'a self) -> bool {
        self.data.len() == 0 || self.data[0].is_empty()
    }

    pub fn first(&'a self) -> Option<(Id, f32)> {
        let length = self.len();
        if length == 0 {
            None
        } else {
            Some((self.data[0], self.priorities[0]))
        }
    }

    pub fn last(&'a self) -> Option<(Id, f32)> {
        let length = self.len();
        if length == 0 {
            None
        } else {
            Some((self.data[length - 1], self.priorities[length - 1]))
        }
    }

    pub fn len(&self) -> usize {
        self.priorities
            .partition_point(|d| OrderedFloat(*d) != OrderedFloat(f32::MAX))
    }

    pub fn capacity(&self) -> usize {
        self.priorities.len()
    }

    pub fn data(&'a self) -> &'a [Id] {
        &self.data
    }

    // Retuns the actual insertion point
    fn insert_at(&mut self, idx: usize, elt: Id, priority: f32) -> usize {
        let mut idx = idx;
        if idx < self.data.len() && self.data[idx] != elt {
            // walk through all elements with exactly the same priority as us
            while self.priorities[idx] == priority && self.data[idx] <= elt {
                // return ourselves if we're already there.
                if self.data[idx] == elt {
                    return idx;
                }
                idx += 1;
                if idx == self.priorities.len() {
                    return idx;
                }
            }
            let data = &mut self.data;
            let priorities = &mut self.priorities;
            let swap_start =
                priorities.partition_point(|d| OrderedFloat(*d) != OrderedFloat(f32::MAX));

            for i in (idx + 1..swap_start + 1).rev() {
                if i == priorities.len() {
                    continue;
                }
                data[i] = data[i - 1];
                priorities[i] = priorities[i - 1];
            }
            data[idx] = elt;
            priorities[idx] = priority;
        }
        idx
    }

    pub fn insert(&mut self, elt: Id, priority: f32) -> usize {
        let idx = self
            .priorities
            .partition_point(|d| OrderedFloat(*d) < OrderedFloat(priority));
        self.insert_at(idx, elt, priority)
    }

    pub fn merge<'b>(&mut self, other_data: &'b [Id], other_priority: &'b [f32]) -> bool {
        let mut did_something = false;
        let mut last_idx = 0;
        for (other_idx, other_distance) in other_priority.iter().enumerate() {
            if last_idx > self.priorities.len() {
                break;
            }
            let i = self.priorities[last_idx..]
                .binary_search_by(|d0| OrderedFloat(*d0).cmp(&OrderedFloat(*other_distance)));
            match i {
                Ok(i) => {
                    // We need to walk to the beginning of the match
                    let mut start_idx = i + last_idx;
                    while start_idx != 0 {
                        if self.priorities[start_idx - 1] != *other_distance {
                            break;
                        } else {
                            start_idx -= 1;
                        }
                    }
                    last_idx = self.insert_at(start_idx, other_data[other_idx], *other_distance);
                    did_something |= last_idx != self.data.len();
                }
                Err(i) => {
                    if i >= self.data.len() {
                        break;
                    } else {
                        last_idx =
                            self.insert_at(i + last_idx, other_data[other_idx], *other_distance);
                        did_something = true;
                    }
                }
            }
        }
        did_something
    }

    pub fn merge_from(&mut self, other: &PriorityQueue<Id>) -> bool {
        self.merge(&other.data, &other.priorities)
    }

    pub fn merge_pairs(&mut self, other: &[(Id, f32)]) -> bool {
        let (ids, priorities): (Vec<Id>, Vec<f32>) = other.iter().copied().unzip();
        self.merge(&ids, &priorities)
    }

    pub fn iter(&'a self) -> PriorityQueueIter<'a, Id> {
        PriorityQueueIter {
            data_iter: &self.data,
            priority_iter: &self.priorities,
        }
    }

    pub fn new(size: usize) -> PriorityQueue<'static, Id> {
        PriorityQueue {
            data: VecOrSlice::Vec(vec![Id::empty(); size]),
            priorities: VecOrSlice::Vec(vec![f32::MAX; size]),
        }
    }

    pub fn with_capacity(size: usize, capacity: usize) -> PriorityQueue<'static, Id> {
        assert!(capacity >= size);
        let mut data = Vec::with_capacity(capacity);
        let mut priorities = Vec::with_capacity(capacity);
        data.resize(size, Id::empty());
        priorities.resize(size, f32::MAX);
        PriorityQueue {
            data: VecOrSlice::Vec(vec![Id::empty(); size]),
            priorities: VecOrSlice::Vec(vec![f32::MAX; size]),
        }
    }

    pub fn from_slices(data: &'a mut [Id], priorities: &'a mut [f32]) -> PriorityQueue<'a, Id> {
        PriorityQueue {
            data: VecOrSlice::Slice(data),
            priorities: VecOrSlice::Slice(priorities),
        }
    }

    pub fn resize_capacity(&mut self, capacity: usize) {
        match (&mut self.data, &mut self.priorities) {
            (VecOrSlice::Vec(data), VecOrSlice::Vec(priorities)) => {
                data.resize(capacity, Id::empty());
                priorities.resize(capacity, f32::MAX)
            }
            _ => panic!("cannot resize queue backed by slices"),
        }
    }
}

pub struct PriorityQueueIter<'iter, Id> {
    data_iter: &'iter [Id],
    priority_iter: &'iter [f32],
}

impl<Id: PartialEq + Copy + EmptyValue> Iterator for PriorityQueueIter<'_, Id> {
    type Item = (Id, f32);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((data_head, data_tail)) = self.data_iter.split_first() {
            if data_head.is_empty() {
                return None;
            }
            if let Some((priority_head, priority_tail)) = self.priority_iter.split_first() {
                self.data_iter = data_tail;
                self.priority_iter = priority_tail;
                Some((*data_head, *priority_head))
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{priority_queue::PriorityQueue, NodeId};

    #[test]
    fn fixed_length_insertion() {
        // At beginning
        let mut data = vec![NodeId(0), NodeId(3), NodeId(!0)];
        let mut priorities = vec![0.1, 1.2, f32::MAX];
        let mut priority_queue = PriorityQueue::from_slices(&mut data, &mut priorities);
        priority_queue.insert(NodeId(4), 0.01);
        assert_eq!(data, vec![NodeId(4), NodeId(0), NodeId(3)]);
        assert_eq!(priorities, vec![0.01, 0.1, 1.2]);

        // into empty
        let mut data = vec![NodeId(!0), NodeId(!0), NodeId(!0)];
        let mut priorities = vec![f32::MAX, f32::MAX, f32::MAX];
        let mut priority_queue = PriorityQueue::from_slices(&mut data, &mut priorities);
        priority_queue.insert(NodeId(4), 0.01);
        assert_eq!(
            data,
            vec![
                NodeId(4),
                NodeId(18446744073709551615),
                NodeId(18446744073709551615)
            ]
        );
        assert_eq!(priorities, vec![0.01, 3.4028235e38, 3.4028235e38]);

        // Don't double count
        let mut data = vec![NodeId(4), NodeId(!0), NodeId(!0)];
        let mut priorities = vec![0.01, f32::MAX, f32::MAX];
        let mut priority_queue = PriorityQueue::from_slices(&mut data, &mut priorities);
        priority_queue.insert(NodeId(4), 0.01);
        assert_eq!(
            data,
            vec![
                NodeId(4),
                NodeId(18446744073709551615),
                NodeId(18446744073709551615)
            ]
        );
        assert_eq!(priorities, vec![0.01, 3.4028235e38, 3.4028235e38]);

        // Push off the end
        let mut data = vec![NodeId(1), NodeId(2), NodeId(3)];
        let mut priorities = vec![0.1, 0.2, 0.4];
        let mut priority_queue = PriorityQueue::from_slices(&mut data, &mut priorities);
        priority_queue.insert(NodeId(4), 0.3);
        assert_eq!(data, vec![NodeId(1), NodeId(2), NodeId(4)]);
        assert_eq!(priorities, vec![0.1, 0.2, 0.3]);

        // Insert past the end
        let mut data = vec![NodeId(1), NodeId(2), NodeId(3)];
        let mut priorities = vec![0.1, 0.2, 0.3];
        let mut priority_queue = PriorityQueue::from_slices(&mut data, &mut priorities);
        priority_queue.insert(NodeId(4), 0.4);
        assert_eq!(data, vec![NodeId(1), NodeId(2), NodeId(3)]);
        assert_eq!(priorities, vec![0.1, 0.2, 0.3]);
    }

    #[test]
    fn fixed_length_merge() {
        // Interleaved
        let mut data1 = vec![NodeId(0), NodeId(2), NodeId(4)];
        let mut priorities1 = vec![0.0, 0.2, 0.4];
        let mut priority_queue1 = PriorityQueue::from_slices(&mut data1, &mut priorities1);

        let mut data2 = vec![NodeId(1), NodeId(3), NodeId(5)];
        let mut priorities2 = vec![0.1, 0.3, 0.5];
        let priority_queue2 = PriorityQueue::from_slices(&mut data2, &mut priorities2);

        priority_queue1.merge_from(&priority_queue2);
        assert_eq!(data1, vec![NodeId(0), NodeId(1), NodeId(2)]);
        assert_eq!(priorities1, vec![0.0, 0.1, 0.2]);
    }

    #[test]
    fn last_element() {
        let mut data = vec![NodeId(0), NodeId(3), NodeId(!0)];
        let mut priorities = vec![0.1, 1.2, f32::MAX];
        let priority_queue = PriorityQueue::from_slices(&mut data, &mut priorities);

        assert_eq!(priority_queue.last(), Some((NodeId(3), 1.2)));
    }

    #[test]
    fn useless_merge() {
        let mut data = vec![NodeId(0), NodeId(3), NodeId(5)];
        let mut priorities = vec![0.0, 0.3, 0.5];

        let mut priority_queue = PriorityQueue::from_slices(&mut data, &mut priorities);

        let mut data2 = vec![NodeId(6), NodeId(7), NodeId(8)];
        let mut priorities2 = vec![0.6, 0.7, 0.8];

        let priority_queue2 = PriorityQueue::from_slices(&mut data2, &mut priorities2);

        let result = priority_queue.merge_from(&priority_queue2);
        assert!(!result);
        assert_eq!(data, vec![NodeId(0), NodeId(3), NodeId(5)]);
    }

    #[test]
    fn productive_merge() {
        let mut data = vec![NodeId(0), NodeId(3), NodeId(5)];
        let mut priorities = vec![0.0, 0.3, 0.5];

        let mut priority_queue = PriorityQueue::from_slices(&mut data, &mut priorities);

        let pairs = vec![(NodeId(1), 0.1), (NodeId(2), 0.2), (NodeId(4), 0.4)];

        let result = priority_queue.merge_pairs(&pairs);
        assert!(result);
        assert_eq!(data, vec![NodeId(0), NodeId(1), NodeId(2)]);
        assert_eq!(priorities, vec![0.0, 0.1, 0.2]);
    }

    #[test]
    fn repeated_merge() {
        let mut data = vec![NodeId(0), NodeId(3), NodeId(5)];
        let mut priorities = vec![0.0, 0.0, 0.0];

        let mut priority_queue = PriorityQueue::from_slices(&mut data, &mut priorities);

        let pairs = vec![(NodeId(0), 0.0), (NodeId(4), 0.0), (NodeId(3), 0.0)];

        let result = priority_queue.merge_pairs(&pairs);
        assert!(result);
        assert_eq!(data, vec![NodeId(0), NodeId(3), NodeId(4)]);
        assert_eq!(priorities, vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn merge_with_empty() {
        // At beginning
        let mut data = vec![NodeId(0), NodeId(3), NodeId(!0)];
        let mut priorities = vec![0.0, 1.2, f32::MAX];
        let mut priority_queue = PriorityQueue::from_slices(&mut data, &mut priorities);

        let pairs = vec![(NodeId(0), 0.0), (NodeId(3), 0.0), (NodeId(4), 0.0)];

        let result = priority_queue.merge_pairs(&pairs);
        assert!(result);
        assert_eq!(data, vec![NodeId(0), NodeId(3), NodeId(4)]);
        assert_eq!(priorities, vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn lots_of_zeros() {
        let mut n1 = vec![
            NodeId(0),
            NodeId(18446744073709551615),
            NodeId(18446744073709551615),
            NodeId(18446744073709551615),
            NodeId(18446744073709551615),
            NodeId(18446744073709551615),
            NodeId(18446744073709551615),
            NodeId(18446744073709551615),
            NodeId(18446744073709551615),
        ];
        let mut p1 = vec![
            0.0,
            3.4028235e38,
            3.4028235e38,
            3.4028235e38,
            3.4028235e38,
            3.4028235e38,
            3.4028235e38,
            3.4028235e38,
            3.4028235e38,
        ];

        let mut priority_queue = PriorityQueue::from_slices(&mut n1, &mut p1);

        let pairs = vec![
            (NodeId(3), 0.29289323),
            (NodeId(4), 0.4227),
            (NodeId(1), 1.0),
            (NodeId(2), 1.0),
            (NodeId(6), 1.0),
            (NodeId(7), 1.0),
        ];

        let result = priority_queue.merge_pairs(&pairs);
        assert!(result);
        assert_eq!(
            n1,
            vec![
                NodeId(0),
                NodeId(3),
                NodeId(4),
                NodeId(1),
                NodeId(2),
                NodeId(6),
                NodeId(7),
                NodeId(18446744073709551615),
                NodeId(18446744073709551615)
            ]
        );
        assert_eq!(
            p1,
            vec![
                0.0,
                0.29289323,
                0.4227,
                1.0,
                1.0,
                1.0,
                1.0,
                3.4028235e38,
                3.4028235e38
            ]
        );
    }
}
