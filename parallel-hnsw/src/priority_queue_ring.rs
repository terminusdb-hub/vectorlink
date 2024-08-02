use crate::types::{EmptyValue, OrderedFloat};
use std::fmt::Debug;

pub struct PriorityQueueRing<'a, Id: Clone> {
    pub head: usize,
    pub length: usize,
    pub data: &'a mut [Id],
    pub priorities: &'a mut [f32],
}

fn absolute_index(head: usize, priorities: &[f32], relative_idx: usize) -> usize {
    if relative_idx < priorities.len() - head {
        head + relative_idx
    } else {
        relative_idx - (priorities.len() - head)
    }
}

fn relative_index(head: usize, priorities: &[f32], absolute_index: usize) -> usize {
    if absolute_index < head {
        absolute_index + (priorities.len() - head)
    } else {
        absolute_index - head
    }
}

#[derive(Debug)]
pub enum Comparison {
    Eq,
    Lt,
}

fn partition_point(head: usize, priorities: &[f32], point: f32, cmp: Comparison) -> usize {
    eprintln!("partition_point({head}, {priorities:?}, {point}, {cmp:?})");
    let closure1 = |d: &f32| match cmp {
        Comparison::Eq => OrderedFloat(*d) != OrderedFloat(point),
        Comparison::Lt => OrderedFloat(*d) < OrderedFloat(point),
    };
    let closure2 = |d: &f32| match cmp {
        Comparison::Eq => OrderedFloat(*d) != OrderedFloat(point),
        Comparison::Lt => OrderedFloat(*d) < OrderedFloat(point),
    };

    let first_half_point = priorities[..head].partition_point(closure1);
    if first_half_point < head {
        dbg!(relative_index(head, priorities, first_half_point))
    } else {
        dbg!(relative_index(
            head,
            priorities,
            head + priorities[head..].partition_point(closure2),
        ))
    }
}

impl<'a, Id: PartialOrd + PartialEq + Copy + EmptyValue + Debug> PriorityQueueRing<'a, Id> {
    pub fn is_empty(&'a self) -> bool {
        self.data.len() == 0 || self.data[self.head].is_empty()
    }

    pub fn first(&'a self) -> Option<(Id, f32)> {
        let length = self.len();
        if length == 0 {
            None
        } else {
            Some((self.data[self.head], self.priorities[self.head]))
        }
    }

    pub fn last_pos(&'a self) -> usize {
        if self.head == 0 {
            self.data.len() - 1
        } else {
            self.head - 1
        }
    }

    pub fn last(&'a self) -> Option<(Id, f32)> {
        let length = self.len();
        if length == 0 {
            None
        } else {
            Some((self.data[self.last_pos()], self.priorities[self.last_pos()]))
        }
    }

    pub fn partition_point(&self, point: f32, cmp: Comparison) -> usize {
        partition_point(self.head, self.priorities, point, cmp)
    }

    pub fn binary_search_from(&self, idx: usize, priority: f32) -> Result<usize, usize> {
        if idx > self.len() - self.head {
            self.priorities[self.absolute_index(idx)..self.head]
                .binary_search_by(|d0| OrderedFloat(*d0).cmp(&OrderedFloat(priority)))
                .map(|i| self.relative_index(i))
                .map_err(|e| self.relative_index(e))
        } else {
            let result = self.priorities[self.absolute_index(idx)..]
                .binary_search_by(|d0| OrderedFloat(*d0).cmp(&OrderedFloat(priority)));
            if result.is_err() {
                let last_idx = result.unwrap_err();
                if last_idx == self.capacity() {
                    self.priorities[..self.head]
                        .binary_search_by(|d0| OrderedFloat(*d0).cmp(&OrderedFloat(priority)))
                        .map(|i| self.relative_index(i))
                        .map_err(|e| self.relative_index(e))
                } else {
                    result
                        .map(|i| self.relative_index(i))
                        .map_err(|e| self.relative_index(e))
                }
            } else {
                result
                    .map(|i| self.relative_index(i))
                    .map_err(|e| self.relative_index(e))
            }
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn capacity(&self) -> usize {
        self.priorities.len()
    }

    pub fn data(&'a self) -> &'a [Id] {
        // note, this is unordered!
        self.data
    }

    pub fn absolute_index(&'a self, relative_index: usize) -> usize {
        absolute_index(self.head, self.priorities, relative_index)
    }

    pub fn relative_index(&'a self, absolute_index: usize) -> usize {
        relative_index(self.head, self.priorities, absolute_index)
    }

    // Retuns the actual insertion point
    fn insert_at(&mut self, idx: usize, elt: Id, priority: f32) -> usize {
        eprintln!("insert_at({idx}, {elt:?}, {priority})");
        let mut idx = idx;
        let mut aidx = dbg!(self.absolute_index(idx));
        if idx < self.data.len() && self.data[aidx] != elt {
            // walk through all elements with exactly the same priority as us
            while self.priorities[aidx] == priority && self.data[aidx] <= elt {
                // return ourselves if we're already there.
                if self.data[aidx] == elt {
                    return idx;
                }
                idx += 1;
                aidx = self.absolute_index(idx);
                if idx == self.priorities.len() {
                    return idx;
                }
            }
            let head = self.head;
            let swap_start = self.length;
            let data = &mut self.data;
            let priorities = &mut self.priorities;

            for i in (idx + 1..swap_start + 1).rev() {
                if i == priorities.len() {
                    continue;
                }
                let ai_minus_1 = absolute_index(head, priorities, i - 1);
                let ai = absolute_index(head, priorities, i);
                data[ai] = data[ai_minus_1];
                priorities[ai] = priorities[ai_minus_1];
            }
            let aidx = absolute_index(head, priorities, idx);
            data[aidx] = elt;
            priorities[aidx] = priority;
        }
        if idx < self.length {
            self.length += 1
        }
        idx
    }

    pub fn insert(&mut self, elt: Id, priority: f32) -> usize {
        let idx = self.partition_point(priority, Comparison::Lt);
        eprintln!("idx: {idx}");
        self.insert_at(idx, elt, priority)
    }

    pub fn merge<'b>(&mut self, other_priority_queue: &'b PriorityQueueRing<'b, Id>) -> bool {
        let mut did_something = false;
        let mut last_idx = 0;
        for (other_idx, (_, other_distance)) in other_priority_queue.iter().enumerate() {
            if last_idx > self.len() {
                break;
            }

            let i = self.binary_search_from(last_idx, other_distance);

            match i {
                Ok(i) => {
                    // We need to walk to the beginning of the match
                    let mut start_idx = i + last_idx;
                    while start_idx != 0 {
                        if self.priorities[self.absolute_index(start_idx - 1)] != other_distance {
                            break;
                        } else {
                            start_idx -= 1;
                        }
                    }
                    last_idx = self.insert_at(
                        start_idx,
                        other_priority_queue.data[other_priority_queue.absolute_index(other_idx)],
                        other_distance,
                    );
                    did_something |= last_idx != self.data.len();
                }
                Err(i) => {
                    if i >= self.data.len() {
                        break;
                    } else {
                        last_idx = self.insert_at(
                            i + last_idx,
                            other_priority_queue.data
                                [other_priority_queue.absolute_index(other_idx)],
                            other_distance,
                        );
                        did_something = true;
                    }
                }
            }
        }
        did_something
    }

    pub fn merge_pairs(&mut self, other: &[(Id, f32)]) -> bool {
        let (mut ids, mut priorities): (Vec<Id>, Vec<f32>) = other
            .iter()
            .take_while(|(_, d)| *d != f32::MAX)
            .copied()
            .unzip();

        self.merge(&PriorityQueueRing {
            length: ids.len(),
            head: 0,
            data: &mut ids,
            priorities: &mut priorities,
        })
    }

    pub fn iter(&'a self) -> PriorityQueueRingIter<'a, Id> {
        PriorityQueueRingIter {
            position: 0,
            head: self.head,
            data: self.data,
            priorities: self.priorities,
        }
    }

    pub fn from_slices(data: &'a mut [Id], priorities: &'a mut [f32]) -> PriorityQueueRing<'a, Id> {
        let length = priorities.partition_point(|d| OrderedFloat(*d) != OrderedFloat(f32::MAX));
        PriorityQueueRing {
            length,
            head: 0,
            data,
            priorities,
        }
    }
}

pub struct PriorityQueueRingIter<'iter, Id> {
    position: usize,
    head: usize,
    data: &'iter [Id],
    priorities: &'iter [f32],
}

impl<Id: PartialEq + Copy + EmptyValue> Iterator for PriorityQueueRingIter<'_, Id> {
    type Item = (Id, f32);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position == self.priorities.len() {
            None
        } else {
            let aidx = absolute_index(self.head, self.priorities, self.position);
            let data_head = self.data[aidx];
            if data_head.is_empty() {
                None
            } else {
                self.position += 1;
                Some((data_head, self.priorities[aidx]))
            }
        }
    }
}

#[cfg(test)]
mod priority_queue_ring_tests {
    use crate::{priority_queue_ring::PriorityQueueRing, NodeId};

    #[test]
    fn fixed_length_insertion() {
        // At beginning
        let mut data = vec![NodeId(0), NodeId(3), NodeId(!0)];
        let mut priorities = vec![0.1, 1.2, f32::MAX];
        let mut priority_queue = PriorityQueueRing::from_slices(&mut data, &mut priorities);
        priority_queue.insert(NodeId(4), 0.01);
        assert_eq!(data, vec![NodeId(4), NodeId(0), NodeId(3)]);
        assert_eq!(priorities, vec![0.01, 0.1, 1.2]);

        // into empty
        let mut data = vec![NodeId(!0), NodeId(!0), NodeId(!0)];
        let mut priorities = vec![f32::MAX, f32::MAX, f32::MAX];
        let mut priority_queue = PriorityQueueRing::from_slices(&mut data, &mut priorities);
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
        let mut priority_queue = PriorityQueueRing::from_slices(&mut data, &mut priorities);
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
        let mut priority_queue = PriorityQueueRing::from_slices(&mut data, &mut priorities);
        priority_queue.insert(NodeId(4), 0.3);
        assert_eq!(data, vec![NodeId(1), NodeId(2), NodeId(4)]);
        assert_eq!(priorities, vec![0.1, 0.2, 0.3]);

        // Insert past the end
        let mut data = vec![NodeId(1), NodeId(2), NodeId(3)];
        let mut priorities = vec![0.1, 0.2, 0.3];
        let mut priority_queue = PriorityQueueRing::from_slices(&mut data, &mut priorities);
        priority_queue.insert(NodeId(4), 0.4);
        assert_eq!(data, vec![NodeId(1), NodeId(2), NodeId(3)]);
        assert_eq!(priorities, vec![0.1, 0.2, 0.3]);
    }

    #[test]
    fn fixed_length_merge() {
        // Interleaved
        let mut data1 = vec![NodeId(0), NodeId(2), NodeId(4)];
        let mut priorities1 = vec![0.0, 0.2, 0.4];
        let mut priority_queue1 = PriorityQueueRing::from_slices(&mut data1, &mut priorities1);

        let mut data2 = vec![NodeId(1), NodeId(3), NodeId(5)];
        let mut priorities2 = vec![0.1, 0.3, 0.5];
        let priority_queue2 = PriorityQueueRing::from_slices(&mut data2, &mut priorities2);

        priority_queue1.merge(&priority_queue2);
        assert_eq!(data1, vec![NodeId(0), NodeId(1), NodeId(2)]);
        assert_eq!(priorities1, vec![0.0, 0.1, 0.2]);
    }

    #[test]
    fn last_element() {
        let mut data = vec![NodeId(0), NodeId(3), NodeId(!0)];
        let mut priorities = vec![0.1, 1.2, f32::MAX];
        let priority_queue = PriorityQueueRing::from_slices(&mut data, &mut priorities);

        assert_eq!(priority_queue.last(), Some((NodeId(3), 1.2)));
    }

    #[test]
    fn useless_merge() {
        let mut data = vec![NodeId(0), NodeId(3), NodeId(5)];
        let mut priorities = vec![0.0, 0.3, 0.5];

        let mut priority_queue = PriorityQueueRing::from_slices(&mut data, &mut priorities);

        let mut data2 = vec![NodeId(6), NodeId(7), NodeId(8)];
        let mut priorities2 = vec![0.6, 0.7, 0.8];

        let priority_queue2 = PriorityQueueRing::from_slices(&mut data2, &mut priorities2);

        let result = priority_queue.merge(&priority_queue2);
        assert!(!result);
        assert_eq!(data, vec![NodeId(0), NodeId(3), NodeId(5)]);
    }

    #[test]
    fn productive_merge() {
        let mut data = vec![NodeId(0), NodeId(3), NodeId(5)];
        let mut priorities = vec![0.0, 0.3, 0.5];

        let mut priority_queue = PriorityQueueRing::from_slices(&mut data, &mut priorities);

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

        let mut priority_queue = PriorityQueueRing::from_slices(&mut data, &mut priorities);

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
        let mut priority_queue = PriorityQueueRing::from_slices(&mut data, &mut priorities);

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

        let mut priority_queue = PriorityQueueRing::from_slices(&mut n1, &mut p1);

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
