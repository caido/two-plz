use super::Header;
use header_plz::const_headers as header;

use fnv::FnvHasher;
use header_plz::Method;

use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::{cmp, mem};

/// HPACK encoder table
#[derive(Debug)]
pub struct Table {
    mask: usize,
    indices: Vec<Option<Pos>>,
    slots: VecDeque<Slot>,
    inserted: usize,
    // Size is in bytes
    size: usize,
    max_size: usize,
}

#[derive(Debug)]
pub enum Index {
    // The header is already fully indexed
    Indexed(usize, Header),

    // The name is indexed, but not the value
    Name(usize, Header),

    // The full header has been inserted into the table.
    Inserted(usize),

    // Only the value has been inserted (hpack table idx, slots idx)
    InsertedValue(usize, usize),

    // The header is not indexed by this table
    NotIndexed(Header),
}

#[derive(Debug)]
struct Slot {
    hash: HashValue,
    header: Header,
    next: Option<usize>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct Pos {
    index: usize,
    hash: HashValue,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct HashValue(usize);

const MAX_SIZE: usize = 1 << 16;
const DYN_OFFSET: usize = 62;

macro_rules! probe_loop {
    ($probe_var: ident < $len: expr, $body: expr) => {
        debug_assert!($len > 0);
        loop {
            if $probe_var < $len {
                $body
                $probe_var += 1;
            } else {
                $probe_var = 0;
            }
        }
    };
}

impl Table {
    pub fn new(max_size: usize, capacity: usize) -> Table {
        if capacity == 0 {
            Table {
                mask: 0,
                indices: vec![],
                slots: VecDeque::new(),
                inserted: 0,
                size: 0,
                max_size,
            }
        } else {
            let capacity =
                cmp::max(to_raw_capacity(capacity).next_power_of_two(), 8);

            Table {
                mask: capacity.wrapping_sub(1),
                indices: vec![None; capacity],
                slots: VecDeque::with_capacity(usable_capacity(capacity)),
                inserted: 0,
                size: 0,
                max_size,
            }
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        usable_capacity(self.indices.len())
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Gets the header stored in the table
    pub fn resolve<'a>(&'a self, index: &'a Index) -> &'a Header {
        use self::Index::*;

        match *index {
            Indexed(_, ref h) => h,
            Name(_, ref h) => h,
            Inserted(idx) => &self.slots[idx].header,
            InsertedValue(_, idx) => &self.slots[idx].header,
            NotIndexed(ref h) => h,
        }
    }

    pub fn resolve_idx(&self, index: &Index) -> usize {
        use self::Index::*;

        match *index {
            Indexed(idx, ..) => idx,
            Name(idx, ..) => idx,
            Inserted(idx) => idx + DYN_OFFSET,
            InsertedValue(_name_idx, slot_idx) => slot_idx + DYN_OFFSET,
            NotIndexed(_) => panic!("cannot resolve index"),
        }
    }

    /// Index the header in the HPACK table.
    pub fn index(&mut self, header: Header) -> Index {
        // Check the static table
        let statik = index_static(&header);

        // Don't index certain headers. This logic is borrowed from nghttp2.
        if header.skip_value_index() {
            // Right now, if this is true, the header name is always in the
            // static table. At some point in the future, this might not be true
            // and this logic will need to be updated.
            debug_assert!(
                statik.is_some(),
                "skip_value_index requires a static name",
            );
            return Index::new(statik, header);
        }

        // If the header is already indexed by the static table, return that
        if let Some((n, true)) = statik.filter(|_| !header.is_sensitive()) {
            return Index::Indexed(n, header);
        }

        // Don't index large headers
        if header.len() * 4 > self.max_size * 3 {
            return Index::new(statik, header);
        }

        self.index_dynamic(header, statik)
    }

    fn index_dynamic(
        &mut self,
        header: Header,
        statik: Option<(usize, bool)>,
    ) -> Index {
        debug_assert!(self.assert_valid_state("one"));

        if header.len() + self.size < self.max_size || !header.is_sensitive() {
            // Only grow internal storage if needed
            self.reserve_one();
        }

        if self.indices.is_empty() {
            // If `indices` is not empty, then it is impossible for all
            // `indices` entries to be `Some`. So, we only need to check for the
            // empty case.
            return Index::new(statik, header);
        }

        let hash = hash_header(&header);

        let desired_pos = desired_pos(self.mask, hash);
        let mut probe = desired_pos;
        let mut dist = 0;

        // Start at the ideal position, checking all slots
        probe_loop!(probe < self.indices.len(), {
            if let Some(pos) = self.indices[probe] {
                // The slot is already occupied, but check if it has a lower
                // displacement.
                let their_dist = probe_distance(self.mask, pos.hash, probe);

                let slot_idx = pos.index.wrapping_add(self.inserted);

                if their_dist < dist {
                    // Index robinhood
                    return self
                        .index_vacant(header, hash, dist, probe, statik);
                } else if pos.hash == hash
                    && self.slots[slot_idx].header.name() == header.name()
                {
                    // Matching name, check values
                    return self.index_occupied(
                        header,
                        hash,
                        pos.index,
                        statik.map(|(n, _)| n),
                    );
                }
            } else {
                return self.index_vacant(header, hash, dist, probe, statik);
            }

            dist += 1;
        });
    }

    fn index_occupied(
        &mut self,
        header: Header,
        hash: HashValue,
        mut index: usize,
        statik: Option<usize>,
    ) -> Index {
        debug_assert!(self.assert_valid_state("top"));

        // There already is a match for the given header name. Check if a value
        // matches. The header will also only be inserted if the table is not at
        // capacity.
        loop {
            // Compute the real index into the VecDeque
            let real_idx = index.wrapping_add(self.inserted);

            if !header.is_sensitive()
                && self.slots[real_idx]
                    .header
                    .value_eq(&header)
            {
                // We have a full match!
                return Index::Indexed(real_idx + DYN_OFFSET, header);
            }

            if let Some(next) = self.slots[real_idx].next {
                index = next;
                continue;
            }

            if header.is_sensitive() {
                // Should we assert this?
                // debug_assert!(statik.is_none());
                return Index::Name(real_idx + DYN_OFFSET, header);
            }

            self.update_size(header.len(), Some(index));

            // Insert the new header
            self.insert(header, hash);

            // Recompute real_idx as it just changed.
            let new_real_idx = index.wrapping_add(self.inserted);

            // The previous node in the linked list may have gotten evicted
            // while making room for this header.
            if new_real_idx < self.slots.len() {
                let idx = 0usize.wrapping_sub(self.inserted);

                self.slots[new_real_idx].next = Some(idx);
            }

            debug_assert!(self.assert_valid_state("bottom"));

            // Even if the previous header was evicted, we can still reference
            // it when inserting the new one...
            return if let Some(n) = statik {
                // If name is in static table, use it instead
                Index::InsertedValue(n, 0)
            } else {
                Index::InsertedValue(real_idx + DYN_OFFSET, 0)
            };
        }
    }

    fn index_vacant(
        &mut self,
        header: Header,
        hash: HashValue,
        mut dist: usize,
        mut probe: usize,
        statik: Option<(usize, bool)>,
    ) -> Index {
        if header.is_sensitive() {
            return Index::new(statik, header);
        }

        debug_assert!(self.assert_valid_state("top"));
        debug_assert!(
            dist == 0
                || self.indices[probe.wrapping_sub(1) & self.mask].is_some()
        );

        // Passing in `usize::MAX` for prev_idx since there is no previous
        // header in this case.
        if self.update_size(header.len(), None) {
            while dist != 0 {
                let back = probe.wrapping_sub(1) & self.mask;

                if let Some(pos) = self.indices[back] {
                    let their_dist = probe_distance(self.mask, pos.hash, back);

                    if their_dist < (dist - 1) {
                        probe = back;
                        dist -= 1;
                    } else {
                        break;
                    }
                } else {
                    probe = back;
                    dist -= 1;
                }
            }
        }

        debug_assert!(self.assert_valid_state("after update"));

        self.insert(header, hash);

        let pos_idx = 0usize.wrapping_sub(self.inserted);

        let prev = self.indices[probe].replace(Pos {
            index: pos_idx,
            hash,
        });

        if let Some(mut prev) = prev {
            // Shift forward
            let mut probe = probe + 1;

            probe_loop!(probe < self.indices.len(), {
                let pos = &mut self.indices[probe];

                prev = match pos.replace(prev) {
                    Some(p) => p,
                    None => break,
                };
            });
        }

        debug_assert!(self.assert_valid_state("bottom"));

        if let Some((n, _)) = statik {
            Index::InsertedValue(n, 0)
        } else {
            Index::Inserted(0)
        }
    }

    fn insert(&mut self, header: Header, hash: HashValue) {
        self.inserted = self.inserted.wrapping_add(1);

        self.slots.push_front(Slot {
            hash,
            header,
            next: None,
        });
    }

    pub fn resize(&mut self, size: usize) {
        self.max_size = size;

        if size == 0 {
            self.size = 0;

            self.indices.fill(None);

            self.slots.clear();
            self.inserted = 0;
        } else {
            self.converge(None);
        }
    }

    fn update_size(&mut self, len: usize, prev_idx: Option<usize>) -> bool {
        self.size += len;
        self.converge(prev_idx)
    }

    fn converge(&mut self, prev_idx: Option<usize>) -> bool {
        let mut ret = false;

        while self.size > self.max_size {
            ret = true;
            self.evict(prev_idx);
        }

        ret
    }

    fn evict(&mut self, prev_idx: Option<usize>) {
        let pos_idx = (self.slots.len() - 1).wrapping_sub(self.inserted);

        debug_assert!(!self.slots.is_empty());
        debug_assert!(self.assert_valid_state("one"));

        // Remove the header
        let slot = self.slots.pop_back().unwrap();
        let mut probe = desired_pos(self.mask, slot.hash);

        // Update the size
        self.size -= slot.header.len();

        debug_assert_eq!(
            self.indices
                .iter()
                .filter_map(|p| *p)
                .filter(|p| p.index == pos_idx)
                .count(),
            1
        );

        // Find the associated position
        probe_loop!(probe < self.indices.len(), {
            debug_assert!(self.indices[probe].is_some());

            let mut pos = self.indices[probe].unwrap();

            if pos.index == pos_idx {
                if let Some(idx) = slot.next {
                    pos.index = idx;
                    self.indices[probe] = Some(pos);
                } else if Some(pos.index) == prev_idx {
                    pos.index = 0usize.wrapping_sub(self.inserted + 1);
                    self.indices[probe] = Some(pos);
                } else {
                    self.indices[probe] = None;
                    self.remove_phase_two(probe);
                }

                break;
            }
        });

        debug_assert!(self.assert_valid_state("two"));
    }

    // Shifts all indices that were displaced by the header that has just been
    // removed.
    fn remove_phase_two(&mut self, probe: usize) {
        let mut last_probe = probe;
        let mut probe = probe + 1;

        probe_loop!(probe < self.indices.len(), {
            if let Some(pos) = self.indices[probe] {
                if probe_distance(self.mask, pos.hash, probe) > 0 {
                    self.indices[last_probe] = self.indices[probe].take();
                } else {
                    break;
                }
            } else {
                break;
            }

            last_probe = probe;
        });

        debug_assert!(self.assert_valid_state("two"));
    }

    fn reserve_one(&mut self) {
        let len = self.slots.len();

        if len == self.capacity() {
            if len == 0 {
                let new_raw_cap = 8;
                self.mask = 8 - 1;
                self.indices = vec![None; new_raw_cap];
            } else {
                let raw_cap = self.indices.len();
                self.grow(raw_cap << 1);
            }
        }
    }

    #[inline]
    fn grow(&mut self, new_raw_cap: usize) {
        // This path can never be reached when handling the first allocation in
        // the map.

        debug_assert!(self.assert_valid_state("top"));

        // find first ideally placed element -- start of cluster
        let mut first_ideal = 0;

        for (i, pos) in self.indices.iter().enumerate() {
            if let Some(pos) = *pos
                && 0 == probe_distance(self.mask, pos.hash, i)
            {
                first_ideal = i;
                break;
            }
        }

        // visit the entries in an order where we can simply reinsert them
        // into self.indices without any bucket stealing.
        let old_indices =
            mem::replace(&mut self.indices, vec![None; new_raw_cap]);
        self.mask = new_raw_cap.wrapping_sub(1);

        for &pos in &old_indices[first_ideal..] {
            self.reinsert_entry_in_order(pos);
        }

        for &pos in &old_indices[..first_ideal] {
            self.reinsert_entry_in_order(pos);
        }

        debug_assert!(self.assert_valid_state("bottom"));
    }

    fn reinsert_entry_in_order(&mut self, pos: Option<Pos>) {
        if let Some(pos) = pos {
            // Find first empty bucket and insert there
            let mut probe = desired_pos(self.mask, pos.hash);

            probe_loop!(probe < self.indices.len(), {
                if self.indices[probe].is_none() {
                    // empty bucket, insert here
                    self.indices[probe] = Some(pos);
                    return;
                }

                debug_assert!({
                    let them = self.indices[probe].unwrap();
                    let their_distance =
                        probe_distance(self.mask, them.hash, probe);
                    let our_distance =
                        probe_distance(self.mask, pos.hash, probe);

                    their_distance >= our_distance
                });
            });
        }
    }

    #[cfg(not(test))]
    fn assert_valid_state(&self, _: &'static str) -> bool {
        true
    }

    #[cfg(test)]
    fn assert_valid_state(&self, msg: &'static str) -> bool {
        // Internal callers may have charged the next insertion's bytes and may
        // retain a root for that not-yet-inserted slot during eviction.
        self.assert_structure(msg, false)
    }

    #[cfg(test)]
    fn assert_structure(&self, msg: &'static str, complete: bool) -> bool {
        let raw = self.indices.len();
        if raw == 0 {
            assert_eq!(self.mask, 0, "{msg}: empty mask");
            assert!(self.slots.is_empty(), "{msg}: slots without buckets");
        } else {
            assert!(raw.is_power_of_two(), "{msg}: bucket count");
            assert_eq!(self.mask, raw - 1, "{msg}: mask");
            assert!(
                self.indices.iter().any(Option::is_none),
                "{msg}: full map"
            );
        }
        assert!(self.slots.len() <= self.capacity(), "{msg}: capacity");
        let bytes = self
            .slots
            .iter()
            .try_fold(0usize, |n, slot| {
                assert_eq!(
                    slot.hash,
                    hash_header(&slot.header),
                    "{msg}: slot hash"
                );
                n.checked_add(slot.header.len())
            })
            .expect("live byte size overflow");
        if complete {
            assert_eq!(bytes, self.size, "{msg}: byte accounting");
            assert!(self.size <= self.max_size, "{msg}: size limit");
        } else {
            assert!(bytes <= self.size, "{msg}: precharged byte accounting");
        }

        let mut reached = vec![false; self.slots.len()];
        let mut roots: Vec<usize> = Vec::new();
        let mut pending = false;
        for (bucket, pos) in self.indices.iter().enumerate() {
            let Some(pos) = *pos else {
                continue;
            };
            let root = pos.index.wrapping_add(self.inserted);
            // Check lookup reachability with a finite probe bound, rather than
            // using probe_loop!, which cannot terminate on a corrupt map.
            let desired = desired_pos(self.mask, pos.hash);
            let distance = probe_distance(self.mask, pos.hash, bucket);
            for dist in 0..=distance {
                let probe = desired.wrapping_add(dist) & self.mask;
                let occupant =
                    self.indices[probe].expect("hole in probe path");
                assert!(
                    probe_distance(self.mask, occupant.hash, probe) >= dist,
                    "{msg}: lookup stops before root"
                );
            }
            if !complete && root == usize::MAX {
                assert!(!pending, "{msg}: duplicate pending root");
                pending = true;
                continue;
            }
            assert!(root < self.slots.len(), "{msg}: root outside live slots");
            assert_eq!(pos.hash, self.slots[root].hash, "{msg}: root hash");
            for &other in &roots {
                assert!(
                    self.slots[other].header.name()
                        != self.slots[root].header.name(),
                    "{msg}: duplicate name roots"
                );
            }
            roots.push(root);
            let mut current = Some(root);
            for _ in 0..self.slots.len() {
                let Some(index) = current else {
                    break;
                };
                assert!(
                    index < self.slots.len(),
                    "{msg}: link outside live slots"
                );
                assert!(!reached[index], "{msg}: cycle or shared node");
                reached[index] = true;
                let slot = &self.slots[index];
                assert_eq!(slot.hash, pos.hash, "{msg}: chain hash");
                assert!(
                    slot.header.name() == self.slots[root].header.name(),
                    "{msg}: mixed names in chain"
                );
                current = slot.next.map(|next| {
                    let next = next.wrapping_add(self.inserted);
                    assert!(
                        next < index,
                        "{msg}: chain must run oldest to newest"
                    );
                    next
                });
            }
            assert!(current.is_none(), "{msg}: chain exceeds live slot count");
        }
        assert!(
            reached.into_iter().all(|seen| seen),
            "{msg}: unreachable slot"
        );
        true
    }
}

#[cfg(test)]
mod invariant_tests {
    use super::*;
    use bytes::Bytes;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    fn field(name: &str, value: &str) -> Header {
        Header::Field {
            name: Bytes::copy_from_slice(name.as_bytes()),
            value: Bytes::copy_from_slice(value.as_bytes()),
        }
    }

    fn insert_checked(table: &mut Table, name: &str, value: &str) {
        let header = field(name, value);
        let result = table.index(header.clone());
        assert!(table.resolve(&result).name() == header.name());
        assert!(table.resolve(&result).value_eq(&header));
        assert!(table.assert_structure("completed insertion", true));
        if !matches!(result, Index::NotIndexed(_)) {
            let index = table.resolve_idx(&result);
            if index >= DYN_OFFSET {
                let slot = &table.slots[index - DYN_OFFSET];
                assert!(slot.header.name() == header.name());
                if !matches!(result, Index::Name(..)) {
                    assert!(slot.header.value_eq(&header));
                }
            }
        }
    }

    #[test]
    fn randomized_table_operations() {
        for seed in [0, 1, 0x4850_4143_4b, u64::MAX] {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut table = Table::new(4096, 0);
            for _ in 0..2000 {
                if rng.random_range(0..5) == 0 {
                    let limits = [0, 1, 40, 80, 128, 256, 1024, 4096];
                    table.resize(limits[rng.random_range(0..limits.len())]);
                    assert!(table.assert_structure("completed resize", true));
                } else {
                    let name = format!("x-name-{}", rng.random_range(0..24));
                    let value = format!(
                        "{}-{}",
                        rng.random_range(0..12),
                        "v".repeat(rng.random_range(0..96))
                    );
                    insert_checked(&mut table, &name, &value);
                    let before =
                        (table.size, table.slots.len(), table.inserted);
                    insert_checked(&mut table, &name, &value);
                    assert_eq!(
                        before,
                        (table.size, table.slots.len(), table.inserted),
                        "repeated lookup must not insert"
                    );
                }
            }
        }
    }

    #[test]
    fn collision_growth_eviction_and_counter_wrap() {
        let mut table = Table::new(8192, 0);
        table.inserted = usize::MAX - 2;
        let names: Vec<_> = (0..10000)
            .map(|n| format!("x-collision-{n}"))
            .filter(|name| hash_header(&field(name, "v")).0 & 7 == 7)
            .take(40)
            .collect();
        assert_eq!(names.len(), 40);
        for name in &names {
            insert_checked(&mut table, name, "first");
        }
        assert!(table.inserted < 40, "insertion counter did not wrap");
        assert!(table.indices.len() > 8, "hash storage did not grow");
        for value in ["second", "third", "fourth"] {
            for name in &names {
                insert_checked(&mut table, name, value);
            }
        }
        for limit in [4096, 1024, 128, 40, 0, 8192] {
            table.resize(limit);
            assert!(table.assert_structure("eviction and resize", true));
        }
        insert_checked(&mut table, &names[0], "after clearing");
    }

    #[test]
    fn checker_rejects_corruption_without_unbounded_walks() {
        for corruption in 0..6 {
            let mut table = Table::new(4096, 0);
            insert_checked(&mut table, "x-chain", "one");
            insert_checked(&mut table, "x-chain", "two");
            let bucket = table
                .indices
                .iter()
                .position(Option::is_some)
                .unwrap();
            match corruption {
                0 => table.size += 1,
                1 => {
                    table.slots[1].next =
                        Some(1usize.wrapping_sub(table.inserted))
                }
                2 => {
                    table.slots[1].next =
                        Some(100usize.wrapping_sub(table.inserted))
                }
                3 => {
                    table.indices[bucket]
                        .as_mut()
                        .unwrap()
                        .hash = HashValue(0)
                }
                4 => table.indices[bucket] = None,
                5 => {
                    let empty = table
                        .indices
                        .iter()
                        .position(Option::is_none)
                        .unwrap();
                    table.indices[empty] = table.indices[bucket];
                }
                _ => unreachable!(),
            }
            assert!(
                std::panic::catch_unwind(|| {
                    table.assert_structure("deliberate corruption", true);
                })
                .is_err(),
                "corruption {corruption} went undetected"
            );
        }
    }
}

#[cfg(test)]
impl Table {
    /// Returns the number of headers in the table
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Returns the table size
    pub fn size(&self) -> usize {
        self.size
    }
}

impl Index {
    fn new(v: Option<(usize, bool)>, e: Header) -> Index {
        match v {
            None => Index::NotIndexed(e),
            Some((n, true)) if !e.is_sensitive() => Index::Indexed(n, e),
            Some((n, true)) => Index::Name(n, e),
            Some((n, false)) => Index::Name(n, e),
        }
    }
}

#[inline]
fn usable_capacity(cap: usize) -> usize {
    cap - cap / 4
}

#[inline]
fn to_raw_capacity(n: usize) -> usize {
    n + n / 3
}

#[inline]
fn desired_pos(mask: usize, hash: HashValue) -> usize {
    hash.0 & mask
}

#[inline]
fn probe_distance(mask: usize, hash: HashValue, current: usize) -> usize {
    current.wrapping_sub(desired_pos(mask, hash)) & mask
}

fn hash_header(header: &Header) -> HashValue {
    const MASK: u64 = (MAX_SIZE as u64) - 1;

    let mut h = FnvHasher::default();
    header.name().hash(&mut h);
    HashValue((h.finish() & MASK) as usize)
}

/// Checks the static table for the header. If found, returns the index and a
/// boolean representing if the value matched as well.
fn index_static(header: &Header) -> Option<(usize, bool)> {
    match *header {
        Header::Sensitive(ref header) => index_static(header),
        Header::Field {
            ref name,
            ref value,
        } => match name.as_ref() {
            header::ACCEPT_CHARSET => Some((15, false)),
            header::ACCEPT_ENCODING => {
                if value.as_ref() == b"gzip, deflate" {
                    Some((16, true))
                } else {
                    Some((16, false))
                }
            }
            header::ACCEPT_LANGUAGE => Some((17, false)),
            header::ACCEPT_RANGES => Some((18, false)),
            header::ACCEPT => Some((19, false)),
            header::ACCESS_CONTROL_ALLOW_ORIGIN => Some((20, false)),
            header::AGE => Some((21, false)),
            header::ALLOW => Some((22, false)),
            header::AUTHORIZATION => Some((23, false)),
            header::CACHE_CONTROL => Some((24, false)),
            header::CONTENT_DISPOSITION => Some((25, false)),
            header::CONTENT_ENCODING => Some((26, false)),
            header::CONTENT_LANGUAGE => Some((27, false)),
            header::CONTENT_LENGTH => Some((28, false)),
            header::CONTENT_LOCATION => Some((29, false)),
            header::CONTENT_RANGE => Some((30, false)),
            header::CONTENT_TYPE => Some((31, false)),
            header::COOKIE => Some((32, false)),
            header::DATE => Some((33, false)),
            header::ETAG => Some((34, false)),
            header::EXPECT => Some((35, false)),
            header::EXPIRES => Some((36, false)),
            header::FROM => Some((37, false)),
            header::HOST => Some((38, false)),
            header::IF_MATCH => Some((39, false)),
            header::IF_MODIFIED_SINCE => Some((40, false)),
            header::IF_NONE_MATCH => Some((41, false)),
            header::IF_RANGE => Some((42, false)),
            header::IF_UNMODIFIED_SINCE => Some((43, false)),
            header::LAST_MODIFIED => Some((44, false)),
            header::LINK => Some((45, false)),
            header::LOCATION => Some((46, false)),
            header::MAX_FORWARDS => Some((47, false)),
            header::PROXY_AUTHENTICATE => Some((48, false)),
            header::PROXY_AUTHORIZATION => Some((49, false)),
            header::RANGE => Some((50, false)),
            header::REFERER => Some((51, false)),
            header::REFRESH => Some((52, false)),
            header::RETRY_AFTER => Some((53, false)),
            header::SERVER => Some((54, false)),
            header::SET_COOKIE => Some((55, false)),
            header::STRICT_TRANSPORT_SECURITY => Some((56, false)),
            header::TRANSFER_ENCODING => Some((57, false)),
            header::USER_AGENT => Some((58, false)),
            header::VARY => Some((59, false)),
            header::VIA => Some((60, false)),
            header::WWW_AUTHENTICATE => Some((61, false)),
            _ => None,
        },
        Header::Authority(_) => Some((1, false)),
        Header::Method(ref v) => match *v {
            Method::GET => Some((2, true)),
            Method::POST => Some((3, true)),
            _ => Some((2, false)),
        },
        Header::Scheme(ref v) => match &**v {
            "http" => Some((6, true)),
            "https" => Some((7, true)),
            _ => Some((6, false)),
        },
        Header::Path(ref v) => match &**v {
            "/" => Some((4, true)),
            "/index.html" => Some((5, true)),
            _ => Some((4, false)),
        },
        Header::Protocol(..) => None,
        Header::Status(ref v) => match u16::from(*v) {
            200 => Some((8, true)),
            204 => Some((9, true)),
            206 => Some((10, true)),
            304 => Some((11, true)),
            400 => Some((12, true)),
            404 => Some((13, true)),
            500 => Some((14, true)),
            _ => Some((8, false)),
        },
    }
}
