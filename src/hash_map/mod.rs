use crate::MyVec;

pub struct MyHashMap<K, V> {
    buckets: MyVec<Option<(K, V)>>,
    size: usize,
}

impl<K: AsRef<[u8]> + Clone, V: Clone> MyHashMap<K, V> {
    pub fn new() -> Self {
        MyHashMap {
            buckets: {
                let mut buckets = MyVec::new();
                buckets.push(None);
                buckets
            },
            size: 1,
        }
    }

    fn to_bytes(key: &K) -> &[u8] {
        key.as_ref()
    }

    fn hash(&self, key: &K) -> usize {
        let mut hash: u32 = 0;
        for byte in Self::to_bytes(key) {
            hash = hash
                .wrapping_add(*byte as u32)
                .wrapping_add(hash << 6)
                .wrapping_add(hash << 16);
        }
        hash as usize
    }

    fn rehash(&mut self) {
        let mut old_buckets = MyVec::new();
        for element in self.buckets.iter() {
            if element.is_some() {
                old_buckets.push(element.clone());
            }
        }

        self.size *= 2;
        self.buckets = MyVec::new();

        for _ in 0..self.size {
            self.buckets.push(None);
        }

        for opt in old_buckets.iter() {
            if let Some((key, value)) = opt {
                self.insert(key.clone(), value.clone());
            }
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        let index = self.hash(&key) % self.size;

        while self.buckets[index].is_some() {
            self.rehash();
        }

        self.buckets[index] = Some((key, value));
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let index = self.hash(&key) % self.size;
        self.buckets[index].as_ref().map(|(_, v)| v)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let index = self.hash(key) % self.size;
        if let Some((_, ref mut value)) = self.buckets[index] {
            Some(value)
        } else {
            None
        }
    }

    pub fn iter(&self) -> MyHashMapIter<K, V> {
        MyHashMapIter {
            map: self,
            index: 0,
        }
    }

    pub fn extend(&mut self, other: MyHashMap<K, V>) {
        for bucket in other.buckets.iter() {
            if let Some((key, value)) = bucket {
                self.insert(key.clone(), value.clone());
            }
        }
    }
}

impl<K: AsRef<[u8]> + Clone, V: Clone> Clone for MyHashMap<K, V> {
    fn clone(&self) -> Self {
        let mut new_map = MyHashMap::new();
        for bucket in self.buckets.iter() {
            if let Some((key, value)) = bucket {
                new_map.insert(key.clone(), value.clone());
            }
        }
        new_map
    }
}

pub struct MyHashMapIter<'a, K, V> {
    map: &'a MyHashMap<K, V>,
    index: usize,
}

impl<'a, K, V> Iterator for MyHashMapIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.map.buckets.len() {
            if let Some((ref key, ref value)) = self.map.buckets[self.index] {
                self.index += 1;
                return Some((key, value));
            }
            self.index += 1; // Переходим к следующему ведру
        }
        None
    }
}

impl<K, V> Drop for MyHashMap<K, V> {
    fn drop(&mut self) {}
}
