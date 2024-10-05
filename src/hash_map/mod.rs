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

        if self.buckets[index].is_some() {
            self.rehash();
        }

        self.buckets[index] = Some((key, value));
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let index = self.hash(&key) % self.size;
        self.buckets[index].as_ref().map(|(_, v)| v)
    }
}
