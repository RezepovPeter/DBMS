use crate::MyVec;

pub struct MyHashMap<K, V> {
    buckets: MyVec<Option<(K, V)>>,
    size: usize,
}

impl<K: AsRef<[u8]> + Clone + Eq, V: Clone> MyHashMap<K, V> {
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
        let old_buckets = std::mem::replace(&mut self.buckets, MyVec::new());

        // Увеличиваем размер в 2 раза
        let new_size = old_buckets.len() * 2;
        self.buckets = MyVec::new();
        for _ in 0..new_size {
            self.buckets.push(None);
        }
        self.size = 0; // Сбрасываем размер до 0, затем добавляем все элементы

        for opt in old_buckets.iter() {
            if let Some((key, value)) = opt {
                self.insert(key.clone(), value.clone()); // Вставляем элементы в новую хеш-таблицу
            }
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        const LOAD_FACTOR: f64 = 0.75;
        if (self.size as f64) >= (self.buckets.len() as f64) * LOAD_FACTOR {
            self.rehash(); // Рехешируем при достижении порога
        }

        let mut index = self.hash(&key) % self.buckets.len();

        // Линейное пробирование для нахождения свободного места
        while self.buckets[index].is_some() {
            if let Some((ref k, _)) = self.buckets[index] {
                if k == &key {
                    // Если ключ уже существует, обновляем значение
                    self.buckets[index] = Some((key, value));
                    return;
                }
            }
            index = (index + 1) % self.buckets.len();
        }

        // Вставляем новый элемент
        self.buckets[index] = Some((key, value));
        self.size += 1; // Увеличиваем размер
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let mut index = self.hash(key) % self.buckets.len(); // Используем количество ведер

        while let Some((ref k, ref v)) = self.buckets[index] {
            if k == key {
                return Some(v); // Возвращаем ссылку на значение
            }
            index = (index + 1) % self.buckets.len(); // Линейное пробирование
        }

        None // Если не нашли ключ
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let mut index = self.hash(key) % self.buckets.len();

        while let Some((ref k, _)) = self.buckets[index] {
            if k == key {
                // Здесь важно создать временную ссылку отдельно
                return self.buckets[index].as_mut().map(|(_, v)| v);
            }
            index = (index + 1) % self.buckets.len();
        }

        None
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

impl<K: AsRef<[u8]> + Clone + Eq, V: Clone> Clone for MyHashMap<K, V> {
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
