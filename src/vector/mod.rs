use std::ptr::{ self, null_mut };
use std::ops::{ Index, IndexMut, RangeFrom, Range };
use std::iter::FromIterator;

pub struct MyVec<T> {
    data: *mut T,
    size: usize,
    capacity: usize,
}

impl<T> MyVec<T> {
    pub fn new() -> Self {
        MyVec {
            data: ptr::null_mut(),
            size: 0,
            capacity: 0,
        }
    }

    pub fn push(&mut self, value: T) {
        if self.size == self.capacity {
            self.resize();
        }
        // Insert the value into data[size]
        unsafe {
            ptr::write(self.data.add(self.size), value);
        }
        self.size += 1;
    }

    fn resize(&mut self) {
        // Fix the initial capacity to 1
        let new_capacity = if self.capacity == 0 { 1 } else { self.capacity * 2 };

        // Allocate new memory
        let new_data = unsafe {
            std::alloc::alloc(std::alloc::Layout::array::<T>(new_capacity).unwrap()) as *mut T
        };

        // Copy old elements to new memory
        unsafe {
            if self.data != null_mut() {
                ptr::copy_nonoverlapping(self.data, new_data, self.size);
            }

            // Deallocate old memory if it was allocated
            if self.capacity != 0 {
                std::alloc::dealloc(
                    self.data as *mut u8,
                    std::alloc::Layout::array::<T>(self.capacity).unwrap()
                );
            }
        }

        // Update the pointer to the new memory and the capacity
        self.data = new_data;
        self.capacity = new_capacity;
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn iter(&self) -> MyVecIter<T> {
        MyVecIter {
            vec: self,
            index: 0,
        }
    }
}

// Implementation of the `Index` trait for access through `[]`
impl<T> Index<usize> for MyVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        if index >= self.size {
            panic!("Index out of bounds");
        }
        unsafe { &*self.data.add(index) }
    }
}

impl<T> Index<RangeFrom<usize>> for MyVec<T> {
    type Output = [T];

    fn index(&self, index: RangeFrom<usize>) -> &Self::Output {
        let start = index.start;

        // Check that the index is within bounds
        if start >= self.size {
            panic!("Index out of bounds");
        }

        // Return a slice of the vector data starting from `start`
        unsafe {
            std::slice::from_raw_parts(self.data.add(start), self.size - start)
        }
    }
}

impl<T: Clone> MyVec<T> {
    pub fn clone(&self) -> MyVec<T> {
        let mut new_vec = MyVec::new();
        for i in 0..self.size {
            unsafe {
                let item = ptr::read(self.data.add(i)); // Copy the element
                new_vec.push(item.clone()); // Clone the element and add it to the new vector
            }
        }
        new_vec
    }
}

impl<T> Index<Range<usize>> for MyVec<T> {
    type Output = [T]; // Specify that the returned type is a slice

    fn index(&self, index: Range<usize>) -> &Self::Output {
        let start = index.start;
        let end = index.end;

        // Check that the indices are within bounds
        if start >= self.size || end > self.size || start > end {
            panic!("Index out of bounds");
        }

        // Return a slice of the vector data from `start` to `end`
        unsafe {
            std::slice::from_raw_parts(self.data.add(start), end - start)
        }
    }
}

// Implementation of the `IndexMut` trait for mutable access through `[]`
impl<T> IndexMut<usize> for MyVec<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        if index >= self.size {
            panic!("Index out of bounds");
        }
        unsafe { &mut *self.data.add(index) }
    }
}

impl<T> FromIterator<T> for MyVec<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut vec = MyVec::new();
        for value in iter {
            vec.push(value);
        }
        vec
    }
}

impl MyVec<&str> {
    pub fn join(&self, sep: &str) -> String {
        if self.size == 0 {
            return String::new(); // If the vector is empty, return an empty string
        }

        let mut result = String::with_capacity(self.size * sep.len()); // Preallocate memory for the string

        for i in 0..self.size {
            if i > 0 {
                result.push_str(sep); // Add the separator before each element, starting from the second
            }
            result.push_str(unsafe { &*self.data.add(i) }); // Add the element to the string
        }

        result
    }
}

pub struct MyVecIter<'a, T> {
    vec: &'a MyVec<T>,
    index: usize,
}

impl<'a, T> Iterator for MyVecIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.vec.size {
            let item = unsafe { &*self.vec.data.add(self.index) };
            self.index += 1;
            Some(item)
        } else {
            None
        }
    }
}

impl<T> Drop for MyVec<T> {
    fn drop(&mut self) {
        // If data is not null_mut, free the memory
        if self.data != null_mut() {
            unsafe {
                // Deallocate memory for elements
                for i in 1..self.size {
                    // Call the destructor for each element
                    ptr::drop_in_place(self.data.add(i));
                }

                // Deallocate the array itself
                std::alloc::dealloc(
                    self.data as *mut u8,
                    std::alloc::Layout::array::<T>(self.capacity).unwrap()
                );
            }
        }
    }
}
