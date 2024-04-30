use super::Filter;

fn num_bits(size: usize, fp_rate: f64) -> usize {
    let num = -1.0f64 * size as f64 * fp_rate.ln();
    let den = 2.0f64.ln().powf(2.0);
    (num / den).ceil() as usize
}
fn num_hashes(m: usize, n: usize) -> usize {
    ((m as f64 / n as f64) * 2.0f64.ln()).ceil() as usize
}

pub struct BloomFilter {
    bit_vec: Vec<u8>,
    hashes: usize,
}

impl BloomFilter {
    pub fn new(size: usize, fp_rate: f64) -> Self {
        let m = num_bits(size, fp_rate);
        let k = num_hashes(m, size);
        Self {
            bit_vec: vec![0; m],
            hashes: k,
        }
    }
}
impl Default for BloomFilter {
    fn default() -> Self {
        Self::new(2, 0.001)
    }
}

impl Filter for BloomFilter {
    fn contains(&self, item: &str) -> bool {
        for i in 0..self.hashes {
            let index =
                fasthash::murmur3::hash32_with_seed(item, i as u32) % self.bit_vec.len() as u32 * 8;
            let pos = index as usize;
            if (1 << (pos % 8)) & self.bit_vec[pos / 8] == 0 {
                return false;
            }
        }
        true
    }

    fn insert(&mut self, item: &str) {
        for i in 0..self.hashes {
            let index =
                fasthash::murmur3::hash32_with_seed(item, i as u32) % self.bit_vec.len() as u32 * 8;
            let pos = index as usize;
            self.bit_vec[pos / 8] |= 1 << (pos % 8);
        }
    }
}
#[cfg(test)]
mod tests {
    use crate::filter::Filter;

    use super::BloomFilter;

    #[test]
    fn test_insert() {
        let mut filter = BloomFilter::new(2, 0.001);
        filter.insert("apple");
        filter.insert("orange");
        filter.insert("grape");
        assert!(filter.contains("apple"));
        assert!(filter.contains("orange"));
        assert!(filter.contains("grape"));
    }
}
