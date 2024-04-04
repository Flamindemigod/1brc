use ahash::{HashMap, HashMapExt};
use memchr::memchr;
use memmap::Mmap;
use std::{
    cmp::{max, min},
    fs::File,
    str::from_utf8_unchecked,
    sync::Arc,
    thread,
};

static NUM_CORES: usize = 16;
static NUM_STATIONS: usize = 413;
#[derive(Debug, Clone)]
pub struct DataPoint {
    min: i32,
    max: i32,
    count: u32,
    sum: i64,
}

impl DataPoint {
    pub fn new(val: i32) -> Self {
        Self {
            min: val,
            max: val,
            count: 1,
            sum: val.into(),
        }
    }

    pub fn add_sample(&mut self, val: i32) {
        self.min = min(self.min, val);
        self.max = max(self.max, val);
        self.sum += val as i64;
        self.count += 1;
    }
}

fn parse_ascii_digits(buffer: &[u8]) -> i32 {
    let size = buffer.len();
    let mut negative_mul = 1;
    let mut accumulator = 0;
    let mut positional_mul = 10_i32.pow(size as u32 - 2);
    buffer.iter().for_each(|byte| {
        match byte {
            45 => {
                // Minus
                negative_mul = -1;
                positional_mul /= 10;
            }
            59 => {} // Semicolon
            46 => {} // Period
            48..=57 => {
                // Digits
                let digit = *byte as i32 - 48;
                accumulator += digit * positional_mul;
                positional_mul /= 10;
            }
            _ => panic!("Unhandled ASCII numerical symbol: {}", byte),
        }
    });
    accumulator *= negative_mul;
    accumulator
}

fn parse_file_chunk(mut start: usize, end: usize, buffer: &[u8]) -> HashMap<&[u8], DataPoint> {
    let mut chunk_results: HashMap<&[u8], DataPoint> = HashMap::with_capacity(NUM_STATIONS);
    while start < end {
        let station_name_offset = memchr(b';', &buffer[start..end]).unwrap();
        let data_offset = memchr(b'\n', &buffer[start..end]).unwrap();
        let station_name = &buffer[start..(start + station_name_offset)];
        let data = &buffer[(start + station_name_offset + 1)..(start + data_offset)];
        if let Some(datapoint) = chunk_results.get_mut(station_name).as_mut() {
            datapoint.add_sample(parse_ascii_digits(data))
        } else {
            chunk_results.insert(station_name, DataPoint::new(parse_ascii_digits(data)));
        };
        start += data_offset + 1;
    }
    chunk_results
}

fn find_next_newline(start: usize, buffer: &[u8]) -> usize {
    start + memchr(b'\n', &buffer[start..]).unwrap_or(0)
}

fn main() {
    let file = File::open("measurements.txt").unwrap();
    let mapped_file = unsafe { Mmap::map(&file).unwrap() };
    // let mmap = &mmap[..];
    let file_size = mapped_file.len();
    let chunk_size = file_size / NUM_CORES;

    let mut chunk_starts: Vec<usize> = (0..NUM_CORES).map(|n| n * chunk_size).collect();
    chunk_starts
        .iter_mut()
        .skip(1)
        .for_each(|f| *f = find_next_newline(*f, &mapped_file) + 1);
    let mut chunks_ends: Vec<usize> = chunk_starts.clone().into_iter().skip(1).collect();
    chunks_ends.push(mapped_file.len());

    // println!("Printing Chunks");
    // for (start, end) in chunk_starts.iter().zip(&chunks_ends) {
    //     println!("{start:>12} => {end:>12} : {:>12}", end - start);
    // }

    let mut results: HashMap<Arc<str>, DataPoint> = HashMap::with_capacity(NUM_STATIONS);
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(NUM_CORES);
        for thread_idx in 0..NUM_CORES {
            let start = chunk_starts[thread_idx];
            let end = chunks_ends[thread_idx];
            let buffer = &mapped_file;
            let handle = scope.spawn(move || parse_file_chunk(start, end, buffer));
            handles.push(handle);
        }

        for handle in handles {
            let chunk_result = handle.join().unwrap();
            chunk_result.into_iter().for_each(|(k, v)| {
                if let Some(data_point) = results.get_mut(unsafe { from_utf8_unchecked(k) }) {
                    data_point.sum += v.sum;
                    data_point.count += v.count;
                    data_point.max = i32::max(data_point.max, v.max);
                    data_point.min = i32::min(data_point.min, v.min);
                } else {
                    results.insert(unsafe { from_utf8_unchecked(k) }.into(), v);
                }
            })
        }
    });
    let mut end_res = results.iter().collect::<Vec<_>>();
    end_res.sort_unstable_by(|(akey, _), (bkey, _)| akey.cmp(bkey));
    end_res.iter().for_each(|(k, v)| {
        let mean = (v.sum as f64 / v.count as f64) / 10.0;
        println!(
            "{}={:.1}/{:.1}/{mean:.1}, ",
            k,
            v.min as f32 / 10.0,
            v.max as f32 / 10.0
        );
    })
}
