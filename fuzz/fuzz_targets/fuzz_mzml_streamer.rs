#![no_main]

use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    // Create an in-memory reader from the fuzz input
    let cursor = Cursor::new(data);
    
    // Try to parse as mzML - we expect this to either succeed or fail gracefully
    // The key is that it should NEVER panic
    let _ = mzpeak::mzml::streamer::MzMLStreamer::new(cursor);
    
    // If parsing succeeded, try to iterate through spectra
    // This will catch panics during actual data processing
    if let Ok(mut streamer) = mzpeak::mzml::streamer::MzMLStreamer::new(Cursor::new(data)) {
        // Try to read up to 100 spectra to catch parsing errors
        for _ in 0..100 {
            match streamer.next_spectrum() {
                Ok(Some(_spectrum)) => {
                    // Successfully parsed a spectrum - continue
                }
                Ok(None) => {
                    // End of file - normal termination
                    break;
                }
                Err(_) => {
                    // Error parsing - acceptable, just break
                    break;
                }
            }
        }
    }
});
