-- =====================================================================
-- DuckDB Query Examples for mzPeak Files
-- =====================================================================
-- 
-- mzPeak files (both .mzpeak containers and .mzpeak.parquet files) can be 
-- queried directly using DuckDB without any preprocessing or conversion.
--
-- Installation: https://duckdb.org/docs/installation/
-- 
-- Usage:
--   duckdb < examples/duckdb_queries.sql
--   OR
--   duckdb -c "SELECT * FROM read_parquet('data.mzpeak/peaks/peaks.parquet') LIMIT 5;"
-- =====================================================================

-- For .mzpeak container format (ZIP archive), extract first or use DuckDB's
-- built-in support for reading from compressed archives if available.
-- For .mzpeak directory bundles, use the path directly.

-- =====================================================================
-- 1. Basic Data Exploration
-- =====================================================================

-- Count total peaks
SELECT COUNT(*) as total_peaks
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet');

-- Get dataset summary statistics
SELECT 
    COUNT(DISTINCT spectrum_id) as num_spectra,
    COUNT(*) as total_peaks,
    MIN(mz) as min_mz,
    MAX(mz) as max_mz,
    AVG(intensity) as avg_intensity,
    MIN(retention_time) as start_rt,
    MAX(retention_time) as end_rt
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet');

-- Show MS level distribution
SELECT 
    ms_level,
    COUNT(DISTINCT spectrum_id) as num_spectra,
    COUNT(*) as num_peaks
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
GROUP BY ms_level
ORDER BY ms_level;

-- =====================================================================
-- 2. Targeted m/z Queries (Extracted Ion Chromatogram)
-- =====================================================================

-- Extract Ion Chromatogram (XIC) for a specific m/z (±5 ppm tolerance)
SELECT 
    retention_time,
    mz,
    intensity,
    spectrum_id
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
WHERE ms_level = 1
  AND mz BETWEEN 500.25 * (1 - 5e-6) AND 500.25 * (1 + 5e-6)
ORDER BY retention_time;

-- XIC with aggregated intensity per retention time
SELECT 
    ROUND(retention_time, 1) as rt_bin,
    AVG(mz) as avg_mz,
    SUM(intensity) as total_intensity,
    COUNT(*) as peak_count
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
WHERE ms_level = 1
  AND mz BETWEEN 500.25 * (1 - 10e-6) AND 500.25 * (1 + 10e-6)
GROUP BY rt_bin
ORDER BY rt_bin;

-- =====================================================================
-- 3. MS/MS (MS2) Queries
-- =====================================================================

-- Find all MS2 spectra for a specific precursor m/z
SELECT 
    spectrum_id,
    scan_number,
    retention_time,
    precursor_mz,
    precursor_charge,
    collision_energy,
    COUNT(*) as num_fragment_peaks
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
WHERE ms_level = 2
  AND precursor_mz BETWEEN 500 AND 600
GROUP BY spectrum_id, scan_number, retention_time, precursor_mz, precursor_charge, collision_energy
ORDER BY retention_time;

-- Get fragment ions for a specific MS2 spectrum
SELECT 
    mz,
    intensity
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
WHERE spectrum_id = 100
ORDER BY mz;

-- Find the most intense fragments across all MS2 spectra
SELECT 
    precursor_mz,
    mz as fragment_mz,
    intensity,
    retention_time
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
WHERE ms_level = 2
  AND intensity > 1e5
ORDER BY intensity DESC
LIMIT 20;

-- =====================================================================
-- 4. Ion Mobility Queries (if available)
-- =====================================================================

-- Check if dataset contains ion mobility data
SELECT 
    COUNT(*) as total_peaks,
    COUNT(ion_mobility) as im_peaks,
    ROUND(COUNT(ion_mobility) * 100.0 / COUNT(*), 2) as im_percentage
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet');

-- Query specific ion mobility range (e.g., timsTOF data)
SELECT 
    spectrum_id,
    retention_time,
    mz,
    intensity,
    ion_mobility
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
WHERE ion_mobility IS NOT NULL
  AND ion_mobility BETWEEN 20 AND 30
  AND mz BETWEEN 400 AND 800
ORDER BY retention_time, ion_mobility;

-- Create 3D histogram (m/z × ion mobility × intensity)
SELECT 
    FLOOR(mz / 10) * 10 as mz_bin,
    FLOOR(ion_mobility / 2) * 2 as im_bin,
    COUNT(*) as peak_count,
    AVG(intensity) as avg_intensity
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
WHERE ion_mobility IS NOT NULL
GROUP BY mz_bin, im_bin
ORDER BY mz_bin, im_bin;

-- =====================================================================
-- 5. Spectral Comparison and Quality Control
-- =====================================================================

-- Find spectra with unusual characteristics (very few or many peaks)
SELECT 
    spectrum_id,
    ms_level,
    retention_time,
    COUNT(*) as num_peaks,
    SUM(intensity) as total_intensity
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
GROUP BY spectrum_id, ms_level, retention_time
HAVING num_peaks < 10 OR num_peaks > 1000
ORDER BY num_peaks DESC;

-- Base Peak Chromatogram (BPC) - maximum intensity per spectrum
SELECT 
    spectrum_id,
    ms_level,
    retention_time,
    MAX(intensity) as base_peak_intensity,
    ARG_MAX(mz, intensity) as base_peak_mz
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
WHERE ms_level = 1
GROUP BY spectrum_id, ms_level, retention_time
ORDER BY retention_time;

-- Total Ion Current (TIC) chromatogram
SELECT 
    spectrum_id,
    retention_time,
    SUM(intensity) as tic
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
WHERE ms_level = 1
GROUP BY spectrum_id, retention_time
ORDER BY retention_time;

-- =====================================================================
-- 6. Advanced Analytics
-- =====================================================================

-- Calculate mass defect distribution (for metabolomics)
SELECT 
    FLOOR(mz) as nominal_mass,
    ROUND(mz - FLOOR(mz), 3) as mass_defect,
    COUNT(*) as frequency
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
WHERE ms_level = 1
  AND mz BETWEEN 100 AND 1000
GROUP BY nominal_mass, mass_defect
HAVING frequency > 10
ORDER BY nominal_mass, mass_defect;

-- Find isotope patterns (peaks separated by ~1.003 Da)
WITH peaks AS (
    SELECT 
        spectrum_id,
        mz,
        intensity
    FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
    WHERE ms_level = 1
)
SELECT 
    p1.spectrum_id,
    p1.mz as mz_monoisotopic,
    p1.intensity as intensity_mono,
    p2.mz as mz_isotope,
    p2.intensity as intensity_iso,
    ROUND(p2.mz - p1.mz, 4) as mass_diff
FROM peaks p1
JOIN peaks p2 ON p1.spectrum_id = p2.spectrum_id
WHERE p2.mz BETWEEN p1.mz + 1.0 AND p1.mz + 1.01
  AND p1.intensity > 1e5
  AND p2.intensity > p1.intensity * 0.1
LIMIT 100;

-- Calculate m/z precision distribution
SELECT 
    FLOOR(mz / 100) * 100 as mz_range,
    COUNT(*) as peak_count,
    STDDEV(mz) as mz_stddev,
    AVG(intensity) as avg_intensity
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
WHERE ms_level = 1
GROUP BY mz_range
ORDER BY mz_range;

-- =====================================================================
-- 7. Data Export and Integration
-- =====================================================================

-- Export filtered data to CSV
COPY (
    SELECT 
        spectrum_id,
        retention_time,
        mz,
        intensity
    FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
    WHERE ms_level = 1
      AND mz BETWEEN 400 AND 600
) TO 'filtered_ms1_peaks.csv' (HEADER, DELIMITER ',');

-- Create a new Parquet file with filtered data
COPY (
    SELECT * 
    FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
    WHERE ms_level = 2
      AND precursor_mz BETWEEN 500 AND 600
) TO 'filtered_ms2.parquet' (FORMAT PARQUET);

-- =====================================================================
-- 8. Performance Optimization Examples
-- =====================================================================

-- Use column projection (only read needed columns)
SELECT retention_time, mz, intensity
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
WHERE ms_level = 1
LIMIT 1000;

-- Use predicate pushdown (filter before reading)
SELECT COUNT(*)
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
WHERE retention_time > 3600 AND ms_level = 2;

-- Parallel query execution (DuckDB automatically parallelizes)
SELECT 
    FLOOR(retention_time / 60) as time_bin_min,
    COUNT(*) as peak_count,
    AVG(intensity) as avg_intensity
FROM read_parquet('demo_run.mzpeak/peaks/peaks.parquet')
GROUP BY time_bin_min
ORDER BY time_bin_min;

-- =====================================================================
-- Notes:
-- 
-- - Replace 'demo_run.mzpeak/peaks/peaks.parquet' with your file path
-- - For .mzpeak.parquet legacy files, use the file path directly
-- - For .mzpeak containers, you may need to extract first:
--     unzip data.mzpeak peaks/peaks.parquet
-- - All queries leverage Parquet's columnar storage for fast execution
-- - DuckDB supports billions of rows with minimal memory usage
-- =====================================================================
