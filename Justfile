# mzpeak conversion + validation pipeline for local test datasets
# Requires: just (https://github.com/casey/just)

# Paths (hardcoded as requested)
repo_dir := "/Users/filiprumenovski/Code/mzpeak-rs"
input_dir := "/Volumes/NVMe 2TB/Test"
output_dir := "/Volumes/NVMe 2TB/Metabolism Finals Prep"

thermo_input := "{{input_dir}}/Fetal_Liver_Gel_Velos_24_f07.raw"
thermo_output := "{{output_dir}}/Fetal_Liver_Gel_Velos_24_f07.mzpeak"

bruker_input := "{{input_dir}}/Sara_Ligandome_Ovarian_20201008_S4_Slot2-44_20-10-16_2943.d"
bruker_output := "{{output_dir}}/Sara_Ligandome_Ovarian_20201008_S4_Slot2-44_20-10-16_2943.mzpeak"

mzml_input := "{{input_dir}}/large_test.mzML"
mzml_output := "{{output_dir}}/large_test.mzpeak"

# Binaries (x86_64 for Rosetta 2)
bin := "{{repo_dir}}/target/x86_64-apple-darwin/release/mzpeak-convert"
tdf_example := "{{repo_dir}}/target/x86_64-apple-darwin/release/examples/quick_tdf_convert"

# Ensure output directory exists
setup:
    mkdir -p "{{output_dir}}"

# Build release binaries for Rosetta 2 (x86_64)
build-release:
    cargo build --release --target x86_64-apple-darwin --features "mzml,thermo,tdf"
    cargo build --release --target x86_64-apple-darwin --example quick_tdf_convert --features "tdf"

# Thermo RAW pipeline (convert + validate)
thermo: setup build-release
    arch -x86_64 "{{bin}}" convert-thermo "{{thermo_input}}" "{{thermo_output}}"
    DOTNET_ROOT="$HOME/.dotnet-x64" arch -x86_64 "{{bin}}" validate "{{thermo_output}}"

# Bruker TDF pipeline (convert via example + validate)
bruker: setup build-release
    arch -x86_64 "{{tdf_example}}"
    mv "/Volumes/NVMe 2TB/mz-peak output/$(basename {{bruker_input}} .d).mzpeak" "{{bruker_output}}"
    DOTNET_ROOT="$HOME/.dotnet-x64" arch -x86_64 "{{bin}}" validate "{{bruker_output}}"

# mzML pipeline (convert + validate)
mzml: setup build-release
    arch -x86_64 "{{bin}}" convert "{{mzml_input}}" "{{mzml_output}}"
    DOTNET_ROOT="$HOME/.dotnet-x64" arch -x86_64 "{{bin}}" validate "{{mzml_output}}"

# Run all three sequentially
all: thermo bruker mzml
