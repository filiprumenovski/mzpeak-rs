use std::process::Command;

#[test]
fn cli_dispatches_to_the_supported_command_modules() {
    let output = Command::new(env!("CARGO_BIN_EXE_mzpeak-convert"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("validate"));
    #[cfg(feature = "thermo")]
    assert!(help.contains("convert"));
    #[cfg(feature = "mzml")]
    {
        let output = Command::new(env!("CARGO_BIN_EXE_mzpeak-convert"))
            .args(["convert", "--help"])
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(String::from_utf8(output.stdout)
            .unwrap()
            .contains("--profile"));
    }
}
