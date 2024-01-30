use std::path::Path;
use std::process::Command;

const PAPERMILL_PATH: &str = "/home/x/.local/bin/papermill";

pub fn create_notebook(input_file: &Path, notebook_input_path: &Path, notebook_output_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating {} using template: {} and input data: {}", notebook_output_path.display(), notebook_input_path.display(), input_file.display());
    let mut papermill_process = Command::new(PAPERMILL_PATH)
        .arg("-p")
        .arg("output_data_path")
        .arg(input_file.to_str().unwrap())
        .arg(notebook_input_path)
        .arg(notebook_output_path)
        .spawn()?;
    papermill_process.wait()?;
    Ok(())
}