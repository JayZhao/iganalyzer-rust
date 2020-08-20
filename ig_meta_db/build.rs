extern crate cpp_build;

fn main() {
    let include_path = "lib";
    cpp_build::Config::new().include(include_path).build("src/main.rs");
}
