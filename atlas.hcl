// Atlas is an external deployment tool. Target database URLs and credentials are supplied by
// the caller and are never owned by a Rust crate or committed here.
env "local" {
  dev = "docker://postgres/18/dev?search_path=public"

  migration {
    dir = "file://migrations"
  }
}
