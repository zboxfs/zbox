# Contributing to Zbox

Zbox welcomes contribution from everyone in the form of suggestions, bug
reports, pull requests, and feedback. This document gives some guidance if you
are thinking of helping us.

Please reach out here in a GitHub issue if we can do anything to help you
contribute.

## Submitting bug reports and feature requests

When reporting a bug or asking for help, please include enough details so that
the people helping you can reproduce the behavior you are seeing. For some tips
on how to approach this, read about how to produce a [Minimal, Complete, and
Verifiable example].

[Minimal, Complete, and Verifiable example]: https://stackoverflow.com/help/mcve

When making a feature request, please make it clear what problem you intend to
solve with the feature, any ideas for how Zbox could support solving that
problem, any possible alternatives, and any disadvantages.

## Formatting code

We are using [rustfmt](https://github.com/rust-lang-nursery/rustfmt) to format
source code. The Formatting rules are defined in `rustfmt.toml` file. Please
make sure you have run `cargo fmt` before submitting code.

You can also use some IDEs, such as Vim, which support `rustfmt` to
automatically format code while you're editing.

## Branching

Zbox has two main branches, both branches should always be compilable and
passed all the unit and integration tests before pushed to GitHub.

- master

  This branch contains latest development code, pull request should be merged
  to this branch.

- stable

  This branch always contains stable code and is mainly for releasing. Release
  tags are applied to this branch.

There are some other short-lifetime branches, such as release branch and bug
fix branch. Those branches should based on `master` branch, and will be
eventually merged back to `master`. Those branches should also regularly use
`rebase` to sync latest commits from `master`.

## Debugging

Zbox uses [env_logger](https://crates.io/crates/env_logger) to output debug
information. You can use the below environment variable to enable debug log
output.

```bash
export RUST_LOG=zbox=debug
```

Also, this `RUST_BACKTRACE` variable could be helpful when debugging.

```bash
export RUST_BACKTRACE=full
```

## Running the test suite

We encourage you to check that the test suite passes locally before submitting a
pull request with your changes. If anything does not pass, typically it will be
easier to iterate and fix it locally than waiting for the CI servers to run
tests for you.

In the `zbox` directory, you can run different test suites. To see more
details, please check [cargo manual](http://doc.crates.io/guide.html).

### Run unit test suite

```bash
cargo test --lib
```

### Run integration test suite

```bash
cargo test --tests
```

### Run documentation test suite

```bash
cargo test --doc
```

### Run fuzz test

Zbox contains fuzz test tools, which are located in `src/bin` directory. To run
those tests, you can run below commands from `zbox` directory.

Run fuzz test for file system.

```bash
cargo run --bin fuzz_fs
```

Run fuzz test for file read and write

```bash
cargo run --bin fuzz_file
```

Run fuzz test for directory.

```bash
cargo run --bin fuzz_dir
```

## Code of Conduct

In all Zbox-related forums, we follow the [Code of Conduct](CODE_OF_CONDUCT.md).
For escalation or moderation issues please contact Bo (support@zbox.io).

