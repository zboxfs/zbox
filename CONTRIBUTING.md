# Contributing to ZboxFS

ZboxFS welcomes contribution from everyone in the form of suggestions, bug
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
solve with the feature, any ideas for how ZboxFS could support solving that
problem, any possible alternatives, and any disadvantages.

## Formatting code

We are using [rustfmt](https://github.com/rust-lang-nursery/rustfmt) to format
source code. The Formatting rules are defined in `rustfmt.toml` file. Please
make sure you have run `cargo fmt` before submitting code.

You can also use some IDEs, such as Vim, which support `rustfmt` to
automatically format code while you're editing.

## Branching

ZboxFS has two main branches, both branches should always be compilable and
passed all the unit and integration tests before pushed to GitHub.

- [master](https://github.com/zboxfs/zbox/tree/master)

  This branch contains latest development code, pull request should be merged
  to this branch.

- [stable](https://github.com/zboxfs/zbox/tree/stable)

  This branch always contains stable code and is mainly for releasing. Release
  tags are applied to this branch.

There are some other short-lifetime branches, such as release branch and bug
fix branch. Those branches should based on `master` branch, and will be
eventually merged back to `master`. Those branches should also regularly use
`rebase` to sync latest commits from `master`.

## Debugging

ZboxFS uses [env_logger](https://crates.io/crates/env_logger) to output debug
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

ZboxFS contains fuzz test, which is included in the integration test suite.
To save test time, the default number of fuzz test iteration is very low.
You can increase the number of batch and round by modifing the
[fuzz.rs](tests/fuzz.rs) file to perform intensive fuzz test.

The fuzz test will save test cases in `fuzz_test` folder under current
directory. Each fuzz test batch will be assigned a unique number which will be
shown on screen during the test. In case of failure, you can use that number to
reproduce the failed test case. Please check more details in the
[fuzz.rs](tests/fuzz.rs) file.

Run the fuzz test separately:

 ```bash
 cargo test --tests fuzz_test --features storage-file -- --nocapture
 ```

### Run random IO error test

For file system test, we need to simulate many IO error scenerios which is
hard because OS file system IO errors are very rare. To solve this problem,
ZboxFS uses a special storage `faulty` to simulate random IO errors. This storage
is based on memory storage, but can generate random IO error deterministically.
The generator can be switched on and off on the fly, and the error probability
is also adjustable.

Run random IO error test, we need to turn on `storage-faulty` feature:

```bash
cargo test --tests fuzz_test --features storage-faulty -- --nocapture
```

### Run performance test

To run performance test cases, we need to turn on the feature `test-perf`. And
the performance test should be run under `release` mode otherwise the result
will not be accurate.

Run performance test:

```bash
cargo test --tests perf_test --release --features test-perf -- --nocapture
```

## Code of Conduct

In all ZboxFS-related forums, we follow the [Code of Conduct](CODE_OF_CONDUCT.md).
For escalation or moderation issues please contact us (support@zbox.io).

