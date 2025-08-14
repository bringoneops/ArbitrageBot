# Contributing

Thank you for investing your time in improving this project! This guide covers the coding style, required checks, and pull request process.

## Coding Style
- Follow idiomatic Rust style and keep the code formatted.
- Run `cargo fmt --all` before committing. CI may reject unformatted code.
- Keep the codebase warning-free by running Clippy and fixing any issues:
  ```bash
  cargo clippy --all-targets --all-features -- -D warnings
  ```

## Testing Requirements
- Ensure the entire workspace builds and tests successfully:
  ```bash
  cargo test
  ```
- Add tests for new features or bug fixes when possible.

## Pull Request Process
1. Fork the repository and create a feature branch off `main`.
2. Make your changes and run the formatting, linting, and test commands above.
3. Commit your work with clear messages and open a pull request.
4. Participate in code review. Address feedback and keep discussions constructive.

Consistent formatting, thorough tests, and thoughtful reviews help maintain a healthy codebase. We appreciate your contributions!
