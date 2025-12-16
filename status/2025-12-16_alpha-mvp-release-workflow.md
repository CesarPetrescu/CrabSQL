# 2025-12-16: alpha-mvp-release-workflow

## Meta
- Date (UTC): 2025-12-16 16:08
- Agent: GPT-5.2 (Codex CLI)
- Goal: Add an `alpha-mvp` GitHub release workflow with Linux/Windows/macOS binaries.

## Changes
- Add GitHub Actions workflow to build + publish prerelease assets for Linux/Windows/macOS on `alpha-mvp*` tags (`.github/workflows/release.yml`).
- Update roadmap to track release automation (`TODO.md`).

## TODO.md Updates
- Touched: "0.1) Repo Workflow (Agents)" → "GitHub Actions release workflow builds `alpha-mvp` binaries (Linux/Windows/macOS) and publishes a prerelease."

## Verification
- `cargo test --all --all-features`

## Notes / Follow-ups
- The release workflow is tag-driven (`alpha-mvp*`). Pushing `alpha-mvp` will trigger the build and publish release assets.
