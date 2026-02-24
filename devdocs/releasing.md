# Publishing a release

## 1. Update CHANGELOG and README

- In `CHANGELOG.md`: add a new `## [X.Y.Z] - YYYY-MM-DD` section under `[Unreleased]` with Added/Changed/Fixed/Removed entries.
- Add a **GitHub Release** link line: `**GitHub Release**: [vX.Y.Z](https://github.com/commondataio/dataportals-registry/releases/tag/vX.Y.Z) - Published Month DD, YYYY`
- In `README.md`: update the "Latest snapshot" date and dataset counts under **Data exports** to match the current build.

## 2. Create the release notes (optional)

- Copy or adapt `.github/RELEASE_NOTES_vX.Y.Z.md` for the new version, or use the CHANGELOG section as the release description.

## 3. Commit, tag, and push

```bash
git add CHANGELOG.md README.md AGENTS.md .github/RELEASE_NOTES_v*.md devdocs/releasing.md
git commit -m "Release vX.Y.Z"
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin main
git push origin vX.Y.Z
```

## 4. Create the GitHub release

1. Go to [Releases](https://github.com/commondataio/dataportals-registry/releases).
2. Click **Draft a new release**.
3. Choose tag `vX.Y.Z`.
4. Set title to `vX.Y.Z` (e.g. `v1.6.0`).
5. Paste the release notes from `.github/RELEASE_NOTES_vX.Y.Z.md` or from the CHANGELOG section.
6. Publish the release.
