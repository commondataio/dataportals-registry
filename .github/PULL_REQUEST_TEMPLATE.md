## Summary

<!-- What does this PR change and why? -->

## Change type

- [ ] New or updated catalog YAML(s)
- [ ] Data quality / fix script
- [ ] Build or validation pipeline
- [ ] Documentation only
- [ ] Other

## Checklist

- [ ] YAML `id` matches filename (lowercase, no special characters)
- [ ] Ran `python scripts/builder.py validate-yaml` (or validated affected file(s))
- [ ] If bulk quality fixes: ran `analyze-quality` and updated `dataquality/baseline_counts.json` when priority counts changed
- [ ] Tests pass locally (`python -m pytest`) for code changes

## Related issues

<!-- Fixes #123 -->
