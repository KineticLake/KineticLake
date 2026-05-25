# GitHub Actions Workflows Reference

This directory contains the CI/CD pipeline workflows for the KineticLake project.

## Files

### 1. `ci.yml` - Continuous Integration
Runs on every push and pull request to validate code quality and tests.

**When it runs:**
- Push to `main` or `develop` branch
- Pull request targeting `main` or `develop`

**What it does:**
- Runs tests on Python 3.10, 3.11, 3.12
- Lints code with Ruff
- Checks code formatting
- Generates coverage reports
- Uploads artifacts

**Key metrics:**
- Test results per Python version
- Linting violations
- Code coverage percentage

### 2. `cd.yml` - Continuous Deployment
Deploys validated code to Databricks environments.

**When it runs:**
- Automatically on successful push to `main` → deploys to `dev`
- Manual trigger via workflow dispatch → deploystaging or `prod`

**Deployment sequence:**
1. Deploy to `dev` (automatic, no approval needed)
2. Deploy to `staging` (after dev succeeds, requires approval)
3. Deploy to `prod` (manual trigger, requires approval)

**Safety features:**
- Environment-based secrets
- Required approvals for staging/prod
- Deployment tags for tracking
- Backup creation before prod deployment

### 3. `test-pr.yml` - Pull Request Testing
Detailed test reporting for pull requests.

**When it runs:**
- Pull request opened/updated to `main` or `develop`

**What it does:**
- Runs full test suite
- Generates coverage report
- Comments on PR with results
- Integrates with Codecov

**PR feedback:**
- Test summary comment
- Coverage metrics
- Links to detailed results

## Secrets Configuration

Required GitHub repository secrets:

### Development
```
DATABRICKS_HOST    - Dev workspace URL
DATABRICKS_TOKEN   - Dev PAT token
```

### Production
```
DATABRICKS_HOST_PROD    - Prod workspace URL  
DATABRICKS_TOKEN_PROD   - Prod PAT token
```

## Environment Protection Rules

Configure in **Settings > Environments**:

### `dev`
- No protection (auto-deploys)

### `staging`
- Required reviewers: 1
- Deployment branch: main only

### `prod`
- Required reviewers: 2
- Deployment branch: main only
- Restrict to manual trigger

## Viewing Results

### GitHub Actions Tab
Navigate to **Actions** to see:
- Workflow runs
- Job status
- Log output
- Artifacts

### Pull Request Checks
PR checks appear in the merge box:
- ✅ All checks passed
- ❌ Some checks failed
- ⏳ Checks running

### Artifacts
Saved after each run:
- `test-results-*.xml` - JUnit test reports
- Codecov coverage data

## Monitoring

### Success Indicators
- All workflows passing on main
- Coverage maintaining or increasing
- Zero lint violations

### Common Issues
- **Python version mismatch** - Ensure local Python matches CI version
- **Missing secrets** - Check GitHub repository secrets are configured
- **Token expired** - Refresh Databricks PAT tokens before expiration
- **Approval delays** - Set up environment reviewers in advance

## Customization

### Adding new workflows
1. Create `name-of-workflow.yml` in this directory
2. Define triggers and jobs
3. Commit and push to main
4. GitHub will enable workflow automatically

### Modifying existing workflows
1. Edit the `.yml` file
2. Use GitHub UI to preview or test
3. Create PR for review
4. Merge to enable changes

### Disabling a workflow
Prepend `DISABLED_` to filename (e.g., `DISABLED_ci.yml`)

## Best Practices

1. **Keep workflows simple** - Complex logic belongs in scripts
2. **Use caching** - Cache pip/uv dependencies for speed
3. **Fail fast** - Stop on first error, don't continue
4. **Clear naming** - Job/step names should be self-documenting
5. **Proper secrets** - Never hardcode credentials
6. **Document triggers** - Clear when workflows run

## References

- [GitHub Actions Docs](https://docs.github.com/en/actions)
- [Databricks Bundle Docs](https://docs.databricks.com/en/dev-tools/bundles)
- [Workflow Syntax](https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions)
