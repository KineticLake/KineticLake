# Quick Start Guide - CI/CD Setup

## 🚀 For First-Time Setup

### 1. Local Testing (5 minutes)

```bash
# Install dependencies
uv sync --all-extras

# Run all tests
uv run pytest tests/unit -v

# Run tests with coverage
uv run pytest tests/unit --cov=src --cov-report=term-missing
```

### 2. GitHub Repository Setup (10 minutes)

#### Add GitHub Secrets
1. Go to your repository on GitHub
2. Settings → Secrets and variables → Actions
3. Add these secrets:

```
DATABRICKS_HOST     = https://your-databricks-workspace.cloud.databricks.com
DATABRICKS_TOKEN    = dapi...your-personal-access-token...
```

#### Configure Environments (Optional but Recommended)
1. Settings → Environments
2. Create `dev`, `staging`, `prod` environments
3. For staging and prod:
   - Add required reviewers
   - Add environment-specific secrets

#### Protect Main Branch
1. Settings → Branches → Branch protection rules
2. Create rule for `main`:
   - ✅ Require pull request reviews (at least 1)
   - ✅ Require status checks to pass (select all CI jobs)
   - ✅ Require branches to be up to date before merging

### 3. Verify Setup (3 minutes)

1. Push a commit to a feature branch
2. Create a pull request to `main`
3. Check GitHub Actions tab - should see workflows running
4. Verify tests pass before merging

## 📊 Available Commands

### Running Tests

```bash
# Run all unit tests
uv run pytest tests/unit -v

# Run specific test file
uv run pytest tests/unit/test_bronze_layer.py -v

# Run with coverage
uv run pytest tests/unit --cov=src --cov-report=html

# Run only fast tests (exclude slow)
uv run pytest tests/unit -v -m "not slow"

# Run tests in parallel
uv run pytest tests/unit -v -n auto
```

### Code Quality

```bash
# Check code style
uv run ruff check src/ tests/

# Format code
uv run ruff format src/ tests/

# Run all checks
uv run ruff check src/ tests/ && uv run ruff format src/ tests/ --check
```

## 📝 Development Workflow

### Making Changes

```bash
# 1. Create feature branch
git checkout -b feature/my-feature

# 2. Make changes and run tests locally
uv run pytest tests/unit -v

# 3. Ensure code is formatted
uv run ruff format src/ tests/

# 4. Commit and push
git add .
git commit -m "Add my feature with tests"
git push origin feature/my-feature

# 5. Create pull request on GitHub
#    - GitHub Actions runs automatically
#    - Once passing, request review
#    - Merge after approval
```

### Adding New Tests

1. Create test file in `tests/unit/test_*.py`
2. Use markers: `@pytest.mark.unit` or `@pytest.mark.integration`
3. Follow naming: `test_*` functions
4. Run: `uv run pytest tests/unit/test_your_file.py -v`

## 🔍 Monitoring

### GitHub Actions Dashboard
- **Actions tab** - See all workflow runs
- **PR page** - Check status checks
- **Artifacts** - Download test results/coverage

### Codecov Integration
- Coverage reports uploaded automatically
- Badge in README shows coverage status
- Trends tracked over time

## 🐛 Common Issues

### Issue: "pytest: command not found"
```bash
# Solution: Install dependencies
uv sync --all-extras
```

### Issue: Tests fail in CI but pass locally
```bash
# Solution: Check Python version matches
python --version  # Should be 3.10+
uv sync --all-extras  # Reinstall deps
```

### Issue: "ModuleNotFoundError: No module named 'databricks'"
```bash
# Solution: For unit tests, no Databricks needed
# Unit tests should mock Spark
# For integration tests, install databricks-connect
uv run pip install databricks-connect
```

### Issue: Deployment fails with auth error
```
1. Check DATABRICKS_HOST is valid (no trailing slash)
2. Check DATABRICKS_TOKEN is valid (hasn't expired)
3. Verify token has workspace and jobs API permissions
```

## ✨ Next Steps

1. **Customize workflows** - Edit `.github/workflows/*.yml` as needed
2. **Add integration tests** - Create `tests/integration/` folder
3. **Monitor coverage** - Check Codecov badges
4. **Document failures** - Add issue templates if needed

## 📚 Documentation

- [Full CI/CD Setup Guide](../CI_CD_SETUP.md)
- [GitHub Actions Reference](README.md)
- [Pytest Documentation](https://docs.pytest.org)
- [Databricks Bundle Docs](https://docs.databricks.com/en/dev-tools/bundles)

## 🆘 Need Help?

1. Check test output: `uv run pytest -v --tb=long`
2. Check workflow logs: GitHub Actions → Workflow → Job logs
3. Review CI_CD_SETUP.md troubleshooting section
4. Check .github/workflows/README.md for workflow details
