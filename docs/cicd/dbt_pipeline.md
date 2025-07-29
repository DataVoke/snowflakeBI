# 🚀 CI/CD: dbt Build Workflows (Development & Production)

This project uses GitHub Actions to automate `dbt build` for both **development** and **production** environments.

Each environment has a dedicated workflow file under `.github/workflows/`, triggered **manually** or **on a schedule**.

---

## 📂 Workflow Files

```
.github/workflows/
├── dbt_build_dev.yml   # Development environment
└── dbt_build_prd.yml   # Production environment
```

---

## 🧪 Development Build (`dbt_build_dev.yml`)

### 🔁 Triggers

- **Scheduled run:** Daily at **09:00 UTC** (**04:00 AM EST**)
- **Manual run:** Via GitHub Actions UI (`workflow_dispatch`)

### ⚠️ Branch Condition

The workflow **only runs if the current branch is `development`**, even if manually triggered or scheduled.

```yaml
if: github.ref == 'refs/heads/development'
```

### 🔧 Configuration

- Target profile: `dev`
- Working directory: `cai_bi`
- dbt command:  
  ```bash
  dbt build --profiles-dir ./profiles --target dev
  ```

---

## ✅ Production Build (`dbt_build_prd.yml`)

### 🔁 Triggers

- **Scheduled run:** Daily at **09:00 UTC** (**04:00 AM EST**)
- **Manual run:** Via GitHub Actions UI (`workflow_dispatch`)

### ⚠️ Branch Condition

The workflow **only runs if the current branch is `main`**:

```yaml
if: github.ref == 'refs/heads/main'
```

### 🔧 Configuration

- Target profile: `prd`
- Working directory: `cai_bi`
- dbt command:  
  ```bash
  dbt build --profiles-dir ./profiles --target prd
  ```

---

## 🔐 Secrets & Authentication

Both workflows rely on GitHub Secrets:

- `SNOWFLAKE_PK_BASE64` – Base64-encoded Snowflake private key
- `NOTIFICATION_MAIL_USERNAME` / `NOTIFICATION_MAIL_PASSWORD` – For email alerts

The private key is decoded and stored at:

```
~/.ssh/snowflake_key.pem
```

---

## 📧 Failure Notifications

If the `dbt build` fails:

- An email is sent to:
  - `itsupport@cagents.com`
  - `evan.gutzwiller@cagents.com`

The email includes:
- Workflow name
- Branch and commit SHA
- GitHub Actions run URL

---

## 🔗 Related Files

- [`profiles.yml`](../../cai_bi/profiles/profiles.yml) – Snowflake profile config for `dev` and `prd`
- [`dbt_project.yml`](../../cai_bi/dbt_project.yml) – Project configuration
- [`development_process.md`](../dbt/development_process.md) – dbt development process and workflow
- [`installation_and_setup.md`](../dbt/installation_and_setup.md) – dbt installation and setup

