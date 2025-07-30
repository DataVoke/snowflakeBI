# 📦 Installing dbt (Data Build Tool) with Snowflake

This guide walks you through installing **dbt Core** with the **Snowflake adapter** on **Windows**, **Linux**, or **macOS**, including Python setup best practices.

---

## ⚙️ Prerequisites

- [Python (3.8–3.11 recommended)](https://www.python.org/)
- [pip](https://pip.pypa.io/)

> ✅ **Recommended:**  
> Use [**pyenv**](https://github.com/pyenv/pyenv) to manage multiple Python versions.  
> Use a [**virtual environment**](https://docs.python.org/3/tutorial/venv.html) (`venv` or `virtualenv`) to isolate your project environment.

---

## 🪟 Windows

1. **Install Python**  
   Download from [python.org](https://www.python.org/downloads/windows/)  
   During installation, check: ✅ *Add Python to PATH*

2. **Install dbt with Snowflake adapter**
   ```powershell
   pip install dbt-snowflake
   ```

3. **Verify Installation**
   ```powershell
   dbt --version
   ```

---

## 🐧 Linux (Ubuntu/Debian)

1. **Install Python and pip**
   ```bash
   sudo apt update
   sudo apt install -y python3 python3-pip
   ```

2. **Install dbt with Snowflake adapter**
   ```bash
   pip install dbt-snowflake
   ```

3. **Verify Installation**
   ```bash
   dbt --version
   ```

---

## 🍎 macOS

1. **Install Python**  
   Install via [Homebrew](https://brew.sh) or [python.org](https://www.python.org/downloads/macos/)

2. **Install dbt with Snowflake adapter**
   ```bash
   pip install dbt-snowflake
   ```

3. **Verify Installation**
   ```bash
   dbt --version
   ```

---

## 🔐 Snowflake Authentication

By default, dbt connects to Snowflake using **username/password login**, which may prompt for **browser-based multi-factor authentication (MFA)** if the user has 2FA enabled.

### 🧪 Development Use Case

To avoid 2FA prompts during development or in CI environments, it's recommended to create a **dedicated service user** with **key-pair authentication**.

### ✅ Key-Pair Authentication Steps

1. **Generate an RSA private key** and store it securely (e.g., `~/.ssh/snowflake_key.pem`)
2. Upload the public key to the Snowflake user
3. In your `profiles.yml`, set:

```yaml
private_key_path: /path/to/snowflake_key.pem
```

This allows fully automated access **without needing a browser prompt** or 2FA.

This is especially important when running `dbt` inside CI/CD pipelines or scheduled jobs.

---

## 🔗 Helpful Links

- [dbt Core Installation Docs](https://docs.getdbt.com/docs/core/installation)
- [dbt Snowflake Adapter Docs](https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile)
- [pyenv GitHub](https://github.com/pyenv/pyenv)
- [Python Virtual Environments](https://docs.python.org/3/tutorial/venv.html)

- [development_process](development_process.md)
