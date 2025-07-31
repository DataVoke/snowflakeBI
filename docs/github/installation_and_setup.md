# 🧰 Installing Git on Windows, macOS, and Linux

This guide walks you through installing **Git**, the version control system used in this project, across all major platforms.

---

## 🪟 Windows

### 📦 Installation

1. Download the installer from:  
   👉 [https://git-scm.com/download/win](https://git-scm.com/download/win)
2. Run the installer and follow the default setup
3. During setup:
   - Leave default options unless you have specific preferences
   - Ensure **Git Bash** is included

### ✅ Verify Installation

Open **Command Prompt** or **Git Bash** and run:

```bash
git --version
```

You should see something like:

```
git version 2.x.x.windows.x
```

---

## 🍎 macOS

### 📦 Installation via Homebrew (Recommended)

1. Install [Homebrew](https://brew.sh) if not already installed
2. Run:

```bash
brew install git
```

### 📦 Alternative: Xcode Developer Tools

You can also install Git by running:

```bash
xcode-select --install
```

This installs Git along with other developer tools.

### ✅ Verify Installation

```bash
git --version
```

Expected output:

```
git version 2.x.x
```

---

## 🐧 Linux (Ubuntu/Debian)

### 📦 Installation

```bash
sudo apt update
sudo apt install git -y
```

### ✅ Verify Installation

```bash
git --version
```

---

## 🛠️ Next Steps

Once Git is installed, configure your identity:

```bash
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
```

You can check the config with:

```bash
git config --list
```

---

## 🔗 Helpful Links

- [Git Downloads](https://git-scm.com/downloads)
- [Git Documentation](https://git-scm.com/doc)
- [GitHub Git Guide](https://docs.github.com/en/get-started/using-git)

- [version_control](version_control.md)
