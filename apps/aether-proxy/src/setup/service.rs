//! Systemd service installation for aether-proxy.
//!
//! Called from the setup TUI when the user enables "Install Service".
//! The unit file points to the binary and config at their current
//! absolute paths -- no files are copied.

use std::path::Path;
use std::process::Command;

const UNIT_PATH: &str = "/etc/systemd/system/aether-proxy.service";
const SERVICE_NAME: &str = "aether-proxy";

/// Whether systemd service installation is possible (systemd present + root).
pub fn is_available() -> bool {
    is_systemd_available() && is_root()
}

/// Install aether-proxy as a systemd service.  Must be run as root.
pub fn install_service(config_path: &Path) -> anyhow::Result<()> {
    if !is_systemd_available() {
        anyhow::bail!("systemd not available");
    }
    if !is_root() {
        anyhow::bail!("root required, use: sudo ./aether-proxy setup");
    }

    let exe_path = std::env::current_exe()?.canonicalize()?;
    let exe_str = exe_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("binary path contains invalid UTF-8"))?;

    let config_abs = std::fs::canonicalize(config_path)?;
    let config_str = config_abs
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("config path contains invalid UTF-8"))?;

    let working_dir = config_abs
        .parent()
        .unwrap_or_else(|| Path::new("/"))
        .to_str()
        .unwrap_or("/");

    // Stop existing service if running (ignore errors)
    if Path::new(UNIT_PATH).exists() {
        eprintln!("  Stopping existing service...");
        let _ = Command::new("systemctl")
            .args(["stop", SERVICE_NAME])
            .status();
    }

    // Write unit file
    eprintln!("  Generating systemd unit file...");
    eprintln!("    Binary:  {}", exe_str);
    eprintln!("    Config:  {}", config_str);
    eprintln!("    WorkDir: {}", working_dir);

    let unit_content = format!(
        "[Unit]\n\
         Description=Aether Proxy\n\
         After=network.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         WorkingDirectory={working_dir}\n\
         Environment=AETHER_PROXY_CONFIG={config_str}\n\
         Environment=AETHER_PROXY_LOG_DESTINATION=both\n\
         Environment=AETHER_PROXY_LOG_DIR=/var/log/aether-proxy\n\
         ExecStart={exe_str}\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         LimitNOFILE=65535\n\
         UMask=0077\n\
         LogsDirectory=aether-proxy\n\
         LogsDirectoryMode=0750\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n",
    );
    std::fs::write(UNIT_PATH, &unit_content)?;

    // Reload and enable
    eprintln!("  Enabling and starting service...");
    run_cmd("systemctl", &["daemon-reload"])?;
    run_cmd("systemctl", &["enable", "--now", SERVICE_NAME])?;

    // Verify
    eprintln!();
    let output = Command::new("systemctl")
        .args(["is-active", SERVICE_NAME])
        .output()?;
    let state = String::from_utf8_lossy(&output.stdout).trim().to_string();

    if state == "active" {
        eprintln!("  Service started successfully!");
    } else {
        eprintln!("  Service state: {} (check logs)", state);
    }

    eprintln!();
    eprintln!("  Commands:");
    eprintln!("    ./aether-proxy status          # service status");
    eprintln!("    ./aether-proxy logs            # tail logs");
    eprintln!("    sudo ./aether-proxy restart    # restart");
    eprintln!("    sudo ./aether-proxy stop       # stop");
    eprintln!("    sudo ./aether-proxy uninstall  # remove service");
    eprintln!();

    Ok(())
}

fn is_systemd_available() -> bool {
    Command::new("systemctl")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

pub(crate) fn is_root() -> bool {
    #[cfg(unix)]
    {
        unsafe { libc::geteuid() == 0 }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

/// Whether a systemd unit file is currently installed.
pub fn is_installed() -> bool {
    Path::new(UNIT_PATH).exists()
}

/// Remove the systemd service (called from setup TUI when Install Service is toggled off).
pub fn uninstall_service() -> anyhow::Result<()> {
    if !Path::new(UNIT_PATH).exists() {
        return Ok(());
    }

    eprintln!("  Stopping and removing existing service...");
    let _ = Command::new("systemctl")
        .args(["disable", "--now", SERVICE_NAME])
        .status();

    std::fs::remove_file(UNIT_PATH)?;
    eprintln!("  Removed {}", UNIT_PATH);
    run_cmd("systemctl", &["daemon-reload"])?;
    eprintln!("  Service uninstalled.");
    eprintln!();

    Ok(())
}

/// Check if the systemd service is currently active.
pub fn is_service_active() -> bool {
    std::path::Path::new(UNIT_PATH).exists()
        && Command::new("systemctl")
            .args(["is-active", "--quiet", SERVICE_NAME])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
}

// ── CLI subcommands (systemd wrappers) ──────────────────────────────────────

fn ensure_service_installed() -> anyhow::Result<()> {
    if !std::path::Path::new(UNIT_PATH).exists() {
        anyhow::bail!("service not installed, run `sudo ./aether-proxy setup` first");
    }
    Ok(())
}

fn ensure_root_and_service() -> anyhow::Result<()> {
    ensure_service_installed()?;
    if !is_root() {
        anyhow::bail!("root required, use: sudo ./aether-proxy <command>");
    }
    Ok(())
}

/// `aether-proxy status` -- show service status.
pub fn cmd_status() -> anyhow::Result<()> {
    ensure_service_installed()?;
    let status = Command::new("systemctl")
        .args(["status", SERVICE_NAME])
        .status()?;
    // systemctl status returns non-zero when inactive; that's fine
    std::process::exit(status.code().unwrap_or(1));
}

/// `aether-proxy logs` -- tail service logs.
pub fn cmd_logs() -> anyhow::Result<()> {
    ensure_service_installed()?;
    let status = Command::new("journalctl")
        .args(["-u", SERVICE_NAME, "-f", "--no-pager", "-n", "100"])
        .status()?;
    std::process::exit(status.code().unwrap_or(1));
}

/// `aether-proxy start` -- start the service.
pub fn cmd_start() -> anyhow::Result<()> {
    ensure_root_and_service()?;
    run_cmd("systemctl", &["start", SERVICE_NAME])?;
    eprintln!("  Service started.");
    Ok(())
}

/// `aether-proxy restart` -- restart the service.
pub fn cmd_restart() -> anyhow::Result<()> {
    ensure_root_and_service()?;
    run_cmd("systemctl", &["restart", SERVICE_NAME])?;
    eprintln!("  Service restarted.");
    Ok(())
}

/// `aether-proxy stop` -- stop the service.
pub fn cmd_stop() -> anyhow::Result<()> {
    ensure_root_and_service()?;
    run_cmd("systemctl", &["stop", SERVICE_NAME])?;
    eprintln!("  Service stopped.");
    Ok(())
}

/// `aether-proxy uninstall` -- disable and remove the systemd service.
pub fn cmd_uninstall() -> anyhow::Result<()> {
    ensure_root_and_service()?;

    eprintln!("  Stopping and disabling service...");
    let _ = Command::new("systemctl")
        .args(["disable", "--now", SERVICE_NAME])
        .status();

    if std::path::Path::new(UNIT_PATH).exists() {
        std::fs::remove_file(UNIT_PATH)?;
        eprintln!("  Removed {}", UNIT_PATH);
    }

    run_cmd("systemctl", &["daemon-reload"])?;
    eprintln!("  Service uninstalled.");
    eprintln!();
    eprintln!("  Config file and TLS certs are preserved. Remove manually if needed.");

    Ok(())
}

pub(crate) fn run_cmd(program: &str, args: &[&str]) -> anyhow::Result<()> {
    let display = format!("{} {}", program, args.join(" "));
    eprintln!("  > {}", display);

    let status = Command::new(program).args(args).status()?;
    if !status.success() {
        anyhow::bail!("command failed: {}", display);
    }
    Ok(())
}
