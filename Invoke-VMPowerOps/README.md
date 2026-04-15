# Invoke-VMPowerOps

**VM Power Operations Manager - vCenter REST API Power Control**

A PowerShell script for enterprise-grade VM power operations against VMware vCenter using the REST API directly - no PowerCLI dependency required. Supports concurrent execution, graceful shutdown escalation, dry-run simulation, VLR/SRM placeholder detection, Linked Mode awareness, and detailed structured logging.

**Author:** Don Horrox - [vchamp.net](https://vchamp.net)  
**Version:** 1.0.0  

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Quick Start](#quick-start)
- [Parameters](#parameters)
- [Operations](#operations)
- [Input File Format](#input-file-format)
- [Concurrency Control](#concurrency-control)
- [Power-Down Escalation Sequence](#power-down-escalation-sequence)
- [Result Output Formats](#result-output-formats)
- [Dry-Run Mode](#dry-run-mode)
- [Logging](#logging)
- [VLR / SRM Placeholder Detection](#vlr--srm-placeholder-detection)
- [Linked Mode Support](#linked-mode-support)
- [vCenter Permissions](#vcenter-permissions)
- [Tunable Constants](#tunable-constants)
- [Examples](#examples)
- [Notes](#notes)

---

## Features

- **No PowerCLI required** - communicates directly with the vCenter REST API
- **Three operation modes** - Power-Down, Power-On, and Power-Cycle
- **Graceful shutdown escalation** - Guest OS Shutdown → Power Off → Hard Stop
- **Concurrent operations** - configurable global, per-host, and per-datastore throttling
- **ETA tracking** - rolling average per-VM duration displayed in the progress bar
- **Dry-run simulation** - full walkthrough without executing any changes
- **VLR / SRM placeholder detection** - automatically skips recovery-site placeholder VMs
- **Linked Mode awareness** - scopes operations to the target vCenter only
- **DRS migration detection** - re-validates VM-to-host placement between power-cycle phases to prevent boot storms
- **Structured logging** - timestamped log file written to the script directory
- **Multiple result output formats** - inline Table, CSV, plain Text, or interactive GridView window
- **Self-signed certificate support** - automatic SSL bypass fallback for lab and self-signed environments

---

## Requirements

| Requirement | Minimum |
|---|---|
| PowerShell | 5.1 (Windows) or 7.4+ (Windows / Linux) |
| vCenter Server | 7.0, 8.0, or 9.0 |
| Network access | HTTPS (port 443) to vCenter |
| Permissions | See [vCenter Permissions](#vcenter-permissions) |

---

## Quick Start

1. Download `Invoke-VMPowerOps.ps1` to a working directory.
2. Create an input file listing VM display names, one per line (see [Input File Format](#input-file-format)).
3. Run the script:

```powershell
.\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.csv -pc -r Table
```

The script will prompt for vCenter credentials and confirm scope before performing any operations.

---

## Parameters

### Required

| Parameter | Alias | Description |
|---|---|---|
| `-VCenterServer` | `-vc` | FQDN or IP address of the target vCenter server |
| `-SourceFile` | `-s` | Path to a CSV or TXT file containing VM display names |

Both parameters can also be entered interactively if not provided on the command line.

### Operation (specify exactly one)

| Parameter | Alias | Description |
|---|---|---|
| `-PowerDown` | `-pd` | Shut down all scoped VMs |
| `-PowerOn` | `-po` | Power on all scoped VMs |
| `-PowerCycle` | `-pc` | Shut down then power on all scoped VMs |

### Options

| Parameter | Alias | Default | Description |
|---|---|---|---|
| `-DryRun` | `-d` | Off | Simulate all operations without making any changes |
| `-ForceOff` | `-f`, `-forcereboot` | Off | Bypass Guest OS Shutdown; begin immediately at Power Off |
| `-VerboseLogging` | `-v` | Off | Display full timestamped log output on the terminal and write debug entries to the log file |
| `-ResultOutput` | `-r` | None | Export results: `Table`, `CSV`, `Text`, or `GridView` |
| `-Help` | `-h` | - | Display usage guidance and exit |

### Concurrency

| Parameter | Alias | Default | Range | Description |
|---|---|---|---|---|
| `-ConcurrentGlobal` | `-cg` | `0` (unlimited) | 0–∞ | Maximum simultaneous operations across all hosts and datastores |
| `-ConcurrentHost` | `-ch` | `5` | 1–10 | Maximum simultaneous operations per ESX host |
| `-ConcurrentDatastore` | `-cd` | `5` | 1–10 | Maximum simultaneous operations per datastore |

---

## Operations

### Power-Down (`-pd`)

Shuts down all scoped VMs using the [escalation sequence](#power-down-escalation-sequence). Already powered-off VMs are skipped.

### Power-On (`-po`)

Powers on all scoped VMs. Already powered-on VMs are skipped.

### Power-Cycle (`-pc`)

Performs a power-down followed by a power-on. Between the two phases:

1. The script reports results from the power-down phase.
2. The user is prompted to confirm before proceeding to power-on.
3. VM-to-host placement is re-validated via a live vCenter query to detect any DRS migrations that occurred while the VMs were powered off. Any detected migrations are logged and the updated placement data is used for power-on concurrency limiting.
4. An inter-phase delay runs for any remaining time in the boot-storm prevention window (default: 15 seconds from the moment the last VM powered off).

---

## Input File Format

The input file may be a plain `.txt` or `.csv` file containing one VM display name per line. Column headers are automatically detected and skipped. Supported header values: `vmname`, `vm_name`, `name`, `vm`, `hostname`, `host_name`, `displayname`. CSV-style quoting is stripped automatically.

**Example - plain text:**
```
web-server-01
db-server-02
app-server-03
```

**Example - CSV with header:**
```csv
vmname
web-server-01
db-server-02
app-server-03
```

> **Note:** VM names are case-sensitive in some vCenter versions. Ensure names match the display name exactly as shown in the vCenter inventory.

---

## Concurrency Control

The script enforces three independent throttling gates simultaneously. A VM operation will not start unless **all three** gates pass:

| Gate | Parameter | Default |
|---|---|---|
| Global | `-cg` | Unlimited (0) |
| Per ESX Host | `-ch` | 5 |
| Per Datastore | `-cd` | 5 |

The engine uses a single polling loop (no background jobs) so it is compatible with both PowerShell 5.1 and 7+. As each VM reaches a terminal state, the freed slot is immediately offered to the next pending VM - subject to the gates above.

**Choosing limits:** Start conservative (e.g., `-ch 3 -cd 3`) for environments with limited storage I/O headroom. Increase for environments with fast NVMe or vSAN-backed datastores. The global gate (`-cg`) is useful when you want to cap total API request rate regardless of host/datastore distribution.

---

## Power-Down Escalation Sequence

Each VM is processed independently through up to three steps:

```
Step 1 - Guest OS Shutdown
    ├── Skipped if VMware Tools is NOT running or not installed
    ├── Skipped entirely if -ForceOff (-f) is specified
    └── Timeout: 300 seconds (configurable via $TIMEOUT_GUEST_SHUTDOWN)

Step 2 - Power Off
    ├── Issued if Step 1 timed out or was skipped
    └── Timeout: 120 seconds (configurable via $TIMEOUT_POWER_OFF)

Step 3 - Hard Stop
    ├── Final Power Off attempt if Step 2 timed out
    └── Timeout: 120 seconds (configurable via $TIMEOUT_POWER_OFF)

Failure
    └── VM is marked as Error; manual intervention required
```

Command-send failures (not timeouts) trigger automatic retry, up to 2 attempts with a 30-second delay between each.

---

## Result Output Formats

Specify an output format with `-r` / `-ResultOutput`. Not available in dry-run mode.

| Format | Description |
|---|---|
| `Table` | Formatted table printed inline in the terminal after the operation completes |
| `CSV` | Comma-separated file written to the script directory (`VMPowerOps_<timestamp>.csv`) |
| `Text` | Plain-text formatted table written to the script directory (`VMPowerOps_<timestamp>.txt`) |
| `GridView` | Interactive, sortable, and filterable grid opened in a separate PowerShell window (Windows only) |

**Result table columns:**

| Column | Description |
|---|---|
| VM Name | Display name of the VM |
| Activity | Operation performed (`Power Down` or `Power On`) |
| Status | `Complete`, `Skipped`, or `Error` |
| ESX Host | Parent ESX host at operation time |
| Datastore | Parent datastore resolved from the VM's primary disk backing |
| Completed At | Timestamp the VM reached its terminal state (`MM/dd/yyyy HH:mm:ss`) |

---

## Dry-Run Mode

Invoke with `-d` / `-DryRun` to simulate an operation without making any changes to vCenter. In this mode:

- All authentication and VM resolution steps run normally against a live vCenter
- Power commands are not sent
- Each VM prints a `[DRY-RUN]` message describing what would have been done, including the VM name, VM ID, and position in the queue
- Poll-interval sleeps are skipped so the simulation completes instantly
- The inter-phase delay is announced but not waited
- The power-cycle confirmation prompt is bypassed with a note
- Placement re-validation (DRS migration check) is skipped with a note
- Result output (`-r`) is automatically suppressed with a warning
- The summary box shows **Operations Planned** instead of Operations Run and omits the Succeeded / Skipped / Failed breakdown

Dry-run is recommended before any first production use of a new VM list or concurrency configuration.

---

## Logging

Every execution writes a timestamped log file to the script directory:

```
VMPowerOps_20260412_130547.log
```

Log entries follow the format:

```
[2026-04-12 13:05:47] [INFO ] Connected to vcenter.corp.local as 'svc-powerops'
[2026-04-12 13:05:52] [OK   ] [1/10] web-server-01: Power-Down complete.
[2026-04-12 13:06:14] [WARN ] [3/10] db-server-02: Guest OS Shutdown timed out after 300s. Issuing Power Off.
[2026-04-12 13:06:16] [ERROR] [5/10] app-server-03: Hard Stop timed out. Marking as failed - manual intervention required.
```

**Log levels:**

| Level | Meaning |
|---|---|
| `INFO` | Normal progress and informational messages |
| `OK` | Successful operation completion |
| `WARN` | Non-fatal issues (timeouts, skipped VMs, DRS migrations detected) |
| `ERROR` | Operation failures requiring attention |
| `DEBUG` | Detailed diagnostic output - only written when `-VerboseLogging` (`-v`) is active |

By default, DEBUG entries are suppressed from both the terminal and the log file. Enable verbose mode (`-v`) to include them.

---

## VLR / SRM Placeholder Detection

The script automatically identifies and skips VMware Live Recovery (VLR) and Site Recovery Manager (SRM) placeholder VMs at the recovery site using three REST API heuristics:

1. **Folder name pattern match** - the VM's inventory folder name is checked against a configurable list of substrings. Default patterns: `vCDR`, `SRM`, `Site Recovery`, `LiveRecovery`, `DR_Placeholder`.
2. **Zero registered disks** - placeholder VMs are commonly registered with no disk backing prior to a test or actual recovery.
3. **Disks present, zero total capacity** - some configurations register VMDK entries with zero allocated bytes.

Skipped placeholder VMs are listed in the terminal output and recorded in the summary.

### Customising detection patterns

Edit `$SRM_FOLDER_PATTERNS` near the top of the script:

```powershell
$SRM_FOLDER_PATTERNS = @('vCDR', 'SRM', 'Site Recovery', 'LiveRecovery', 'DR_Placeholder', 'MyCustomFolder')
```

> **Note:** For authoritative detection, the SOAP API `VirtualMachine.config.managedBy.extensionKey` is the definitive source. SRM uses `com.vmware.vcDr`; VLR uses `com.vmware.liverecover`. If heuristic detection is insufficient in your environment, the `Test-IsSRMPlaceholder` function in the script can be extended accordingly.

---

## Linked Mode Support

When vCenter is participating in Enhanced Linked Mode, inventory searches may return VMs from all linked vCenter servers. To prevent unintended cross-vCenter operations, the script:

1. Enumerates all ESX hosts registered to the **target** vCenter at startup.
2. Builds a reverse map of VM-to-host relationships from the per-host VM lists.
3. Skips any VM whose resolved parent host does not belong to the target vCenter.

VMs skipped for this reason are listed on the terminal and counted separately in the summary. Operations are strictly scoped to the vCenter specified with `-vc`.

---

## vCenter Permissions

Create a dedicated service account and custom role. Assign the role at the **vCenter Server** level with **Propagate to Children** enabled. For tighter scope, assign at the individual Host or VM Folder level instead.

### Steps to configure

1. Navigate to **Administration > Access Control > Roles** in vCenter.
2. Clone the built-in **Read-Only** role and name it (e.g., `VM Power Ops`).
3. Add the privileges below to the cloned role.
4. Assign the role to the service account at the **vCenter Server** level with **Propagate to children** checked.

### Required privileges

| Privilege Category | Privilege |
|---|---|
| Virtual Machine > Interaction | Power On |
| Virtual Machine > Interaction | Power Off |
| Virtual Machine > Change Configuration > Change Settings | Change Settings |
| Datastore | Browse Datastore |
| Sessions | Validate session |
| Global > Licenses | *(no sub-privilege required)* |

> The Read-Only role covers most inventory traversal needs. The VM Interaction and Change Settings privileges are the only additions required on top of it.

---

## Tunable Constants

The following constants are defined near the top of the script and can be adjusted for your environment without modifying any logic:

| Constant | Default | Description |
|---|---|---|
| `$TIMEOUT_GUEST_SHUTDOWN` | `300` | Seconds to wait for Guest OS Shutdown before escalating to Power Off |
| `$TIMEOUT_POWER_OFF` | `120` | Seconds to wait for Power Off or Hard Stop before declaring failure |
| `$TIMEOUT_POWERON` | `180` | Seconds to wait for power-on confirmation |
| `$POLL_INTERVAL` | `10` | Seconds between power state polls during active operations |
| `$RETRY_DELAY` | `30` | Seconds to wait before retrying a failed command send |
| `$MAX_RETRIES` | `2` | Maximum command-send retries per VM before marking as failed |
| `$INTER_PHASE_DELAY` | `15` | Desired pause (seconds) between power-down and power-on phases in a power-cycle |
| `$SRM_FOLDER_PATTERNS` | See script | Folder name substrings used for VLR/SRM placeholder detection |

---

## Examples

### Power-cycle with per-host limit and inline table output

```powershell
.\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.csv -pc -r Table -ch 3
```

### Power-down with global concurrency cap and CSV output, verbose logging

```powershell
.\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.txt -pd -r CSV -cg 10 -v
```

### Force power-off (bypass Guest OS Shutdown)

```powershell
.\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.csv -pd -f
```

### Dry-run power-on simulation

```powershell
.\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.csv -po -d
```

### Power-cycle with GridView results and tight concurrency

```powershell
.\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.csv -pc -r GridView -ch 2 -cd 2
```

### Display built-in help

```powershell
.\Invoke-VMPowerOps.ps1 -h
```

---

## Notes

- VM names in the input file are matched against vCenter display names. The match is case-sensitive in some vCenter versions - ensure names are exact.
- Input file headers (`vmname`, `name`, `vm`, etc.) are automatically detected and skipped.
- SSL certificate validation is attempted first. If it fails (common with self-signed certificates), the script automatically retries with certificate validation disabled and logs a warning.
- All timestamps in log files and result exports use the format `MM/dd/yyyy HH:mm:ss` for consistent rendering across locales and spreadsheet applications.
- The GridView output format (`-r GridView`) requires Windows PowerShell or PowerShell 7+ on Windows. It is not available on Linux or macOS.
- The `-ForceOff` flag (`-f`) has no effect when combined with `-PowerOn` and is silently ignored if specified together.
- The script creates one log file per execution in the script directory. Log files are not automatically rotated or cleaned up.
