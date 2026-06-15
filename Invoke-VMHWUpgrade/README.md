# Invoke-VMHWUpgrade

**VM Hardware Upgrade Manager — vCenter VI/JSON API Hardware Compatibility Upgrade**

A PowerShell script for enterprise-grade virtual machine hardware compatibility upgrades against VMware vCenter using the vCenter REST and VI/JSON APIs. Automatically powers down scoped VMs through a graceful escalation sequence, captures optional pre-upgrade snapshots once VMs are confirmed off, upgrades each VM's hardware compatibility version, and optionally powers VMs back on. A dedicated cleanup mode removes pre-upgrade snapshots in a separate run after the operator has validated the results.

**Author:** Don Horrox — [vchamp.net](https://vchamp.net)  
**Version:** 1.0.0  
**License:** See [LICENSE](LICENSE)

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Quick Start](#quick-start)
- [Parameters](#parameters)
- [Execution Modes](#execution-modes)
- [Execution Phases](#execution-phases)
- [Hardware Version Selection](#hardware-version-selection)
- [Hardware Version Reference](#hardware-version-reference)
- [Input File Format](#input-file-format)
- [Concurrency Control](#concurrency-control)
- [Power-Down Escalation Sequence](#power-down-escalation-sequence)
- [Snapshot Behavior](#snapshot-behavior)
- [Snapshot Cleanup](#snapshot-cleanup)
- [Result Output Formats](#result-output-formats)
- [Dry-Run Mode](#dry-run-mode)
- [Logging](#logging)
- [VI/JSON Release Detection](#vijson-release-detection)
- [VLR / SRM Placeholder Detection](#vlr--srm-placeholder-detection)
- [Linked Mode Support](#linked-mode-support)
- [vCenter Permissions](#vcenter-permissions)
- [Tunable Constants](#tunable-constants)
- [Examples](#examples)
- [Notes](#notes)

---

## Features

- **Automated power-down** — graceful Guest OS Shutdown → Power Off → Hard Stop escalation before upgrade
- **Pre-upgrade snapshots** — crash-consistent snapshots captured after power-down, immediately before the upgrade command; must be removed manually after validation
- **Interactive version selection** — numbered menu (highest first, no default) when `-TargetVersion` is not specified; selection is required
- **Target version validation** — VMs already at or above the target are skipped; no unnecessary action taken
- **Four-phase execution** — Power-Down → Snapshot → Upgrade → Power-On (optional) with per-VM failure isolation at each gate
- **Optional auto power-on** — VMs shut down by the script can be powered back on after upgrade via `-apo`; VMs already powered off before the run are unaffected
- **Snapshot cleanup mode** — separate `-cs` run removes only pre-upgrade snapshots by exact name; all other snapshots are left untouched
- **Concurrent operations** — configurable global, per-host, and per-datastore throttling across all four phases including snapshot removal
- **ETA tracking** — rolling average duration displayed in the progress bar for all concurrent phases
- **Dry-run simulation** — full walkthrough including live VM resolution against vCenter without executing any changes
- **VLR / SRM placeholder detection** — automatically skips recovery-site placeholder VMs
- **Linked Mode awareness** — scopes operations to the target vCenter only
- **Structured logging** — timestamped log file written to the script directory
- **Multiple result output formats** — inline Table, CSV, plain Text, or interactive GridView window
- **Self-signed certificate support** — automatic SSL bypass retry plus explicit `-k` flag for lab environments

---

## Requirements

<table>
  <thead>
    <tr style="background-color:#0d2137;">
      <th style="color:#90c8f8; padding:8px 12px;">Requirement</th>
      <th style="color:#90c8f8; padding:8px 12px;">Minimum</th>
    </tr>
  </thead>
  <tbody>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;">PowerShell</td>
      <td style="color:#c9d1d9; padding:7px 12px;">PowerShell 7.0 or later (Windows / Linux)</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;">vCenter Server</td>
      <td style="color:#c9d1d9; padding:7px 12px;">8.0 or 9.0 — vSphere 7.x and older are not supported</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;">Network access</td>
      <td style="color:#c9d1d9; padding:7px 12px;">HTTPS (port 443) to vCenter</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;">Permissions</td>
      <td style="color:#c9d1d9; padding:7px 12px;">See <a href="#vcenter-permissions">vCenter Permissions</a></td>
    </tr>
  </tbody>
</table>

---

## Quick Start

1. Download `Invoke-VMHWUpgrade.ps1` to a working directory.
2. Create an input file listing VM display names, one per line (see [Input File Format](#input-file-format)).
3. Run the upgrade:

```powershell
.\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -tv VMX_22 -snap -r Table
```

4. After validating the upgraded VMs, remove pre-upgrade snapshots:

```powershell
.\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -cs
```

The script prompts for vCenter credentials (username and password separately at the terminal), resolves all VMs against the live vCenter, and displays a full pre-run action summary requiring `CONFIRM` before performing any operations.

---

## Parameters

### Required

<table>
  <thead>
    <tr style="background-color:#0d2137;">
      <th style="color:#90c8f8; padding:8px 12px;">Parameter</th>
      <th style="color:#90c8f8; padding:8px 12px;">Alias</th>
      <th style="color:#90c8f8; padding:8px 12px;">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-VCenterServer</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-vc</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">FQDN or IP address of the target vCenter server</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-SourceFile</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-s</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Path to a CSV or TXT file containing VM display names (one per line)</td>
    </tr>
  </tbody>
</table>

Both parameters can also be entered interactively if not provided on the command line.

### Options

<table>
  <thead>
    <tr style="background-color:#0d2137;">
      <th style="color:#90c8f8; padding:8px 12px;">Parameter</th>
      <th style="color:#90c8f8; padding:8px 12px;">Alias</th>
      <th style="color:#90c8f8; padding:8px 12px;">Default</th>
      <th style="color:#90c8f8; padding:8px 12px;">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-TargetVersion</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-tv</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">None</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Target hardware version in <code>VMX_N</code> format (e.g., <code>VMX_22</code>). If omitted, an interactive menu is displayed — a selection is required, there is no default</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-Snapshot</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-snap</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Off</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Capture a crash-consistent pre-upgrade snapshot for each VM after power-down, before the upgrade command. If omitted, the script prompts for preference</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-AutoPowerOn</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-apo</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Off</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Power on VMs that were shut down by this script after upgrade completes, including on upgrade failure. VMs already powered off before the run are not started</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-CleanupSnaps</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-cs</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Off</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Removes pre-upgrade snapshots for all scoped VMs. Intended as a separate run after validation. Cannot be combined with <code>-tv</code>, <code>-snap</code>, or <code>-apo</code></td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-SkipCertificateCheck</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-k</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Off</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Disables SSL certificate validation before the first connection attempt. Required for vCenter servers with self-signed certificates</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-DryRun</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-d</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Off</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Simulate all operations without making any changes. VM resolution always runs against the live vCenter</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-VerboseLogging</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-v</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Off</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Display full timestamped log output on the terminal and write DEBUG entries to the log file</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-ResultOutput</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-r</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">None</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Export results: <code>Table</code>, <code>CSV</code>, <code>Text</code>, or <code>GridView</code>. Not available in dry-run mode</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-Help</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-h</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">—</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Display usage guidance and exit</td>
    </tr>
  </tbody>
</table>

### Concurrency

<table>
  <thead>
    <tr style="background-color:#0d2137;">
      <th style="color:#90c8f8; padding:8px 12px;">Parameter</th>
      <th style="color:#90c8f8; padding:8px 12px;">Alias</th>
      <th style="color:#90c8f8; padding:8px 12px;">Default</th>
      <th style="color:#90c8f8; padding:8px 12px;">Range</th>
      <th style="color:#90c8f8; padding:8px 12px;">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-ConcurrentGlobal</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-cg</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>0</code> (unlimited)</td>
      <td style="color:#c9d1d9; padding:7px 12px;">0–∞</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Maximum simultaneous operations across all hosts and datastores</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-ConcurrentHost</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-ch</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>5</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">1–10</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Maximum simultaneous operations per ESX host</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-ConcurrentDatastore</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-cd</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>5</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">1–10</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Maximum simultaneous operations per datastore</td>
    </tr>
  </tbody>
</table>

> Concurrency limits apply to the **Power-Down**, **Power-On**, and **Snapshot Cleanup** phases. The Snapshot capture and Hardware Upgrade phases run sequentially.

---

## Execution Modes

### Upgrade Mode (default)

The standard mode. Upgrades VM hardware compatibility versions across four sequential phases. Triggered by any combination of parameters that does not include `-cs`.

### Cleanup Mode (`-cs`)

Runs as a completely separate execution — no upgrade phases are performed. Queries each scoped VM's snapshot tree for snapshots matching the prescribed name (`Pre-VM Hardware Version Upgrade`) and removes them concurrently. All other snapshots are left untouched regardless of name or age. VM power state is not modified.

`-CleanupSnaps` cannot be combined with `-TargetVersion` (`-tv`), `-Snapshot` (`-snap`), or `-AutoPowerOn` (`-apo`). Running with these arguments together exits immediately with an error.

---

## Execution Phases

### Upgrade Mode

The script executes in four sequential phases. A failure in any phase excludes that VM from all subsequent phases — other VMs in the batch are not affected.

```
Phase 1 — Power-Down
    ├── VMs already powered off are marked Skipped and pass through to Phase 2
    ├── VMs that are powered on are shut down using the escalation sequence
    └── VMs that cannot be powered off are excluded from Phases 2–4

Phase 2 — Snapshot  (only if enabled via -snap or prompt)
    ├── Runs after all VMs are confirmed in a powered-off state
    ├── One crash-consistent snapshot per VM: "Pre-VM Hardware Version Upgrade"
    └── VMs whose snapshot fails are excluded from Phase 3

Phase 3 — Hardware Version Upgrade
    ├── POST /sdk/vim25/{release}/VirtualMachine/{vmMoId}/UpgradeVM_Task
    ├── Version string converted internally: VMX_22 → vmx-22 for the API call
    ├── Task completion confirmed by polling GET /Task/{taskId}/info
    ├── Hardware version verified via GET /api/vcenter/vm/{vm}/hardware after task completes
    └── VMs are left powered off — Phase 4 only runs if -apo is specified

Phase 4 — Power-On  (only if -AutoPowerOn / -apo is specified)
    ├── Powers on all VMs that were shut down by Phase 1
    ├── VMs already powered off before the run are NOT powered on
    └── Runs regardless of Phase 3 outcome for each VM
```

> Hardware version upgrades cannot be rolled back through vCenter directly. A pre-upgrade snapshot is the only available revert path — and only if one was captured in Phase 2.

### Pre-Run Summary and Confirmation

Before any phase executes, the script displays a full action plan showing each VM's current version, target version, power state (colour-coded), snapshot flag, and planned action sequence. Operators must type `CONFIRM` to proceed. The prompt is displayed even in dry-run mode, and is bypassed only when all operations are simulated interactively.

### Cleanup Mode

```
Snapshot Inventory
    └── Queries GET /sdk/vim25/{release}/VirtualMachine/{vmMoId}/snapshot for each VM
        and searches the full snapshot tree for name matches

Pre-Run Summary
    └── Displays VM name, power state, matched snapshot name, creation timestamp,
        and planned action for each VM. Requires CONFIRM to proceed.

Snapshot Removal Phase  (concurrent)
    ├── POST /sdk/vim25/{release}/VirtualMachineSnapshot/{snapshotMoId}/RemoveSnapshot_Task
    ├── consolidate=true ensures disk files are cleaned up after removal
    └── Task completion confirmed by polling GET /Task/{taskId}/info
```

---

## Hardware Version Selection

If `-TargetVersion` is not provided, the script displays an interactive numbered menu after VM resolution completes. The menu is ordered highest-to-lowest and requires an explicit selection — there is no default:

```
  ── Target Hardware Version ──────────────────────────────────────────

  Select the target hardware compatibility version for this upgrade:

  [ 1]  VMX_22  (vSphere 9.0)
  [ 2]  VMX_21  (vSphere 8.0 U2)
  [ 3]  VMX_20  (vSphere 8.0)

  Enter selection [1-3]
```

Only versions above the lowest current version in the scoped VM set are presented. When passed as a command-line argument, the value must match the `VMX_N` format exactly:

```powershell
-tv VMX_22
```

Any value that does not match this format causes the script to exit with an error before connecting to vCenter.

---

## Hardware Version Reference

<table>
  <thead>
    <tr style="background-color:#0d2137;">
      <th style="color:#90c8f8; padding:8px 12px;">Version String</th>
      <th style="color:#90c8f8; padding:8px 12px;">VMX Level</th>
      <th style="color:#90c8f8; padding:8px 12px;">Introduced With</th>
      <th style="color:#90c8f8; padding:8px 12px;">Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>VMX_20</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">20</td>
      <td style="color:#c9d1d9; padding:7px 12px;">vSphere 8.0</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Minimum selectable version — vSphere 8.0+ is required</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>VMX_21</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">21</td>
      <td style="color:#c9d1d9; padding:7px 12px;">vSphere 8.0 U2</td>
      <td style="color:#c9d1d9; padding:7px 12px;"></td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>VMX_22</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">22</td>
      <td style="color:#c9d1d9; padding:7px 12px;">vSphere 9.0</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Highest defined level per Broadcom KB 315655</td>
    </tr>
  </tbody>
</table>

> The upgrade API will return an error if the target version is not supported by the physical host running the VM. VMs in this state are marked `Error` and the actual version is reported in the `Version After` column.

---

## Input File Format

The input file may be a plain `.txt` or `.csv` file containing one VM display name per line. Column headers are automatically detected and skipped. Supported header values: `vmname`, `vm_name`, `name`, `vm`, `hostname`, `host_name`, `displayname`. CSV-style quoting is stripped automatically.

**Example — plain text:**
```
web-server-01
db-server-02
app-server-03
```

**Example — CSV with header:**
```csv
vmname
web-server-01
db-server-02
app-server-03
```

The same input file is used for both upgrade runs and cleanup runs (`-cs`). The cleanup mode queries each resolved VM independently and only removes snapshots by exact name match.

> **Note:** VM names are matched against vCenter display names. The match is case-sensitive in some vCenter versions — ensure names match the inventory display name exactly.

---

## Concurrency Control

Concurrency limits apply to the **Power-Down**, **Power-On**, and **Snapshot Cleanup** phases. All three phases enforce the same three independent throttling gates simultaneously. A work item will not be dispatched unless **all three** gates pass:

<table>
  <thead>
    <tr style="background-color:#0d2137;">
      <th style="color:#90c8f8; padding:8px 12px;">Gate</th>
      <th style="color:#90c8f8; padding:8px 12px;">Parameter</th>
      <th style="color:#90c8f8; padding:8px 12px;">Default</th>
    </tr>
  </thead>
  <tbody>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;">Global</td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-cg</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Unlimited (0)</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;">Per ESX Host</td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-ch</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">5</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;">Per Datastore</td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>-cd</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">5</td>
    </tr>
  </tbody>
</table>

The engine uses a single polling loop with no background jobs. As each item reaches a terminal state, the freed slot is immediately offered to the next pending item — subject to the gates above. A rolling ETA is tracked and displayed in the progress bar for all concurrent phases.

**Choosing limits:** Start conservative (e.g., `-ch 3 -cd 3`) for environments with limited storage I/O headroom. Increase for NVMe or vSAN-backed datastores. The global gate (`-cg`) is useful for capping total API request rate regardless of host/datastore distribution.

---

## Power-Down Escalation Sequence

Each VM is processed independently through up to three steps:

```
Step 1 — Guest OS Shutdown
    ├── Skipped automatically if VMware Tools is NOT running or not installed
    └── Timeout: 300 seconds (configurable via $TIMEOUT_GUEST_SHUTDOWN)

Step 2 — Power Off
    ├── Issued if Step 1 timed out or was skipped
    └── Timeout: 120 seconds (configurable via $TIMEOUT_POWER_OFF)

Step 3 — Hard Stop
    ├── Final Power Off attempt if Step 2 timed out
    └── Timeout: 120 seconds (configurable via $TIMEOUT_POWER_OFF)

Failure
    └── VM is marked as Error and excluded from Snapshot and Upgrade phases
```

Command-send failures (not timeouts) trigger automatic retry, up to 2 attempts with a 30-second delay between each.

---

## Snapshot Behavior

When enabled (via `-snap` or the interactive prompt), the script captures one crash-consistent snapshot per eligible VM in Phase 2, after all power-down operations have completed and before any upgrade command is issued.

- **Timing:** Snapshots are taken with the VM in a confirmed powered-off state, immediately before the upgrade command. This produces a clean, restorable disk image with no memory-state considerations.
- **Consistency:** Snapshots use `memory=false`. No in-guest quiescing is performed.
- **API:** `POST /sdk/vim25/{release}/VirtualMachine/{vmMoId}/CreateSnapshotEx_Task`
- **Name / Description:** Both fields are set to `Pre-VM Hardware Version Upgrade`.
- **Failure handling:** If snapshot creation fails for a VM, that VM is excluded from Phase 3. Other VMs continue normally.
- **Scope:** Only VMs that will be upgraded receive a snapshot. VMs already at or above the target version are excluded entirely.
- **Manual removal:** Pre-upgrade snapshots are **not** removed automatically. Use the `-cs` flag in a separate run after validating the upgrade results. See [Snapshot Cleanup](#snapshot-cleanup).

> Snapshots are the only rollback path after a hardware version upgrade. The upgrade cannot be undone through vCenter directly once applied.

---

## Snapshot Cleanup

The `-CleanupSnaps` (`-cs`) flag enables a dedicated cleanup mode intended to run in a **separate script execution** after the operator has validated that hardware upgrades completed successfully.

### How it works

1. The script connects to vCenter and resolves all VMs in the source file exactly as in an upgrade run.
2. For each VM, the full snapshot tree is queried via `GET /sdk/vim25/{release}/VirtualMachine/{vmMoId}/snapshot` and traversed recursively.
3. Only snapshots whose name exactly matches `Pre-VM Hardware Version Upgrade` are targeted. All other snapshots — regardless of name, age, or position in the tree — are left completely untouched.
4. A pre-run summary table is displayed showing each VM's power state, matched snapshot name, and creation timestamp (formatted `MM/dd/yy h:mm:ss AM/PM` in local time).
5. The operator must type `CONFIRM` to proceed.
6. Matched snapshots are removed concurrently via `POST /sdk/vim25/{release}/VirtualMachineSnapshot/{snapshotMoId}/RemoveSnapshot_Task` with `consolidate=true` to clean up disk files.

### Constraints

- Cannot be combined with `-TargetVersion` (`-tv`), `-Snapshot` (`-snap`), or `-AutoPowerOn` (`-apo`).
- VM power state is not checked or modified. Snapshot removal proceeds regardless of whether the VM is powered on or off.
- Snapshot removal is permanent and cannot be undone.

### Usage

```powershell
# Preview what would be removed without deleting anything
.\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -cs -d

# Remove all matching pre-upgrade snapshots
.\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -cs
```

---

## Result Output Formats

Specify an output format with `-r` / `-ResultOutput`. Not available in dry-run mode. Applies to upgrade runs only; cleanup runs display results in the terminal summary.

<table>
  <thead>
    <tr style="background-color:#0d2137;">
      <th style="color:#90c8f8; padding:8px 12px;">Format</th>
      <th style="color:#90c8f8; padding:8px 12px;">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>Table</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Formatted table printed inline in the terminal after all phases complete</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>CSV</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Comma-separated file written to the script directory (<code>VMHWUpgrade_&lt;timestamp&gt;.csv</code>)</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>Text</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Plain-text formatted table written to the script directory (<code>VMHWUpgrade_&lt;timestamp&gt;.txt</code>)</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>GridView</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Interactive, sortable, and filterable grid opened in a separate PowerShell window (Windows only)</td>
    </tr>
  </tbody>
</table>

**Result table columns:**

<table>
  <thead>
    <tr style="background-color:#0d2137;">
      <th style="color:#90c8f8; padding:8px 12px;">Column</th>
      <th style="color:#90c8f8; padding:8px 12px;">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;">VM Name</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Display name of the VM</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;">ESX Host</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Parent ESX host at operation time</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;">Datastore</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Parent datastore resolved from the VM's primary disk backing</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;">Version Before</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Hardware version at the time of VM resolution (e.g., <code>VMX_20</code>)</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;">Version After</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Hardware version confirmed after upgrade, or <code>N/A</code> if upgrade was not reached</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;">Snapshot</td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>Complete</code>, <code>Error</code>, or <code>N/A</code></td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;">Power-Down</td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>Complete</code>, <code>Skipped</code> (already off), or <code>Error</code></td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;">Upgrade</td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>Complete</code>, <code>Skipped</code> (already at target), <code>Error</code>, or <code>N/A</code></td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;">Power-On</td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>Complete</code>, <code>Error</code>, or <code>N/A</code> (when <code>-apo</code> was not specified)</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;">Status</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Overall result: <code>Complete</code>, <code>Partial</code>, <code>Skipped</code>, or <code>Error</code></td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;">Notes</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Reason for failure or skip, if applicable</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;">Completed At</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Timestamp the VM reached its terminal state (<code>MM/dd/yyyy HH:mm:ss</code>)</td>
    </tr>
  </tbody>
</table>

The **Status** column reflects the aggregate outcome across all phases. A VM that was successfully upgraded but had an already-off power state (skipped Phase 1) records `Complete`.

---

## Dry-Run Mode

Invoke with `-d` / `-DryRun` to simulate an operation without making any changes to vCenter. In this mode:

- **VM resolution always runs against the live vCenter** — power state, hardware version, host, and datastore are all queried using real credentials
- The VI/JSON release schema is detected from vCenter — it is never assumed
- Power commands, snapshot API calls, upgrade commands, and snapshot removal calls are not sent
- Each work item prints a `[DRY-RUN]` message describing what would have been done
- Poll-interval sleeps are skipped so the simulation completes instantly
- The snapshot preference prompt still appears (snapshots will not actually be taken)
- The `CONFIRM` prompt is bypassed with a note
- Result output (`-r`) is automatically suppressed with a warning
- The summary box shows **Operations Planned** instead of a completed breakdown

Dry-run is recommended before any first production use of a new VM list, version target, or concurrency configuration.

---

## Logging

Every execution writes a timestamped log file to the script directory:

```
VMHWUpgrade_20260428_143022.log
```

Log entries follow the format:

```
[2026-04-28 14:30:22] [INFO ] Detecting VI/JSON release schema...
[2026-04-28 14:30:23] [OK   ] VI/JSON release schema: 9.0.0.0.
[2026-04-28 14:30:35] [INFO ] [1/12] db-server-01: Sending Guest OS Shutdown.
[2026-04-28 14:35:41] [OK   ] [1/12] db-server-01: Powered off.
[2026-04-28 14:35:43] [OK   ] [1/12] db-server-01: Snapshot 'Pre-VM Hardware Version Upgrade' created (task-1044).
[2026-04-28 14:35:46] [OK   ] [1/12] db-server-01: Hardware version upgraded to VMX_22.
[2026-04-28 14:36:12] [WARN ] [4/12] app-server-04: Guest OS Shutdown timed out (300s). Issuing Power Off.
[2026-04-28 14:37:18] [ERROR] [7/12] web-server-07: Snapshot creation failed — this VM will be excluded from upgrade.
```

**Log levels:**

<table>
  <thead>
    <tr style="background-color:#0d2137;">
      <th style="color:#90c8f8; padding:8px 12px;">Level</th>
      <th style="color:#90c8f8; padding:8px 12px;">Meaning</th>
    </tr>
  </thead>
  <tbody>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>INFO</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Normal progress and informational messages</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>OK</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Successful completion of an operation or phase</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>WARN</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Non-fatal issues — escalation events, skipped VMs, version already at target</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>ERROR</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Operation failures requiring attention — power-down failure, snapshot failure, upgrade not confirmed</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>DEBUG</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">API calls, task IDs, poll results, host resolution, and placement details — only written when <code>-VerboseLogging</code> (<code>-v</code>) is active</td>
    </tr>
  </tbody>
</table>

By default, DEBUG entries are suppressed from both the terminal and the log file. Enable verbose mode (`-v`) to include them.

---

## VI/JSON Release Detection

The script uses the vSphere VI/JSON API for snapshot creation, snapshot removal, and hardware version upgrade operations. Before any of these can run, it probes the vCenter to determine the highest supported VI/JSON release schema. This detection happens once per session immediately after authentication and is cached for the duration of the run.

**Candidates probed (in descending order):**

```
9.1.0.0 → 9.0.0.0 → 8.0.3.0 → 8.0.2.0 → 8.0.1.0
```

The first candidate that returns a successful response from `GET /sdk/vim25/{release}/ServiceInstance/ServiceInstance/content` is used for all subsequent VI/JSON API calls.

> Release detection is **never skipped**, even in dry-run mode — the snapshot inventory query (used in `-cs` and dry-run preview) requires the confirmed release to resolve correctly.

---

## VLR / SRM Placeholder Detection

The script automatically identifies and skips VMware Live Recovery (VLR) and Site Recovery Manager (SRM) placeholder VMs at the recovery site using three REST API heuristics:

1. **Folder name pattern match** — the VM's inventory folder name is checked against a configurable list of substrings. Default patterns: `vCDR`, `SRM`, `Site Recovery`, `LiveRecovery`, `DR_Placeholder`.
2. **Zero registered disks** — placeholder VMs are commonly registered with no disk backing prior to a test or actual recovery.
3. **Disks present, zero total capacity** — some configurations register VMDK entries with zero allocated bytes.

Skipped placeholder VMs are listed in the terminal output and counted separately in the summary. They do not appear in the result export.

### Customizing detection patterns

Edit `$SRM_FOLDER_PATTERNS` near the top of the script:

```powershell
$SRM_FOLDER_PATTERNS = @('vCDR', 'SRM', 'Site Recovery', 'LiveRecovery', 'DR_Placeholder', 'MyCustomFolder')
```

> **Note:** For authoritative detection, the SOAP API `VirtualMachine.config.managedBy.extensionKey` is the definitive source. SRM uses `com.vmware.vcDr`; VLR uses `com.vmware.liverecover`. If heuristic detection is insufficient in your environment, the `Test-IsSRMPlaceholder` function can be extended accordingly.

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
2. Clone the built-in **Read-Only** role and name it (e.g., `VM HW Upgrade`).
3. Add the privileges listed below to the cloned role.
4. Assign the role to the service account at the **vCenter Server** level with **Propagate to children** checked.

### Required privileges

<table>
  <thead>
    <tr style="background-color:#0d2137;">
      <th style="color:#90c8f8; padding:8px 12px;">Privilege Category</th>
      <th style="color:#90c8f8; padding:8px 12px;">Privilege</th>
      <th style="color:#90c8f8; padding:8px 12px;">Required For</th>
    </tr>
  </thead>
  <tbody>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;">Virtual Machine &gt; Change Configuration</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Upgrade virtual machine compatibility</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Hardware version upgrade (Phase 3)</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;">Virtual Machine &gt; Interaction</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Power Off</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Power-down escalation (Phase 1)</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;">Virtual Machine &gt; Interaction</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Power On</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Auto power-on after upgrade (Phase 4, when <code>-apo</code> is specified)</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;">Virtual Machine &gt; Snapshot management</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Create snapshot</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Pre-upgrade snapshots (Phase 2, when enabled)</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;">Virtual Machine &gt; Snapshot management</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Remove Snapshot</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Snapshot cleanup mode (<code>-cs</code>)</td>
    </tr>
  </tbody>
</table>

> All five privileges sit within the **Virtual Machine** object type. Assign them at the vCenter Server level with **Propagate to Children** enabled.

---

## Tunable Constants

The following constants are defined near the top of the script and can be adjusted for your environment without modifying any logic:

<table>
  <thead>
    <tr style="background-color:#0d2137;">
      <th style="color:#90c8f8; padding:8px 12px;">Constant</th>
      <th style="color:#90c8f8; padding:8px 12px;">Default</th>
      <th style="color:#90c8f8; padding:8px 12px;">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>$TIMEOUT_GUEST_SHUTDOWN</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>300</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Seconds to wait for Guest OS Shutdown before escalating to Power Off</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>$TIMEOUT_POWER_OFF</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>120</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Seconds to wait for Power Off or Hard Stop before declaring failure</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>$TIMEOUT_UPGRADE</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>60</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Seconds to poll for hardware version change confirmation after the upgrade task completes</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>$TIMEOUT_SNAPSHOT</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>120</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Seconds allocated for snapshot creation or removal task completion</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>$POLL_INTERVAL</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>10</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Seconds between polls during power-down, power-on, and snapshot removal operations</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>$POLL_INTERVAL_FAST</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>5</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Seconds between polls for upgrade task completion verification</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>$RETRY_DELAY</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>30</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Seconds to wait before retrying a failed API command dispatch</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>$MAX_RETRIES</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>2</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Maximum command-dispatch retries per item before marking as failed</td>
    </tr>
    <tr style="background-color:#0a1a2e;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>$SNAPSHOT_NAME</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;"><code>Pre-VM Hardware Version Upgrade</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">Name and description applied to all pre-upgrade snapshots. Also used as the exact-match filter in cleanup mode — changing this will cause <code>-cs</code> to not find existing snapshots</td>
    </tr>
    <tr style="background-color:#0f2236;">
      <td style="color:#c9d1d9; padding:7px 12px;"><code>$SRM_FOLDER_PATTERNS</code></td>
      <td style="color:#c9d1d9; padding:7px 12px;">See script</td>
      <td style="color:#c9d1d9; padding:7px 12px;">Folder name substrings used for VLR/SRM placeholder detection</td>
    </tr>
  </tbody>
</table>

---

## Examples

### Upgrade to VMX_22 with snapshots and inline table output

```powershell
.\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -tv VMX_22 -snap -r Table
```

### Upgrade with auto power-on after completion and CSV output

```powershell
.\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -tv VMX_22 -snap -apo -r CSV
```

### Upgrade with concurrency limits and verbose logging

```powershell
.\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -tv VMX_22 -snap -r CSV -cg 10 -ch 3 -cd 3 -v
```

### Interactive version selection (no -tv specified)

```powershell
.\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -snap -r Table
```

### Dry-run to preview scope and phase actions before committing

```powershell
.\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -tv VMX_22 -snap -d
```

### Preview what snapshots would be removed before cleanup

```powershell
.\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -cs -d
```

### Remove pre-upgrade snapshots after validation

```powershell
.\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -cs
```

### Remove snapshots with concurrency limits and verbose logging

```powershell
.\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -cs -cg 5 -v
```

### Display built-in help

```powershell
.\Invoke-VMHWUpgrade.ps1 -h
```

---

## Notes

- Requires vSphere 8.0 or later. vSphere 7.x and older are not supported.
- VM names in the input file are matched against vCenter display names. The match is case-sensitive in some vCenter versions — ensure names are exact.
- Input file headers (`vmname`, `name`, `vm`, etc.) are automatically detected and skipped.
- SSL certificate validation is attempted first. If it fails, the script automatically retries with validation disabled and logs a warning. Pass `-k` to bypass from the first connection attempt — recommended for all lab and self-signed environments.
- All timestamps in log files and result exports use the format `MM/dd/yyyy HH:mm:ss` for consistent rendering across locales and spreadsheet applications.
- The GridView output format (`-r GridView`) requires PowerShell 7+ on Windows. It is not available on Linux or macOS.
- The script creates one log file per execution in the script directory. Log files are not automatically rotated or cleaned up.
- VMs are intentionally left **powered off** after upgrade unless `-apo` is specified. Only VMs the script shut down in Phase 1 are powered back on — VMs that were already off before the run remain off.
- The hardware version upgrade is applied immediately upon task completion and takes effect on the VM's next power-on. The version confirmed in the log and result export reflects the state queried from vCenter inventory after the task completes.
- If `$SNAPSHOT_NAME` is changed after snapshots have already been captured, the `-cs` cleanup mode will not find those snapshots (the filter is an exact name match). The original name must be restored, or snapshots must be removed manually.
- SRM/VLR placeholder VMs are excluded from all operations in both upgrade and cleanup modes.
- In Linked Mode, only VMs on the target vCenter are processed in both modes.
