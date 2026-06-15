#Requires -Version 7.0

<#
.SYNOPSIS
    VM Hardware Upgrade Manager — vCenter REST API Hardware Compatibility Upgrade

.DESCRIPTION
    Upgrades virtual machine hardware compatibility versions using the vCenter REST
    and VI/JSON APIs. VMs are automatically powered down before upgrade using a
    graceful Guest OS Shutdown sequence with escalation to Power Off, then left powered
    off after upgrade completes.

    Optional pre-upgrade snapshots (crash-consistent, no memory) are captured after
    the VM is confirmed powered off, immediately before the hardware upgrade command
    is issued. VMs whose snapshots fail are excluded from upgrade. Supports concurrent
    power-down operations with per-host and per-datastore throttling, dry-run
    simulation, and detailed structured logging.

    SRM/VLR placeholder VMs are automatically detected and excluded from all operations.
    When operating against a vCenter in Linked Mode, only VMs hosted by the specified
    vCenter are targeted; VMs belonging to linked vCenters are skipped.

.PARAMETER VCenterServer
    FQDN or IP address of the target vCenter server.
    Alias: -vc

.PARAMETER SourceFile
    Path to a CSV or plain-text file containing VM display names (one per line).
    Optional headers such as vmname, name, or vm are automatically skipped.
    Alias: -s

.PARAMETER TargetVersion
    Target hardware compatibility version in VMX_N format (e.g., VMX_22).
    If not provided, an interactive menu is displayed with the option to select
    the version. VMs already at or above the target version are skipped.
    Alias: -tv

.PARAMETER Snapshot
    Captures a crash-consistent pre-upgrade snapshot for each VM after it has been
    confirmed powered off and before the hardware upgrade command is issued.
    Snapshot name and description: "Pre-VM Hardware Version Upgrade".
    Snapshots are taken only for VMs that will actually be upgraded.
    If not specified, the script will prompt whether to take snapshots.
    Alias: -snap

.PARAMETER AutoPowerOn
    Automatically powers on VMs after the upgrade phase completes, respecting the
    defined concurrency limits (-cg, -ch, -cd). Applies to all VMs that were
    powered down by this script, regardless of whether the hardware upgrade
    succeeded or failed for each individual VM.
    Alias: -apo

.PARAMETER SkipCertificateCheck
    Disables SSL certificate validation before the first connection attempt.
    Required when connecting to vCenter servers with self-signed certificates.
    Without this flag, SSL errors during the initial connection are surfaced as
    an authentication failure and require an interactive retry prompt.
    Alias: -k

.PARAMETER VerboseLogging
    Enables verbose/debug output to both the console and the log file.
    Alias: -v

.PARAMETER DryRun
    Simulates all operations without executing them against vCenter.
    Result output (-r) is not permitted in dry-run mode.
    Alias: -d

.PARAMETER ConcurrentGlobal
    Maximum number of concurrent power-down operations across all hosts.
    A value of 0 (default) imposes no global limit.
    Alias: -cg

.PARAMETER ConcurrentHost
    Maximum number of concurrent power-down operations per ESX host. Range: 1-10. Default: 5.
    Alias: -ch

.PARAMETER ConcurrentDatastore
    Maximum number of concurrent power-down operations per parent datastore. Range: 1-10. Default: 5.
    Alias: -cd

.PARAMETER ResultOutput
    Exports operation results in the specified format: Table, CSV, Text, or GridView.
    CSV and Text files are written to the script directory.
    GridView opens an interactive, filterable table in a separate PowerShell window.
    Not permitted in dry-run mode.
    Alias: -r

.PARAMETER CleanupSnaps
    Removes pre-upgrade snapshots created by this script for all scoped VMs.
    Only snapshots whose name exactly matches the prescribed snapshot name
    ("Pre-VM Hardware Version Upgrade") are removed. All other snapshots
    are left completely untouched. VM power state is not modified.
    Intended to be run as a separate script execution after the operator has
    validated that hardware upgrades completed successfully.
    Cannot be used together with -TargetVersion (-tv), -Snapshot (-snap),
    or -AutoPowerOn (-apo).
    Alias: -cs

.PARAMETER Help
    Displays usage guidance and exits.
    Alias: -h

.EXAMPLE
    .\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -tv VMX_22 -snap -r Table

.EXAMPLE
    .\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -tv VMX_22 -r CSV -cg 10 -v

.EXAMPLE
    .\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -tv VMX_21 -d

.EXAMPLE
    .\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv

.NOTES
    Version    : 1.0.0
    Author     : Don Horrox
    Website    : https://vchamp.net
    Requires   : PowerShell 7.0+ | vCenter 8.0 or 9.0
    API        : vCenter REST API / VI JSON API
    Tested On  : PowerShell 7.6 (Windows / Linux)

    Power-Down Escalation Sequence (pre-upgrade):
        1. Guest OS Shutdown  (graceful, via VMware Tools)
           Skipped automatically if VMware Tools is not running or not installed.
        2. Power Off          (forced via vSphere API, if step 1 times out or is skipped)
        3. Hard Stop          (final attempt, if step 2 times out)

    Post-Upgrade Behavior:
        VMs are left powered off after upgrade. Power-on is a deliberate,
        manual step to allow validation before returning VMs to service.

    Snapshot Behavior:
        Snapshots are crash-consistent (memory=false, quiesce=false). They are taken
        after the VM is confirmed powered off, immediately before the hardware upgrade
        command is issued. VMs whose snapshots fail are excluded from upgrade.

    SRM/VLR Detection:
        Placeholder VMs are identified using available REST API indicators (disk
        configuration, capacity). For authoritative detection in environments where
        heuristics may be insufficient, see Test-IsSRMPlaceholder and configure
        $SRM_FOLDER_PATTERNS for your site's folder naming conventions.
#>

[CmdletBinding()]
param (
    # Target vCenter server
    [Alias('vc')]
    [string]$VCenterServer,

    # Input file path
    [Alias('s')]
    [string]$SourceFile,

    # Target hardware version (e.g., VMX_22)
    [Alias('tv')]
    [string]$TargetVersion,

    # Capture pre-upgrade snapshots
    [Alias('snap')]
    [switch]$Snapshot,

    # Automatically power on VMs after upgrade (regardless of upgrade result)
    [Alias('apo')]
    [switch]$AutoPowerOn,

    # Skip SSL certificate validation (use for self-signed vCenter certificates)
    [Alias('k')]
    [switch]$SkipCertificateCheck,

    # Enable verbose/debug output
    [Alias('v')]
    [switch]$VerboseLogging,

    # Dry-run simulation mode
    [Alias('d')]
    [switch]$DryRun,

    # Global concurrent power-down limit (0 = unlimited)
    [Alias('cg')]
    [ValidateRange(0, [int]::MaxValue)]
    [int]$ConcurrentGlobal = 0,

    # Per-host concurrent power-down limit
    [Alias('ch')]
    [ValidateRange(1, 10)]
    [int]$ConcurrentHost = 5,

    # Per-datastore concurrent power-down limit
    [Alias('cd')]
    [ValidateRange(1, 10)]
    [int]$ConcurrentDatastore = 5,

    # Result output format
    [Alias('r')]
    [ValidateSet('Table', 'CSV', 'Text', 'GridView')]
    [string]$ResultOutput,

    # Remove pre-upgrade snapshots created by this script for all scoped VMs
    [Alias('cs')]
    [switch]$CleanupSnaps,

    # Display help and exit
    [Alias('h')]
    [switch]$Help
)

$ErrorActionPreference = 'Continue'

#region ── Script Constants ────────────────────────────────────────────────────
$SCRIPT_NAME    = 'VM Hardware Upgrade Manager'
$SCRIPT_VERSION = '1.0.0'
$SCRIPT_AUTHOR  = 'Don Horrox'
$SCRIPT_WEBSITE = 'https://vchamp.net'
$SCRIPT_FILE    = $MyInvocation.MyCommand.Name

# Resolve script directory regardless of call context
$SCRIPT_DIR = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
if ([string]::IsNullOrEmpty($SCRIPT_DIR)) { $SCRIPT_DIR = (Get-Location).Path }

# Timeout values (seconds)
$TIMEOUT_GUEST_SHUTDOWN = 300    # Max wait for Guest OS Shutdown to complete
$TIMEOUT_POWER_OFF      = 120    # Max wait for Power Off / Hard Stop to complete
$TIMEOUT_UPGRADE        = 60     # Max wait for hardware version change confirmation
$TIMEOUT_SNAPSHOT       = 120    # Max wait for snapshot creation confirmation
$POLL_INTERVAL          = 10     # Delay between power state polls (seconds)
$POLL_INTERVAL_FAST     = 5      # Delay between polls for fast operations (seconds)
$RETRY_DELAY            = 30     # Delay before retrying a failed API command (seconds)
$MAX_RETRIES            = 2      # Maximum command-send retry attempts per VM

# VM tracking state constants
$ST_PENDING  = 'Pending'
$ST_ACTIVE   = 'Active'
$ST_COMPLETE = 'Complete'
$ST_FAILED   = 'Failed'

# Operation result constants
$RES_COMPLETE = 'Complete'
$RES_SKIPPED  = 'Skipped'
$RES_ERROR    = 'Error'
$RES_NA       = 'N/A'

# Power-down phase constants (escalation order)
$PH_GUEST_SHUTDOWN = 'GuestShutdown'
$PH_POWER_OFF      = 'PowerOff'
$PH_HARD_STOP      = 'HardStop'

# Snapshot name/description — used in both the API call and log messages
$SNAPSHOT_NAME = 'Pre-VM Hardware Version Upgrade'

# Known valid VMX hardware version strings for vSphere ESXi 8.0+, in ascending order.
# This script requires vSphere 8.0 or later. Versions below VMX_20 (vSphere 7.x and
# older) are excluded. Per Broadcom KB 315655, VMX_22 is the highest defined level.
# The upgrade API will reject any version not supported by the target host.
$KNOWN_VMX_VERSIONS = @('VMX_20', 'VMX_21', 'VMX_22')

# SRM/VLR placeholder detection: folder name substrings that indicate recovery-site
# SRM-managed folders. Customize this list for your environment.
$SRM_FOLDER_PATTERNS = @('vCDR', 'SRM', 'Site Recovery', 'LiveRecovery', 'DR_Placeholder')
#endregion

#region ── Required vCenter Permissions ───────────────────────────────────────
# The vCenter account used to authenticate this script requires the following
# privileges. Create a custom vCenter role and assign it at the vCenter Server
# level with "Propagate to Children" enabled.
#
# Required Privileges:
#
#   Virtual Machine > Change Configuration:
#     - Upgrade virtual machine compatibility  (hardware version upgrade)
#
#   Virtual Machine > Interaction:
#     - Power Off                              (power-down escalation before upgrade)
#     - Power On                               (auto power-on after upgrade, when -apo is used)
#
#   Virtual Machine > Snapshot management:
#     - Create snapshot                        (pre-upgrade snapshots, when -snap is used)
#     - Remove Snapshot                        (snapshot cleanup, when -cs is used)
#endregion

#region ── Script-Level State ─────────────────────────────────────────────────
$Script:LogFile        = $null
$Script:SessionId      = $null
$Script:SkipCert       = $false
$Script:IsVerbose      = $VerboseLogging.IsPresent -or ($VerbosePreference -ne 'SilentlyContinue')
$Script:IsDryRun       = $DryRun.IsPresent
$Script:StartTime      = Get-Date
$Script:VCenter        = $VCenterServer
$Script:HostCache      = @{}
$Script:DatastoreCache = @{}
$Script:SkippedVMs     = [System.Collections.Generic.List[string]]::new()
$Script:VmToHostMap    = @{}
$Script:AutoPowerOn    = $AutoPowerOn.IsPresent   # Resolved from -AutoPowerOn / -apo switch
$Script:TakeSnapshot   = $false                   # Resolved snapshot preference (set in main execution)
$Script:ViJsonRelease  = $null                    # VI/JSON API release schema (e.g. '8.0.3.0'), set after auth
$Script:CleanupSnaps   = $CleanupSnaps.IsPresent  # Snapshot cleanup mode — mutually exclusive with upgrade

# HashSet of ESX host MOR IDs belonging to the connected vCenter instance.
# Populated after authentication; used to exclude VMs from linked vCenters.
$Script:LocalHostIds = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
#endregion

#region ── Logging ────────────────────────────────────────────────────────────
function Write-Log {
    <#
    .SYNOPSIS Writes a timestamped, level-tagged entry to the log file.

    Console behavior:
      - DEBUG   : Suppressed unless -VerboseLogging is active.
      - All other levels:
          Normal mode  : Displays message text only (clean, no timestamp or level tag).
          Verbose mode : Displays the full formatted log line [timestamp] [LEVEL] message.
      - NoConsole : Suppresses all console output for this entry (log file only).
    #>
    param(
        [string]$Message,
        [string]$Level = 'INFO',
        [System.ConsoleColor]$Color = [System.ConsoleColor]::Gray,
        [switch]$NoConsole
    )

    $ts      = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
    $padded  = $Level.ToUpper().PadRight(5)
    $logLine = "[$ts] [$padded] $Message"

    if ($Script:LogFile -and ($Level -ne 'DEBUG' -or $Script:IsVerbose)) {
        try { Add-Content -Path $Script:LogFile -Value $logLine -Encoding UTF8 }
        catch { <# Swallow to prevent recursion on log write failure #> }
    }

    if ($NoConsole) { return }
    if ($Level -eq 'DEBUG' -and -not $Script:IsVerbose) { return }

    if ($Script:IsVerbose) {
        Write-Host $logLine -ForegroundColor $Color
    } else {
        Write-Host "  $Message" -ForegroundColor $Color
    }
}

function Write-LogInfo    { param([string]$m, [switch]$nc) Write-Log -Message $m -Level 'INFO'  -Color Cyan    -NoConsole:$nc }
function Write-LogOK      { param([string]$m, [switch]$nc) Write-Log -Message $m -Level 'OK'    -Color Green   -NoConsole:$nc }
function Write-LogWarn    { param([string]$m, [switch]$nc) Write-Log -Message $m -Level 'WARN'  -Color Yellow  -NoConsole:$nc }
function Write-LogError   { param([string]$m, [switch]$nc) Write-Log -Message $m -Level 'ERROR' -Color Red     -NoConsole:$nc }
function Write-LogDebug   { param([string]$m)              Write-Log -Message $m -Level 'DEBUG' -Color DarkGray }
function Write-LogDryRun  { param([string]$m)              Write-Log -Message "[DRY-RUN] $m" -Level 'INFO' -Color Magenta }
#endregion

#region ── SSL / TLS Configuration ───────────────────────────────────────────
function Enable-CertBypass {
    <#
    .SYNOPSIS Disables SSL certificate validation for environments using self-signed
    vCenter certificates. Sets $Script:SkipCert = $true, which signals all subsequent
    Invoke-RestMethod calls to include -SkipCertificateCheck.
    #>
    $Script:SkipCert = $true
    Write-LogWarn 'SSL certificate verification is disabled — proceeding without certificate validation.'
}
#endregion

#region ── REST API Wrapper ───────────────────────────────────────────────────
function Invoke-VCenterAPI {
    <#
    .SYNOPSIS Central wrapper for all vCenter REST API calls. Handles URI construction,
    session header injection, optional SSL bypass, and error propagation.
    #>
    param(
        [Parameter(Mandatory)]
        [ValidateSet('GET', 'POST', 'PUT', 'PATCH', 'DELETE')]
        [string]$Method,

        [Parameter(Mandatory)]
        [string]$Endpoint,

        [object]$Body,
        [string]$SessionToken,
        [hashtable]$QueryParams
    )

    $uri = "https://$($Script:VCenter)$Endpoint"
    if ($QueryParams -and $QueryParams.Count -gt 0) {
        $qs  = ($QueryParams.GetEnumerator() | ForEach-Object {
            "$([Uri]::EscapeDataString($_.Key))=$([Uri]::EscapeDataString($_.Value))"
        }) -join '&'
        $uri = "${uri}?${qs}"
    }

    $headers = @{ 'Content-Type' = 'application/json' }
    if ($SessionToken) { $headers['vmware-api-session-id'] = $SessionToken }

    $splat = @{
        Method      = $Method
        Uri         = $uri
        Headers     = $headers
        ErrorAction = 'Stop'
    }

    if ($Body) { $splat.Body = ($Body | ConvertTo-Json -Depth 10 -Compress) }
    if ($Script:SkipCert) { $splat.SkipCertificateCheck = $true }

    Write-LogDebug "API $Method $uri"

    try {
        return Invoke-RestMethod @splat
    } catch {
        $code = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 0 }
        Write-LogDebug "API Error [HTTP $code]: $($_.Exception.Message)"
        throw
    }
}
#endregion

#region ── vCenter Session Management ─────────────────────────────────────────
function Connect-VCenter {
    <#
    .SYNOPSIS Creates a vCenter REST API session using Basic auth credentials.
    The plain-text password is zeroed from memory immediately after encoding.
    Returns the session token on success, or $null on failure.
    #>
    param(
        [Parameter(Mandatory)][string]$Username,
        [Parameter(Mandatory)][System.Security.SecureString]$Password
    )

    $bstr  = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($Password)
    $plain = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr)
    [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)

    $creds = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("${Username}:${plain}"))
    $plain = $null

    $headers = @{
        'Authorization' = "Basic $creds"
        'Content-Type'  = 'application/json'
    }

    $splat = @{
        Method      = 'POST'
        Uri         = "https://$($Script:VCenter)/api/session"
        Headers     = $headers
        ErrorAction = 'Stop'
    }

    if ($Script:SkipCert) { $splat.SkipCertificateCheck = $true }

    try {
        $raw = Invoke-RestMethod @splat
        return ($raw -replace '"', '').Trim()
    } catch {
        Write-LogDebug "Session creation failed: $($_.Exception.Message)"
        return $null
    }
}

function Disconnect-VCenter {
    <# Gracefully terminates the active vCenter REST API session. #>
    if (-not $Script:SessionId) { return }
    try {
        Invoke-VCenterAPI -Method DELETE -Endpoint '/api/session' -SessionToken $Script:SessionId | Out-Null
        Write-LogInfo 'vCenter API session terminated.' -nc
    } catch {
        Write-LogWarn "Session termination failed (non-fatal): $($_.Exception.Message)" -nc
    }
    $Script:SessionId = $null
}
#endregion

#region ── Linked Mode: Local Host Enumeration ────────────────────────────────
function Rebuild-VmToHostMap {
    <#
    .SYNOPSIS Rebuilds $Script:VmToHostMap by querying each known ESX host for its
    current VM list (GET /api/vcenter/vm?hosts={hostId}).
    #>
    $Script:VmToHostMap.Clear()

    foreach ($hostId in $Script:LocalHostIds) {
        $hostName = if ($Script:HostCache.ContainsKey($hostId)) { $Script:HostCache[$hostId] } else { $hostId }
        try {
            $vmsOnHost = Invoke-VCenterAPI -Method GET -Endpoint '/api/vcenter/vm' `
                -SessionToken $Script:SessionId -QueryParams @{ 'hosts' = $hostId }
            foreach ($v in $vmsOnHost) {
                if ($v.vm) {
                    $Script:VmToHostMap[$v.vm] = [PSCustomObject]@{
                        HostId   = $hostId
                        HostName = $hostName
                    }
                }
            }
            Write-LogDebug "Rebuild-VmToHostMap: mapped $(@($vmsOnHost).Count) VM(s) to host '$hostName'."
        } catch {
            Write-LogWarn "Rebuild-VmToHostMap: failed to enumerate VMs on host '$hostName': $($_.Exception.Message)" -nc
        }
    }

    Write-LogInfo "VM-to-host map built: $($Script:VmToHostMap.Count) VM(s) mapped across $($Script:LocalHostIds.Count) host(s)." -nc
}

function Initialize-LocalHostIds {
    <#
    .SYNOPSIS Fetches all ESX host MOR IDs registered to the connected vCenter,
    populates $Script:LocalHostIds, $Script:HostCache, and the vm->host reverse map.
    #>
    try {
        $hosts = Invoke-VCenterAPI -Method GET -Endpoint '/api/vcenter/host' -SessionToken $Script:SessionId
        $Script:LocalHostIds.Clear()
        $Script:VmToHostMap.Clear()

        foreach ($h in $hosts) {
            if (-not $h.host) { continue }
            [void]$Script:LocalHostIds.Add($h.host)
            if ($h.name) { $Script:HostCache[$h.host] = $h.name }
        }

        Write-LogInfo "Linked Mode guard initialized: $($Script:LocalHostIds.Count) ESX host(s) registered to this vCenter." -nc
        Write-LogDebug "Local host IDs: $($Script:LocalHostIds -join ', ')"

        Rebuild-VmToHostMap
        return $true
    } catch {
        Write-LogWarn "Could not enumerate local ESX hosts: $($_.Exception.Message)" -nc
        return $false
    }
}
#endregion

#region ── SRM / VLR Placeholder Detection ────────────────────────────────────
function Test-IsSRMPlaceholder {
    <#
    .SYNOPSIS Evaluates whether a VM is an SRM or VMware Live Recovery (VLR) placeholder.

    Detection heuristics (REST API):
      1. Folder name pattern match against $SRM_FOLDER_PATTERNS.
      2. Zero registered disks.
      3. Disks present but zero total capacity (shadow VMDK backing).
    #>
    param(
        [string]$VmName,
        [PSCustomObject]$VmDetail,
        [string]$FolderName = ''
    )

    if (-not $VmDetail) { return $false }

    # Heuristic 1: Folder name pattern
    if ($FolderName) {
        foreach ($pattern in $SRM_FOLDER_PATTERNS) {
            if ($FolderName -like "*$pattern*") {
                Write-LogDebug "'$VmName' identified as VLR placeholder — folder '$FolderName' matches pattern '$pattern'."
                return $true
            }
        }
    }

    # Heuristics 2 & 3: Disk configuration
    $diskCount     = 0
    $totalCapacity = [long]0

    if ($VmDetail.disks) {
        foreach ($diskProp in $VmDetail.disks.PSObject.Properties) {
            $diskCount++
            if ($diskProp.Value.capacity) { $totalCapacity += [long]$diskProp.Value.capacity }
        }
    }

    if ($diskCount -eq 0) {
        Write-LogDebug "'$VmName' identified as VLR placeholder — no disks registered."
        return $true
    }

    if ($diskCount -gt 0 -and $totalCapacity -eq 0) {
        Write-LogDebug "'$VmName' identified as VLR placeholder — $diskCount disk(s), 0 bytes total capacity."
        return $true
    }

    return $false
}

function Resolve-FolderName {
    <# Returns the display name for a folder MOR ID. Returns empty string on failure. #>
    param([string]$FolderId)
    if ([string]::IsNullOrEmpty($FolderId)) { return '' }
    try {
        $f = Invoke-VCenterAPI -Method GET -Endpoint "/api/vcenter/folder/$FolderId" -SessionToken $Script:SessionId
        return if ($f.name) { $f.name } else { '' }
    } catch { return '' }
}
#endregion

#region ── VM Resolution ──────────────────────────────────────────────────────
function Resolve-VMByName {
    <#
    .SYNOPSIS Looks up a VM in vCenter by display name. Returns the first matching
    summary object. Logs a warning if multiple VMs share the same name.
    #>
    param([Parameter(Mandatory)][string]$Name)

    try {
        $result = Invoke-VCenterAPI -Method GET -Endpoint '/api/vcenter/vm' `
            -SessionToken $Script:SessionId -QueryParams @{ 'names' = $Name }

        if (-not $result -or $result.Count -eq 0) { return $null }

        if ($result.Count -gt 1) {
            Write-LogWarn "Multiple VMs found matching name '$Name' ($($result.Count) results) — using first match."
        }
        return $result[0]
    } catch {
        Write-LogDebug "VM lookup failed for '$Name': $($_.Exception.Message)"
        return $null
    }
}

function Get-VMDetail {
    <# Returns the full VM configuration object including placement and disk info. #>
    param([Parameter(Mandatory)][string]$VmId)
    try {
        return Invoke-VCenterAPI -Method GET -Endpoint "/api/vcenter/vm/$VmId" -SessionToken $Script:SessionId
    } catch {
        Write-LogDebug "VM detail retrieval failed for '$VmId': $($_.Exception.Message)"
        return $null
    }
}

function Get-VMPowerState {
    <# Returns the current power state: POWERED_ON, POWERED_OFF, or SUSPENDED. #>
    param([Parameter(Mandatory)][string]$VmId)
    try {
        $r = Invoke-VCenterAPI -Method GET -Endpoint "/api/vcenter/vm/$VmId/power" -SessionToken $Script:SessionId
        return $r.state
    } catch {
        Write-LogDebug "Power state query failed for '$VmId': $($_.Exception.Message)"
        return $null
    }
}

function Get-VMToolsRunning {
    <#
    .SYNOPSIS Returns $true if VMware Tools is actively running inside the VM guest.
    API: GET /api/vcenter/vm/{vm}/tools — relevant field: run_state
    #>
    param([Parameter(Mandatory)][string]$VmId)
    try {
        $r = Invoke-VCenterAPI -Method GET -Endpoint "/api/vcenter/vm/$VmId/tools" -SessionToken $Script:SessionId
        return ($r.run_state -eq 'RUNNING')
    } catch {
        Write-LogDebug "Tools status query failed for '$VmId' — assuming not running: $($_.Exception.Message)"
        return $false
    }
}

function Get-VMHardwareInfo {
    <#
    .SYNOPSIS Returns a PSCustomObject with the VM's current hardware version and
    the scheduled upgrade version (if set).
    API: GET /api/vcenter/vm/{vm}/hardware
    Returns: @{ Version = 'VMX_19'; UpgradeVersion = 'VMX_22' }
    Returns $null on failure.
    #>
    param([Parameter(Mandatory)][string]$VmId)
    try {
        $r = Invoke-VCenterAPI -Method GET -Endpoint "/api/vcenter/vm/$VmId/hardware" -SessionToken $Script:SessionId
        return [PSCustomObject]@{
            Version        = if ($r.version)         { $r.version.ToUpper()         } else { 'Unknown' }
            UpgradeVersion = if ($r.upgrade_version) { $r.upgrade_version.ToUpper() } else { $null }
        }
    } catch {
        Write-LogDebug "Hardware info retrieval failed for '$VmId': $($_.Exception.Message)"
        return $null
    }
}

function Resolve-HostName {
    <#
    .SYNOPSIS Returns the human-readable ESX hostname for a host MOR ID, with caching.
    Falls back to a live query if a cache miss occurs (e.g., after a vMotion).
    #>
    param([string]$HostId)

    if ([string]::IsNullOrEmpty($HostId)) { return 'Unknown' }
    if ($Script:HostCache.ContainsKey($HostId)) { return $Script:HostCache[$HostId] }

    try {
        $result = Invoke-VCenterAPI -Method GET -Endpoint '/api/vcenter/host' `
            -SessionToken $Script:SessionId -QueryParams @{ 'hosts' = $HostId }
        $name = if ($result -and $result.Count -gt 0 -and $result[0].name) { $result[0].name } else { $HostId }
    } catch {
        Write-LogDebug "Host name lookup failed for '$HostId': $($_.Exception.Message)"
        $name = $HostId
    }
    $Script:HostCache[$HostId] = $name
    return $name
}

function Resolve-VMHost {
    <#
    .SYNOPSIS Resolves the ESX host currently serving a given VM from the reverse map.
    On a cache miss, refreshes by re-querying VMs for each local host.
    #>
    param(
        [Parameter(Mandatory)][string]$VmId,
        [string]$PlacementHostId
    )

    if ($Script:VmToHostMap.ContainsKey($VmId)) {
        $entry = $Script:VmToHostMap[$VmId]
        Write-LogDebug "ESX host resolved from VM->host map: $($entry.HostName) ($($entry.HostId))"
        return [PSCustomObject]@{ Id = $entry.HostId; Name = $entry.HostName }
    }

    Write-LogDebug "VM '$VmId' not in startup map; refreshing from live host scan."
    foreach ($hostId in $Script:LocalHostIds) {
        try {
            $vmsOnHost = Invoke-VCenterAPI -Method GET -Endpoint '/api/vcenter/vm' `
                -SessionToken $Script:SessionId -QueryParams @{ 'hosts' = $hostId }
            foreach ($v in $vmsOnHost) {
                if ($v.vm -eq $VmId) {
                    $hostName = Resolve-HostName -HostId $hostId
                    $Script:VmToHostMap[$VmId] = [PSCustomObject]@{ HostId = $hostId; HostName = $hostName }
                    return [PSCustomObject]@{ Id = $hostId; Name = $hostName }
                }
            }
        } catch {
            Write-LogDebug "Live scan failed for host '$hostId': $($_.Exception.Message)"
        }
    }

    Write-LogDebug "ESX host could not be resolved for VM '$VmId'."
    return [PSCustomObject]@{ Id = $null; Name = 'Unknown' }
}

function Get-DatastoreNameFromVMDetail {
    <#
    .SYNOPSIS Extracts the parent datastore name from an already-fetched VM detail object.
    Reads the VMDK backing path of 'Hard disk 1': '[DatastoreName] folder/file.vmdk'.
    Returns 'Unknown' if the disk, backing, or path cannot be parsed.
    #>
    param([PSCustomObject]$VmDetail)

    if (-not $VmDetail -or -not $VmDetail.disks) { return 'Unknown' }

    foreach ($diskProp in $VmDetail.disks.PSObject.Properties) {
        $disk = $diskProp.Value
        if ($disk.label -eq 'Hard disk 1') {
            if ($disk.backing -and $disk.backing.vmdk_file) {
                if ($disk.backing.vmdk_file -match '^\[([^\]]+)\]') {
                    Write-LogDebug "Datastore resolved from disk backing: '$($Matches[1])'"
                    return $Matches[1]
                }
            }
            Write-LogDebug 'Hard disk 1 found but backing vmdk_file path could not be parsed.'
            break
        }
    }
    return 'Unknown'
}
#endregion

#region ── Hardware Version Utilities ─────────────────────────────────────────
function Get-VMXVersionNumber {
    <# Converts a VMX version string (VMX_22) to its integer value (22). Returns 0 on failure. #>
    param([string]$Version)
    if ($Version -match '^VMX_(\d+)$') { return [int]$Matches[1] }
    return 0
}

function Test-ValidVMXVersion {
    <# Returns $true if the input string matches the VMX_N version format. #>
    param([string]$Version)
    return ($Version -match '^VMX_\d+$')
}
#endregion

#region ── Target Version Selection ───────────────────────────────────────────
function Select-TargetVersion {
    <#
    .SYNOPSIS Displays an interactive version selection menu and returns the chosen
    VMX version string. The menu is built from known versions that are higher than
    the minimum current version across the eligible VM set.

    $MinCurrentNum : Integer of the lowest current HW version across eligible VMs.
                     Only versions above this are presented.
    #>
    param([int]$MinCurrentNum)

    # Build the candidate list: known versions higher than the current minimum, highest first
    $candidates = @($KNOWN_VMX_VERSIONS |
        Where-Object { (Get-VMXVersionNumber $_) -gt $MinCurrentNum } |
        Sort-Object { Get-VMXVersionNumber $_ } -Descending)

    if ($candidates.Count -eq 0) {
        Write-LogError "No known VMX versions are available above VMX_$MinCurrentNum. Use -TargetVersion to specify a version manually."
        return $null
    }

    Write-Host ''
    Write-Host '  ── Target Hardware Version ─────────────────────────────────────' -ForegroundColor DarkCyan
    Write-Host ''
    Write-Host '  Select the target hardware compatibility version for this upgrade:' -ForegroundColor Cyan
    Write-Host ''

    for ($i = 0; $i -lt $candidates.Count; $i++) {
        $ver   = $candidates[$i]
        $label = switch ($ver) {
            'VMX_17' { '(vSphere 7.0)' }
            'VMX_18' { '(vSphere 7.0 U1)' }
            'VMX_19' { '(vSphere 7.0 U2)' }
            'VMX_20' { '(vSphere 8.0)' }
            'VMX_21' { '(vSphere 8.0 U2)' }
            'VMX_22' { '(vSphere 9.0)' }
            default  { '' }
        }
        $line = "  [{0,2}]  {1}  {2}" -f ($i + 1), $ver, $label
        Write-Host $line -ForegroundColor Gray
    }

    Write-Host ''
    $choice = Read-Host "  Enter selection [1-$($candidates.Count)]"

    if ([string]::IsNullOrWhiteSpace($choice)) {
        Write-LogError 'No version selected. A target version is required — re-run and enter a selection.'
        return $null
    }

    $idx = 0
    if ([int]::TryParse($choice.Trim(), [ref]$idx) -and $idx -ge 1 -and $idx -le $candidates.Count) {
        $selected = $candidates[$idx - 1]
        Write-LogInfo "Target version selected: $selected" -nc
        return $selected
    }

    Write-LogError "Invalid selection '$choice'. Please re-run and enter a number between 1 and $($candidates.Count)."
    return $null
}
#endregion

#region ── Command Functions ──────────────────────────────────────────────────
function Send-GuestShutdown {
    <#
    .SYNOPSIS Issues a Guest OS Shutdown via VMware Tools (graceful).
    In dry-run mode, returns $true silently — the caller emits the dry-run message.
    #>
    param([Parameter(Mandatory)][string]$VmId)

    if ($Script:IsDryRun) { return $true }

    try {
        Invoke-VCenterAPI -Method POST -Endpoint "/api/vcenter/vm/$VmId/guest/power" `
            -SessionToken $Script:SessionId -QueryParams @{ action = 'shutdown' } | Out-Null
        return $true
    } catch {
        Write-LogDebug "Guest OS Shutdown failed for '$VmId': $($_.Exception.Message)"
        return $false
    }
}

function Send-PowerOff {
    <#
    .SYNOPSIS Issues a forced Power Off command. Used for both the Power Off
    and Hard Stop escalation steps.
    #>
    param([Parameter(Mandatory)][string]$VmId)

    if ($Script:IsDryRun) { return $true }

    try {
        Invoke-VCenterAPI -Method POST -Endpoint "/api/vcenter/vm/$VmId/power" `
            -SessionToken $Script:SessionId -QueryParams @{ action = 'stop' } | Out-Null
        return $true
    } catch {
        Write-LogDebug "Power Off failed for '$VmId': $($_.Exception.Message)"
        return $false
    }
}

function Search-SnapshotTree {
    <#
    .SYNOPSIS Recursively walks a VIM JSON snapshot tree (rootSnapshotList /
    childSnapshotList) and appends every node whose name matches $SNAPSHOT_NAME
    to the $Matches list. Each matched entry carries name, snapshot MoRef value,
    and the creation timestamp formatted as MM/dd/yy h:mm:ss tt (local time).
    #>
    param(
        [object[]]$SnapshotList,
        [System.Collections.Generic.List[object]]$Matches
    )
    foreach ($node in $SnapshotList) {
        if ($node.name -eq $SNAPSHOT_NAME) {
            $created = if ($node.createTime) {
                try   { [datetime]::Parse($node.createTime).ToLocalTime().ToString('MM/dd/yy h:mm:ss tt') }
                catch { $node.createTime }
            } else { '—' }

            $Matches.Add([PSCustomObject]@{
                name     = $node.name
                snapshot = $node.snapshot.value   # MoRef value e.g. "snapshot-1044"
                created  = $created
            })
        }
        if ($node.childSnapshotList) {
            Search-SnapshotTree -SnapshotList $node.childSnapshotList -Matches $Matches
        }
    }
}

function Get-VMSnapshots {
    <#
    .SYNOPSIS Returns all snapshots for a VM whose name exactly matches $SNAPSHOT_NAME,
    using the VIM JSON API. Recursively searches the full snapshot tree.
    Returns an empty array if no matches exist, $null if the API query failed.
    API: GET /sdk/vim25/{release}/VirtualMachine/{vmMoId}/snapshot
    Response field used: rootSnapshotList[].name / rootSnapshotList[].snapshot.value
    #>
    param([Parameter(Mandatory)][string]$VmId)

    if ([string]::IsNullOrEmpty($Script:ViJsonRelease)) {
        Write-LogDebug "Get-VMSnapshots: VI/JSON release not available."
        return $null
    }

    try {
        $result = Invoke-VCenterAPI -Method GET `
            -Endpoint "/sdk/vim25/$($Script:ViJsonRelease)/VirtualMachine/$VmId/snapshot" `
            -SessionToken $Script:SessionId

        if (-not $result -or -not $result.rootSnapshotList) { return @() }

        $matches = [System.Collections.Generic.List[object]]::new()
        Search-SnapshotTree -SnapshotList $result.rootSnapshotList -Matches $matches
        return @($matches)
    } catch {
        Write-LogDebug "Snapshot query failed for '$VmId': $($_.Exception.Message)"
        return $null
    }
}

function Send-SnapshotRemoval {
    <#
    .SYNOPSIS Dispatches a RemoveSnapshot_Task for the given snapshot MoRef ID and
    returns the task ID string immediately — does not wait for completion.
    Invoke-SnapshotCleanupPhase polls the returned task ID concurrently.
    Returns the task ID string on successful dispatch, $null on failure.
    In dry-run mode, returns a simulated task ID string.
    API: POST /sdk/vim25/{release}/VirtualMachineSnapshot/{snapshotMoId}/RemoveSnapshot_Task
    #>
    param([Parameter(Mandatory)][string]$SnapshotId)

    if ($Script:IsDryRun) { return 'task-dryrun' }

    if ([string]::IsNullOrEmpty($Script:ViJsonRelease)) {
        Write-LogDebug "Send-SnapshotRemoval: VI/JSON release not available."
        return $null
    }

    try {
        $body = @{
            removeChildren = $false
            consolidate    = $true
        }

        $result = Invoke-VCenterAPI -Method POST `
            -Endpoint "/sdk/vim25/$($Script:ViJsonRelease)/VirtualMachineSnapshot/$SnapshotId/RemoveSnapshot_Task" `
            -SessionToken $Script:SessionId `
            -Body $body

        # Response is a task MoRef: { "_typeName": "ManagedObjectReference", "type": "Task", "value": "task-NNN" }
        $taskId = if ($result -and $result.value) { $result.value } else { "$result".Trim('"').Trim() }
        Write-LogDebug "Snapshot removal task dispatched for '$SnapshotId' — task: $taskId"
        return $taskId
    } catch {
        Write-LogDebug "Snapshot removal dispatch failed for '$SnapshotId': $($_.Exception.Message)"
        return $null
    }
}

function Get-ViJsonRelease {
    <#
    .SYNOPSIS Probes the VI/JSON API to determine the highest supported release
    schema on the connected vCenter. Tries candidates in descending version order
    and returns the first that responds successfully. Returns $null if none found.
    #>
    $candidates = @('9.1.0.0', '9.0.0.0', '8.0.3.0', '8.0.2.0', '8.0.1.0')

    foreach ($release in $candidates) {
        try {
            Invoke-VCenterAPI -Method GET `
                -Endpoint "/sdk/vim25/$release/ServiceInstance/ServiceInstance/content" `
                -SessionToken $Script:SessionId | Out-Null
            Write-LogDebug "VI/JSON release schema probe succeeded: $release"
            return $release
        } catch {
            $msg  = $_.Exception.Message
            $code = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 0 }
            if ($msg -match 'Unknown release ID' -or $code -in @(400, 404)) {
                Write-LogDebug "VI/JSON release $release not supported — trying next."
                continue
            }
            Write-LogDebug "VI/JSON release probe error for $release ($msg) — trying next."
            continue
        }
    }
    return $null
}

function Wait-VimTask {
    <#
    .SYNOPSIS Polls a VIM JSON API task until it reaches success or error state,
    or until $Timeout seconds elapse. Returns $true on success, $false otherwise.
    #>
    param(
        [Parameter(Mandatory)][string]$TaskId,
        [int]$Timeout = $TIMEOUT_SNAPSHOT
    )

    $deadline = (Get-Date).AddSeconds($Timeout)

    while ((Get-Date) -lt $deadline) {
        try {
            $info  = Invoke-VCenterAPI -Method GET `
                -Endpoint "/sdk/vim25/$($Script:ViJsonRelease)/Task/$TaskId/info" `
                -SessionToken $Script:SessionId
            $state = if ($info -and $info.state) { $info.state } else { 'unknown' }
            Write-LogDebug "Task '$TaskId' state: $state"

            switch ($state) {
                'success' { return $true }
                'error'   {
                    $reason = if ($info.error -and $info.error.localizedMessage) {
                        $info.error.localizedMessage
                    } else { 'Unknown error' }
                    Write-LogDebug "Task '$TaskId' failed: $reason"
                    return $false
                }
                default   { Start-Sleep -Seconds $POLL_INTERVAL_FAST }
            }
        } catch {
            Write-LogDebug "Task poll error for '$TaskId': $($_.Exception.Message)"
            Start-Sleep -Seconds $POLL_INTERVAL_FAST
        }
    }

    Write-LogDebug "Task '$TaskId' did not complete within ${Timeout}s."
    return $false
}

function Send-VMSnapshot {
    <#
    .SYNOPSIS Creates a crash-consistent pre-upgrade snapshot using the VIM JSON
    CreateSnapshotEx_Task API. Returns the task ID string on success, $null on failure.
    In dry-run mode, returns a simulated ID string without contacting vCenter.
    API: POST /sdk/vim25/{release}/VirtualMachine/{vmMoId}/CreateSnapshotEx_Task
    #>
    param([Parameter(Mandatory)][string]$VmId)

    if ($Script:IsDryRun) { return 'snapshot-dryrun' }

    if ([string]::IsNullOrEmpty($Script:ViJsonRelease)) {
        Write-LogDebug "Send-VMSnapshot: VI/JSON release not available — cannot create snapshot."
        return $null
    }

    try {
        $body = @{
            name        = $SNAPSHOT_NAME
            description = $SNAPSHOT_NAME
            memory      = $false
        }

        $result = Invoke-VCenterAPI -Method POST `
            -Endpoint "/sdk/vim25/$($Script:ViJsonRelease)/VirtualMachine/$VmId/CreateSnapshotEx_Task" `
            -SessionToken $Script:SessionId `
            -Body $body

        # Response is a task MoRef: { "_typeName": "ManagedObjectReference", "type": "Task", "value": "task-NNN" }
        $taskId = if ($result -and $result.value) { $result.value } else { "$result".Trim('"').Trim() }
        Write-LogDebug "Snapshot task dispatched for '$VmId' — task: $taskId"

        if (Wait-VimTask -TaskId $taskId) {
            return $taskId
        }
        Write-LogDebug "Snapshot task '$taskId' did not complete successfully for '$VmId'."
        return $null
    } catch {
        Write-LogDebug "Snapshot creation failed for '$VmId': $($_.Exception.Message)"
        return $null
    }
}

function Send-HardwareUpgrade {
    <#
    .SYNOPSIS Sends the hardware version upgrade command via the VIM JSON
    UpgradeVM_Task API and waits for the async task to complete before returning.
    Returns $true on success, $false on failure.
    API: POST /sdk/vim25/{release}/VirtualMachine/{vmMoId}/UpgradeVM_Task
    Body: { "version": "vmx-22" }
    Note: The VIM JSON API expects lowercase hyphenated version format (vmx-22),
    not the VMX_22 format used internally by this script for display and comparison.
    #>
    param(
        [Parameter(Mandatory)][string]$VmId,
        [Parameter(Mandatory)][string]$Version
    )

    if ($Script:IsDryRun) { return $true }

    if ([string]::IsNullOrEmpty($Script:ViJsonRelease)) {
        Write-LogDebug "Send-HardwareUpgrade: VI/JSON release not available."
        return $false
    }

    try {
        # Convert VMX_22 → vmx-22 as required by the VIM JSON API
        $apiVersion = $Version.ToLower().Replace('vmx_', 'vmx-')

        $body   = @{ version = $apiVersion }
        $result = Invoke-VCenterAPI -Method POST `
            -Endpoint "/sdk/vim25/$($Script:ViJsonRelease)/VirtualMachine/$VmId/UpgradeVM_Task" `
            -SessionToken $Script:SessionId `
            -Body $body

        # Response is a task MoRef: { "_typeName": "ManagedObjectReference", "type": "Task", "value": "task-NNN" }
        $taskId = if ($result -and $result.value) { $result.value } else { "$result".Trim('"').Trim() }
        Write-LogDebug "Hardware upgrade task dispatched for '$VmId' (target: $apiVersion) — task: $taskId"

        if (Wait-VimTask -TaskId $taskId -Timeout $TIMEOUT_UPGRADE) {
            return $true
        }
        Write-LogDebug "Hardware upgrade task '$taskId' did not complete successfully for '$VmId'."
        return $false
    } catch {
        Write-LogDebug "Hardware upgrade command failed for '$VmId' (target: $Version): $($_.Exception.Message)"
        return $false
    }
}

function Send-PowerOn {
    <#
    .SYNOPSIS Issues a Power On command for the specified VM.
    In dry-run mode, returns $true silently.
    #>
    param([Parameter(Mandatory)][string]$VmId)

    if ($Script:IsDryRun) { return $true }

    try {
        Invoke-VCenterAPI -Method POST -Endpoint "/api/vcenter/vm/$VmId/power" `
            -SessionToken $Script:SessionId -QueryParams @{ action = 'start' } | Out-Null
        return $true
    } catch {
        Write-LogDebug "Power On failed for '$VmId': $($_.Exception.Message)"
        return $false
    }
}
#endregion

#region ── Snapshot Phase ─────────────────────────────────────────────────────
function Invoke-SnapshotPhase {
    <#
    .SYNOPSIS Captures a pre-upgrade snapshot for each VM in $VMList. Runs sequentially
    since vCenter snapshot creation is synchronous. Returns an ordered hashtable keyed
    by VmId with the per-VM snapshot result (RES_COMPLETE or RES_ERROR).

    VMs whose snapshots fail are identified by the caller and excluded from upgrade.
    #>
    param([Parameter(Mandatory)][object[]]$VMList)

    $totalVMs = $VMList.Count
    $results  = [ordered]@{}
    $seqNum   = 0

    Write-Host ''
    Write-Host "  ── Snapshot Phase $('─' * 50)" -ForegroundColor DarkCyan
    Write-Host "  Scope      : $totalVMs VM(s)" -ForegroundColor Cyan
    Write-Host ''
    Write-LogInfo "Snapshot phase starting — $totalVMs VM(s) in scope." -nc

    foreach ($vm in $VMList) {
        $seqNum++
        $pos = "[$seqNum/$totalVMs]"

        Write-Progress -Activity 'Snapshot Phase' `
            -Status "$seqNum/$totalVMs — $($vm.VmName)" `
            -PercentComplete ([int](($seqNum / $totalVMs) * 100))

        if ($Script:IsDryRun) {
            Write-LogDryRun "$pos $($vm.VmName) ($($vm.VmId)): Would create snapshot '$SNAPSHOT_NAME'."
            $results[$vm.VmId] = $RES_COMPLETE
            continue
        }

        Write-LogInfo "$pos $($vm.VmName): Creating snapshot '$SNAPSHOT_NAME'..."
        $snapshotId = Send-VMSnapshot -VmId $vm.VmId

        if ($snapshotId) {
            Write-LogOK "$pos $($vm.VmName): Snapshot created (ID: $snapshotId)."
            $results[$vm.VmId] = $RES_COMPLETE
        } else {
            Write-LogError "$pos $($vm.VmName): Snapshot creation failed — this VM will be excluded from upgrade."
            $results[$vm.VmId] = $RES_ERROR
        }
    }

    Write-Progress -Activity 'Snapshot Phase' -Completed

    $succeeded = @($results.GetEnumerator() | Where-Object { $_.Value -eq $RES_COMPLETE }).Count
    $failed    = @($results.GetEnumerator() | Where-Object { $_.Value -eq $RES_ERROR   }).Count
    Write-LogInfo "Snapshot phase complete — $succeeded succeeded, $failed failed." -nc

    return $results
}
#endregion

#region ── Snapshot Cleanup Phase ────────────────────────────────────────────
function Invoke-SnapshotCleanupPhase {
    <#
    .SYNOPSIS Removes pre-upgrade snapshots concurrently, dispatching
    RemoveSnapshot_Task for each item and polling task status — respecting
    the same global, per-host, and per-datastore concurrency gates used by
    the power-down and power-on phases.

    $RemovalList entries must contain:
        VmId, VmName, HostId, HostName, DatastoreId, DatastoreName,
        SnapshotId, SnapshotName
    Returns an array of per-item result objects.
    #>
    param([Parameter(Mandatory)][object[]]$RemovalList)

    $totalItems = $RemovalList.Count

    Write-Host ''
    Write-Host "  ── Snapshot Removal Phase $('─' * 50)" -ForegroundColor DarkCyan
    Write-Host "  Scope      : $totalItems snapshot(s)" -ForegroundColor Cyan
    $concGlobalStr = if ($ConcurrentGlobal -eq 0) { 'Unlimited' } else { "$ConcurrentGlobal" }
    Write-Host "  Concurrency: Global=$concGlobalStr | Per-Host=$ConcurrentHost | Per-Datastore=$ConcurrentDatastore" -ForegroundColor Cyan
    Write-Host ''
    Write-LogInfo "Snapshot removal phase starting — $totalItems snapshot(s) in scope." -nc

    $tracker   = [ordered]@{}
    $seqNum    = 0

    foreach ($item in $RemovalList) {
        $seqNum++
        $tracker[$item.SnapshotId] = [PSCustomObject]@{
            SeqNum             = $seqNum
            VmId               = $item.VmId
            VmName             = $item.VmName
            HostId             = $item.HostId
            HostName           = $item.HostName
            DatastoreId        = $item.DatastoreId
            DatastoreName      = $item.DatastoreName
            SnapshotId         = $item.SnapshotId
            SnapshotName       = $item.SnapshotName
            State              = $ST_PENDING
            TaskId             = $null
            PhaseStart         = $null
            RetryCount         = 0
            NextRetryAt        = $null
            OperationStartedAt = $null
            Result             = $null
            CompletedAt        = $null
            Notes              = $null
        }
    }

    $completedCount      = 0
    $iteration           = 0
    $completionDurations = [System.Collections.Generic.List[double]]::new()

    while ($true) {
        $iteration++

        # ── 1. Poll active entries for task completion or timeout ──────────
        $activeNow = @($tracker.Values | Where-Object { $_.State -eq $ST_ACTIVE })

        foreach ($entry in $activeNow) {
            $elapsed = ((Get-Date) - $entry.PhaseStart).TotalSeconds

            if ($Script:IsDryRun) {
                $now               = Get-Date
                $entry.State       = $ST_COMPLETE
                $entry.Result      = $RES_COMPLETE
                $entry.CompletedAt = $now.ToString('MM/dd/yyyy HH:mm:ss')
                $completedCount++
                $completionDurations.Add(($now - $entry.OperationStartedAt).TotalSeconds)
                continue
            }

            $taskDone   = $false
            $taskFailed = $false

            try {
                $info  = Invoke-VCenterAPI -Method GET `
                    -Endpoint "/sdk/vim25/$($Script:ViJsonRelease)/Task/$($entry.TaskId)/info" `
                    -SessionToken $Script:SessionId
                $state = if ($info -and $info.state) { $info.state } else { 'unknown' }
                Write-LogDebug "Task '$($entry.TaskId)' state: $state"

                switch ($state) {
                    'success' { $taskDone = $true }
                    'error'   {
                        $entry.Notes = if ($info.error -and $info.error.localizedMessage) {
                            $info.error.localizedMessage
                        } else { 'Task reported error.' }
                        $taskFailed = $true
                    }
                }
            } catch {
                Write-LogDebug "Task poll error for '$($entry.TaskId)': $($_.Exception.Message)"
            }

            if ($taskDone) {
                $now               = Get-Date
                $entry.State       = $ST_COMPLETE
                $entry.Result      = $RES_COMPLETE
                $entry.CompletedAt = $now.ToString('MM/dd/yyyy HH:mm:ss')
                $completedCount++
                $completionDurations.Add(($now - $entry.OperationStartedAt).TotalSeconds)
                Write-LogOK "[$($entry.SeqNum)/$totalItems] $($entry.VmName): Snapshot '$($entry.SnapshotName)' removed."
            } elseif ($taskFailed) {
                $entry.State       = $ST_FAILED
                $entry.Result      = $RES_ERROR
                $entry.CompletedAt = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
                $completedCount++
                Write-LogError "[$($entry.SeqNum)/$totalItems] $($entry.VmName): Snapshot removal failed — $($entry.Notes)"
            } elseif ($elapsed -gt $TIMEOUT_SNAPSHOT) {
                $entry.State       = $ST_FAILED
                $entry.Result      = $RES_ERROR
                $entry.Notes       = "Task did not complete within ${TIMEOUT_SNAPSHOT}s."
                $entry.CompletedAt = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
                $completedCount++
                Write-LogError "[$($entry.SeqNum)/$totalItems] $($entry.VmName): Snapshot removal timed out."
            }
        }

        # ── 2. Dispatch pending items within concurrency limits ────────────
        $pendingNow = @($tracker.Values | Where-Object { $_.State -eq $ST_PENDING })

        foreach ($entry in $pendingNow) {
            if ($entry.NextRetryAt -and (Get-Date) -lt $entry.NextRetryAt) { continue }

            $activeAll  = @($tracker.Values | Where-Object { $_.State -eq $ST_ACTIVE })
            $activeHost = @($activeAll | Where-Object { $_.HostId -eq $entry.HostId })
            $activeDS   = @($activeAll | Where-Object { $_.DatastoreId -eq $entry.DatastoreId })

            if ($ConcurrentGlobal -gt 0 -and $activeAll.Count  -ge $ConcurrentGlobal) { continue }
            if ($activeHost.Count -ge $ConcurrentHost)                                  { continue }
            if ($activeDS.Count   -ge $ConcurrentDatastore)                             { continue }

            $pos         = "[$($entry.SeqNum)/$totalItems]"
            $retrySuffix = if ($entry.RetryCount -gt 0) { " (retry $($entry.RetryCount)/$MAX_RETRIES)" } else { '' }

            if ($Script:IsDryRun) {
                Write-LogDryRun "$pos $($entry.VmName): Would remove snapshot '$($entry.SnapshotName)' (ID: $($entry.SnapshotId))${retrySuffix}."
                $now                      = Get-Date
                $entry.State              = $ST_ACTIVE
                $entry.PhaseStart         = $now
                $entry.OperationStartedAt = $now
                $entry.TaskId             = 'task-dryrun'
                continue
            }

            Write-LogInfo "$pos $($entry.VmName): Removing snapshot '$($entry.SnapshotName)' (ID: $($entry.SnapshotId))${retrySuffix}..."
            $taskId = Send-SnapshotRemoval -SnapshotId $entry.SnapshotId

            if ($taskId) {
                $now                      = Get-Date
                $entry.TaskId             = $taskId
                $entry.PhaseStart         = $now
                $entry.OperationStartedAt = $now
                $entry.State              = $ST_ACTIVE
                $entry.NextRetryAt        = $null
            } else {
                if ($entry.RetryCount -lt $MAX_RETRIES) {
                    $entry.RetryCount++
                    $entry.NextRetryAt = (Get-Date).AddSeconds($RETRY_DELAY)
                    Write-LogWarn "$pos $($entry.VmName): Dispatch failed. Retry $($entry.RetryCount)/$MAX_RETRIES in ${RETRY_DELAY}s."
                } else {
                    $entry.State       = $ST_FAILED
                    $entry.Result      = $RES_ERROR
                    $entry.Notes       = "Could not dispatch after $MAX_RETRIES retries."
                    $entry.CompletedAt = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
                    $completedCount++
                    Write-LogError "$pos $($entry.VmName): Snapshot removal dispatch failed after all retries."
                }
            }
        }

        # ── 3. Progress display with rolling ETA ───────────────────────────
        $stillAct = @($tracker.Values | Where-Object { $_.State -eq $ST_ACTIVE  }).Count
        $stillPen = @($tracker.Values | Where-Object { $_.State -eq $ST_PENDING }).Count
        $pct      = if ($totalItems -gt 0) { [int](($completedCount / $totalItems) * 100) } else { 100 }

        $etaStr = ''
        if ($completionDurations.Count -gt 0 -and ($stillAct + $stillPen) -gt 0) {
            $avgSec = ($completionDurations | Measure-Object -Average).Average
            $remSec = [int]($avgSec * ($stillAct + $stillPen))
            $etaAt  = (Get-Date).AddSeconds($remSec)
            $remStr = if ($remSec -ge 3600) {
                          '{0}h {1:D2}m {2:D2}s' -f [int]($remSec / 3600), [int](($remSec % 3600) / 60), ($remSec % 60)
                      } elseif ($remSec -ge 60) {
                          '{0}m {1:D2}s' -f [int]($remSec / 60), ($remSec % 60)
                      } else { "${remSec}s" }
            $etaStr = " | Avg: $([int]$avgSec)s/snapshot | ETA: ~$remStr ($($etaAt.ToString('HH:mm:ss')))"
        }

        Write-Progress -Activity 'Snapshot Removal' `
            -Status "$completedCount/$totalItems complete | $stillAct active | $stillPen pending | $pct%$etaStr" `
            -PercentComplete $pct

        Write-LogDebug "Poll #$iteration — Complete: $completedCount | Active: $stillAct | Pending: $stillPen"

        if (($stillAct + $stillPen) -eq 0) { break }

        if ($Script:IsDryRun) {
            Write-LogDryRun "Would pause ${POLL_INTERVAL}s while waiting for snapshot removal tasks."
        } else {
            Start-Sleep -Seconds $POLL_INTERVAL
        }
    }

    Write-Progress -Activity 'Snapshot Removal' -Completed

    $succeeded = @($tracker.Values | Where-Object { $_.Result -eq $RES_COMPLETE }).Count
    $failed    = @($tracker.Values | Where-Object { $_.Result -eq $RES_ERROR    }).Count
    Write-LogInfo "Snapshot removal phase complete — $succeeded succeeded, $failed failed." -nc

    return @($tracker.Values)
}
#endregion

#region ── Power-On Phase ────────────────────────────────────────────────────
function Invoke-PowerOnPhase {
    <#
    .SYNOPSIS Powers on a list of VMs concurrently after the upgrade phase, using the
    same global, per-host, and per-datastore throttling gates as the power-down phase.
    Applied to all VMs that were successfully powered down by Phase 1, regardless of
    upgrade outcome.
    Returns an array of per-VM result objects.
    #>
    param([Parameter(Mandatory)][object[]]$VMList)

    $totalVMs    = $VMList.Count
    $targetState = 'POWERED_ON'
    $TIMEOUT_POWERON = 120

    Write-Host ''
    Write-Host "  ── Power-On Phase $('─' * 50)" -ForegroundColor DarkCyan
    Write-Host "  Scope      : $totalVMs VM(s)" -ForegroundColor Cyan
    $concGlobalStr = if ($ConcurrentGlobal -eq 0) { 'Unlimited' } else { "$ConcurrentGlobal" }
    Write-Host "  Concurrency: Global=$concGlobalStr | Per-Host=$ConcurrentHost | Per-Datastore=$ConcurrentDatastore" -ForegroundColor Cyan
    Write-Host ''
    Write-LogInfo "Power-On phase starting — $totalVMs VM(s) in scope." -nc

    $tracker        = [ordered]@{}
    $seqNum         = 0
    $completedCount = 0
    $completionDurations = [System.Collections.Generic.List[double]]::new()

    foreach ($vm in $VMList) {
        $seqNum++
        $tracker[$vm.VmId] = [PSCustomObject]@{
            SeqNum             = $seqNum
            VmId               = $vm.VmId
            VmName             = $vm.VmName
            HostId             = $vm.HostId
            HostName           = $vm.HostName
            DatastoreId        = $vm.DatastoreId
            DatastoreName      = $vm.DatastoreName
            State              = $ST_PENDING
            PhaseStart         = $null
            RetryCount         = 0
            NextRetryAt        = $null
            OperationStartedAt = $null
            Result             = $null
            CompletedAt        = $null
            Notes              = $null
        }
    }

    $iteration = 0

    while ($true) {
        $iteration++

        # 1. Poll active VMs for power state confirmation
        $activeNow = @($tracker.Values | Where-Object { $_.State -eq $ST_ACTIVE })

        foreach ($entry in $activeNow) {
            $elapsed    = ((Get-Date) - $entry.PhaseStart).TotalSeconds
            $powerState = if ($Script:IsDryRun) { $targetState } else { Get-VMPowerState -VmId $entry.VmId }

            if ($powerState -eq $targetState) {
                $now               = Get-Date
                $entry.State       = $ST_COMPLETE
                $entry.Result      = $RES_COMPLETE
                $entry.CompletedAt = $now.ToString('MM/dd/yyyy HH:mm:ss')
                $completedCount++
                $completionDurations.Add(($now - $entry.OperationStartedAt).TotalSeconds)
                if (-not $Script:IsDryRun) {
                    Write-LogOK "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): Powered on."
                }
            } elseif ($elapsed -gt $TIMEOUT_POWERON) {
                $entry.State  = $ST_FAILED
                $entry.Result = $RES_ERROR
                $entry.Notes  = "Did not reach POWERED_ON within ${TIMEOUT_POWERON}s."
                $entry.CompletedAt = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
                $completedCount++
                Write-LogError "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): Power-on timed out."
            }
        }

        # 2. Start pending VMs within concurrency limits
        $pendingNow = @($tracker.Values | Where-Object { $_.State -eq $ST_PENDING })

        foreach ($entry in $pendingNow) {
            if ($entry.NextRetryAt -and (Get-Date) -lt $entry.NextRetryAt) { continue }

            $activeAll  = @($tracker.Values | Where-Object { $_.State -eq $ST_ACTIVE })
            $activeHost = @($activeAll       | Where-Object { $_.HostId -eq $entry.HostId })
            $activeDS   = @($activeAll       | Where-Object { $_.DatastoreId -eq $entry.DatastoreId })

            if ($ConcurrentGlobal -gt 0 -and $activeAll.Count  -ge $ConcurrentGlobal) { continue }
            if ($activeHost.Count -ge $ConcurrentHost)                                  { continue }
            if ($activeDS.Count   -ge $ConcurrentDatastore)                             { continue }

            $pos         = "[$($entry.SeqNum)/$totalVMs]"
            $vmLabel     = "$($entry.VmName) ($($entry.VmId))"
            $retrySuffix = if ($entry.RetryCount -gt 0) { " (retry $($entry.RetryCount)/$MAX_RETRIES)" } else { '' }

            if ($Script:IsDryRun) {
                Write-LogDryRun "$pos ${vmLabel}: Would send Power On${retrySuffix}."
            } else {
                Write-LogInfo "$pos $($entry.VmName): Sending Power On${retrySuffix}."
            }

            $cmdOk = Send-PowerOn -VmId $entry.VmId

            if ($cmdOk) {
                $now                      = Get-Date
                $entry.PhaseStart         = $now
                $entry.OperationStartedAt = $now
                $entry.State              = $ST_ACTIVE
                $entry.NextRetryAt        = $null
            } else {
                if ($entry.RetryCount -lt $MAX_RETRIES) {
                    $entry.RetryCount++
                    $entry.NextRetryAt = (Get-Date).AddSeconds($RETRY_DELAY)
                    Write-LogWarn "$pos $($entry.VmName): Command failed. Retry $($entry.RetryCount)/$MAX_RETRIES in ${RETRY_DELAY}s."
                } else {
                    $entry.State  = $ST_FAILED
                    $entry.Result = $RES_ERROR
                    $entry.Notes  = "Power-on command could not be sent after $MAX_RETRIES retries."
                    $entry.CompletedAt = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
                    $completedCount++
                    Write-LogError "$pos $($entry.VmName): Power-on failed after all retries."
                }
            }
        }

        # 3. Progress display
        $stillAct = @($tracker.Values | Where-Object { $_.State -eq $ST_ACTIVE  }).Count
        $stillPen = @($tracker.Values | Where-Object { $_.State -eq $ST_PENDING }).Count
        $pct      = if ($totalVMs -gt 0) { [int](($completedCount / $totalVMs) * 100) } else { 100 }

        $etaStr = ''
        if ($completionDurations.Count -gt 0 -and ($stillAct + $stillPen) -gt 0) {
            $avgSec = ($completionDurations | Measure-Object -Average).Average
            $remSec = [int]($avgSec * ($stillAct + $stillPen))
            $etaAt  = (Get-Date).AddSeconds($remSec)
            $remStr = if ($remSec -ge 3600) {
                          '{0}h {1:D2}m {2:D2}s' -f [int]($remSec / 3600), [int](($remSec % 3600) / 60), ($remSec % 60)
                      } elseif ($remSec -ge 60) {
                          '{0}m {1:D2}s' -f [int]($remSec / 60), ($remSec % 60)
                      } else {
                          "${remSec}s"
                      }
            $etaStr = " | Avg: $([int]$avgSec)s/VM | ETA: ~$remStr ($($etaAt.ToString('HH:mm:ss')))"
        }

        Write-Progress -Activity 'Power-On Operations' `
            -Status "$completedCount/$totalVMs complete | $stillAct active | $stillPen pending | $pct%$etaStr" `
            -PercentComplete $pct

        Write-LogDebug "Poll #$iteration — Complete: $completedCount | Active: $stillAct | Pending: $stillPen"

        if (($stillAct + $stillPen) -eq 0) { break }

        if ($Script:IsDryRun) {
            Write-LogDryRun "Would pause ${POLL_INTERVAL}s while waiting for VM power state changes."
        } else {
            Start-Sleep -Seconds $POLL_INTERVAL
        }
    }

    Write-Progress -Activity 'Power-On Operations' -Completed

    $succeeded = @($tracker.Values | Where-Object { $_.Result -eq $RES_COMPLETE }).Count
    $failed    = @($tracker.Values | Where-Object { $_.Result -eq $RES_ERROR    }).Count
    Write-LogInfo "Power-on phase complete — $succeeded succeeded, $failed failed." -nc

    return @($tracker.Values)
}
#endregion

#region ── Power-Down Phase ───────────────────────────────────────────────────
function Invoke-PowerDownPhase {
    <#
    .SYNOPSIS Executes power-down operations across a list of resolved VMs with
    configurable global, per-host, and per-datastore concurrency limits.

    Escalation sequence per VM:
        1. Guest OS Shutdown  (graceful, skipped if VMware Tools not running)
        2. Power Off          (forced, if step 1 times out or is unavailable)
        3. Hard Stop          (final attempt, if step 2 times out)

    Returns an array of result objects (same structure as Invoke-UpgradePhase).
    #>
    param([Parameter(Mandatory)][object[]]$VMList)

    $totalVMs    = $VMList.Count
    $targetState = 'POWERED_OFF'

    Write-Host ''
    Write-Host "  ── Power-Down Phase $('─' * 50)" -ForegroundColor DarkCyan
    Write-Host "  Scope      : $totalVMs VM(s)" -ForegroundColor Cyan
    $concGlobalStr = if ($ConcurrentGlobal -eq 0) { 'Unlimited' } else { "$ConcurrentGlobal" }
    Write-Host "  Concurrency: Global=$concGlobalStr | Per-Host=$ConcurrentHost | Per-Datastore=$ConcurrentDatastore" -ForegroundColor Cyan
    Write-Host ''
    Write-LogInfo "Power-Down phase starting — $totalVMs VM(s) in scope." -nc

    # Build per-VM operation tracker
    $tracker    = [ordered]@{}
    $seqNum     = 0

    foreach ($vm in $VMList) {
        $seqNum++
        $tracker[$vm.VmId] = [PSCustomObject]@{
            SeqNum             = $seqNum
            VmId               = $vm.VmId
            VmName             = $vm.VmName
            HostId             = $vm.HostId
            HostName           = $vm.HostName
            DatastoreId        = $vm.DatastoreId
            DatastoreName      = $vm.DatastoreName
            State              = $ST_PENDING
            Phase              = $null
            PhaseStart         = $null
            RetryCount         = 0
            NextRetryAt        = $null
            OperationStartedAt = $null
            Result             = $null
            CompletedAt        = $null
            Notes              = $null
        }
    }

    $completedCount      = 0
    $iteration           = 0
    $completionDurations = [System.Collections.Generic.List[double]]::new()

    # Main polling loop
    while ($true) {
        $iteration++

        # 1. Poll all active VMs for state changes and phase escalation
        $activeNow = @($tracker.Values | Where-Object { $_.State -eq $ST_ACTIVE })

        foreach ($entry in $activeNow) {
            $elapsed    = ((Get-Date) - $entry.PhaseStart).TotalSeconds
            $powerState = if ($Script:IsDryRun) { $targetState } else { Get-VMPowerState -VmId $entry.VmId }

            if ($powerState -eq $targetState) {
                $now               = Get-Date
                $entry.State       = $ST_COMPLETE
                $entry.Result      = $RES_COMPLETE
                $entry.CompletedAt = $now.ToString('MM/dd/yyyy HH:mm:ss')
                $completedCount++
                if ($entry.OperationStartedAt) {
                    $completionDurations.Add(($now - $entry.OperationStartedAt).TotalSeconds)
                }
                if (-not $Script:IsDryRun) {
                    Write-LogOK "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): Powered off."
                }

            } else {
                switch ($entry.Phase) {

                    $PH_GUEST_SHUTDOWN {
                        if ($elapsed -gt $TIMEOUT_GUEST_SHUTDOWN) {
                            Write-LogWarn "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): Guest OS Shutdown timed out (${elapsed}s). Issuing Power Off."
                            $ok = Send-PowerOff -VmId $entry.VmId
                            $entry.Phase      = $PH_POWER_OFF
                            $entry.PhaseStart = Get-Date
                            if (-not $ok) {
                                Write-LogDebug "$($entry.VmName): Power Off command failed — will retry on next poll."
                            }
                        }
                    }

                    $PH_POWER_OFF {
                        if ($elapsed -gt $TIMEOUT_POWER_OFF) {
                            Write-LogWarn "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): Power Off timed out (${elapsed}s). Issuing Hard Stop."
                            $ok = Send-PowerOff -VmId $entry.VmId
                            if ($ok) {
                                $entry.Phase      = $PH_HARD_STOP
                                $entry.PhaseStart = Get-Date
                            } else {
                                $entry.State  = $ST_FAILED
                                $entry.Result = $RES_ERROR
                                $entry.Notes  = 'Hard Stop command could not be sent.'
                                $completedCount++
                                Write-LogError "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): Hard Stop command failed. Manual intervention required."
                            }
                        }
                    }

                    $PH_HARD_STOP {
                        if ($elapsed -gt $TIMEOUT_POWER_OFF) {
                            $entry.State  = $ST_FAILED
                            $entry.Result = $RES_ERROR
                            $entry.Notes  = 'VM did not respond to Hard Stop within timeout.'
                            $completedCount++
                            Write-LogError "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): Hard Stop timed out. Marking failed — manual intervention required."
                        }
                    }
                }
            }
        }

        # 2. Start pending VMs within concurrency limits
        $pendingNow = @($tracker.Values | Where-Object { $_.State -eq $ST_PENDING })

        foreach ($entry in $pendingNow) {
            if ($entry.NextRetryAt -and (Get-Date) -lt $entry.NextRetryAt) { continue }

            $activeAll  = @($tracker.Values | Where-Object { $_.State -eq $ST_ACTIVE })
            $activeHost = @($activeAll       | Where-Object { $_.HostId -eq $entry.HostId })
            $activeDS   = @($activeAll       | Where-Object { $_.DatastoreId -eq $entry.DatastoreId })

            if ($ConcurrentGlobal -gt 0 -and $activeAll.Count  -ge $ConcurrentGlobal) { continue }
            if ($activeHost.Count -ge $ConcurrentHost)                                  { continue }
            if ($activeDS.Count   -ge $ConcurrentDatastore)                             { continue }

            $pos         = "[$($entry.SeqNum)/$totalVMs]"
            $vmLabel     = "$($entry.VmName) ($($entry.VmId))"
            $retrySuffix = if ($entry.RetryCount -gt 0) { " (retry $($entry.RetryCount)/$MAX_RETRIES)" } else { '' }

            $curState = if ($Script:IsDryRun) { 'POWERED_ON' } else { Get-VMPowerState -VmId $entry.VmId }

            if ($curState -eq 'POWERED_OFF') {
                # VM is already off — skip the power-down step
                $entry.State       = $ST_COMPLETE
                $entry.Result      = $RES_SKIPPED
                $entry.Notes       = 'Already powered off.'
                $entry.CompletedAt = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
                $completedCount++
                Write-LogInfo "$pos $($entry.VmName): Already powered off — skipping power-down."
                continue
            }

            $toolsRunning = if ($Script:IsDryRun) { $true } else { Get-VMToolsRunning -VmId $entry.VmId }
            $startPhase   = $null
            $cmdOk        = $false

            if ($toolsRunning) {
                if ($Script:IsDryRun) {
                    Write-LogDryRun "$pos ${vmLabel}: Would send Guest OS Shutdown${retrySuffix}."
                } else {
                    Write-LogInfo "$pos $($entry.VmName): Sending Guest OS Shutdown${retrySuffix}."
                }
                $cmdOk      = Send-GuestShutdown -VmId $entry.VmId
                $startPhase = $PH_GUEST_SHUTDOWN
            } else {
                if ($Script:IsDryRun) {
                    Write-LogDryRun "$pos ${vmLabel}: VMware Tools not running — Would send Power Off${retrySuffix}."
                } else {
                    Write-LogInfo "$pos $($entry.VmName): VMware Tools not running — issuing Power Off${retrySuffix}."
                }
                $cmdOk      = Send-PowerOff -VmId $entry.VmId
                $startPhase = $PH_POWER_OFF
            }

            if ($cmdOk) {
                $now                       = Get-Date
                $entry.Phase               = $startPhase
                $entry.PhaseStart          = $now
                $entry.OperationStartedAt  = $now
                $entry.State               = $ST_ACTIVE
                $entry.NextRetryAt         = $null
            } else {
                if ($entry.RetryCount -lt $MAX_RETRIES) {
                    $entry.RetryCount++
                    $entry.NextRetryAt = (Get-Date).AddSeconds($RETRY_DELAY)
                    Write-LogWarn "$pos $($entry.VmName): Command failed. Retry $($entry.RetryCount)/$MAX_RETRIES in ${RETRY_DELAY}s."
                } else {
                    $entry.State  = $ST_FAILED
                    $entry.Result = $RES_ERROR
                    $entry.Notes  = "Command could not be sent after $MAX_RETRIES retries."
                    $completedCount++
                    Write-LogError "$pos $($entry.VmName): Command failed after all retries. Skipping."
                }
            }
        }

        # 3. Update progress display with rolling ETA
        $stillAct = @($tracker.Values | Where-Object { $_.State -eq $ST_ACTIVE  }).Count
        $stillPen = @($tracker.Values | Where-Object { $_.State -eq $ST_PENDING }).Count
        $pct      = if ($totalVMs -gt 0) { [int](($completedCount / $totalVMs) * 100) } else { 100 }

        $etaStr = ''
        if ($completionDurations.Count -gt 0 -and ($stillAct + $stillPen) -gt 0) {
            $avgSec = ($completionDurations | Measure-Object -Average).Average
            $remSec = [int]($avgSec * ($stillAct + $stillPen))
            $etaAt  = (Get-Date).AddSeconds($remSec)
            $remStr = if ($remSec -ge 3600) {
                          '{0}h {1:D2}m {2:D2}s' -f [int]($remSec / 3600), [int](($remSec % 3600) / 60), ($remSec % 60)
                      } elseif ($remSec -ge 60) {
                          '{0}m {1:D2}s' -f [int]($remSec / 60), ($remSec % 60)
                      } else {
                          "${remSec}s"
                      }
            $etaStr = " | Avg: $([int]$avgSec)s/VM | ETA: ~$remStr ($($etaAt.ToString('HH:mm:ss')))"
        } elseif ($completionDurations.Count -eq 0 -and ($stillAct + $stillPen) -gt 0) {
            $etaStr = ' | ETA: calculating...'
        }

        Write-Progress -Activity 'Power-Down Operations' `
            -Status "$completedCount/$totalVMs complete | $stillAct active | $stillPen pending | $pct%$etaStr" `
            -PercentComplete $pct

        Write-LogDebug "Poll #$iteration — Complete: $completedCount | Active: $stillAct | Pending: $stillPen"

        # 4. Exit when all VMs have reached a terminal state
        if (($stillAct + $stillPen) -eq 0) { break }

        if ($Script:IsDryRun) {
            Write-LogDryRun "Would pause ${POLL_INTERVAL}s while waiting for VM power state changes."
        } else {
            Start-Sleep -Seconds $POLL_INTERVAL
        }
    }

    Write-Progress -Activity 'Power-Down Operations' -Completed
    Write-LogInfo "Power-down phase complete — $completedCount/$totalVMs VM(s) processed." -nc

    return @($tracker.Values)
}
#endregion

#region ── Hardware Upgrade Phase ─────────────────────────────────────────────
function Invoke-UpgradePhase {
    <#
    .SYNOPSIS Sends the hardware upgrade command to each VM in $VMList and verifies
    the version change via GET /api/vcenter/vm/{vm}/hardware. Runs sequentially
    since hardware upgrade is a fast, synchronous metadata operation.

    Returns an array of result objects with VersionBefore, VersionAfter, Result, Notes.
    #>
    param([Parameter(Mandatory)][object[]]$VMList)

    $totalVMs = $VMList.Count
    $seqNum   = 0
    $results  = [System.Collections.Generic.List[object]]::new()

    Write-Host ''
    Write-Host "  ── Hardware Upgrade Phase $('─' * 50)" -ForegroundColor DarkCyan
    Write-Host "  Scope      : $totalVMs VM(s)" -ForegroundColor Cyan
    Write-Host "  Target     : $($Script:TargetVersion)" -ForegroundColor Cyan
    Write-Host ''
    Write-LogInfo "Hardware upgrade phase starting — $totalVMs VM(s) | target: $($Script:TargetVersion)." -nc

    foreach ($vm in $VMList) {
        $seqNum++
        $pos = "[$seqNum/$totalVMs]"

        Write-Progress -Activity 'Hardware Upgrade' `
            -Status "$seqNum/$totalVMs — $($vm.VmName)" `
            -PercentComplete ([int](($seqNum / $totalVMs) * 100))

        $result = [PSCustomObject]@{
            VmId          = $vm.VmId
            VmName        = $vm.VmName
            HostName      = $vm.HostName
            DatastoreName = $vm.DatastoreName
            VersionBefore = $vm.CurrentVersion
            VersionAfter  = $null
            Result        = $null
            Notes         = $null
            CompletedAt   = $null
        }

        if ($Script:IsDryRun) {
            Write-LogDryRun "$pos $($vm.VmName) ($($vm.VmId)): Would upgrade hardware version $($vm.CurrentVersion) -> $($Script:TargetVersion)."
            $result.VersionAfter = $Script:TargetVersion
            $result.Result       = $RES_COMPLETE
            $result.CompletedAt  = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
            $results.Add($result)
            continue
        }

        Write-LogInfo "$pos $($vm.VmName): Upgrading hardware version $($vm.CurrentVersion) -> $($Script:TargetVersion)..."
        $ok = Send-HardwareUpgrade -VmId $vm.VmId -Version $Script:TargetVersion

        if (-not $ok) {
            Write-LogError "$pos $($vm.VmName): Upgrade command could not be sent."
            $result.Result      = $RES_ERROR
            $result.Notes       = 'Upgrade API call failed.'
            $result.CompletedAt = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
            $results.Add($result)
            continue
        }

        # Verify the version changed — poll up to $TIMEOUT_UPGRADE seconds
        $deadline    = (Get-Date).AddSeconds($TIMEOUT_UPGRADE)
        $confirmed   = $false
        $finalVersion = $null

        while ((Get-Date) -lt $deadline) {
            $hwInfo = Get-VMHardwareInfo -VmId $vm.VmId
            if ($hwInfo -and $hwInfo.Version -eq $Script:TargetVersion) {
                $confirmed    = $true
                $finalVersion = $hwInfo.Version
                break
            }
            $finalVersion = if ($hwInfo) { $hwInfo.Version } else { 'Unknown' }
            Write-LogDebug "$pos $($vm.VmName): Post-upgrade version check — current: $finalVersion | target: $($Script:TargetVersion). Waiting ${POLL_INTERVAL_FAST}s..."
            Start-Sleep -Seconds $POLL_INTERVAL_FAST
        }

        $result.VersionAfter = $finalVersion
        $result.CompletedAt  = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')

        if ($confirmed) {
            $result.Result = $RES_COMPLETE
            Write-LogOK "$pos $($vm.VmName): Hardware version upgraded to $finalVersion."
        } else {
            $result.Result = $RES_ERROR
            $result.Notes  = "Version not confirmed as $($Script:TargetVersion) within ${TIMEOUT_UPGRADE}s. Actual: $finalVersion."
            Write-LogError "$pos $($vm.VmName): Upgrade not confirmed within timeout. Actual version: $finalVersion."
        }

        $results.Add($result)
    }

    Write-Progress -Activity 'Hardware Upgrade' -Completed

    $succeeded = @($results | Where-Object { $_.Result -eq $RES_COMPLETE }).Count
    $failed    = @($results | Where-Object { $_.Result -eq $RES_ERROR    }).Count
    Write-LogInfo "Hardware upgrade phase complete — $succeeded succeeded, $failed failed." -nc

    return @($results)
}
#endregion

#region ── Input File Handling ────────────────────────────────────────────────
function Read-VMList {
    <#
    .SYNOPSIS Reads a CSV or plain-text VM name file and returns a string array.
    Strips blank lines, common CSV column headers, and surrounding quote characters.
    #>
    param([Parameter(Mandatory)][string]$FilePath)

    $raw   = Get-Content -Path $FilePath -Encoding UTF8
    $names = [System.Collections.Generic.List[string]]::new()

    foreach ($line in $raw) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed)) { continue }
        if ($trimmed -imatch '^(vmname|vm_name|name|vm|hostname|host_name|displayname)$') { continue }
        $trimmed = $trimmed.Trim('"').Trim("'").Trim()
        if (-not [string]::IsNullOrWhiteSpace($trimmed)) { $names.Add($trimmed) }
    }

    return $names.ToArray()
}
#endregion

#region ── Result Output ──────────────────────────────────────────────────────
function Export-Results {
    <#
    .SYNOPSIS Outputs hardware upgrade results in the requested format.
    Accepted formats: Table, CSV, Text, GridView.
    #>
    param(
        [object[]]$Results,
        [string]$Format,
        [string]$BaseName
    )

    $rows = $Results | Select-Object `
        @{N = 'VM Name';         E = { $_.VmName }},
        @{N = 'ESX Host';        E = { $_.HostName }},
        @{N = 'Datastore';       E = { $_.DatastoreName }},
        @{N = 'Version Before';  E = { if ($_.VersionBefore) { $_.VersionBefore } else { 'N/A' } }},
        @{N = 'Version After';   E = { if ($_.VersionAfter)  { $_.VersionAfter  } else { 'N/A' } }},
        @{N = 'Snapshot';        E = { if ($_.SnapshotResult)  { $_.SnapshotResult  } else { 'N/A' } }},
        @{N = 'Power-Down';      E = { if ($_.PowerDownResult) { $_.PowerDownResult } else { 'N/A' } }},
        @{N = 'Upgrade';         E = { if ($_.UpgradeResult)   { $_.UpgradeResult   } else { 'N/A' } }},
        @{N = 'Power-On';        E = { if ($_.PowerOnResult)   { $_.PowerOnResult   } else { 'N/A' } }},
        @{N = 'Status';          E = { $_.OverallResult }},
        @{N = 'Completed At';    E = { if ($_.CompletedAt) { $_.CompletedAt } else { 'N/A' } }}

    switch ($Format) {
        'Table' {
            Write-Host ''
            Write-Host '  ── Results ─────────────────────────────────────────────────' -ForegroundColor Cyan
            Write-Host ($rows | Format-Table -AutoSize | Out-String -Width 300)
        }
        'CSV' {
            $path = Join-Path $SCRIPT_DIR "${BaseName}.csv"
            $rows | Export-Csv -Path $path -NoTypeInformation -Encoding UTF8
            Write-LogOK "Results saved to: $path"
        }
        'Text' {
            $path = Join-Path $SCRIPT_DIR "${BaseName}.txt"
            ($rows | Format-Table -AutoSize | Out-String -Width 300) | Set-Content -Path $path -Encoding UTF8
            Write-LogOK "Results saved to: $path"
        }
        'GridView' {
            try {
                $rows | Out-GridView -Title "$SCRIPT_NAME — Results"
            } catch {
                Write-LogWarn "GridView is not available in this environment: $($_.Exception.Message)"
            }
        }
    }
}

function Export-CleanupResults {
    <#
    .SYNOPSIS Outputs snapshot cleanup results in the requested format.
    Accepted formats: Table, CSV, Text, GridView.
    #>
    param(
        [object[]]$Results,
        [string]$Format,
        [string]$BaseName
    )

    $rows = $Results | Select-Object `
        @{N = 'VM Name';       E = { $_.VmName }},
        @{N = 'ESX Host';      E = { if ($_.HostName)     { $_.HostName }     else { 'N/A' } }},
        @{N = 'Snapshot Name'; E = { if ($_.SnapshotName) { $_.SnapshotName } else { 'N/A' } }},
        @{N = 'Snapshot ID';   E = { if ($_.SnapshotId)   { $_.SnapshotId }   else { 'N/A' } }},
        @{N = 'Result';        E = { $_.Result }},
        @{N = 'Completed At';  E = { if ($_.CompletedAt) { $_.CompletedAt } else { 'N/A' } }}

    switch ($Format) {
        'Table' {
            Write-Host ''
            Write-Host '  ── Cleanup Results ──────────────────────────────────────────' -ForegroundColor Cyan
            Write-Host ($rows | Format-Table -AutoSize | Out-String -Width 300)
        }
        'CSV' {
            $path = Join-Path $SCRIPT_DIR "${BaseName}_cleanup.csv"
            $rows | Export-Csv -Path $path -NoTypeInformation -Encoding UTF8
            Write-LogOK "Cleanup results saved to: $path"
        }
        'Text' {
            $path = Join-Path $SCRIPT_DIR "${BaseName}_cleanup.txt"
            ($rows | Format-Table -AutoSize | Out-String -Width 300) | Set-Content -Path $path -Encoding UTF8
            Write-LogOK "Cleanup results saved to: $path"
        }
        'GridView' {
            try {
                $rows | Out-GridView -Title "$SCRIPT_NAME — Cleanup Results"
            } catch {
                Write-LogWarn "GridView is not available in this environment: $($_.Exception.Message)"
            }
        }
    }
}
#endregion

#region ── Pre-Run Display and Acknowledgment ─────────────────────────────────
function Show-PreRunSummary {
    <#
    .SYNOPSIS Displays a detailed pre-run action plan and requires the operator to
    type CONFIRM to proceed. Returns $true if confirmed, $false if cancelled.

    $EligibleVMs  : List of VM objects that will be acted upon.
    $SkippedVMs   : List of VM objects that are already at/above target version.
    #>
    param(
        [Parameter(Mandatory)][System.Collections.Generic.List[object]]$EligibleVMs,
        [System.Collections.Generic.List[object]]$SkippedAlready
    )

    $poweredOnCount  = @($EligibleVMs | Where-Object { $_.PowerState -ne 'POWERED_OFF' }).Count
    $snapshotEnabled = $Script:TakeSnapshot

    Write-Host ''
    Write-Host "  ╔$('═' * 68)╗" -ForegroundColor Yellow
    Write-Host "  ║   $('Pre-Run Action Summary'.PadRight(65))║" -ForegroundColor Yellow
    Write-Host "  ╚$('═' * 68)╝" -ForegroundColor Yellow
    Write-Host ''
    Write-Host ("  {0,-28}: {1}" -f 'Target Hardware Version', $Script:TargetVersion)  -ForegroundColor Cyan
    Write-Host ("  {0,-28}: {1}" -f 'Pre-Upgrade Snapshots',   $(if ($snapshotEnabled) { 'Enabled' } else { 'Disabled' })) `
        -ForegroundColor $(if ($snapshotEnabled) { 'Green' } else { 'Gray' })
    Write-Host ("  {0,-28}: {1}" -f 'VMs to Upgrade',          $EligibleVMs.Count)     -ForegroundColor Cyan
    if ($SkippedAlready -and $SkippedAlready.Count -gt 0) {
        Write-Host ("  {0,-28}: {1}" -f 'Already at Target (skip)', $SkippedAlready.Count) -ForegroundColor DarkGray
    }
    Write-Host ("  {0,-28}: {1}" -f 'VMs to Power Down',       "$poweredOnCount (currently powered on)") `
        -ForegroundColor $(if ($poweredOnCount -gt 0) { 'Yellow' } else { 'Gray' })
    Write-Host ''

    # Table header
    $col1 = 24; $col2 = 10; $col3 = 10; $col4 = 14; $col5 = 10; $col6 = 0
    $header = "  {0,-$col1}  {1,-$col2}  {2,-$col3}  {3,-$col4}  {4,-$col5}  {5}" `
        -f 'VM NAME', 'CURRENT', 'TARGET', 'POWER STATE', 'SNAPSHOT', 'PLANNED ACTION'
    $divider = "  $('─' * ($col1))  $('─' * $col2)  $('─' * $col3)  $('─' * $col4)  $('─' * $col5)  $('─' * 32)"

    Write-Host $header  -ForegroundColor DarkGray
    Write-Host $divider -ForegroundColor DarkGray

    # Eligible VMs (will be processed)
    foreach ($vm in $EligibleVMs) {
        $snapLabel   = if ($snapshotEnabled) { 'Yes' } else { 'No' }
        $actionParts = @('Upgrade')
        if ($snapshotEnabled)                 { $actionParts = @('Snapshot') + $actionParts }
        if ($vm.PowerState -ne 'POWERED_OFF') { $actionParts = @('Shutdown') + $actionParts }
        if ($Script:AutoPowerOn -and $vm.PowerState -ne 'POWERED_OFF') { $actionParts += 'Power-On' }
        $action    = $actionParts -join ' + '
        $name      = if ($vm.VmName.Length -gt $col1) { $vm.VmName.Substring(0, $col1 - 1) + '…' } else { $vm.VmName }
        $psDisplay = Get-PowerStateDisplay -PowerState $vm.PowerState

        Write-Host ("  {0,-$col1}  {1,-$col2}  {2,-$col3}  " -f $name, $vm.CurrentVersion, $Script:TargetVersion) -NoNewline -ForegroundColor White
        Write-Host ("{0,-$col4}" -f $psDisplay.Label) -NoNewline -ForegroundColor $psDisplay.Color
        Write-Host ("  {0,-$col5}  {1}" -f $snapLabel, $action) -ForegroundColor White
    }

    # Skipped VMs (already at target)
    if ($SkippedAlready -and $SkippedAlready.Count -gt 0) {
        foreach ($vm in $SkippedAlready) {
            $name      = if ($vm.VmName.Length -gt $col1) { $vm.VmName.Substring(0, $col1 - 1) + '…' } else { $vm.VmName }
            $psDisplay = Get-PowerStateDisplay -PowerState $vm.PowerState

            Write-Host ("  {0,-$col1}  {1,-$col2}  {2,-$col3}  " -f $name, $vm.CurrentVersion, $Script:TargetVersion) -NoNewline -ForegroundColor DarkGray
            Write-Host ("{0,-$col4}" -f $psDisplay.Label) -NoNewline -ForegroundColor $psDisplay.Color
            Write-Host ("  {0,-$col5}  {1}" -f 'No', 'SKIPPED — already at or above target') -ForegroundColor DarkGray
        }
    }

    Write-Host ''
    Write-Host "  ⚠  NOTICE — Please read before proceeding:" -ForegroundColor Yellow
    Write-Host '     · Hardware version upgrades cannot be rolled back through vCenter directly.' -ForegroundColor Yellow
    Write-Host '       The only revert path is a pre-upgrade snapshot — and only if one was captured.' -ForegroundColor Yellow
    if ($Script:AutoPowerOn) {
        Write-Host '     · VMs shut down by this script will be powered ON after upgrade (Phase 4).' -ForegroundColor Yellow
        Write-Host '       VMs already powered off before this run will remain off.' -ForegroundColor Yellow
    } else {
        Write-Host '     · VMs will NOT be automatically powered on after upgrade.' -ForegroundColor Yellow
    }
    Write-Host '     · Snapshots are taken after power-down, immediately before upgrade.' -ForegroundColor Yellow
    Write-Host '     · VMs that fail snapshot creation will be excluded from upgrade.' -ForegroundColor Yellow
    Write-Host '     · Snapshots are crash-consistent (memory=off, quiesce=off).' -ForegroundColor Yellow
    Write-Host '     · Pre-upgrade snapshots must be removed manually after validation.' -ForegroundColor Yellow
    Write-Host ''

    if ($Script:IsDryRun) {
        Write-LogDryRun 'In a live run, operator would be required to type CONFIRM to proceed.'
        Write-Host '  [DRY-RUN] Acknowledgment step skipped — no changes will be made.' -ForegroundColor Magenta
        return $true
    }

    $confirm = Read-Host "  To proceed, type CONFIRM and press Enter (or press Enter to cancel)"
    if ($confirm -ceq 'CONFIRM') {
        Write-LogInfo 'Operator confirmed. Proceeding with upgrade.' -nc
        return $true
    } else {
        Write-LogInfo 'Operator did not confirm. Operation cancelled.' -nc
        return $false
    }
}
#endregion

#region ── Display Helpers ────────────────────────────────────────────────────
function Get-PowerStateDisplay {
    <# Returns a PSCustomObject with Label (friendly string) and Color for a raw API power state value. #>
    param([string]$PowerState)
    switch ($PowerState) {
        'POWERED_ON'  { return [PSCustomObject]@{ Label = 'Powered On';  Color = 'Green'  } }
        'POWERED_OFF' { return [PSCustomObject]@{ Label = 'Powered Off'; Color = 'Red'    } }
        'SUSPENDED'   { return [PSCustomObject]@{ Label = 'Suspended';   Color = 'Yellow' } }
        default       { return [PSCustomObject]@{ Label = $PowerState;   Color = 'Gray'   } }
    }
}

function Show-Banner {
    # Inner width: content between ║ and ║ = 66 characters.
    # All content lines use "   " (3-char) left padding + PadRight(63) = 66.
    # Author/Website lines use "   <key 10-char>  " prefix (13 chars total) + PadRight(53) = 66.
    $inner = 66

    Write-Host ''
    Write-Host "  ╔$('═' * $inner)╗" -ForegroundColor Cyan
    Write-Host "  ║$(' ' * $inner)║" -ForegroundColor Cyan
    Write-Host "  ║   $($SCRIPT_NAME.PadRight($inner - 3))║" -ForegroundColor Cyan
    Write-Host "  ║   Version $($SCRIPT_VERSION.PadRight($inner - 11))║" -ForegroundColor Cyan
    Write-Host "  ║$(' ' * $inner)║" -ForegroundColor Cyan
    Write-Host "  ║   Author  : $($SCRIPT_AUTHOR.PadRight($inner - 13))║" -ForegroundColor Cyan
    Write-Host "  ║   Website : $($SCRIPT_WEBSITE.PadRight($inner - 13))║" -ForegroundColor Cyan
    Write-Host "  ║$(' ' * $inner)║" -ForegroundColor Cyan
    Write-Host "  ╚$('═' * $inner)╝" -ForegroundColor Cyan
    Write-Host ''

    if ($Script:IsDryRun) {
        Write-Host '  ┌─────────────────────────────────────────────────────────────┐' -ForegroundColor Magenta
        Write-Host '  │  DRY-RUN MODE — Simulating operations. No changes will be   │' -ForegroundColor Magenta
        Write-Host '  │  made to any virtual machines in vCenter.                   │' -ForegroundColor Magenta
        Write-Host '  └─────────────────────────────────────────────────────────────┘' -ForegroundColor Magenta
        Write-Host ''
    }
}

function Show-Section {
    param([string]$Title)
    $pad = [Math]::Max(0, 54 - $Title.Length)
    Write-Host ''
    Write-Host "  ── $Title $('─' * $pad)" -ForegroundColor DarkCyan
}

function Write-SummaryRow {
    <#
    .SYNOPSIS Writes one row of the summary box with guaranteed cyan borders,
    regardless of the content color. This prevents colored content (e.g., green
    for "Upgraded") from bleeding into the left and right border characters.

    Box inner width = 64 characters:
      "  " (2) + label.PadRight(27) + " : " (3) + value.PadRight(32) = 64
    #>
    param(
        [string]$Label,
        [string]$Value,
        [string]$Color = 'Cyan'
    )
    $content = "  {0,-27} : {1,-32}" -f $Label, $Value
    Write-Host '  ' -NoNewline
    Write-Host '│' -NoNewline -ForegroundColor Cyan
    Write-Host $content -NoNewline -ForegroundColor $Color
    Write-Host '│' -ForegroundColor Cyan
}

function Show-Help {
    $w = 68

    Write-Host ''
    Write-Host "  ╔$('═' * $w)╗" -ForegroundColor Cyan
    Write-Host "  ║   $('VM Hardware Upgrade Manager — Usage Guide'.PadRight($w - 3))║" -ForegroundColor Cyan
    Write-Host "  ╚$('═' * $w)╝" -ForegroundColor Cyan
    Write-Host ''

    $h = { param([string]$line = '', [System.ConsoleColor]$c = [System.ConsoleColor]::Gray)
           Write-Host "  $line" -ForegroundColor $c }

    & $h 'DESCRIPTION' Cyan
    & $h '  Upgrades VM hardware compatibility versions via the vCenter REST API.'
    & $h '  VMs are automatically powered down (Guest OS Shutdown -> Power Off -> Hard Stop)'
    & $h '  before upgrade and left powered off upon completion. Optional pre-upgrade'
    & $h '  snapshots can be taken before any changes are made.'
    Write-Host ''

    & $h 'USAGE' Cyan
    & $h '  .\Invoke-VMHWUpgrade.ps1 -vc <fqdn> -s <file> -tv <version> [options]'
    Write-Host ''

    & $h 'REQUIRED' Cyan
    & $h '  -VCenterServer / -vc <fqdn|ip>   Target vCenter server FQDN or IP address'
    & $h '  -SourceFile    / -s  <path>       CSV or TXT file with VM display names (one per line)'
    Write-Host ''

    & $h 'OPTIONS' Cyan
    & $h '  -TargetVersion / -tv <VMX_N>      Target hardware version (e.g., VMX_22)'
    & $h '                                    If omitted, an interactive menu is displayed (selection required)'
    & $h '  -Snapshot      / -snap            Capture pre-upgrade snapshots after power-down, before upgrade'
    & $h '                                    If omitted, the script will prompt for preference'
    & $h '  -AutoPowerOn   / -apo             Power VMs back on after upgrade completes (including on failure)'
    & $h '  -SkipCertificateCheck / -k        Disable SSL certificate validation (required for self-signed certs)'
    & $h '  -CleanupSnaps / -cs               Remove pre-upgrade snapshots for all scoped VMs (separate run only)'
    & $h '                                    Cannot be combined with -tv, -snap, or -apo'
    & $h '  -DryRun        / -d               Simulate operations without making changes'
    & $h '  -VerboseLogging / -v              Display full timestamped log output in terminal'
    & $h '  -ResultOutput  / -r <format>      Table, CSV, Text, or GridView  (not available with -DryRun)'
    & $h '                                    GridView opens an interactive window (Windows only)'
    & $h '  -Help          / -h               Display this help and exit'
    Write-Host ''

    & $h 'CONCURRENCY  (applies to the power-down phase)' Cyan
    & $h '  -ConcurrentGlobal    / -cg <n>    Max total concurrent power-downs  (0 = unlimited, default)'
    & $h '  -ConcurrentHost      / -ch <n>    Max concurrent power-downs per ESX host  (1-10, default: 5)'
    & $h '  -ConcurrentDatastore / -cd <n>    Max concurrent power-downs per datastore  (1-10, default: 5)'
    Write-Host ''

    & $h 'EXAMPLES' Cyan
    & $h '  .\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -tv VMX_22 -snap -r Table'
    & $h '  .\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -tv VMX_22 -r CSV -cg 10 -v'
    & $h '  .\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv -tv VMX_21 -d'
    & $h '  .\Invoke-VMHWUpgrade.ps1 -vc vcenter.corp.local -s vms.csv'
    Write-Host ''

    & $h 'REQUIRED VCENTER PERMISSIONS' Cyan
    & $h '  Assign a custom role at the vCenter Server level (Propagate to Children).'
    & $h ''
    & $h '    Virtual Machine > Change Configuration:'
    & $h '      Upgrade virtual machine compatibility'
    & $h '    Virtual Machine > Interaction:'
    & $h '      Power Off, Power On'
    & $h '    Virtual Machine > Snapshot management:'
    & $h '      Create snapshot, Remove Snapshot'
    Write-Host ''

    & $h 'NOTES' Cyan
    & $h '  - Requires vSphere 8.0 or later. vSphere 7.x and older are not supported.'
    & $h '  - Input file headers (vmname, name, vm, etc.) are automatically skipped.'
    & $h '  - SRM/VLR placeholder VMs are detected and excluded automatically.'
    & $h '  - In Linked Mode, only VMs on the target vCenter are processed.'
    & $h '  - VMs already at or above the target version are skipped.'
    & $h '  - VMs that fail snapshot creation are excluded from upgrade.'
    & $h '  - VMs shut down by this script can optionally be powered on via -apo.'
    & $h '    VMs already powered off before the run are unaffected by -apo.'
    & $h '  - Pre-upgrade snapshots must be removed manually after validation.'
    & $h '    Use -CleanupSnaps (-cs) in a separate run to remove them via this script.'
    & $h '  - All activity is logged to a timestamped file in the script directory.'
    Write-Host ''
}
#endregion

#region ── Main Execution ─────────────────────────────────────────────────────
try {

    # ── Show help and exit if -Help/-h was passed ─────────────────────────────
    if ($Help.IsPresent) {
        Show-Banner
        Show-Help
        exit 0
    }

    # ── Initialize log file before the banner so the header is captured ──────
    $dateStamp      = (Get-Date).ToString('yyyyMMdd_HHmmss')
    $Script:LogFile = Join-Path $SCRIPT_DIR "VMHWUpgrade_${dateStamp}.log"
    $baseOutName    = "VMHWUpgrade_${dateStamp}"

    Show-Banner

    Write-LogInfo "$SCRIPT_NAME v$SCRIPT_VERSION initializing." -nc
    Write-LogInfo "Script file   : $SCRIPT_FILE"  -nc
    Write-LogInfo "PowerShell    : $($PSVersionTable.PSVersion)" -nc
    Write-LogInfo "Script dir    : $SCRIPT_DIR"   -nc
    Write-LogInfo "Log file      : $Script:LogFile" -nc
    if ($Script:IsDryRun)  { Write-LogInfo 'Mode          : DRY-RUN' -nc }
    if ($Script:IsVerbose) { Write-LogInfo 'Logging       : Verbose/debug enabled' -nc }

    # ── Resolve effective result output format ────────────────────────────────
    $activeResultOutput = $ResultOutput
    if ($Script:IsDryRun -and $activeResultOutput) {
        Write-Host '  Note: Result output (-r) is not available in dry-run mode and will be ignored.' -ForegroundColor Yellow
        Write-LogInfo 'ResultOutput ignored — not permitted in dry-run mode.' -nc
        $activeResultOutput = ''
    }

    # ── Validate CleanupSnaps mutual exclusivity ──────────────────────────────
    if ($Script:CleanupSnaps) {
        $conflicts = @()
        if (-not [string]::IsNullOrEmpty($TargetVersion)) { $conflicts += '-TargetVersion (-tv)' }
        if ($Snapshot.IsPresent)                          { $conflicts += '-Snapshot (-snap)'    }
        if ($AutoPowerOn.IsPresent)                       { $conflicts += '-AutoPowerOn (-apo)'  }
        if ($conflicts.Count -gt 0) {
            Write-LogError "-CleanupSnaps cannot be used with: $($conflicts -join ', '). Run without upgrade arguments to clean up snapshots."
            exit 1
        }
        Write-LogInfo 'Mode: Snapshot Cleanup — upgrade phases will not run.' -nc
    }

    # ── Validate required parameters ──────────────────────────────────────────
    if ([string]::IsNullOrEmpty($VCenterServer)) {
        Show-Section 'vCenter Server'
        Write-Host ''
        $VCenterServer    = Read-Host '  Enter vCenter FQDN or IP address'
        $Script:VCenter   = $VCenterServer
        if ([string]::IsNullOrEmpty($VCenterServer)) {
            Write-LogError 'vCenter server is required. Use -VCenterServer (-vc) or enter when prompted.'
            exit 1
        }
    }

    # ── Validate TargetVersion format if provided as argument ─────────────────
    if (-not [string]::IsNullOrEmpty($TargetVersion)) {
        $TargetVersion = $TargetVersion.ToUpper().Trim()
        if (-not (Test-ValidVMXVersion -Version $TargetVersion)) {
            Write-LogError "Invalid -TargetVersion format: '$TargetVersion'. Expected format: VMX_N (e.g., VMX_22)."
            exit 1
        }
        Write-LogInfo "Target version (argument): $TargetVersion" -nc
    }
    # $TargetVersion is now either the validated/normalized argument or '' (not provided).
    # $Script:TargetVersion is the same variable at script scope — no separate assignment needed.

    # ── Source file resolution ────────────────────────────────────────────────
    if (-not [string]::IsNullOrEmpty($SourceFile)) {
        if (-not (Test-Path $SourceFile)) {
            Write-LogError "Source file not found: $SourceFile"
            exit 1
        }
        Write-LogInfo "Source file: $SourceFile" -nc
    } else {
        Show-Section 'Input File'

        $candidates = @(
            Get-ChildItem -Path $SCRIPT_DIR -Include '*.csv', '*.txt' -File -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -notlike 'VMHWUpgrade_*' } |
            Sort-Object LastWriteTime -Descending
        )

        if ($candidates.Count -eq 1) {
            Write-Host ''
            Write-Host '  Detected input file: ' -NoNewline
            Write-Host $candidates[0].Name -ForegroundColor Green
            $use = Read-Host '  Use this file? [Y/n]'
            if ($use -notmatch '^[Nn]') {
                $SourceFile = $candidates[0].FullName
                Write-LogInfo "Auto-detected source file: $SourceFile" -nc
            }
        } elseif ($candidates.Count -gt 1) {
            Write-Host ''
            Write-Host '  Multiple input files detected:' -ForegroundColor Cyan
            for ($i = 0; $i -lt [Math]::Min($candidates.Count, 10); $i++) {
                Write-Host ("  [{0}]  {1}" -f ($i + 1), $candidates[$i].Name)
            }
            Write-Host ''
            $pick = Read-Host "  Enter number [1-$([Math]::Min($candidates.Count, 10))] or leave blank to enter path manually"
            $idx = 0
            if ([int]::TryParse($pick.Trim(), [ref]$idx) -and $idx -ge 1 -and $idx -le $candidates.Count) {
                $SourceFile = $candidates[$idx - 1].FullName
                Write-LogInfo "Selected source file: $SourceFile" -nc
            }
        }

        if ([string]::IsNullOrEmpty($SourceFile)) {
            Write-Host ''
            $SourceFile = Read-Host '  Enter full path to VM list file'
            if ([string]::IsNullOrEmpty($SourceFile)) {
                Write-LogError 'No source file provided.'
                exit 1
            }
        }

        if (-not (Test-Path $SourceFile)) {
            Write-LogError "Source file not found: $SourceFile"
            exit 1
        }
    }

    # ── Read VM names from source file ────────────────────────────────────────
    Show-Section 'Loading VM List'
    $vmNames = @(Read-VMList -FilePath $SourceFile)

    if ($vmNames.Count -eq 0) {
        Write-LogError "No VM names found in source file: $SourceFile"
        exit 1
    }

    Write-LogInfo "$($vmNames.Count) VM name(s) loaded from: $(Split-Path -Leaf $SourceFile)" -nc

    # ── Authenticate to vCenter ───────────────────────────────────────────────
    Show-Section 'Authentication'
    Write-Host ''

    # Apply certificate bypass before the first connection attempt if -k was specified.
    if ($SkipCertificateCheck.IsPresent) {
        Enable-CertBypass
    }

    $vcUsername = Read-Host '  vCenter Username'
    $vcPassword = Read-Host '  vCenter Password' -AsSecureString
    Write-Host ''

    if ([string]::IsNullOrEmpty($vcUsername)) {
        Write-LogError 'No username provided.'
        exit 1
    }

    Write-LogInfo "Authenticating to $($Script:VCenter) as '$vcUsername'..." -nc

    # First attempt — standard SSL certificate validation
    $token = Connect-VCenter -Username $vcUsername -Password $vcPassword

    if (-not $token) {
        if (-not $SkipCertificateCheck.IsPresent) {
            # SSL certificate failure is the most common cause in lab/enterprise environments.
            # Automatically retry with bypass rather than surfacing a misleading auth error.
            Write-LogWarn 'Authentication failed. Retrying with SSL certificate bypass...' -nc
            Write-Host '  Certificate validation failed — retrying with bypass enabled...' -ForegroundColor Yellow
            Enable-CertBypass
            $token = Connect-VCenter -Username $vcUsername -Password $vcPassword
        }
    }

    if (-not $token) {
        if ($Script:IsDryRun) {
            Write-LogWarn 'Authentication failed — dry-run continuing with a simulated session.' -nc
            $token = 'dry-run-session'
        } else {
            Write-LogError "Authentication failed for '$vcUsername' on '$($Script:VCenter)'."
            Write-Host '  Authentication failed. Verify credentials and vCenter reachability.' -ForegroundColor Red
            exit 1
        }
    }

    $Script:SessionId = $token
    Write-LogOK "Connected to $($Script:VCenter) as '$vcUsername'."

    # ── Detect VI/JSON release schema ─────────────────────────────────────────
    # Required for snapshot query, creation, removal, and upgrade API calls.
    # Always queried from vCenter — the release must be confirmed, not assumed,
    # even in dry-run mode. Snapshot inventory cannot be resolved without it.
    Write-LogInfo 'Detecting VI/JSON release schema...' -nc
    $Script:ViJsonRelease = Get-ViJsonRelease
    if ($Script:ViJsonRelease) {
        Write-LogOK "VI/JSON release schema: $($Script:ViJsonRelease)." -nc
    } else {
        Write-LogWarn 'Could not detect VI/JSON release schema — snapshot and upgrade API calls will not be available.' -nc
    }

    # ── Initialize local host IDs (Linked Mode guard + host map) ─────────────
    Show-Section 'Host Enumeration'
    [void](Initialize-LocalHostIds)

    # ── Resolve VMs and collect hardware/placement data ───────────────────────
    Show-Section 'VM Resolution'
    Write-Host ''
    Write-LogInfo "Resolving $($vmNames.Count) VM(s) and collecting hardware version data..." -nc

    $resolvedVMs    = [System.Collections.Generic.List[object]]::new()
    $unresolvedVMs  = [System.Collections.Generic.List[string]]::new()
    $skippedLinked  = [System.Collections.Generic.List[string]]::new()
    $skippedSRM     = [System.Collections.Generic.List[string]]::new()

    $resIdx = 0
    foreach ($name in $vmNames) {
        $resIdx++
        Write-Progress -Activity 'Resolving VMs' `
            -Status "[$resIdx/$($vmNames.Count)] $name" `
            -PercentComplete ([int](($resIdx / $vmNames.Count) * 100))

        # Look up the VM by name
        $vmSummary = Resolve-VMByName -Name $name
        if (-not $vmSummary) {
            Write-LogWarn "[$resIdx/$($vmNames.Count)] Not found: '$name'"
            $unresolvedVMs.Add($name)
            continue
        }

        $vmId = $vmSummary.vm

        # Check if this VM's host belongs to the connected vCenter (Linked Mode guard)
        $hostEntry = $Script:VmToHostMap[$vmId]
        if ($hostEntry) {
            $hostId = $hostEntry.HostId
            if (-not $Script:LocalHostIds.Contains($hostId)) {
                Write-LogWarn "[$resIdx/$($vmNames.Count)] '$name' belongs to a linked vCenter — skipping."
                $skippedLinked.Add($name)
                continue
            }
        }

        # Get full VM detail (placement, disk info)
        $detail = Get-VMDetail -VmId $vmId
        if (-not $detail) {
            Write-LogWarn "[$resIdx/$($vmNames.Count)] Could not retrieve detail for '$name' — skipping."
            $unresolvedVMs.Add($name)
            continue
        }

        # SRM/VLR placeholder check
        $folderId   = if ($detail.placement) { $detail.placement.folder } else { $null }
        $folderName = if ($folderId) { Resolve-FolderName -FolderId $folderId } else { '' }
        if (Test-IsSRMPlaceholder -VmName $name -VmDetail $detail -FolderName $folderName) {
            Write-LogWarn "[$resIdx/$($vmNames.Count)] '$name' is a VLR placeholder — skipping."
            $skippedSRM.Add($name)
            continue
        }

        # Resolve placement
        $resolvedHost = Resolve-VMHost -VmId $vmId -PlacementHostId ($detail.placement.host)
        $dsName       = Get-DatastoreNameFromVMDetail -VmDetail $detail
        $dsId         = if ($detail.placement) { $detail.placement.datastore } else { $null }

        # Get current power state
        $powerState = Get-VMPowerState -VmId $vmId
        if (-not $powerState) { $powerState = 'Unknown' }

        # Get hardware version info
        $hwInfo         = Get-VMHardwareInfo -VmId $vmId
        $currentVersion = if ($hwInfo) { $hwInfo.Version } else { 'Unknown' }

        Write-LogDebug "Resolved [$resIdx/$($vmNames.Count)] '$name' ($vmId)"
        Write-LogDebug "  ESX Host      : $($resolvedHost.Name) ($($resolvedHost.Id))"
        Write-LogDebug "  Datastore     : $dsName"
        Write-LogDebug "  Power State   : $powerState"
        Write-LogDebug "  Hardware Ver  : $currentVersion"

        $resolvedVMs.Add([PSCustomObject]@{
            VmId           = $vmId
            VmName         = $name
            HostId         = $resolvedHost.Id
            HostName       = $resolvedHost.Name
            DatastoreId    = $dsId
            DatastoreName  = $dsName
            CurrentVersion = $currentVersion
            PowerState     = $powerState
        })
    }

    Write-Progress -Activity 'Resolving VMs' -Completed
    Write-LogInfo "Resolution complete: $($resolvedVMs.Count) resolved | $($unresolvedVMs.Count) not found | $($skippedSRM.Count) VLR placeholder | $($skippedLinked.Count) linked vCenter" -nc

    # Report resolution results
    if ($unresolvedVMs.Count -gt 0) {
        Write-Host ''
        Write-Host "  ⚠  $($unresolvedVMs.Count) VM(s) not found in inventory:" -ForegroundColor Yellow
        $unresolvedVMs | ForEach-Object { Write-Host "     - $_" -ForegroundColor Yellow }
    }
    if ($skippedLinked.Count -gt 0) {
        Write-Host ''
        Write-Host "  ⚠  $($skippedLinked.Count) VM(s) skipped — hosted by a linked vCenter:" -ForegroundColor Yellow
        $skippedLinked | ForEach-Object { Write-Host "     - $_" -ForegroundColor Yellow }
    }
    if ($skippedSRM.Count -gt 0) {
        Write-Host ''
        Write-Host "  ⚠  $($skippedSRM.Count) VM(s) skipped — identified as VLR placeholder:" -ForegroundColor Yellow
        $skippedSRM | ForEach-Object { Write-Host "     - $_" -ForegroundColor Yellow }
    }

    if ($resolvedVMs.Count -eq 0) {
        Write-LogError 'No eligible VMs remain after filtering. Nothing to do.'
        Disconnect-VCenter
        exit 1
    }

    # ─────────────────────────────────────────────────────────────────────────
    # CLEANUP MODE — Snapshot removal. Executes when -CleanupSnaps (-cs) is set.
    # Does not run any upgrade phases. Exits on completion.
    # ─────────────────────────────────────────────────────────────────────────
    if ($Script:CleanupSnaps) {

        # ── Query snapshot inventory ──────────────────────────────────────────
        Show-Section 'Snapshot Inventory'
        Write-Host ''
        Write-LogInfo "Querying snapshot inventory for $($resolvedVMs.Count) VM(s)..." -nc

        $cleanupTargets = [System.Collections.Generic.List[object]]::new()
        $totalToRemove  = 0
        $seqNum         = 0

        foreach ($vm in $resolvedVMs) {
            $seqNum++
            Write-Progress -Activity 'Querying Snapshots' `
                -Status "[$seqNum/$($resolvedVMs.Count)] $($vm.VmName)" `
                -PercentComplete ([int](($seqNum / $resolvedVMs.Count) * 100))

            $snapshots    = Get-VMSnapshots -VmId $vm.VmId
            $queryFailed  = ($null -eq $snapshots)
            $count        = if ($queryFailed) { 0 } else { $snapshots.Count }
            $totalToRemove += $count

            $cleanupTargets.Add([PSCustomObject]@{
                VmId          = $vm.VmId
                VmName        = $vm.VmName
                HostId        = $vm.HostId
                HostName      = $vm.HostName
                DatastoreId   = $vm.DatastoreId
                DatastoreName = $vm.DatastoreName
                PowerState    = $vm.PowerState
                Snapshots     = $snapshots
                SnapshotCount = $count
                QueryFailed   = $queryFailed
            })
        }
        Write-Progress -Activity 'Querying Snapshots' -Completed

        # ── Pre-run summary ───────────────────────────────────────────────────
        Write-Host ''
        Write-Host "  ╔$('═' * 68)╗" -ForegroundColor Yellow
        Write-Host "  ║   $('Snapshot Cleanup — Pre-Run Summary'.PadRight(65))║" -ForegroundColor Yellow
        Write-Host "  ╚$('═' * 68)╝" -ForegroundColor Yellow
        Write-Host ''
        Write-Host ("  {0,-28}: {1}" -f 'Snapshot Name',       $SNAPSHOT_NAME)       -ForegroundColor Cyan
        Write-Host ("  {0,-28}: {1}" -f 'VMs Scoped',          $resolvedVMs.Count)   -ForegroundColor Cyan
        Write-Host ("  {0,-28}: {1}" -f 'Snapshots to Remove', $totalToRemove)       -ForegroundColor $(if ($totalToRemove -gt 0) { 'Yellow' } else { 'Gray' })
        Write-Host ''

        $maxVmNameLen = ($cleanupTargets | ForEach-Object { $_.VmName.Length } | Measure-Object -Maximum).Maximum
        $csCol1 = [Math]::Max($maxVmNameLen, 7)   # minimum 7 = length of 'VM NAME' header
        $csCol2 = 13; $csCol3 = 32; $csCol4 = 20
        $csHeader  = "  {0,-$csCol1}  {1,-$csCol2}  {2,-$csCol3}  {3,-$csCol4}  {4}" `
                     -f 'VM NAME', 'POWER STATE', 'SNAPSHOT NAME', 'CREATED', 'ACTION'
        $csDivider = "  $('─' * $csCol1)  $('─' * $csCol2)  $('─' * $csCol3)  $('─' * $csCol4)  $('─' * 22)"
        Write-Host $csHeader  -ForegroundColor DarkGray
        Write-Host $csDivider -ForegroundColor DarkGray

        foreach ($entry in $cleanupTargets) {
            $psDisplay = Get-PowerStateDisplay -PowerState $entry.PowerState
            $vmName    = if ($entry.VmName.Length -gt $csCol1) { $entry.VmName.Substring(0, $csCol1 - 1) + '…' } else { $entry.VmName }

            if ($entry.QueryFailed) {
                Write-Host ("  {0,-$csCol1}  " -f $vmName) -NoNewline -ForegroundColor DarkGray
                Write-Host ("{0,-$csCol2}  " -f $psDisplay.Label) -NoNewline -ForegroundColor $psDisplay.Color
                Write-Host ("{0,-$csCol3}  {1,-$csCol4}  {2}" -f '(query failed)', '—', 'Skip — query error') -ForegroundColor DarkGray
            } elseif ($entry.SnapshotCount -eq 0) {
                Write-Host ("  {0,-$csCol1}  " -f $vmName) -NoNewline -ForegroundColor DarkGray
                Write-Host ("{0,-$csCol2}  " -f $psDisplay.Label) -NoNewline -ForegroundColor $psDisplay.Color
                Write-Host ("{0,-$csCol3}  {1,-$csCol4}  {2}" -f '(none found)', '—', 'Skip — none found') -ForegroundColor DarkGray
            } else {
                $firstRow = $true
                foreach ($snap in $entry.Snapshots) {
                    if ($firstRow) {
                        Write-Host ("  {0,-$csCol1}  " -f $vmName) -NoNewline -ForegroundColor White
                        Write-Host ("{0,-$csCol2}  " -f $psDisplay.Label) -NoNewline -ForegroundColor $psDisplay.Color
                        $firstRow = $false
                    } else {
                        # Continuation row — blank VM name and power state, indent aligns with first row
                        Write-Host ("  {0,-$csCol1}  {1,-$csCol2}  " -f '', '') -NoNewline -ForegroundColor White
                    }
                    Write-Host ("{0,-$csCol3}  {1,-$csCol4}  {2}" -f $snap.name, $snap.created, 'Remove') -ForegroundColor White
                }
            }
        }

        Write-Host ''

        if ($totalToRemove -eq 0) {
            Write-LogInfo "No snapshots named '$SNAPSHOT_NAME' found across all scoped VMs." -nc
            Write-Host '  No matching snapshots found. Nothing to remove.' -ForegroundColor Gray
            Disconnect-VCenter
            exit 0
        }

        Write-Host '  ⚠  NOTICE — Snapshot removal is permanent and cannot be undone.' -ForegroundColor Yellow
        Write-Host ''

        if ($Script:IsDryRun) {
            Write-LogDryRun 'In a live run, operator would be required to type CONFIRM to proceed.'
            Write-Host '  [DRY-RUN] Acknowledgment step skipped — no changes will be made.' -ForegroundColor Magenta
            Write-Host ''
        } else {
            $csConfirm = Read-Host '  To proceed, type CONFIRM and press Enter (or press Enter to cancel)'
            if ($csConfirm -cne 'CONFIRM') {
                Write-LogInfo 'Snapshot cleanup cancelled by operator.' -nc
                Write-Host '  Cleanup cancelled.' -ForegroundColor Yellow
                Disconnect-VCenter
                exit 0
            }
            Write-LogInfo 'Operator confirmed. Proceeding with snapshot removal.' -nc
        }

        # ── Execute removals — build per-snapshot work list, run concurrent phase ──
        $removalList = [System.Collections.Generic.List[object]]::new()
        foreach ($entry in $cleanupTargets) {
            if (-not $entry.QueryFailed -and $entry.SnapshotCount -gt 0) {
                foreach ($snap in $entry.Snapshots) {
                    $removalList.Add([PSCustomObject]@{
                        VmId          = $entry.VmId
                        VmName        = $entry.VmName
                        HostId        = $entry.HostId
                        HostName      = $entry.HostName
                        DatastoreId   = $entry.DatastoreId
                        DatastoreName = $entry.DatastoreName
                        SnapshotId    = $snap.snapshot
                        SnapshotName  = $snap.name
                    })
                }
            }
        }

        $phaseResults = @(Invoke-SnapshotCleanupPhase -RemovalList $removalList.ToArray())
        $totalRemoved = @($phaseResults | Where-Object { $_.Result -eq $RES_COMPLETE }).Count
        $totalFailed  = @($phaseResults | Where-Object { $_.Result -eq $RES_ERROR    }).Count

        # ── Cleanup summary ───────────────────────────────────────────────────
        Show-Section 'Summary'
        $elapsed = (Get-Date) - $Script:StartTime
        $elStr   = '{0:D2}h {1:D2}m {2:D2}s' -f $elapsed.Hours, $elapsed.Minutes, $elapsed.Seconds

        Write-Host ''
        Write-Host "  ┌$('─' * 64)┐" -ForegroundColor Cyan
        Write-SummaryRow 'Snapshot Name'   $SNAPSHOT_NAME
        Write-SummaryRow 'VMs Scoped'      $resolvedVMs.Count.ToString()
        Write-SummaryRow 'Snapshots Found' $totalToRemove.ToString()
        if ($Script:IsDryRun) {
            Write-SummaryRow 'Would Remove' $totalRemoved.ToString() 'Magenta'
        } else {
            Write-SummaryRow 'Removed' $totalRemoved.ToString() $(if ($totalRemoved -gt 0) { 'Green' } else { 'Cyan' })
            Write-SummaryRow 'Failed'  $totalFailed.ToString()  $(if ($totalFailed  -gt 0) { 'Red'   } else { 'Cyan' })
        }
        Write-SummaryRow 'Elapsed Time' $elStr
        Write-SummaryRow 'Log File'     ([IO.Path]::GetFileName($Script:LogFile))
        Write-Host "  └$('─' * 64)┘" -ForegroundColor Cyan
        Write-Host ''

        Write-LogInfo "CLEANUP SUMMARY — Name: '$SNAPSHOT_NAME' | VMs: $($resolvedVMs.Count) | Found: $totalToRemove | Removed: $totalRemoved | Failed: $totalFailed | Elapsed: $elStr" -nc

        # ── Export cleanup results ────────────────────────────────────────────
        if ($activeResultOutput) {
            $allCleanupResults = [System.Collections.Generic.List[object]]::new()

            # Phase results — VMs that had matching snapshots (one entry per snapshot)
            foreach ($entry in $phaseResults) {
                $allCleanupResults.Add([PSCustomObject]@{
                    VmName       = $entry.VmName
                    HostName     = $entry.HostName
                    SnapshotName = $entry.SnapshotName
                    SnapshotId   = $entry.SnapshotId
                    Result       = $entry.Result
                    Notes        = $entry.Notes
                    CompletedAt  = $entry.CompletedAt
                })
            }

            # Skip / error entries — VMs with no matching snapshots or query failures
            foreach ($entry in $cleanupTargets) {
                if ($entry.QueryFailed -or $entry.SnapshotCount -eq 0) {
                    $allCleanupResults.Add([PSCustomObject]@{
                        VmName       = $entry.VmName
                        HostName     = $entry.HostName
                        SnapshotName = $null
                        SnapshotId   = $null
                        Result       = if ($entry.QueryFailed) { $RES_ERROR } else { $RES_SKIPPED }
                        Notes        = if ($entry.QueryFailed) { 'Snapshot query failed.' } else { 'No matching snapshots found.' }
                        CompletedAt  = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
                    })
                }
            }

            Export-CleanupResults -Results $allCleanupResults.ToArray() -Format $activeResultOutput -BaseName $baseOutName
        }

        Disconnect-VCenter
        exit 0
    }

    # ── Resolve target version (argument or interactive menu) ─────────────────
    if ([string]::IsNullOrEmpty($Script:TargetVersion)) {
        Show-Section 'Target Version Selection'

        # Determine the minimum current version across all resolved VMs to scope the menu
        $minCurrentNum = ($resolvedVMs | ForEach-Object { Get-VMXVersionNumber $_.CurrentVersion } |
                          Measure-Object -Minimum).Minimum
        if ($minCurrentNum -lt 1) { $minCurrentNum = 10 }   # Fallback if version is Unknown

        $Script:TargetVersion = Select-TargetVersion -MinCurrentNum $minCurrentNum

        if ([string]::IsNullOrEmpty($Script:TargetVersion)) {
            Write-LogError 'No target version selected. Exiting.'
            Disconnect-VCenter
            exit 1
        }
    }

    $targetVersionNum = Get-VMXVersionNumber -Version $Script:TargetVersion

    # ── Classify VMs: eligible vs already at/above target ─────────────────────
    $eligibleVMs   = [System.Collections.Generic.List[object]]::new()
    $skippedTarget = [System.Collections.Generic.List[object]]::new()

    foreach ($vm in $resolvedVMs) {
        $currentNum = Get-VMXVersionNumber -Version $vm.CurrentVersion

        if ($currentNum -ge $targetVersionNum) {
            Write-LogInfo "VM '$($vm.VmName)' is already at $($vm.CurrentVersion) (>= target $($Script:TargetVersion)) — skipping." -nc
            $skippedTarget.Add($vm)
        } else {
            $eligibleVMs.Add($vm)
        }
    }

    Write-LogInfo "$($eligibleVMs.Count) VM(s) eligible for upgrade. $($skippedTarget.Count) already at/above target." -nc

    if ($eligibleVMs.Count -eq 0) {
        Write-Host ''
        Write-LogWarn "All resolved VMs are already at or above $($Script:TargetVersion). Nothing to upgrade."
        Disconnect-VCenter
        exit 0
    }

    # ── Resolve snapshot preference (argument or prompt) ──────────────────────
    if ($Snapshot.IsPresent) {
        $Script:TakeSnapshot = $true
        Write-LogInfo 'Snapshot mode: Enabled (via -Snapshot argument).' -nc
    } else {
        Show-Section 'Snapshot Preference'
        Write-Host ''
        Write-Host '  Pre-upgrade snapshots capture a crash-consistent restore point before' -ForegroundColor Cyan
        Write-Host '  any changes are made. Snapshots are taken only for VMs that will be upgraded.' -ForegroundColor Cyan
        Write-Host ''
        if ($Script:IsDryRun) {
            Write-LogDryRun 'Dry-run: prompting for snapshot preference (actual snapshots will not be taken).'
        }
        $snapAnswer = Read-Host '  Capture pre-upgrade snapshots for eligible VMs? [Y/n]'
        $Script:TakeSnapshot = ($snapAnswer -notmatch '^[Nn]')
        Write-LogInfo "Snapshot mode: $(if ($Script:TakeSnapshot) { 'Enabled' } else { 'Disabled' }) (via prompt)." -nc
    }

    # ── Pre-run summary and operator acknowledgment ───────────────────────────
    Show-Section 'Pre-Run Summary'

    $confirmed = Show-PreRunSummary -EligibleVMs $eligibleVMs -SkippedAlready $skippedTarget

    if (-not $confirmed) {
        Write-Host ''
        Write-Host '  Operation cancelled by operator.' -ForegroundColor Yellow
        Write-LogInfo 'Operation cancelled — operator did not confirm.' -nc
        Disconnect-VCenter
        exit 0
    }

    # ── Master results collection (one entry per eligible VM) ─────────────────
    # Initialized here; fields populated progressively through each phase.
    $masterResults = [ordered]@{}
    foreach ($vm in $eligibleVMs) {
        $masterResults[$vm.VmId] = [PSCustomObject]@{
            VmId            = $vm.VmId
            VmName          = $vm.VmName
            HostName        = $vm.HostName
            DatastoreName   = $vm.DatastoreName
            VersionBefore   = $vm.CurrentVersion
            VersionAfter    = $null
            SnapshotResult  = $RES_NA
            PowerDownResult = $RES_NA
            UpgradeResult   = $RES_NA
            PowerOnResult   = $RES_NA
            OverallResult   = $null
            Notes           = $null
            CompletedAt     = $null
        }
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 1 — Power-Down (only VMs currently powered on)
    # ─────────────────────────────────────────────────────────────────────────
    $powerDownList    = @($eligibleVMs | Where-Object { $_.PowerState -ne 'POWERED_OFF' })
    $alreadyOffList   = @($eligibleVMs | Where-Object { $_.PowerState -eq 'POWERED_OFF' })
    $powerDownFailed  = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)

    if ($alreadyOffList.Count -gt 0) {
        foreach ($vm in $alreadyOffList) {
            if ($masterResults.Contains($vm.VmId)) {
                $masterResults[$vm.VmId].PowerDownResult = $RES_SKIPPED
            }
        }
        Write-LogInfo "$($alreadyOffList.Count) VM(s) already powered off — skipping power-down for those." -nc
    }

    if ($powerDownList.Count -gt 0) {
        $pdResults = @(Invoke-PowerDownPhase -VMList $powerDownList)

        foreach ($entry in $pdResults) {
            if ($masterResults.Contains($entry.VmId)) {
                $masterResults[$entry.VmId].PowerDownResult = $entry.Result
            }
            if ($entry.Result -eq $RES_ERROR) {
                [void]$powerDownFailed.Add($entry.VmId)
                if ($masterResults.Contains($entry.VmId)) {
                    $masterResults[$entry.VmId].SnapshotResult = $RES_NA
                    $masterResults[$entry.VmId].UpgradeResult  = $RES_NA
                    $masterResults[$entry.VmId].OverallResult  = $RES_ERROR
                    $masterResults[$entry.VmId].Notes          = "Power-down failed: $($entry.Notes)"
                    $masterResults[$entry.VmId].CompletedAt    = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
                }
            }
        }

        if ($powerDownFailed.Count -gt 0) {
            Write-Host ''
            Write-Host "  ⚠  $($powerDownFailed.Count) VM(s) excluded from snapshot and upgrade due to power-down failure:" -ForegroundColor Yellow
            foreach ($vid in $powerDownFailed) {
                $failedName = ($eligibleVMs | Where-Object { $_.VmId -eq $vid } | Select-Object -First 1).VmName
                Write-Host "     - $failedName" -ForegroundColor Yellow
            }
        }
    } else {
        Write-LogInfo 'No VMs require power-down — all eligible VMs are already powered off.' -nc
    }

    # Filter to VMs that are powered off and ready for snapshot/upgrade
    $postPowerDownList = [System.Collections.Generic.List[object]]::new()
    foreach ($vm in $eligibleVMs) {
        if (-not $powerDownFailed.Contains($vm.VmId)) {
            $postPowerDownList.Add($vm)
        }
    }

    if ($postPowerDownList.Count -eq 0) {
        Write-LogError 'All eligible VMs failed during power-down. Nothing to snapshot or upgrade.'
        if ($activeResultOutput) {
            foreach ($vm in $eligibleVMs) {
                if (-not $masterResults[$vm.VmId].OverallResult) {
                    $masterResults[$vm.VmId].OverallResult = $RES_ERROR
                }
            }
            Export-Results -Results @($masterResults.Values) -Format $activeResultOutput -BaseName $baseOutName
        }
        Disconnect-VCenter
        exit 1
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 2 — Snapshot (VMs are now confirmed powered off)
    # ─────────────────────────────────────────────────────────────────────────
    $snapshotFailedVmIds = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)

    if ($Script:TakeSnapshot) {
        $snapshotResults = Invoke-SnapshotPhase -VMList $postPowerDownList.ToArray()

        foreach ($vmId in $snapshotResults.Keys) {
            $res = $snapshotResults[$vmId]
            if ($masterResults.Contains($vmId)) {
                $masterResults[$vmId].SnapshotResult = $res
            }
            if ($res -eq $RES_ERROR) {
                [void]$snapshotFailedVmIds.Add($vmId)
                if ($masterResults.Contains($vmId)) {
                    $masterResults[$vmId].UpgradeResult = $RES_NA
                    $masterResults[$vmId].OverallResult = $RES_ERROR
                    $masterResults[$vmId].Notes         = 'Excluded — snapshot creation failed.'
                    $masterResults[$vmId].CompletedAt   = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
                }
            }
        }

        if ($snapshotFailedVmIds.Count -gt 0) {
            Write-Host ''
            Write-Host "  ⚠  $($snapshotFailedVmIds.Count) VM(s) excluded from upgrade due to snapshot failure:" -ForegroundColor Yellow
            foreach ($vid in $snapshotFailedVmIds) {
                $failedName = ($postPowerDownList | Where-Object { $_.VmId -eq $vid } | Select-Object -First 1).VmName
                Write-Host "     - $failedName" -ForegroundColor Yellow
            }
        }
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 3 — Hardware Upgrade
    # ─────────────────────────────────────────────────────────────────────────
    $upgradeReadyList = [System.Collections.Generic.List[object]]::new()
    foreach ($vm in $postPowerDownList) {
        if (-not $snapshotFailedVmIds.Contains($vm.VmId)) {
            $upgradeReadyList.Add($vm)
        }
    }

    if ($upgradeReadyList.Count -eq 0) {
        Write-LogError 'No VMs are ready for upgrade after snapshot phase.'
    } else {
        $upgradeResults = @(Invoke-UpgradePhase -VMList $upgradeReadyList.ToArray())

        foreach ($entry in $upgradeResults) {
            if ($masterResults.Contains($entry.VmId)) {
                $masterResults[$entry.VmId].VersionAfter  = $entry.VersionAfter
                $masterResults[$entry.VmId].UpgradeResult = $entry.Result
                $masterResults[$entry.VmId].Notes         = $entry.Notes
                $masterResults[$entry.VmId].CompletedAt   = $entry.CompletedAt
            }
        }
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 4 — Power-On  (only when -AutoPowerOn / -apo is specified)
    # Applies to all VMs that were successfully powered down in Phase 1,
    # regardless of upgrade outcome.
    # ─────────────────────────────────────────────────────────────────────────
    if ($Script:AutoPowerOn) {
        # Collect VMs that the script shut down successfully
        $powerOnCandidates = [System.Collections.Generic.List[object]]::new()
        foreach ($vm in $powerDownList) {
            $entry = $masterResults[$vm.VmId]
            if ($entry -and $entry.PowerDownResult -eq $RES_COMPLETE) {
                $powerOnCandidates.Add($vm)
            }
        }

        if ($powerOnCandidates.Count -gt 0) {
            $poResults = @(Invoke-PowerOnPhase -VMList $powerOnCandidates.ToArray())
            foreach ($entry in $poResults) {
                if ($masterResults.Contains($entry.VmId)) {
                    $masterResults[$entry.VmId].PowerOnResult = $entry.Result
                    # Append power-on notes without overwriting existing upgrade notes
                    if ($entry.Result -eq $RES_ERROR -and $entry.Notes) {
                        $existing = $masterResults[$entry.VmId].Notes
                        $masterResults[$entry.VmId].Notes = if ($existing) { "$existing | Power-On: $($entry.Notes)" } else { $entry.Notes }
                    }
                }
            }
        } else {
            Write-LogInfo 'No VMs require power-on (none were shut down by this script or all power-downs failed).' -nc
        }
    }

    # ── Compute overall result for each VM ────────────────────────────────────
    foreach ($entry in $masterResults.Values) {
        if ($entry.OverallResult) { continue }   # Already set (e.g., snapshot/power failure)

        $upgradeOk = ($entry.UpgradeResult -eq $RES_COMPLETE)
        $pdOk      = ($entry.PowerDownResult -in @($RES_COMPLETE, $RES_SKIPPED))
        $snapOk    = ($entry.SnapshotResult  -in @($RES_COMPLETE, $RES_NA))

        if ($upgradeOk -and $pdOk -and $snapOk) {
            $entry.OverallResult = $RES_COMPLETE
        } elseif ($upgradeOk) {
            $entry.OverallResult = 'Partial'   # Upgrade succeeded but another phase had issues
        } else {
            $entry.OverallResult = $RES_ERROR
        }
    }

    # Also build result rows for VMs that were skipped (already at target)
    $allResults = [System.Collections.Generic.List[object]]::new()
    $allResults.AddRange(@($masterResults.Values))

    foreach ($vm in $skippedTarget) {
        $allResults.Add([PSCustomObject]@{
            VmId            = $vm.VmId
            VmName          = $vm.VmName
            HostName        = $vm.HostName
            DatastoreName   = $vm.DatastoreName
            VersionBefore   = $vm.CurrentVersion
            VersionAfter    = $vm.CurrentVersion
            SnapshotResult  = $RES_NA
            PowerDownResult = $RES_NA
            UpgradeResult   = $RES_SKIPPED
            OverallResult   = $RES_SKIPPED
            Notes           = "Already at $($vm.CurrentVersion) — no action taken."
            CompletedAt     = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
        })
    }

    # ── Export results ────────────────────────────────────────────────────────
    if ($activeResultOutput) {
        Export-Results -Results $allResults.ToArray() -Format $activeResultOutput -BaseName $baseOutName
    }

    # ── Final summary ─────────────────────────────────────────────────────────
    Show-Section 'Summary'

    $elapsed   = (Get-Date) - $Script:StartTime
    $elStr     = '{0:D2}h {1:D2}m {2:D2}s' -f $elapsed.Hours, $elapsed.Minutes, $elapsed.Seconds
    $total     = $allResults.Count
    $complete  = @($allResults | Where-Object { $_.OverallResult -eq $RES_COMPLETE }).Count
    $partial   = @($allResults | Where-Object { $_.OverallResult -eq 'Partial'     }).Count
    $skipped   = @($allResults | Where-Object { $_.OverallResult -eq $RES_SKIPPED  }).Count
    $failed    = @($allResults | Where-Object { $_.OverallResult -eq $RES_ERROR    }).Count
    $notFound  = $unresolvedVMs.Count
    $srmCount  = $skippedSRM.Count

    $failColor    = if ($failed   -gt 0) { 'Red'    } else { 'Cyan' }
    $partialColor = if ($partial  -gt 0) { 'Yellow' } else { 'Cyan' }
    $skipColor    = if ($skipped  -gt 0) { 'Yellow' } else { 'Cyan' }
    $warnColor    = if ($notFound -gt 0) { 'Yellow' } else { 'Cyan' }
    $srmColor     = if ($srmCount -gt 0) { 'Yellow' } else { 'Cyan' }

    Write-Host ''
    Write-Host "  ┌$('─' * 64)┐" -ForegroundColor Cyan

    Write-SummaryRow 'Target Version'             $Script:TargetVersion
    Write-SummaryRow 'Pre-Upgrade Snapshots'      $(if ($Script:TakeSnapshot) { 'Enabled' } else { 'Disabled' })
    Write-SummaryRow 'Auto Power-On'              $(if ($Script:AutoPowerOn)  { 'Enabled' } else { 'Disabled' })
    Write-SummaryRow 'Input VMs'                  $vmNames.Count.ToString()
    Write-SummaryRow 'Resolved'                   $resolvedVMs.Count.ToString()
    Write-SummaryRow 'Not Found'                  $notFound.ToString()  $warnColor
    Write-SummaryRow 'VLR Placeholder VMs Skipped' $srmCount.ToString() $srmColor

    if ($Script:IsDryRun) {
        Write-SummaryRow 'Operations Planned' $total.ToString() 'Magenta'
    } else {
        Write-SummaryRow 'Total Scoped'   $total.ToString()
        Write-SummaryRow 'Upgraded'       $complete.ToString()  'Green'
        Write-SummaryRow 'Partial'        $partial.ToString()   $partialColor
        Write-SummaryRow 'Skipped'        $skipped.ToString()   $skipColor
        Write-SummaryRow 'Failed'         $failed.ToString()    $failColor
    }

    Write-SummaryRow 'Elapsed Time' $elStr
    Write-SummaryRow 'Log File'     ([IO.Path]::GetFileName($Script:LogFile))

    Write-Host "  └$('─' * 64)┘" -ForegroundColor Cyan
    Write-Host ''

    if (-not $Script:IsDryRun -and $complete -gt 0) {
        if ($Script:AutoPowerOn) {
            Write-Host '  Upgrade complete. VMs shut down by this script have been powered back on.' -ForegroundColor Green
            Write-Host '  VMs that were already powered off before this run were not started.' -ForegroundColor Yellow
        } else {
            Write-Host '  VMs are powered off and ready for validation.' -ForegroundColor Green
            Write-Host '  Power-on is a manual step — verify compatibility before returning VMs to service.' -ForegroundColor Yellow
        }
        Write-Host ''
    }

    $snapshotsTaken = @($allResults | Where-Object { $_.SnapshotResult -eq $RES_COMPLETE }).Count
    if (-not $Script:IsDryRun -and $snapshotsTaken -gt 0) {
        Write-Host '  ⚠  Pre-upgrade snapshots must be removed manually once VMs are validated.' -ForegroundColor Yellow
        Write-Host ''
    }

    Write-LogInfo "SUMMARY — Target: $($Script:TargetVersion) | Input: $($vmNames.Count) | Resolved: $($resolvedVMs.Count) | NotFound: $notFound | VLR: $srmCount | Total: $total | Upgraded: $complete | Partial: $partial | Skipped: $skipped | Failed: $failed | Elapsed: $elStr" -nc

} catch {
    Write-LogError "Unhandled exception: $($_.Exception.Message)" -nc
    Write-LogDebug "Stack trace: $($_.ScriptStackTrace)"
    Write-Host ''
    Write-Host '  An unexpected error occurred. Review the log file for details.' -ForegroundColor Red
    if ($Script:LogFile) { Write-Host "  Log: $Script:LogFile" -ForegroundColor Gray }

} finally {
    if ($Script:SessionId) {
        Disconnect-VCenter
    }

    Write-LogInfo "$SCRIPT_NAME v$SCRIPT_VERSION execution complete." -nc
    Write-Host ''
}
#endregion
