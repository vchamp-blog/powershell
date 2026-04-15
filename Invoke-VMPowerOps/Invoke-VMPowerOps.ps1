#Requires -Version 5.1

<#
.SYNOPSIS
    VM Power Operations Manager — vCenter REST API Power Control

.DESCRIPTION
    Performs power-down, power-on, and power-cycle operations on VMware virtual machines
    using the vCenter REST API (no PowerCLI required). Supports concurrent operations
    with per-host and per-datastore throttling, a graceful shutdown sequence with
    automatic escalation, dry-run simulation, and detailed structured logging.

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

.PARAMETER VerboseLogging
    Enables verbose/debug output to both the console and the log file.
    Log entries are displayed with full timestamp and level formatting when active.
    Alias: -v

.PARAMETER DryRun
    Simulates all operations without executing them against vCenter.
    Result output (-r) is not permitted in dry-run mode.
    Alias: -d

.PARAMETER ForceOff
    Bypasses the Guest OS Shutdown step and begins the power-down sequence
    directly with Power Off, escalating to Hard Stop as needed.
    Aliases: -f, -forcereboot

.PARAMETER ConcurrentGlobal
    Maximum number of concurrent power operations across all hosts and datastores.
    A value of 0 (default) imposes no global limit.
    Alias: -cg

.PARAMETER ConcurrentHost
    Maximum number of concurrent power operations per ESX host. Range: 1-10. Default: 5.
    Alias: -ch

.PARAMETER ConcurrentDatastore
    Maximum number of concurrent power operations per parent datastore. Range: 1-10. Default: 5.
    Alias: -cd

.PARAMETER PowerDown
    Executes shutdown operations only.
    Shutdown sequence: Guest OS Shutdown -> Power Off -> Hard Stop
    Alias: -pd

.PARAMETER PowerOn
    Executes power-on operations only.
    Alias: -po

.PARAMETER PowerCycle
    Executes shutdown followed by power-on for all scoped VMs. VM placement
    (ESX host and datastore) is re-validated between phases to account for
    DRS migrations that may occur during or after the power-down phase.
    Alias: -pc

.PARAMETER ResultOutput
    Exports operation results in the specified format: Table, CSV, Text, or GridView.
    CSV and Text files are written to the script directory.
    GridView opens an interactive, filterable table in a separate PowerShell window.
    Not permitted in dry-run mode.
    Alias: -r

.PARAMETER Help
    Displays usage guidance and exits.
    Alias: -h

.EXAMPLE
    .\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.csv -pc -r Table -ch 3

.EXAMPLE
    .\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.txt -pd -r CSV -cg 10 -v

.EXAMPLE
    .\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.csv -pd -f

.EXAMPLE
    .\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.csv -po -d

.NOTES
    Version    : 1.0.0
    Author     : Don Horrox
    Website    : https://vchamp.net
    Requires   : PowerShell 5.1+ | vCenter 7.0, 8.0, or 9.0
    API        : vCenter REST API -- no PowerCLI dependency
    Tested On  : PowerShell 5.1 (Windows), PowerShell 7.4+ (Windows/Linux)

    Shutdown Escalation Sequence (Power-Down):
        1. Guest OS Shutdown (graceful, via VMware Tools)
           Skipped automatically if VMware Tools is not running or not installed.
           Skipped entirely when -ForceOff (-f) is specified.
        2. Power Off (forced via vSphere API, if step 1 times out or is skipped)
        3. Hard Stop (final attempt, if step 2 times out)

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

    # Enable verbose/debug output
    [Alias('v')]
    [switch]$VerboseLogging,

    # Dry-run simulation mode
    [Alias('d')]
    [switch]$DryRun,

    # Skip Guest OS Shutdown; begin power-down directly at Power Off
    [Alias('f', 'forcereboot')]
    [switch]$ForceOff,

    # Global concurrent operation limit (0 = unlimited)
    [Alias('cg')]
    [ValidateRange(0, [int]::MaxValue)]
    [int]$ConcurrentGlobal = 0,

    # Per-host concurrent operation limit
    [Alias('ch')]
    [ValidateRange(1, 10)]
    [int]$ConcurrentHost = 5,

    # Per-datastore concurrent operation limit
    [Alias('cd')]
    [ValidateRange(1, 10)]
    [int]$ConcurrentDatastore = 5,

    # Power operation selectors
    [Alias('pd')]
    [switch]$PowerDown,

    [Alias('po')]
    [switch]$PowerOn,

    [Alias('pc')]
    [switch]$PowerCycle,

    # Result output format
    [Alias('r')]
    [ValidateSet('Table', 'CSV', 'Text', 'GridView')]
    [string]$ResultOutput,

    # Display help and exit
    [Alias('h')]
    [switch]$Help
)

$ErrorActionPreference = 'Continue'

#region ── Script Constants ────────────────────────────────────────────────────
$SCRIPT_NAME    = 'VM Power Operations Manager'
$SCRIPT_VERSION = '1.0.0'
$SCRIPT_AUTHOR  = 'Don Horrox'
$SCRIPT_WEBSITE = 'https://vchamp.net'
$SCRIPT_FILE    = $MyInvocation.MyCommand.Name

# Resolve the script directory regardless of call context
$SCRIPT_DIR = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
if ([string]::IsNullOrEmpty($SCRIPT_DIR)) { $SCRIPT_DIR = (Get-Location).Path }

# Timeout values (seconds)
$TIMEOUT_GUEST_SHUTDOWN = 300    # Max wait for Guest OS Shutdown to complete
$TIMEOUT_POWER_OFF      = 120    # Max wait for Power Off / Hard Stop to complete
$TIMEOUT_POWERON        = 180    # Max wait for power-on confirmation
$POLL_INTERVAL          = 10     # Delay between power state polls (seconds)
$RETRY_DELAY            = 30     # Delay before retrying a failed API command (seconds)
$MAX_RETRIES            = 2      # Maximum command-send retry attempts per VM
$INTER_PHASE_DELAY      = 15     # Desired delay between power-down and power-on phases (seconds)

# VM tracking state constants
$ST_PENDING  = 'Pending'
$ST_ACTIVE   = 'Active'
$ST_COMPLETE = 'Complete'
$ST_FAILED   = 'Failed'

# Operation result constants
$RES_COMPLETE = 'Complete'   # Operation performed and confirmed
$RES_SKIPPED  = 'Skipped'   # VM was already in the desired state; no action taken
$RES_ERROR    = 'Error'      # Operation failed after all retries

# Power-down phase constants — in escalation order:
#   1. Guest OS Shutdown  (graceful, requires VMware Tools to be running)
#   2. Power Off          (forced via vSphere API)
#   3. Hard Stop          (final Power Off attempt before marking the VM as failed)
$PH_GUEST_SHUTDOWN = 'GuestShutdown'
$PH_POWER_OFF      = 'PowerOff'
$PH_HARD_STOP      = 'HardStop'
$PH_POWERON        = 'PowerOn'

# SRM/VLR placeholder detection: folder name substrings that indicate recovery-site
# SRM-managed folders. Customize this list for your environment.
$SRM_FOLDER_PATTERNS = @('vCDR', 'SRM', 'Site Recovery', 'LiveRecovery', 'DR_Placeholder')
#endregion

#region ── Required vCenter Permissions ───────────────────────────────────────
# The vCenter account used to authenticate this script requires the following
# privileges. Create a custom vCenter role and assign it at the vCenter Server
# level with "Propagate to Children" enabled. This grants access to all objects
# beneath it. For a more granular scope, assign the role at the individual Host
# or VM Folder level instead.
#
# Recommended approach:
#   1. In vCenter, navigate to Administration > Access Control > Roles.
#   2. Clone the built-in "Read-Only" role and name it (e.g. "VM Power Ops").
#   3. Add the privileges listed below to the cloned role.
#   4. In the vCenter inventory, assign the role to the service account at the
#      vCenter Server level with "Propagate to children" checked.
#
# Required Privileges:
#
#   Virtual Machine > Interaction:
#     - Power On                (power-on and power-cycle operations)
#     - Power Off               (force power-off and hard stop escalation)
#
#   Virtual Machine > Change Configuration > Change Settings:
#     - Change Settings         (used during VM detail and placement resolution)
#
#   Datastore:
#     - Browse Datastore        (reads VM disk backing paths for datastore resolution)
#
#   Sessions:
#     - Validate session        (required for REST API session management)
#
#   Global > Licenses:
#     - (no sub-privilege required; read-only inventory traversal)
#
# Minimum built-in role equivalent: Read-Only + the VM Interaction privileges above.
# Note: Overly permissive roles (e.g. Administrator) work but are discouraged for
# service accounts that perform only power operations.
#endregion

#region ── Script-Level State ─────────────────────────────────────────────────
$Script:LogFile        = $null
$Script:SessionId      = $null
$Script:SkipCert       = $false
$Script:IsVerbose      = $VerboseLogging.IsPresent -or ($VerbosePreference -ne 'SilentlyContinue')
$Script:IsDryRun       = $DryRun.IsPresent
$Script:ForceOff       = $ForceOff.IsPresent
$Script:StartTime      = Get-Date
$Script:Operation      = $null      # 'PowerDown' | 'PowerOn' | 'PowerCycle'
$Script:VCenter        = $VCenterServer
$Script:HostCache      = @{}
$Script:DatastoreCache = @{}
$Script:SkippedVMs     = [System.Collections.Generic.List[string]]::new()
$Script:VmToHostMap = @{}   # vmId -> @{ HostId; HostName }

# HashSet of ESX host MOR IDs belonging to the connected vCenter instance.
# Populated after authentication; used to exclude VMs from linked vCenters.
$Script:LocalHostIds   = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)

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

    # Write to the log file — DEBUG entries are suppressed unless verbose is active
    if ($Script:LogFile -and ($Level -ne 'DEBUG' -or $Script:IsVerbose)) {
        try { Add-Content -Path $Script:LogFile -Value $logLine -Encoding UTF8 }
        catch { <# Swallow to prevent recursion on log write failure #> }
    }

    if ($NoConsole) { return }
    if ($Level -eq 'DEBUG' -and -not $Script:IsVerbose) { return }

    if ($Script:IsVerbose) {
        # Verbose: full formatted line so operators can correlate with the log file
        Write-Host $logLine -ForegroundColor $Color
    } else {
        # Normal: clean message text only — no timestamp, no level tag
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
    vCenter certificates. Handles PS 5.1 (ServicePointManager) and PS 7+ (per-request).
    #>
    $Script:SkipCert = $true
    Write-LogWarn 'SSL certificate verification is disabled — proceeding without certificate validation.'

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        try {
            if (-not ([System.Management.Automation.PSTypeName]'TrustAllCertsPolicy').Type) {
                Add-Type -TypeDefinition @'
using System.Net;
using System.Security.Cryptography.X509Certificates;
public class TrustAllCertsPolicy : ICertificatePolicy {
    public bool CheckValidationResult(
        ServicePoint srvPoint, X509Certificate certificate,
        WebRequest request, int certificateProblem) { return true; }
}
'@
            }
            [System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy
            [System.Net.ServicePointManager]::SecurityProtocol  = [System.Net.SecurityProtocolType]::Tls12
        } catch {
            Write-LogWarn "Could not configure TLS bypass: $($_.Exception.Message)"
        }
    }
    # PowerShell 7+ uses -SkipCertificateCheck per Invoke-RestMethod call (see Invoke-VCenterAPI)
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
    if ($PSVersionTable.PSVersion.Major -ge 6 -and $Script:SkipCert) {
        $splat.SkipCertificateCheck = $true
    }

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
    $plain = $null  # Clear plain-text from memory immediately

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

    if ($PSVersionTable.PSVersion.Major -ge 6 -and $Script:SkipCert) {
        $splat.SkipCertificateCheck = $true
    }

    try {
        $raw = Invoke-RestMethod @splat
        # vCenter returns the token as a JSON-quoted string: "session-token-value"
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
    .SYNOPSIS Rebuilds $Script:VmToHostMap from live vCenter data by querying each
    known ESX host for its current VM list (GET /api/vcenter/vm?hosts={hostId}).

    Called at startup via Initialize-LocalHostIds and again by Update-VMPlacements
    before the power-on phase so that any vMotions that occurred while VMs were
    powered off are reflected in the map before concurrency limits are applied.
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
    populates $Script:LocalHostIds, $Script:HostCache, and the vm->host reverse
    map used for placement resolution.

    When Linked Mode is active, inventory queries may return results from all
    linked vCenters. Any VM whose resolved host is NOT in this set is excluded.
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

        # Build the initial vm->host reverse map
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
    .SYNOPSIS Evaluates whether a VM is an SRM or VMware Live Recovery (VLR) placeholder
    that should be excluded from all power operations.

    Detection uses vCenter REST API indicators. The authoritative check --
    VirtualMachine.config.managedBy.extensionKey in the SOAP API -- is not directly
    exposed by the REST API. The following best-effort heuristics are applied:

      Heuristic 1 — Folder name pattern match:
        If the VM's placement folder name contains any substring defined in
        $SRM_FOLDER_PATTERNS, the VM is classified as a placeholder. Customize
        that array at the top of this script for your site's folder conventions.

      Heuristic 2 — Zero registered disks:
        Placeholder VMs at the recovery site are commonly registered with no
        disk backing prior to a test or actual recovery event.

      Heuristic 3 — Disks present but zero total capacity:
        Some placeholder configurations register disk entries with zero allocated
        bytes, indicating a shadow/placeholder VMDK backing.

    For authoritative detection using the SOAP API, query:
      VirtualMachine.config.managedBy.extensionKey
      SRM extension key : com.vmware.vcDr
      VLR extension key : com.vmware.liverecover (verify for your product version)
    #>
    param(
        [string]$VmName,
        [PSCustomObject]$VmDetail,
        [string]$FolderName = ''
    )

    if (-not $VmDetail) { return $false }

    # ── Heuristic 1: Folder name pattern match ─────────────────────────────
    if ($FolderName) {
        foreach ($pattern in $SRM_FOLDER_PATTERNS) {
            if ($FolderName -like "*$pattern*") {
                Write-LogDebug "'$VmName' identified as VLR placeholder — folder '$FolderName' matches pattern '$pattern'."
                return $true
            }
        }
    }

    # ── Heuristic 2 & 3: Disk configuration analysis ───────────────────────
    $diskCount     = 0
    $totalCapacity = [long]0

    if ($VmDetail.disks) {
        foreach ($diskProp in $VmDetail.disks.PSObject.Properties) {
            $diskCount++
            if ($diskProp.Value.capacity) {
                $totalCapacity += [long]$diskProp.Value.capacity
            }
        }
    }

    # No disks at all — strong indicator of a lightweight placeholder shell
    if ($diskCount -eq 0) {
        Write-LogDebug "'$VmName' identified as VLR placeholder — no disks registered."
        return $true
    }

    # Disks present but total allocated capacity is zero — placeholder VMDK backing
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
    } catch {
        return ''
    }
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
            Write-LogWarn "Multiple VMs found matching name '$Name' ($($result.Count) results) — using first match. Verify name uniqueness."
        }
        return $result[0]
    } catch {
        Write-LogDebug "VM lookup failed for '$Name': $($_.Exception.Message)"
        return $null
    }
}

function Get-VMDetail {
    <# Returns the full VM configuration object including placement (host, datastore, folder). #>
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

    Checked before attempting a Guest OS Shutdown. If tools are not installed or not
    running, the shutdown sequence skips to Power Off immediately.

    API endpoint: GET /api/vcenter/vm/{vm}/tools
    Relevant response field: run_state (NOT_INSTALLED | NOT_RUNNING | RUNNING)
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

function Resolve-HostName {
    <#
    .SYNOPSIS Returns the human-readable ESX hostname for a host MOR ID, with in-memory caching.

    Cache is pre-populated by Initialize-LocalHostIds at startup, so this function typically
    returns immediately. If a cache miss occurs (e.g. for a host not in the initial set), it
    falls back to GET /api/vcenter/host with a hosts query parameter — the list endpoint
    with a filter is more broadly supported than the individual GET /api/vcenter/host/{id} path.
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
    .SYNOPSIS Resolves the ESX host currently serving a given VM.
    .DESCRIPTION
        vSphere REST does not expose placement on VM read responses, so we use the
        reverse map built at startup by Initialize-LocalHostIds (VMs enumerated per
        host via VM.FilterSpec.hosts). On a cache miss — which can happen if a VM
        vMotioned after startup — we refresh by re-querying VMs for each local host
        until we find it.
    #>
    param(
        [Parameter(Mandatory)][string]$VmId,
        [string]$PlacementHostId   # ignored — kept for call-site compatibility
    )

    # Stage 1: reverse-map cache hit (the common path)
    if ($Script:VmToHostMap.ContainsKey($VmId)) {
        $entry = $Script:VmToHostMap[$VmId]
        Write-LogDebug "ESX host resolved from VM->host map: $($entry.HostName) ($($entry.HostId))"
        return [PSCustomObject]@{ Id = $entry.HostId; Name = $entry.HostName }
    }

    # Stage 2: post-vMotion refresh — re-scan local hosts for this specific VM
    Write-LogDebug "VM '$VmId' not in startup map; refreshing from live host scan."
    foreach ($hostId in $Script:LocalHostIds) {
        try {
            $vmsOnHost = Invoke-VCenterAPI -Method GET -Endpoint '/api/vcenter/vm' `
                -SessionToken $Script:SessionId -QueryParams @{ 'hosts' = $hostId }
            foreach ($v in $vmsOnHost) {
                if ($v.vm -eq $VmId) {
                    $hostName = Resolve-HostName -HostId $hostId
                    $Script:VmToHostMap[$VmId] = [PSCustomObject]@{
                        HostId = $hostId; HostName = $hostName
                    }
                    Write-LogDebug "ESX host resolved via live scan: $hostName ($hostId)"
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

    Reads the VMDK backing path of the disk labelled 'Hard disk 1'. The backing path format
    is '[DatastoreName] folder/file.vmdk', so the datastore name is the text between the
    first pair of square brackets.

    This approach avoids a separate datastore MOR ID lookup and works reliably across
    vCenter versions where the individual /api/vcenter/datastore/{id} endpoint may not
    return the expected response.

    Returns 'Unknown' if the disk, backing, or path cannot be found.
    #>
    param([PSCustomObject]$VmDetail)

    if (-not $VmDetail -or -not $VmDetail.disks) { return 'Unknown' }

    foreach ($diskProp in $VmDetail.disks.PSObject.Properties) {
        $disk = $diskProp.Value
        if ($disk.label -eq 'Hard disk 1') {
            if ($disk.backing -and $disk.backing.vmdk_file) {
                # Backing path format: '[DatastoreName] folder/file.vmdk'
                if ($disk.backing.vmdk_file -match '^\[([^\]]+)\]') {
                    Write-LogDebug "Datastore resolved from disk backing: '$($Matches[1])'"
                    return $Matches[1]
                }
            }
            # Found the disk but the backing path is missing or unparseable
            Write-LogDebug 'Hard disk 1 found but backing vmdk_file path could not be parsed.'
            break
        }
    }
    return 'Unknown'
}
#endregion

#region ── VM Placement Refresh ───────────────────────────────────────────────
function Update-VMPlacements {
    <#
    .SYNOPSIS Detects host/datastore migrations that occurred while VMs were powered
    off, and updates the VM objects in place so the power-on phase uses accurate
    placement data for per-host and per-datastore concurrency limiting.

    Strategy:
      1. Rebuild $Script:VmToHostMap from live vCenter data (GET /api/vcenter/vm
         ?hosts={hostId} per host). This clears the stale startup map so that
         Resolve-VMHost returns current placement, not pre-migration placement.
      2. For each VM, call Resolve-VMHost to get the current host from the fresh map.
      3. Compare current vs pre-power-down host by BOTH ID and name. Name comparison
         catches cases where HostId was null at resolution time.
      4. Log and update any VM objects where placement has changed.
    #>
    param([System.Collections.Generic.List[object]]$VMList)

    Write-LogInfo "Refreshing VM-to-host map from live vCenter data before checking for migrations..." -nc

    # Rebuild the map so Resolve-VMHost sees current post-migration placement,
    # not the stale startup snapshot.
    Rebuild-VmToHostMap

    Write-LogInfo "Checking placement for $($VMList.Count) VM(s)..." -nc
    $migrated = 0

    foreach ($vm in $VMList) {
        try {
            # Resolve-VMHost now reads from the freshly rebuilt map
            $resolvedHost = Resolve-VMHost -VmId $vm.VmId -PlacementHostId $null
            $newHostId    = $resolvedHost.Id
            $newHostName  = $resolvedHost.Name

            # Refresh datastore from VM detail (VMDK backing path)
            $detail    = Get-VMDetail -VmId $vm.VmId
            $newDsId   = if ($detail -and $detail.placement) { $detail.placement.datastore } else { $vm.DatastoreId }
            $newDsName = if ($detail) { Get-DatastoreNameFromVMDetail -VmDetail $detail } else { $vm.DatastoreName }

            # Compare by ID when both are non-null; fall back to name comparison
            # when IDs are not available (handles environments where resolution
            # returned a name but ID was null at initial resolution time).
            $hostIdChanged   = $newHostId   -and $vm.HostId   -and ($newHostId   -ne $vm.HostId)
            $hostNameChanged = $newHostName -and $vm.HostName -and ($newHostName -ne 'Unknown') -and ($newHostName -ne $vm.HostName)
            $dsChanged       = $newDsId    -and $vm.DatastoreId -and ($newDsId   -ne $vm.DatastoreId)

            if ($hostIdChanged -or $hostNameChanged -or $dsChanged) {
                $oldHostName = $vm.HostName
                $oldDsName   = $vm.DatastoreName

                $vm.HostId        = $newHostId
                $vm.HostName      = $newHostName
                $vm.DatastoreId   = $newDsId
                $vm.DatastoreName = $newDsName

                $parts = @()
                if ($hostIdChanged -or $hostNameChanged) { $parts += "Host: $oldHostName -> $newHostName" }
                if ($dsChanged)                          { $parts += "DS: $oldDsName -> $newDsName" }
                Write-LogWarn "$($vm.VmName) migrated since power-down — $($parts -join ' | ')"
                $migrated++
            } else {
                Write-LogDebug "$($vm.VmName): placement unchanged — $newHostName | $newDsName"
                # Always refresh with current values in case a field was Unknown before
                if ($newHostName -ne 'Unknown') {
                    $vm.HostId   = $newHostId
                    $vm.HostName = $newHostName
                }
                if ($newDsName -ne 'Unknown') { $vm.DatastoreName = $newDsName }
            }
        } catch {
            Write-LogDebug "Could not refresh placement for '$($vm.VmName)': $($_.Exception.Message)"
        }
    }

    Write-Host ''
    if ($migrated -gt 0) {
        Write-LogWarn "$migrated VM(s) changed host or datastore since power-down. Placement data updated for power-on."
    } else {
        Write-LogInfo 'No VM migrations detected. Placement data is current.'
    }
}
#endregion

#region ── Power Command Functions ────────────────────────────────────────────
function Send-GuestShutdown {
    <#
    .SYNOPSIS Issues a Guest OS Shutdown command via VMware Tools (graceful).
    In dry-run mode, returns $true silently — the caller emits the dry-run message.
    Returns $true on successful command dispatch, $false on API failure.
    #>
    param([Parameter(Mandatory)][string]$VmId)

    if ($Script:IsDryRun) { return $true }

    try {
        Invoke-VCenterAPI -Method POST -Endpoint "/api/vcenter/vm/$VmId/guest/power" `
            -SessionToken $Script:SessionId -QueryParams @{ action = 'shutdown' } | Out-Null
        return $true
    } catch {
        Write-LogDebug "Guest OS Shutdown command failed for '$VmId': $($_.Exception.Message)"
        return $false
    }
}

function Send-PowerOff {
    <#
    .SYNOPSIS Issues a forced Power Off command via the vSphere API.
    Used for both the Power Off and Hard Stop escalation steps.
    In dry-run mode, returns $true silently — the caller emits the dry-run message.
    Returns $true on successful command dispatch, $false on API failure.
    #>
    param([Parameter(Mandatory)][string]$VmId)

    if ($Script:IsDryRun) { return $true }

    try {
        Invoke-VCenterAPI -Method POST -Endpoint "/api/vcenter/vm/$VmId/power" `
            -SessionToken $Script:SessionId -QueryParams @{ action = 'stop' } | Out-Null
        return $true
    } catch {
        Write-LogDebug "Power Off command failed for '$VmId': $($_.Exception.Message)"
        return $false
    }
}

function Send-PowerOn {
    <#
    .SYNOPSIS Issues a Power On command via the vSphere API.
    In dry-run mode, returns $true silently — the caller emits the dry-run message.
    Returns $true on success.
    #>
    param([Parameter(Mandatory)][string]$VmId)

    if ($Script:IsDryRun) { return $true }

    try {
        Invoke-VCenterAPI -Method POST -Endpoint "/api/vcenter/vm/$VmId/power" `
            -SessionToken $Script:SessionId -QueryParams @{ action = 'start' } | Out-Null
        return $true
    } catch {
        Write-LogDebug "Power On command failed for '$VmId': $($_.Exception.Message)"
        return $false
    }
}
#endregion

#region ── Concurrent Operation Engine ───────────────────────────────────────
function Invoke-PowerOperation {
    <#
    .SYNOPSIS Executes power-down or power-on operations across a list of resolved VMs
    with configurable global, per-host, and per-datastore concurrency limits.

    Power-Down Escalation (per VM):
        Step 1 — Guest OS Shutdown
            Issued only if VMware Tools is RUNNING and -ForceOff is NOT set.
            Timeout: $TIMEOUT_GUEST_SHUTDOWN seconds.
        Step 2 — Power Off
            Issued when step 1 is skipped or times out.
            Timeout: $TIMEOUT_POWER_OFF seconds.
        Step 3 — Hard Stop
            Final Power Off attempt when step 2 times out.
            Timeout: $TIMEOUT_POWER_OFF seconds.
        Failure
            VM is marked as Error and added to the skipped summary.

    Result values:
        Complete — Operation performed and confirmed.
        Skipped  — VM was already in the desired state; no action taken.
        Error    — Operation failed after all retries.

    In dry-run mode:
        - Per-VM "Sending" and "Complete" console messages are suppressed.
        - "[DRY-RUN] [n/total] VmName (VmId): Would send..." messages are shown instead.
        - The polling sleep is skipped so the simulation completes instantly.
    #>
    param(
        [Parameter(Mandatory)][object[]]$VMList,
        [Parameter(Mandatory)][ValidateSet('PowerDown', 'PowerOn')][string]$Operation
    )

    $totalVMs    = $VMList.Count
    $opLabel     = if ($Operation -eq 'PowerDown') { 'Power-Down' } else { 'Power-On' }
    $activityLabel = if ($Operation -eq 'PowerDown') { 'Power Down' } else { 'Power On' }
    $targetState = if ($Operation -eq 'PowerDown') { 'POWERED_OFF' } else { 'POWERED_ON' }

    Write-Host ''
    Write-Host "  ── $opLabel Phase $('─' * 50)" -ForegroundColor DarkCyan
    Write-Host "  Scope      : $totalVMs VM(s)" -ForegroundColor Cyan
    $concGlobalStr = if ($ConcurrentGlobal -eq 0) { 'Unlimited' } else { "$ConcurrentGlobal" }
    Write-Host "  Concurrency: Global=$concGlobalStr | Per-Host=$ConcurrentHost | Per-Datastore=$ConcurrentDatastore" -ForegroundColor Cyan
    if ($Operation -eq 'PowerDown' -and $Script:ForceOff) {
        Write-Host '  Mode       : Force-Off (Guest OS Shutdown bypassed for all VMs)' -ForegroundColor Yellow
    }
    Write-Host ''
    Write-LogInfo "$opLabel phase starting — $totalVMs VM(s) in scope." -nc

    # ── Build per-VM operation tracker ────────────────────────────────────
    $tracker = [ordered]@{}
    $seqNum  = 0

    foreach ($vm in $VMList) {
        $seqNum++
        $tracker[$vm.VmId] = [PSCustomObject]@{
            SeqNum        = $seqNum
            VmId          = $vm.VmId
            VmName        = $vm.VmName
            Activity      = $activityLabel
            HostId        = $vm.HostId
            HostName      = $vm.HostName
            DatastoreId   = $vm.DatastoreId
            DatastoreName = $vm.DatastoreName
            State         = $ST_PENDING
            Phase         = $null
            PhaseStart    = $null
            RetryCount          = 0
            NextRetryAt         = $null   # Throttles rapid command-send retries
            OperationStartedAt  = $null   # DateTime when VM first went Active; set once, never overwritten
            Result              = $null
            CompletedAt         = $null
            Notes               = $null
        }
    }

    $completedCount      = 0
    $iteration           = 0
    # Tracks elapsed seconds for each VM that successfully completed this phase.
    # Used to compute a rolling average and derive the ETA shown in the status bar.
    # Scoped to this function call, so it resets automatically between power phases.
    $completionDurations = [System.Collections.Generic.List[double]]::new()

    # ── Main polling loop ──────────────────────────────────────────────────
    while ($true) {
        $iteration++

        # ── 1. Poll all active VMs for state changes and phase escalation ──
        $activeNow = @($tracker.Values | Where-Object { $_.State -eq $ST_ACTIVE })

        foreach ($entry in $activeNow) {
            $elapsed    = ((Get-Date) - $entry.PhaseStart).TotalSeconds
            $powerState = if ($Script:IsDryRun) { $targetState } else { Get-VMPowerState -VmId $entry.VmId }

            if ($powerState -eq $targetState) {
                # VM reached the desired power state — operation succeeded
                $now               = Get-Date
                $entry.State       = $ST_COMPLETE
                $entry.Result      = $RES_COMPLETE
                $entry.CompletedAt = $now.ToString('MM/dd/yyyy HH:mm:ss')
                $completedCount++
                # Record duration for ETA calculation (only for VMs that actually ran)
                if ($entry.OperationStartedAt) {
                    $completionDurations.Add(($now - $entry.OperationStartedAt).TotalSeconds)
                }
                # Suppress "complete" confirmation in dry-run; the DRY-RUN "Would send" message is sufficient
                if (-not $Script:IsDryRun) {
                    Write-LogOK "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): $opLabel complete."
                }

            } elseif ($Operation -eq 'PowerDown') {
                # ── Power-down escalation: Guest OS Shutdown -> Power Off -> Hard Stop
                switch ($entry.Phase) {

                    $PH_GUEST_SHUTDOWN {
                        if ($elapsed -gt $TIMEOUT_GUEST_SHUTDOWN) {
                            Write-LogWarn "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): Guest OS Shutdown timed out after ${elapsed}s. Issuing Power Off."
                            $ok = Send-PowerOff -VmId $entry.VmId
                            $entry.Phase      = $PH_POWER_OFF
                            $entry.PhaseStart = Get-Date
                            if (-not $ok) {
                                Write-LogDebug "$($entry.VmName): Power Off command failed after Guest Shutdown timeout — will retry on next poll."
                            }
                        }
                    }

                    $PH_POWER_OFF {
                        if ($elapsed -gt $TIMEOUT_POWER_OFF) {
                            Write-LogWarn "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): Power Off timed out after ${elapsed}s. Issuing Hard Stop."
                            $ok = Send-PowerOff -VmId $entry.VmId  # Hard Stop uses the same API — final attempt
                            if ($ok) {
                                $entry.Phase      = $PH_HARD_STOP
                                $entry.PhaseStart = Get-Date
                            } else {
                                $entry.State    = $ST_FAILED
                                $entry.Result   = $RES_ERROR
                                $entry.Notes    = 'Hard Stop command could not be sent.'
                                $completedCount++
                                $Script:SkippedVMs.Add($entry.VmName)
                                Write-LogError "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): Hard Stop command failed. Manual intervention required."
                            }
                        }
                    }

                    $PH_HARD_STOP {
                        if ($elapsed -gt $TIMEOUT_POWER_OFF) {
                            # All three escalation steps exhausted
                            $entry.State    = $ST_FAILED
                            $entry.Result   = $RES_ERROR
                            $entry.Notes    = 'VM did not respond to Hard Stop within timeout. Manual intervention required.'
                            $completedCount++
                            $Script:SkippedVMs.Add($entry.VmName)
                            Write-LogError "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): Hard Stop timed out. Marking as failed — manual intervention required."
                        }
                    }
                }

            } elseif ($Operation -eq 'PowerOn') {
                # ── Power-on timeout and retry handling ──────────────────
                if ($elapsed -gt $TIMEOUT_POWERON) {
                    if ($entry.RetryCount -lt $MAX_RETRIES) {
                        $entry.RetryCount++
                        $entry.State       = $ST_PENDING  # Re-queue for retry
                        $entry.NextRetryAt = (Get-Date).AddSeconds($RETRY_DELAY)
                        Write-LogWarn "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): Power-on timed out. Retry $($entry.RetryCount)/$MAX_RETRIES in ${RETRY_DELAY}s."
                    } else {
                        $entry.State    = $ST_FAILED
                        $entry.Result   = $RES_ERROR
                        $entry.Notes    = "Power-on failed after $MAX_RETRIES retries."
                        $completedCount++
                        $Script:SkippedVMs.Add($entry.VmName)
                        Write-LogError "[$($entry.SeqNum)/$totalVMs] $($entry.VmName): Power-on failed after all retries. Skipping."
                    }
                }
            }
        }

        # ── 2. Start pending VMs within concurrency limits ──────────────────
        $pendingNow = @($tracker.Values | Where-Object { $_.State -eq $ST_PENDING })

        foreach ($entry in $pendingNow) {
            # Honor retry delay — do not attempt until the scheduled window has passed
            if ($entry.NextRetryAt -and (Get-Date) -lt $entry.NextRetryAt) { continue }

            # Re-evaluate concurrency counts per candidate to stay accurate
            $activeAll  = @($tracker.Values | Where-Object { $_.State -eq $ST_ACTIVE })
            $activeHost = @($activeAll       | Where-Object { $_.HostId -eq $entry.HostId })
            $activeDS   = @($activeAll       | Where-Object { $_.DatastoreId -eq $entry.DatastoreId })

            # All three gates must pass before an operation is started
            if ($ConcurrentGlobal -gt 0 -and $activeAll.Count  -ge $ConcurrentGlobal) { continue }
            if ($activeHost.Count -ge $ConcurrentHost)                                  { continue }
            if ($activeDS.Count   -ge $ConcurrentDatastore)                             { continue }

            $pos         = "[$($entry.SeqNum)/$totalVMs]"
            $vmLabel     = "$($entry.VmName) ($($entry.VmId))"
            $retrySuffix = if ($entry.RetryCount -gt 0) { " (retry $($entry.RetryCount)/$MAX_RETRIES)" } else { '' }

            # ── Power-Down start logic ──────────────────────────────────────
            if ($Operation -eq 'PowerDown') {

                $curState = if ($Script:IsDryRun) { 'POWERED_ON' } else { Get-VMPowerState -VmId $entry.VmId }
                if ($curState -eq 'POWERED_OFF') {
                    $entry.State       = $ST_COMPLETE
                    $entry.Result      = $RES_SKIPPED
                    $entry.Notes       = 'Already powered off.'
                    $entry.CompletedAt = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
                    $completedCount++
                    Write-LogInfo "$pos $($entry.VmName): Already powered off — skipping."
                    continue
                }

                $startPhase = $null
                $cmdOk      = $false

                if ($Script:ForceOff) {
                    if ($Script:IsDryRun) {
                        Write-LogDryRun "$pos ${vmLabel}: Would send Power Off (force-off mode)${retrySuffix}."
                    } else {
                        Write-LogInfo "$pos $($entry.VmName): Issuing Power Off (force-off mode)${retrySuffix}."
                    }
                    $cmdOk      = Send-PowerOff -VmId $entry.VmId
                    $startPhase = $PH_POWER_OFF

                } else {
                    $toolsRunning = if ($Script:IsDryRun) { $true } else { Get-VMToolsRunning -VmId $entry.VmId }

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
                            Write-LogInfo "$pos $($entry.VmName): VMware Tools not running — skipping Guest OS Shutdown, issuing Power Off${retrySuffix}."
                        }
                        $cmdOk      = Send-PowerOff -VmId $entry.VmId
                        $startPhase = $PH_POWER_OFF
                    }
                }

                if ($cmdOk) {
                    $now                       = Get-Date
                    $entry.Phase               = $startPhase
                    $entry.PhaseStart          = $now
                    $entry.OperationStartedAt  = $now   # Set once; measures total op duration
                    $entry.State               = $ST_ACTIVE
                    $entry.NextRetryAt         = $null
                } else {
                    if ($entry.RetryCount -lt $MAX_RETRIES) {
                        $entry.RetryCount++
                        $entry.NextRetryAt = (Get-Date).AddSeconds($RETRY_DELAY)
                        Write-LogWarn "$pos $($entry.VmName): Command failed. Retry $($entry.RetryCount)/$MAX_RETRIES in ${RETRY_DELAY}s."
                    } else {
                        $entry.State    = $ST_FAILED
                        $entry.Result   = $RES_ERROR
                        $entry.Notes    = "Command could not be sent after $MAX_RETRIES retries."
                        $completedCount++
                        $Script:SkippedVMs.Add($entry.VmName)
                        Write-LogError "$pos $($entry.VmName): Command failed after all retries. Skipping."
                    }
                }

            # ── Power-On start logic ────────────────────────────────────────
            } elseif ($Operation -eq 'PowerOn') {

                $curState = if ($Script:IsDryRun) { 'POWERED_OFF' } else { Get-VMPowerState -VmId $entry.VmId }
                if ($curState -eq 'POWERED_ON') {
                    $entry.State       = $ST_COMPLETE
                    $entry.Result      = $RES_SKIPPED
                    $entry.Notes       = 'Already powered on.'
                    $entry.CompletedAt = (Get-Date).ToString('MM/dd/yyyy HH:mm:ss')
                    $completedCount++
                    Write-LogInfo "$pos $($entry.VmName): Already powered on — skipping."
                    continue
                }

                if ($Script:IsDryRun) {
                    Write-LogDryRun "$pos ${vmLabel}: Would send Power On${retrySuffix}."
                } else {
                    Write-LogInfo "$pos $($entry.VmName): Sending Power On${retrySuffix}."
                }
                $ok = Send-PowerOn -VmId $entry.VmId

                if ($ok) {
                    $now                       = Get-Date
                    $entry.Phase               = $PH_POWERON
                    $entry.PhaseStart          = $now
                    $entry.OperationStartedAt  = $now   # Set once; measures total op duration
                    $entry.State               = $ST_ACTIVE
                    $entry.NextRetryAt         = $null
                } else {
                    if ($entry.RetryCount -lt $MAX_RETRIES) {
                        $entry.RetryCount++
                        $entry.NextRetryAt = (Get-Date).AddSeconds($RETRY_DELAY)
                        Write-LogWarn "$pos $($entry.VmName): Power On command failed. Retry $($entry.RetryCount)/$MAX_RETRIES in ${RETRY_DELAY}s."
                    } else {
                        $entry.State    = $ST_FAILED
                        $entry.Result   = $RES_ERROR
                        $entry.Notes    = "Power On could not be sent after $MAX_RETRIES retries."
                        $completedCount++
                        $Script:SkippedVMs.Add($entry.VmName)
                        Write-LogError "$pos $($entry.VmName): Power On failed after all retries. Skipping."
                    }
                }
            }
        }

        # ── 3. Update progress display with rolling ETA ────────────────────
        $stillAct = @($tracker.Values | Where-Object { $_.State -eq $ST_ACTIVE  }).Count
        $stillPen = @($tracker.Values | Where-Object { $_.State -eq $ST_PENDING }).Count
        $pct      = if ($totalVMs -gt 0) { [int](($completedCount / $totalVMs) * 100) } else { 100 }

        # Build ETA string from rolling average of completed VM durations.
        # Resets each phase since power-down and power-on have different timing profiles.
        $etaStr = ''
        if ($completionDurations.Count -gt 0 -and ($stillAct + $stillPen) -gt 0) {
            $avgSec    = ($completionDurations | Measure-Object -Average).Average
            $remSec    = [int]($avgSec * ($stillAct + $stillPen))
            $etaAt     = (Get-Date).AddSeconds($remSec)
            $remStr    = if ($remSec -ge 3600) {
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

        Write-Progress -Activity "$opLabel Operations" `
            -Status "$completedCount/$totalVMs complete | $stillAct active | $stillPen pending | $pct%$etaStr" `
            -PercentComplete $pct

        Write-LogDebug "Poll #$iteration — Complete: $completedCount | Active: $stillAct | Pending: $stillPen"

        # ── 4. Exit when all VMs have reached a terminal state ──────────────
        if (($stillAct + $stillPen) -eq 0) { break }

        # In dry-run, skip the real sleep and note that a pause would occur
        if ($Script:IsDryRun) {
            Write-LogDryRun "Would pause ${POLL_INTERVAL}s while waiting for VM power state changes."
        } else {
            Start-Sleep -Seconds $POLL_INTERVAL
        }
    }

    Write-Progress -Activity "$opLabel Operations" -Completed
    Write-LogInfo "$opLabel phase complete — $completedCount/$totalVMs VM(s) processed." -nc

    return @($tracker.Values)
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

        # Skip common column header values (case-insensitive)
        if ($trimmed -imatch '^(vmname|vm_name|name|vm|hostname|host_name|displayname)$') { continue }

        # Strip CSV-style quoting
        $trimmed = $trimmed.Trim('"').Trim("'").Trim()
        if (-not [string]::IsNullOrWhiteSpace($trimmed)) { $names.Add($trimmed) }
    }

    return $names.ToArray()
}
#endregion

#region ── Result Output ──────────────────────────────────────────────────────
function Export-Results {
    <#
    .SYNOPSIS Outputs operation results in the requested format:
      Table    — formatted table printed inline in the terminal.
      CSV      — comma-separated file written to the script directory.
      Text     — plain-text table file written to the script directory.
      GridView — interactive, filterable Out-GridView window (Windows only).
    Not available in dry-run mode (blocked before this function is called).
    #>
    param(
        [object[]]$Results,
        [string]$Format,
        [string]$BaseName
    )

    $rows = $Results | Select-Object `
        @{N = 'VM Name';      E = { $_.VmName }},
        @{N = 'Activity';     E = { $_.Activity }},
        @{N = 'Status';       E = { $_.Result }},
        @{N = 'ESX Host';     E = { $_.HostName }},
        @{N = 'Datastore';    E = { $_.DatastoreName }},
        @{N = 'Completed At'; E = { if ($_.CompletedAt) { $_.CompletedAt } else { 'N/A' } }}

    switch ($Format) {
        'Table' {
            Write-Host ''
            Write-Host '  ── Results ─────────────────────────────────────────────────' -ForegroundColor Cyan
            Write-Host ($rows | Format-Table -AutoSize | Out-String)
        }
        'CSV' {
            $path = Join-Path $SCRIPT_DIR "${BaseName}.csv"
            $rows | Export-Csv -Path $path -NoTypeInformation -Encoding UTF8
            Write-LogOK "Results saved to: $path"
        }
        'Text' {
            $path = Join-Path $SCRIPT_DIR "${BaseName}.txt"
            $rows | Format-Table -AutoSize | Out-String | Set-Content -Path $path -Encoding UTF8
            Write-LogOK "Results saved to: $path"
        }
        'GridView' {
            Write-LogInfo 'Opening results in GridView window...' -nc
            $rows | Out-GridView -Title "$SCRIPT_NAME — Operation Results"
        }
    }
}
#endregion

#region ── Display Helpers ────────────────────────────────────────────────────
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
    for "Succeeded") from bleeding into the left and right border characters.

    Box inner width = 62 characters:
      "  " (2) + label.PadRight(27) + " : " (3) + value.PadRight(30) = 62
    #>
    param(
        [string]$Label,
        [string]$Value,
        [string]$Color = 'Cyan'
    )
    $content = "  {0,-27} : {1,-30}" -f $Label, $Value
    Write-Host '  ' -NoNewline
    Write-Host '│' -NoNewline -ForegroundColor Cyan
    Write-Host $content -NoNewline -ForegroundColor $Color
    Write-Host '│' -ForegroundColor Cyan
}

function Show-Help {
    $w = 68  # total width between outer edges of box chars

    Write-Host ''
    Write-Host "  ╔$('═' * $w)╗" -ForegroundColor Cyan
    Write-Host "  ║   $('VM Power Operations Manager — Usage Guide'.PadRight($w - 3))║" -ForegroundColor Cyan
    Write-Host "  ╚$('═' * $w)╝" -ForegroundColor Cyan
    Write-Host ''

    $h = {
        param([string]$line = '', [System.ConsoleColor]$c = [System.ConsoleColor]::Gray)
        Write-Host "  $line" -ForegroundColor $c
    }

    & $h 'DESCRIPTION' Cyan
    & $h '  Performs power operations on VMware VMs via the vCenter REST API.'
    & $h '  Supports concurrent execution with per-host and per-datastore throttling,'
    & $h '  graceful shutdown escalation, dry-run simulation, and structured logging.'
    Write-Host ''

    & $h 'USAGE' Cyan
    & $h '  .\Invoke-VMPowerOps.ps1 [operation] [options]'
    Write-Host ''

    & $h 'OPERATIONS  (specify exactly one)' Cyan
    & $h '  -PowerDown  / -pd    Shutdown: Guest OS Shutdown -> Power Off -> Hard Stop'
    & $h '  -PowerOn    / -po    Power on all scoped VMs'
    & $h '  -PowerCycle / -pc    Shutdown then power-on (prompts between phases)'
    Write-Host ''

    & $h 'REQUIRED' Cyan
    & $h '  -VCenterServer / -vc <fqdn|ip>   Target vCenter server FQDN or IP address'
    & $h '  -SourceFile    / -s  <path>       CSV or TXT file with VM display names (one per line)'
    Write-Host ''

    & $h 'OPTIONS' Cyan
    & $h '  -DryRun         / -d              Simulate operations without making changes'
    & $h '  -ForceOff       / -f              Bypass Guest OS Shutdown; begin at Power Off'
    & $h '  -VerboseLogging / -v              Display full timestamped log output in terminal'
    & $h '  -ResultOutput   / -r <format>     Table, CSV, Text, or GridView  (not available with -DryRun)'
    & $h '                                    GridView opens an interactive window (Windows only)'
    & $h '  -Help           / -h              Display this help and exit'
    Write-Host ''

    & $h 'CONCURRENCY' Cyan
    & $h '  -ConcurrentGlobal    / -cg <n>    Max total concurrent ops  (0 = unlimited, default)'
    & $h '  -ConcurrentHost      / -ch <n>    Max concurrent ops per ESX host  (1-10, default: 5)'
    & $h '  -ConcurrentDatastore / -cd <n>    Max concurrent ops per datastore  (1-10, default: 5)'
    Write-Host ''

    & $h 'EXAMPLES' Cyan
    & $h '  .\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.csv -pc -r Table -ch 3'
    & $h '  .\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.txt -pd -r CSV -cg 10 -v'
    & $h '  .\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.csv -pd -f'
    & $h '  .\Invoke-VMPowerOps.ps1 -vc vcenter.corp.local -s vms.csv -po -d'
    Write-Host ''

    & $h 'REQUIRED VCENTER PERMISSIONS' Cyan
    & $h '  Assign a custom role at the vCenter Server level (Propagate to Children).'
    & $h '  For a more granular scope, assign at the Host or VM Folder level instead.'
    & $h '  The service account needs at minimum:'
    & $h ''
    & $h '    Virtual Machine > Interaction:'
    & $h '      Power On, Power Off'
    & $h '    Virtual Machine > Change Configuration > Change Settings:'
    & $h '      Change Settings'
    & $h '    Datastore:'
    & $h '      Browse Datastore  (resolves parent datastore from VMDK backing path)'
    & $h '    Sessions > Validate session  (REST API session management)'
    & $h '    Global > Licenses'
    & $h ''
    & $h '  Tip: Clone the built-in Read-Only role and add the VM Interaction privileges.'
    & $h '  See the #region Required vCenter Permissions block in the script for full detail.'
    Write-Host ''

    & $h 'NOTES' Cyan
    & $h '  - Input file headers (vmname, name, vm, etc.) are automatically skipped.'
    & $h '  - SRM/VLR placeholder VMs are detected and excluded automatically.'
    & $h '  - In Linked Mode environments, only VMs on the target vCenter are processed.'
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
    $Script:LogFile = Join-Path $SCRIPT_DIR "VMPowerOps_${dateStamp}.log"
    $baseOutName    = "VMPowerOps_${dateStamp}"

    Show-Banner

    # Diagnostic init entries go to the log only — banner covers user-facing context
    Write-LogInfo "$SCRIPT_NAME v$SCRIPT_VERSION initializing." -nc
    Write-LogInfo "Script file   : $SCRIPT_FILE" -nc
    Write-LogInfo "PowerShell    : $($PSVersionTable.PSVersion)" -nc
    Write-LogInfo "Script dir    : $SCRIPT_DIR" -nc
    Write-LogInfo "Log file      : $Script:LogFile" -nc
    if ($Script:IsDryRun)  { Write-LogInfo 'Mode          : DRY-RUN' -nc }
    if ($Script:ForceOff)  { Write-LogInfo 'Force-Off     : Enabled' -nc }
    if ($Script:IsVerbose) { Write-LogInfo 'Logging       : Verbose/debug enabled' -nc }

    # ── Resolve effective result output format ────────────────────────────────
    # $ResultOutput carries a [ValidateSet] attribute and cannot be reassigned to an
    # empty string — doing so triggers a validation error at runtime. Copy the value
    # into a plain local variable so it can be suppressed in dry-run without error.
    $activeResultOutput = $ResultOutput
    if ($Script:IsDryRun -and $activeResultOutput) {
        Write-Host '  Note: Result output (-r) is not available in dry-run mode and will be ignored.' -ForegroundColor Yellow
        Write-LogInfo 'ResultOutput ignored — not permitted in dry-run mode.' -nc
        $activeResultOutput = ''
    }

    # ── Validate mutually exclusive power operation flags ────────────────────
    $opFlagCount = 0
    if ($PowerDown.IsPresent)  { $opFlagCount++; $Script:Operation = 'PowerDown' }
    if ($PowerOn.IsPresent)    { $opFlagCount++; $Script:Operation = 'PowerOn' }
    if ($PowerCycle.IsPresent) { $opFlagCount++; $Script:Operation = 'PowerCycle' }

    if ($opFlagCount -gt 1) {
        Write-LogError 'Only one operation flag may be specified: -PowerDown (-pd), -PowerOn (-po), or -PowerCycle (-pc).'
        exit 1
    }

    # -ForceOff is not applicable to power-on only operations
    if ($Script:ForceOff -and $Script:Operation -eq 'PowerOn') {
        Write-LogWarn '-ForceOff has no effect with -PowerOn and will be ignored.'
        $Script:ForceOff = $false
    }

    # ── Source file resolution ───────────────────────────────────────────────
    # Only show the "Input File" section header when the user needs to provide the file.
    # If it was supplied as a command-line argument, validate silently.

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
            Where-Object { $_.Name -notlike 'VMPowerOps_*' } |
            Sort-Object LastWriteTime -Descending
        )

        if ($candidates.Count -eq 1) {
            Write-Host ''
            Write-Host '  Detected input file: ' -NoNewline
            Write-Host $candidates[0].FullName -ForegroundColor Yellow
            $confirm = Read-Host '  Use this file? [Y/n]'
            $SourceFile = if ($confirm -match '^[Nn]') {
                Read-Host '  Enter the full path to the VM input file'
            } else {
                $candidates[0].FullName
            }
        } elseif ($candidates.Count -gt 1) {
            Write-Host ''
            Write-Host '  Multiple input files found in script directory:' -ForegroundColor Yellow
            for ($i = 0; $i -lt [Math]::Min($candidates.Count, 9); $i++) {
                Write-Host "    [$($i + 1)] $($candidates[$i].Name)"
            }
            Write-Host ''
            $sel = Read-Host '  Enter selection number or full file path'
            $SourceFile = if ($sel -match '^\d+$' -and [int]$sel -ge 1 -and [int]$sel -le $candidates.Count) {
                $candidates[[int]$sel - 1].FullName
            } else { $sel }
        } else {
            Write-Host ''
            Write-Host '  No CSV or TXT input files detected in the script directory.' -ForegroundColor Yellow
            $SourceFile = Read-Host '  Enter the full path to the VM input file'
        }

        if ([string]::IsNullOrEmpty($SourceFile) -or -not (Test-Path $SourceFile)) {
            Write-LogError "Source file not found: $SourceFile"
            exit 1
        }
    }

    Write-LogInfo "Source file resolved: $SourceFile" -nc

    # ── Read VM list ─────────────────────────────────────────────────────────
    $vmNames = Read-VMList -FilePath $SourceFile

    if ($vmNames.Count -eq 0) {
        Write-LogError 'The input file contains no valid VM names after parsing.'
        exit 1
    }

    Write-LogInfo "Loaded $($vmNames.Count) VM name(s) from: $SourceFile" -nc

    # ── Determine power operation if not provided as an argument ─────────────
    if (-not $Script:Operation) {
        Show-Section 'Select Operation'
        Write-Host ''
        Write-Host '  Choose the operation to perform on the scoped VMs:' -ForegroundColor Cyan
        Write-Host '    [1]  Power-Down  — Guest OS Shutdown -> Power Off -> Hard Stop'
        Write-Host '    [2]  Power-On    — Power on all scoped VMs'
        Write-Host '    [3]  Power-Cycle — Shutdown then power-on'
        Write-Host ''

        do { $opChoice = Read-Host '  Enter choice [1-3]' }
        until ($opChoice -in @('1', '2', '3'))

        $Script:Operation = switch ($opChoice) {
            '1' { 'PowerDown'  }
            '2' { 'PowerOn'    }
            '3' { 'PowerCycle' }
        }
    }

    Write-LogInfo "Operation selected: $($Script:Operation)" -nc

    # ── Confirm scope before collecting credentials ──────────────────────────
    Show-Section 'Confirmation'
    Write-Host ''
    Write-Host '  Operation  : ' -NoNewline; Write-Host $Script:Operation -ForegroundColor Yellow
    Write-Host '  VM Count   : ' -NoNewline; Write-Host $vmNames.Count    -ForegroundColor Yellow
    Write-Host '  Source     : ' -NoNewline; Write-Host $SourceFile        -ForegroundColor Gray
    if ($Script:ForceOff) {
        Write-Host '  Shutdown   : ' -NoNewline; Write-Host 'Force-Off (Guest OS Shutdown bypassed)' -ForegroundColor Yellow
    }
    if ($ConcurrentGlobal -gt 0) {
        Write-Host '  Concurrency: ' -NoNewline
        Write-Host "Global=$ConcurrentGlobal, Per-Host=$ConcurrentHost, Per-DS=$ConcurrentDatastore" -ForegroundColor Gray
    }
    if ($Script:IsDryRun) {
        Write-Host '  Mode       : ' -NoNewline; Write-Host 'DRY-RUN — no changes will be made' -ForegroundColor Magenta
    }

    Write-Host ''
    $confirm = Read-Host "  Confirm $($Script:Operation) for $($vmNames.Count) VM(s)? [Y/n]"
    if ($confirm -match '^[Nn]') {
        Write-LogInfo 'Operation cancelled by user at confirmation prompt.' -nc
        Write-Host '  Operation cancelled.' -ForegroundColor Yellow
        exit 0
    }

    # ── Pre-flight file I/O checks ───────────────────────────────────────────
    Show-Section 'Pre-flight Checks'
    Write-Host ''

    try {
        [void](Get-Content -Path $SourceFile -TotalCount 1 -ErrorAction Stop)
        Write-LogOK 'Input file is readable.'
    } catch {
        Write-Host '  Cannot read the input file. Try running this script as Administrator.' -ForegroundColor Red
        Write-LogError "Read access failed for '$SourceFile': $($_.Exception.Message)" -nc
        exit 1
    }

    try {
        $initTs = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
        Add-Content -Path $Script:LogFile -Value "[$initTs] [INFO ] $SCRIPT_NAME v$SCRIPT_VERSION log initialized." -Encoding UTF8 -ErrorAction Stop
        Write-LogOK "Log file created: $Script:LogFile"
    } catch {
        Write-Host '  Cannot write to the script directory. Try running as Administrator.' -ForegroundColor Red
        Write-LogError "Log file creation failed: $($_.Exception.Message)" -nc
        exit 1
    }

    if ($activeResultOutput -in @('CSV', 'Text')) {
        $testOut = Join-Path $SCRIPT_DIR "${baseOutName}_test.tmp"
        try {
            'test' | Set-Content -Path $testOut -Encoding UTF8 -ErrorAction Stop
            Remove-Item $testOut -Force -ErrorAction SilentlyContinue
            Write-LogOK 'Output directory is writable.'
        } catch {
            Write-Host '  Cannot write output files to the script directory. Try running as Administrator.' -ForegroundColor Red
            Write-LogError "Output directory write access failed: $($_.Exception.Message)" -nc
            exit 1
        }
    }

    # ── vCenter server ────────────────────────────────────────────────────────
    if ([string]::IsNullOrEmpty($Script:VCenter)) {
        Write-Host ''
        $Script:VCenter = Read-Host '  Enter vCenter server FQDN or IP address'
    }

    if ([string]::IsNullOrEmpty($Script:VCenter)) {
        Write-LogError 'No vCenter server specified.'
        exit 1
    }

    Write-LogInfo "Target vCenter: $($Script:VCenter)" -nc

    # ── Collect credentials securely from the terminal ───────────────────────
    Show-Section 'Authentication'
    Write-Host ''
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
        # Second attempt — with SSL certificate bypass (handles self-signed certs)
        Write-LogWarn "Authentication failed. Retrying with SSL certificate bypass..." -nc
        Write-Host '  Certificate validation failed — retrying with bypass enabled...' -ForegroundColor Yellow
        Enable-CertBypass
        $token = Connect-VCenter -Username $vcUsername -Password $vcPassword
    }

    if (-not $token) {
        Write-LogError "Authentication failed for '$vcUsername' on '$($Script:VCenter)'." -nc
        Write-Host '  Authentication failed. Verify credentials and vCenter reachability.' -ForegroundColor Red
        exit 1
    }

    $Script:SessionId = $token
    Write-LogOK "Connected to $($Script:VCenter) as '$vcUsername'."

    # ── Enumerate local ESX hosts (Linked Mode guard) ────────────────────────
    $localHostsOk = Initialize-LocalHostIds
    if (-not $localHostsOk) {
        Write-Host '  Warning: Could not enumerate local ESX hosts. Linked Mode filtering is inactive.' -ForegroundColor Yellow
        Write-Host '           All resolved VMs will be processed regardless of which vCenter hosts them.' -ForegroundColor Yellow
    }

    # ── Resolve VMs from vCenter inventory ───────────────────────────────────
    Show-Section 'Resolving VMs'
    Write-Host ''
    Write-LogInfo "Resolving $($vmNames.Count) VM(s) against $($Script:VCenter)..." -nc

    $resolvedVMs   = [System.Collections.Generic.List[object]]::new()
    $unresolvedVMs = [System.Collections.Generic.List[string]]::new()
    $skippedSRM    = [System.Collections.Generic.List[string]]::new()
    $skippedLinked = [System.Collections.Generic.List[string]]::new()
    $resolveIdx    = 0

    foreach ($vmName in $vmNames) {
        $resolveIdx++

        Write-Progress -Activity 'Resolving VMs' `
            -Status "[$resolveIdx/$($vmNames.Count)] $vmName" `
            -PercentComplete ([int](($resolveIdx / $vmNames.Count) * 100))

        $vmSummary = Resolve-VMByName -Name $vmName

        if (-not $vmSummary) {
            Write-LogWarn "[$resolveIdx/$($vmNames.Count)] Not found: '$vmName'"
            $unresolvedVMs.Add($vmName)
            $Script:SkippedVMs.Add($vmName)
            continue
        }

        $vmDetail    = Get-VMDetail -VmId $vmSummary.vm
        $hostId      = $null
        $datastoreId = $null
        $folderId    = $null

        if ($vmDetail -and $vmDetail.placement) {
            $hostId      = $vmDetail.placement.host
            $datastoreId = $vmDetail.placement.datastore
            $folderId    = $vmDetail.placement.folder
        }

        # Resolve the ESX host authoritatively.
        # For cluster VMs, placement.host is often null — Resolve-VMHost falls back
        # to GET /api/vcenter/host?vms={vmId} which always returns the serving host.
        $resolvedHost = Resolve-VMHost -VmId $vmSummary.vm -PlacementHostId $hostId
        $hostId       = $resolvedHost.Id     # Authoritative MOR ID (may differ from placement)
        $hostName     = $resolvedHost.Name

        # ── Linked Mode filter ────────────────────────────────────────────────
        # Uses the authoritatively resolved host ID, not the raw placement value.
        if ($Script:LocalHostIds.Count -gt 0 -and $hostId -and -not $Script:LocalHostIds.Contains($hostId)) {
            Write-LogWarn "[$resolveIdx/$($vmNames.Count)] '$($vmSummary.name)' belongs to a linked vCenter — skipping."
            $skippedLinked.Add($vmSummary.name)
            $Script:SkippedVMs.Add($vmSummary.name)
            continue
        }

        # ── VLR / SRM placeholder filter ─────────────────────────────────────
        $folderName = Resolve-FolderName -FolderId $folderId
        if (Test-IsSRMPlaceholder -VmName $vmSummary.name -VmDetail $vmDetail -FolderName $folderName) {
            Write-LogWarn "[$resolveIdx/$($vmNames.Count)] '$($vmSummary.name)' is a VLR placeholder — skipping."
            $skippedSRM.Add($vmSummary.name)
            $Script:SkippedVMs.Add($vmSummary.name)
            continue
        }

        $datastoreName = Get-DatastoreNameFromVMDetail -VmDetail $vmDetail

        $resolved = [PSCustomObject]@{
            VmId          = $vmSummary.vm
            VmName        = $vmSummary.name
            HostId        = $hostId
            HostName      = $hostName
            DatastoreId   = $datastoreId
            DatastoreName = $datastoreName
            PowerState    = $vmSummary.power_state
        }

        $resolvedVMs.Add($resolved)
        Write-LogDebug "Resolved [$resolveIdx/$($vmNames.Count)] '$($vmSummary.name)'"
        Write-LogDebug "  ESX Host  : $hostName ($hostId)"
        Write-LogDebug "  Datastore : $datastoreName"
        Write-LogDebug "  Power State: $($vmSummary.power_state)"
    }

    Write-Progress -Activity 'Resolving VMs' -Completed
    Write-LogInfo "Resolution complete: $($resolvedVMs.Count) eligible | $($unresolvedVMs.Count) not found | $($skippedSRM.Count) VLR placeholder | $($skippedLinked.Count) linked vCenter" -nc

    # Surface each skip category to the console
    if ($unresolvedVMs.Count -gt 0) {
        Write-Host ''
        Write-Host "  $($unresolvedVMs.Count) VM(s) not found in inventory:" -ForegroundColor Yellow
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

    Write-LogInfo "$($resolvedVMs.Count) VM(s) queued for $($Script:Operation)." -nc

    # ── Execute power operations ──────────────────────────────────────────────
    $allPhaseResults  = [System.Collections.Generic.List[object]]::new()
    $phaseCompleteTime = $null   # Records when the power-down phase finished

    # ╔══ POWER-DOWN PHASE ══════════════════════════════════════════════════╗
    if ($Script:Operation -in @('PowerDown', 'PowerCycle')) {
        Write-LogInfo "Starting power-down phase — $($resolvedVMs.Count) VM(s)." -nc

        # Log pre-power-down host snapshot so Update-VMPlacements can diff
        # against live data after the user confirms proceeding to power-on.
        if ($Script:Operation -eq 'PowerCycle') {
            Write-LogInfo 'Pre-power-down host snapshot (for migration detection):' -nc
            foreach ($vm in $resolvedVMs) {
                Write-LogInfo "  $($vm.VmName): $($vm.HostName) ($($vm.VmId))" -nc
            }
        }

        $downResults       = Invoke-PowerOperation -VMList $resolvedVMs.ToArray() -Operation 'PowerDown'
        $phaseCompleteTime = Get-Date   # Capture the moment the phase finished
        $allPhaseResults.AddRange($downResults)

        $downFailed = @($downResults | Where-Object { $_.Result -eq $RES_ERROR })
        if ($downFailed.Count -gt 0) {
            Write-Host ''
            Write-Host "  ⚠  $($downFailed.Count) VM(s) failed during power-down:" -ForegroundColor Yellow
            $downFailed | ForEach-Object { Write-Host "     - $($_.VmName)  [$($_.Notes)]" -ForegroundColor Yellow }
        }

        if ($Script:Operation -eq 'PowerCycle') {
            Write-Host ''
            Write-Host '  Power-down phase complete.' -ForegroundColor Cyan
            if ($downFailed.Count -gt 0) {
                Write-Host "  $($downFailed.Count) VM(s) encountered errors — review the list above before proceeding." -ForegroundColor Yellow
            }

            if ($Script:IsDryRun) {
                Write-LogDryRun 'In a live run, you would be prompted here to confirm proceeding to the power-on phase.'
            } else {
                $proceed = Read-Host "  Proceed to power-on phase for $($resolvedVMs.Count) VM(s)? [Y/n]"
                if ($proceed -match '^[Nn]') {
                    Write-LogInfo 'Power-on phase cancelled by user.' -nc
                    Write-Host '  Power-on phase cancelled.' -ForegroundColor Yellow
                    if ($activeResultOutput) { Export-Results -Results $downResults -Format $activeResultOutput -BaseName $baseOutName }
                    Disconnect-VCenter
                    exit 0
                }
            }

            # ── Re-validate VM placement before power-on ──────────────────────
            Show-Section 'Placement Re-validation'
            Write-Host ''
            if ($Script:IsDryRun) {
                Write-LogDryRun 'Would re-query each VM''s current ESX host and compare against pre-power-down placement to detect migrations.'
            } else {
                Update-VMPlacements -VMList $resolvedVMs
            }

            # ── Inter-phase delay (boot storm prevention) ─────────────────────
            # Only wait the time remaining since the power-down phase completed.
            # If the user took longer than $INTER_PHASE_DELAY to confirm, skip the wait.
            $elapsedSincePhase = ((Get-Date) - $phaseCompleteTime).TotalSeconds
            $remaining         = [Math]::Max(0, [int]($INTER_PHASE_DELAY - $elapsedSincePhase))

            Write-Host ''
            if ($remaining -gt 0) {
                Write-LogInfo "Inter-phase delay: ${remaining}s remaining before power-on." -nc
                if ($Script:IsDryRun) {
                    Write-LogDryRun "Would wait ${remaining}s before power-on phase (boot storm prevention). Skipping in dry-run."
                } else {
                    Write-Host "  Waiting ${remaining}s before power-on phase..." -ForegroundColor DarkGray
                    for ($i = $remaining; $i -gt 0; $i--) {
                        Write-Progress -Activity 'Inter-Phase Delay' `
                            -Status "Resuming power-on in ${i}s..." `
                            -PercentComplete ([int](($remaining - $i) / $remaining * 100))
                        Start-Sleep -Seconds 1
                    }
                    Write-Progress -Activity 'Inter-Phase Delay' -Completed
                }
            } else {
                Write-LogInfo "Inter-phase delay not needed — ${INTER_PHASE_DELAY}s already elapsed since power-down completed."
            }
        }
    }

    # ╔══ POWER-ON PHASE ════════════════════════════════════════════════════╗
    if ($Script:Operation -in @('PowerOn', 'PowerCycle')) {
        Write-LogInfo "Starting power-on phase — $($resolvedVMs.Count) VM(s)." -nc

        $onResults = Invoke-PowerOperation -VMList $resolvedVMs.ToArray() -Operation 'PowerOn'
        $allPhaseResults.AddRange($onResults)

        $onFailed = @($onResults | Where-Object { $_.Result -eq $RES_ERROR })
        if ($onFailed.Count -gt 0) {
            Write-Host ''
            Write-Host "  ⚠  $($onFailed.Count) VM(s) failed during power-on:" -ForegroundColor Yellow
            $onFailed | ForEach-Object { Write-Host "     - $($_.VmName)  [$($_.Notes)]" -ForegroundColor Yellow }
        }
    }

    # ── Export results ────────────────────────────────────────────────────────
    if ($activeResultOutput) {
        Export-Results -Results $allPhaseResults.ToArray() -Format $activeResultOutput -BaseName $baseOutName
    }

    # ── Final summary ─────────────────────────────────────────────────────────
    Show-Section 'Summary'

    $elapsed     = (Get-Date) - $Script:StartTime
    $elStr       = '{0:D2}h {1:D2}m {2:D2}s' -f $elapsed.Hours, $elapsed.Minutes, $elapsed.Seconds
    $total       = $allPhaseResults.Count
    $succeeded   = @($allPhaseResults | Where-Object { $_.Result -eq $RES_COMPLETE }).Count
    $alreadyDone = @($allPhaseResults | Where-Object { $_.Result -eq $RES_SKIPPED  }).Count
    $failed      = @($allPhaseResults | Where-Object { $_.Result -eq $RES_ERROR    }).Count
    $unresolved  = $unresolvedVMs.Count
    $srmCount    = $skippedSRM.Count

    # Color selectors for conditional rows
    $failColor = if ($failed -gt 0)      { 'Red'    } else { 'Cyan'   }
    $skipColor = if ($alreadyDone -gt 0) { 'Yellow' } else { 'Cyan'   }
    $warnColor = if ($unresolved -gt 0)  { 'Yellow' } else { 'Cyan'   }
    $srmColor  = if ($srmCount -gt 0)    { 'Yellow' } else { 'Cyan'   }

    # Summary box: inner width = 62 chars between │ and │
    # Format per row: "  " + label.PadRight(27) + " : " + value.PadRight(30) = 62
    Write-Host ''
    Write-Host "  ┌$('─' * 62)┐" -ForegroundColor Cyan

    Write-SummaryRow 'Operation'      $Script:Operation
    Write-SummaryRow 'Input VMs'      $vmNames.Count.ToString()
    Write-SummaryRow 'Resolved'       $resolvedVMs.Count.ToString()
    Write-SummaryRow 'Not Found'      $unresolved.ToString()  $warnColor
    Write-SummaryRow 'VLR Placeholder VMs Skipped' $srmCount.ToString() $srmColor

    # "Operations" row changes label and color in dry-run
    if ($Script:IsDryRun) {
        Write-SummaryRow 'Operations Planned' $total.ToString() 'Magenta'
    } else {
        Write-SummaryRow 'Operations Run'     $total.ToString()
        Write-SummaryRow 'Succeeded'          $succeeded.ToString()   'Green'
        Write-SummaryRow 'Skipped'            $alreadyDone.ToString() $skipColor
        Write-SummaryRow 'Failed'             $failed.ToString()      $failColor
    }

    Write-SummaryRow 'Elapsed Time' $elStr
    Write-SummaryRow 'Log File'     ([IO.Path]::GetFileName($Script:LogFile))

    Write-Host "  └$('─' * 62)┘" -ForegroundColor Cyan
    Write-Host ''

    Write-LogInfo "SUMMARY — Op: $($Script:Operation) | Input: $($vmNames.Count) | Resolved: $($resolvedVMs.Count) | NotFound: $unresolved | VLR: $srmCount | Ops: $total | Succeeded: $succeeded | Skipped: $alreadyDone | Failed: $failed | Elapsed: $elStr" -nc

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
