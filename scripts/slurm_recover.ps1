#!/usr/bin/env pwsh
# slurm_recover.ps1
# Usage: pwsh -File .\scripts\slurm_recover.ps1
# Требования: ssh в PATH, aliases bastion/worker1/worker2 настроены в ~/.ssh/config

$ErrorActionPreference = "Stop"

# ---- CONFIG ----
$Bastion = "bastion"
$Workers = @("worker1", "worker2")
$AllHosts = @($Bastion) + $Workers

# OpenSSH options: accept-new не ломает безопасность, но снимает ручной "yes" на новом ключе
$SshOpts = @(
  "-o", "BatchMode=yes",
  "-o", "ConnectTimeout=10",
  "-o", "ServerAliveInterval=5",
  "-o", "ServerAliveCountMax=2",
  "-o", "StrictHostKeyChecking=accept-new"
)

function Run-SSH {
  param(
    [Parameter(Mandatory=$true)][string]$Host,
    [Parameter(Mandatory=$true)][string]$Cmd
  )
  $args = @("ssh") + $SshOpts + @($Host, $Cmd)
  $p = Start-Process -FilePath $args[0] -ArgumentList $args[1..($args.Count-1)] -NoNewWindow -PassThru -Wait `
        -RedirectStandardOutput "$env:TEMP\ssh_out.txt" -RedirectStandardError "$env:TEMP\ssh_err.txt"

  $out = Get-Content "$env:TEMP\ssh_out.txt" -Raw
  $err = Get-Content "$env:TEMP\ssh_err.txt" -Raw
  return @{
    Code = $p.ExitCode
    Out  = $out.TrimEnd()
    Err  = $err.TrimEnd()
  }
}

function Step($title) {
  Write-Host ""
  Write-Host "== $title ==" -ForegroundColor Cyan
}

function Ok($msg)  { Write-Host "OK  $msg" -ForegroundColor Green }
function Warn($msg){ Write-Host "WARN $msg" -ForegroundColor Yellow }
function Fail($msg){ Write-Host "FAIL $msg" -ForegroundColor Red }

# 1) SSH connectivity checks
Step "STEP 1: SSH connectivity"
foreach ($h in $AllHosts) {
  $r = Run-SSH -Host $h -Cmd "echo OK; hostname"
  if ($r.Code -ne 0) {
    Fail "$h: ssh failed (code=$($r.Code))`n$($r.Err)"
    throw "SSH to $h failed"
  } else {
    $lines = $r.Out.Split("`n") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
    Ok "$h -> $($lines[-1])"
  }
}

# 2) Ensure services on bastion
Step "STEP 2: Ensure services on bastion (munge, slurmctld)"
$cmdBastion = @"
set -e
sudo systemctl enable --now munge
sudo systemctl enable --now slurmctld
echo '--- is-active ---'
sudo systemctl is-active munge slurmctld || true
echo '--- status slurmctld (head) ---'
sudo systemctl --no-pager --full status slurmctld | head -n 30
"@
$r = Run-SSH -Host $Bastion -Cmd "bash -lc `"$cmdBastion`""
if ($r.Code -ne 0) { Fail "bastion services failed`n$($r.Err)`n$($r.Out)"; throw "bastion services failed" }
Ok "bastion services OK"
Write-Host $r.Out

# 3) Ensure services on workers
Step "STEP 3: Ensure services on workers (munge, slurmd)"
foreach ($w in $Workers) {
  $cmdW = @"
set -e
sudo systemctl enable --now munge
sudo systemctl enable --now slurmd
echo '--- is-active ---'
sudo systemctl is-active munge slurmd || true
echo '--- listen 6818 ---'
sudo ss -lntp | egrep ':6818\b' || true
echo '--- last slurmd errors (if any) ---'
sudo journalctl -u slurmd -n 25 --no-pager | egrep -i 'fatal|error|NodeName' || true
"@
  $rr = Run-SSH -Host $w -Cmd "bash -lc `"$cmdW`""
  if ($rr.Code -ne 0) { Fail "$w: failed`n$($rr.Err)`n$($rr.Out)"; throw "$w slurmd failed" }
  Ok "$w services OK"
  Write-Host $rr.Out
}

# 4) Reconfigure + resume nodes
Step "STEP 4: Reconfigure + resume nodes on bastion"
$cmdReconf = @"
set -e
sudo scontrol reconfigure
sleep 1
sudo scontrol update nodename=worker1 state=resume reason="" || true
sudo scontrol update nodename=worker2 state=resume reason="" || true
sleep 1
echo '--- sinfo -N -l ---'
sinfo -N -l || true
echo '--- nodes (state/reason) ---'
scontrol show node worker1 | egrep 'NodeName=|NodeHostName=|NodeAddr=|State=|Reason=|SlurmdStartTime=' || true
echo '---'
scontrol show node worker2 | egrep 'NodeName=|NodeHostName=|NodeAddr=|State=|Reason=|SlurmdStartTime=' || true
"@
$r = Run-SSH -Host $Bastion -Cmd "bash -lc `"$cmdReconf`""
if ($r.Code -ne 0) { Fail "reconfigure failed`n$($r.Err)`n$($r.Out)"; throw "reconfigure failed" }
Ok "reconfigure/resume done"
Write-Host $r.Out

# 5) Smoke test srun
Step "STEP 5: Smoke test (srun hostname)"
$cmdTest = @"
set -e
echo '--- test worker1 ---'
srun -N1 -n1 -w worker1 hostname
echo '--- test worker2 ---'
srun -N1 -n1 -w worker2 hostname
echo '--- test both ---'
srun -N2 -n2 -w worker1,worker2 hostname
"@
$r = Run-SSH -Host $Bastion -Cmd "bash -lc `"$cmdTest`""
if ($r.Code -ne 0) { Fail "srun test failed`n$($r.Err)`n$($r.Out)"; throw "srun test failed" }
Ok "srun OK"
Write-Host $r.Out

Write-Host ""
Write-Host "DONE: Slurm looks operational." -ForegroundColor Green
