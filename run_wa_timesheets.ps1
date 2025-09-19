# --- Config (igual que tenías) ---
$root    = "C:\NETSISTEMAS\ODOO_PROYECT\ALFANET"
$script  = Join-Path $root "wa_sesiones_a_timesheets.py"
$exportScript = Join-Path $root "importar_diarios_helpdesk.py"
$logsDir = Join-Path $root "logs"
$prevDir = Join-Path $root "previews"
$proyectoNombre = "Soporte WhatsApp"

# Fecha a procesar = HOY
# $y =  (Get-Date).AddDays(-1).ToString('yyyy-MM-dd')
$y = (Get-Date).ToString('yyyy-MM-dd')
$log    = Join-Path $logsDir ("run_{0}.log" -f $y)
$preview = Join-Path $prevDir ("preview_wa_{0}.csv" -f $y)
$exportFile = Join-Path $logsDir ("whatsapp_{0}.xlsx" -f $y)

$DryRun = $false   # ponelo $true si querés validar primero

$py = "python"

function Invoke-PythonScript {
  param(
    [string]$ScriptPath,
    [string[]]$Arguments,
    [string]$LogPath,
    [switch]$SupportsDryRun
  )

  $argList = @("`"$ScriptPath`"") + $Arguments
  if ($SupportsDryRun -and $DryRun) { $argList += "--dry-run" }

  "`n==== $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ====" | Out-File -FilePath $LogPath -Append -Encoding utf8
  ("$py " + ($argList -join " ")) | Out-File -FilePath $LogPath -Append -Encoding utf8

  $pinfo = New-Object System.Diagnostics.ProcessStartInfo
  $pinfo.FileName = $py
  $pinfo.Arguments = ($argList -join " ")
  $pinfo.WorkingDirectory = $root
  $pinfo.RedirectStandardOutput = $true
  $pinfo.RedirectStandardError  = $true
  $pinfo.UseShellExecute = $false
  $pinfo.CreateNoWindow = $true

  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $pinfo
  $null = $proc.Start()
  $stdout = $proc.StandardOutput.ReadToEnd()
  $stderr = $proc.StandardError.ReadToEnd()
  $proc.WaitForExit()

  $stdout | Out-File -FilePath $LogPath -Append -Encoding utf8
  if ($stderr) {
    "STDERR:" | Out-File -FilePath $LogPath -Append -Encoding utf8
    $stderr   | Out-File -FilePath $LogPath -Append -Encoding utf8
  }
  if ($proc.ExitCode -ne 0) { throw "Python salió con código $($proc.ExitCode)" }
}

$waArgs = @(
  "--proyecto", "`"$proyectoNombre`"",
  "--fecha", $y,
  "--preview", "`"$preview`""
)

$exportArgs = @(
  "--desde", $y,
  "--hasta", $y,
  "--archivo", "`"$exportFile`""
)

Invoke-PythonScript -ScriptPath $script -Arguments $waArgs -LogPath $log -SupportsDryRun
Invoke-PythonScript -ScriptPath $exportScript -Arguments $exportArgs -LogPath $log
