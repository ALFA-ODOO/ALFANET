# --- Config (igual que tenías) ---
$root    = "C:\NETSISTEMAS\ODOO_PROYECT\ALFANET"
$script  = Join-Path $root "wa_sesiones_a_timesheets.py"
$venvAct = Join-Path $root "venv\Scripts\Activate.ps1"
$logsDir = Join-Path $root "logs"
$prevDir = Join-Path $root "previews"
$proyectoNombre = "Soporte WhatsApp"

# Fecha a procesar = HOY
# $y =  (Get-Date).AddDays(-1).ToString('yyyy-MM-dd')
$y = (Get-Date).ToString('yyyy-MM-dd')
$log    = Join-Path $logsDir ("run_{0}.log" -f $y)
$preview = Join-Path $prevDir ("preview_wa_{0}.csv" -f $y)

$DryRun = $false   # ponelo $true si querés validar primero

# 🔧 Construí los argumentos YA con comillas donde corresponde
$py = "python"
$argList = @(
    "`"$script`"",
    "--proyecto", "`"$proyectoNombre`"",   # ← comillas
    "--fecha", $y,
    "--preview", "`"$preview`""            # ← comillas
)
if ($DryRun) { $argList += "--dry-run" }

# Loguear el comando exacto
"`n==== $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ====" | Out-File -FilePath $log -Append -Encoding utf8
("$py " + ($argList -join " ")) | Out-File -FilePath $log -Append -Encoding utf8

# Ejecutar capturando salida
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

$stdout | Out-File -FilePath $log -Append -Encoding utf8
if ($stderr) {
  "STDERR:" | Out-File -FilePath $log -Append -Encoding utf8
  $stderr   | Out-File -FilePath $log -Append -Encoding utf8
}
if ($proc.ExitCode -ne 0) { throw "Python salió con código $($proc.ExitCode)" }
