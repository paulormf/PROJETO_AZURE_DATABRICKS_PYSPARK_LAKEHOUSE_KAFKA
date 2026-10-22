param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$DbtArguments
)

$projectRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $projectRoot ".env"

if (-not (Test-Path -LiteralPath $envFile)) {
    throw "Arquivo .env não encontrado em $projectRoot. Copie .env.example para .env e preencha os valores."
}

Get-Content -LiteralPath $envFile | ForEach-Object {
    $line = $_.Trim()

    if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#")) {
        return
    }

    $separator = $line.IndexOf("=")
    if ($separator -lt 1) {
        return
    }

    $name = $line.Substring(0, $separator).Trim()
    $value = $line.Substring($separator + 1).Trim()

    if ($value.Length -ge 2 -and
        (($value.StartsWith('"') -and $value.EndsWith('"')) -or
         ($value.StartsWith("'") -and $value.EndsWith("'")))) {
        $value = $value.Substring(1, $value.Length - 2)
    }

    [Environment]::SetEnvironmentVariable($name, $value, "Process")
}

$dbtProject = Join-Path $projectRoot "dbt"
& dbt @DbtArguments --project-dir $dbtProject --profiles-dir $dbtProject
exit $LASTEXITCODE
