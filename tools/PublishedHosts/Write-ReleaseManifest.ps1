[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $PublishRoot,
    [Parameter(Mandatory)] [string] $RuntimeIdentifier,
    [Parameter(Mandatory)] [string] $OutputPath
)

$ErrorActionPreference = 'Stop'
$publishDirectory = (Resolve-Path -LiteralPath $PublishRoot).Path
$commit = (& git rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Cannot resolve the source commit.' }
$tree = (& git rev-parse 'HEAD^{tree}').Trim()
if ($LASTEXITCODE -ne 0) { throw 'Cannot resolve the source tree.' }
$sourceChanges = @(& git status --porcelain --untracked-files=normal)
if ($LASTEXITCODE -ne 0 -or $sourceChanges.Count -ne 0) {
    throw 'A release manifest requires a clean committed source worktree.'
}
$files = @(Get-ChildItem -LiteralPath $publishDirectory -Recurse -File | Sort-Object FullName | ForEach-Object {
    [ordered]@{
        path = [IO.Path]::GetRelativePath($publishDirectory, $_.FullName).Replace('\', '/')
        bytes = $_.Length
        sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    }
})
if ($files.Count -eq 0) { throw 'No published files were found.' }
foreach ($component in 'Application', 'Gateway') {
    if (-not ($files.path -contains "$component/Mk8.Sava.$component.dll")) {
        throw "Missing published $component assembly."
    }
}
$manifest = [ordered]@{
    schemaVersion = 1
    commit = $commit
    tree = $tree
    runtimeIdentifier = $RuntimeIdentifier
    selfContained = $false
    framework = 'net10.0'
    files = $files
}
$manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $OutputPath -Encoding utf8NoBOM
Write-Output "Frozen $($files.Count) files for $commit ($RuntimeIdentifier)."
