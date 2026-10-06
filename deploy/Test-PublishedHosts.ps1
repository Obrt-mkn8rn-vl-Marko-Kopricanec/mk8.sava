[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $PublishRoot,
    [Parameter(Mandatory)] [string] $EvidencePath
)

$ErrorActionPreference = 'Stop'
$publishDirectory = (Resolve-Path -LiteralPath $PublishRoot).Path
$evidenceDirectory = [IO.Path]::GetFullPath($EvidencePath)
[IO.Directory]::CreateDirectory($evidenceDirectory) | Out-Null
$runtimeDirectory = Join-Path ([IO.Path]::GetTempPath()) ('mk8-sava-published-' + [Guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($runtimeDirectory) | Out-Null
$accessKeyFile = Join-Path $runtimeDirectory 'application-access.key'
$accessKeyBytes = [Security.Cryptography.RandomNumberGenerator]::GetBytes(32)
[IO.File]::WriteAllText($accessKeyFile, [Convert]::ToBase64String($accessKeyBytes))
if (-not $IsWindows) {
    & chmod 700 $runtimeDirectory
    if ($LASTEXITCODE -ne 0) { throw 'Cannot protect the disposable runtime directory.' }
    & chmod 600 $accessKeyFile
    if ($LASTEXITCODE -ne 0) { throw 'Cannot protect the disposable transport key.' }
}
$account = 'devstoreaccount1'
# Public emulator credential, never a deployment credential.
$accountKey = 'Eby8vdM02xNOcqFeqCnf2WmjO1GwSW3eF4J6tq/K1SZFPTOtr/KBHBeksoGMGwBNPajQKDaZhQ=='
$application = $null
$gateway = $null
$storageDirectory = Join-Path $runtimeDirectory 'storage'
$handler = [Net.Http.SocketsHttpHandler]::new()
$handler.UseProxy = $false
$handler.AllowAutoRedirect = $false
$client = [Net.Http.HttpClient]::new($handler)
$client.Timeout = [TimeSpan]::FromSeconds(30)

function Get-TestPorts {
    $first = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $second = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    try {
        $first.Start()
        $second.Start()
        return @($first.LocalEndpoint.Port, $second.LocalEndpoint.Port)
    }
    finally {
        $first.Stop()
        $second.Stop()
    }
}

function Start-TestHost([string] $Component, [string] $Address, [string] $ApplicationEndpoint, [string] $LogName = '') {
    if (-not $LogName) { $LogName = $Component }
    $executable = Join-Path $publishDirectory "$Component/Mk8.Sava.$Component"
    if ($IsWindows) { $executable += '.exe' }
    if (-not (Test-Path -LiteralPath $executable -PathType Leaf)) { throw "Missing published $Component apphost." }
    $environment = @{
        ApplicationTransport__Endpoint = $ApplicationEndpoint
        ApplicationTransport__AccessKeyFile = $accessKeyFile
        Sava__DefaultAccount = $account
        Sava__Accounts__devstoreaccount1 = $accountKey
        Sava__DataPath = $(if ($Component -eq 'Application') { $storageDirectory } else { Join-Path $runtimeDirectory 'gateway-unused' })
        Gateway__StagingPath = (Join-Path $runtimeDirectory 'gateway-staging')
        ASPNETCORE_URLS = $Address
    }
    $start = New-TestStartInfo $executable $environment
    return Start-TestProcess $start $LogName
}

function New-TestStartInfo([string] $Executable, [hashtable] $Environment) {
    $start = [Diagnostics.ProcessStartInfo]::new($Executable)
    $start.WorkingDirectory = $runtimeDirectory
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($name in @($start.Environment.Keys)) {
        $normalized = $name.ToLowerInvariant()
        if ($normalized -match '^(sava__|gateway__|applicationtransport__|applicationhosting__|kestrel__|aspnetcore_)' -or
            $normalized -eq 'dotnet_environment') {
            [void] $start.Environment.Remove($name)
        }
    }
    foreach ($name in $Environment.Keys) { $start.Environment[$name] = $Environment[$name] }
    $start.Environment['ASPNETCORE_ENVIRONMENT'] = 'Production'
    $start.Environment['DOTNET_ENVIRONMENT'] = 'Production'
    return $start
}

function Start-TestProcess([Diagnostics.ProcessStartInfo] $Start, [string] $LogName) {
    $process = [Diagnostics.Process]::Start($Start)
    $process | Add-Member -NotePropertyName SavaOutputTask -NotePropertyValue $process.StandardOutput.ReadToEndAsync()
    $process | Add-Member -NotePropertyName SavaErrorTask -NotePropertyValue $process.StandardError.ReadToEndAsync()
    $process | Add-Member -NotePropertyName SavaLogName -NotePropertyValue $LogName
    return $process
}

function Stop-TestHost($Process) {
    if ($null -eq $Process) { return }
    $Process.Refresh()
    if (-not $Process.HasExited) {
        $Process.Kill($true)
        if (-not $Process.WaitForExit(10000)) { throw "Published host $($Process.Id) did not stop." }
    }
    try {
        [IO.File]::WriteAllText((Join-Path $evidenceDirectory "$($Process.SavaLogName).stdout.log"),
            $Process.SavaOutputTask.GetAwaiter().GetResult())
        [IO.File]::WriteAllText((Join-Path $evidenceDirectory "$($Process.SavaLogName).stderr.log"),
            $Process.SavaErrorTask.GetAwaiter().GetResult())
    }
    finally { $Process.Dispose() }
}

function Invoke-TestOperator([string] $Name, [string] $Argument, [string] $DataRoot) {
    $executable = Join-Path $publishDirectory 'Application/Mk8.Sava.Application'
    if ($IsWindows) { $executable += '.exe' }
    $environment = @{
        Sava__DefaultAccount = $account
        Sava__Accounts__devstoreaccount1 = $accountKey
        Sava__DataPath = $DataRoot
    }
    $start = New-TestStartInfo $executable $environment
    $start.ArgumentList.Add("--$Name")
    $start.ArgumentList.Add($Argument)
    $process = Start-TestProcess $start $Name
    try {
        if (-not $process.WaitForExit(60000)) { throw "Application operator $Name exceeded its disposable smoke deadline." }
        if ($process.ExitCode -ne 0) { throw "Application operator $Name exited with $($process.ExitCode)." }
    }
    finally { Stop-TestHost $process }
}

function Get-HttpStatus([string] $Address, [string] $Method = 'GET') {
    $request = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::new($Method), $Address)
    $deadline = [Threading.CancellationTokenSource]::new([TimeSpan]::FromSeconds(1))
    try {
        $response = $client.SendAsync($request, $deadline.Token).GetAwaiter().GetResult()
        try { return [int] $response.StatusCode }
        finally { $response.Dispose() }
    }
    finally {
        $request.Dispose()
        $deadline.Dispose()
    }
}

function Wait-TestStatus([string] $Address, [int] $Expected, $Process) {
    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    do {
        $Process.Refresh()
        if ($Process.HasExited) { throw "Published host exited before $Address was available." }
        try {
            if ((Get-HttpStatus $Address) -eq $Expected) { return }
        }
        catch [Net.Http.HttpRequestException] { }
        catch [OperationCanceledException] { }
        Start-Sleep -Milliseconds 100
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "$Address did not return $Expected within the existing 10-second startup budget."
}

function Invoke-StorageRequest([string] $Method, [string] $Address, [byte[]] $Body, [string] $Range = '') {
    $uri = [Uri] $Address
    $headers = @{
        'x-ms-date' = [DateTimeOffset]::UtcNow.ToString('R', [Globalization.CultureInfo]::InvariantCulture)
        'x-ms-version' = '2026-04-06'
    }
    $contentType = ''
    $contentLength = ''
    if ($null -ne $Body -and $Body.Length -gt 0) {
        $contentType = 'application/octet-stream'
        $contentLength = $Body.Length.ToString([Globalization.CultureInfo]::InvariantCulture)
        if ($Method -eq 'PUT') { $headers['x-ms-blob-type'] = 'BlockBlob' }
    }
    $canonicalHeaders = ($headers.Keys | Sort-Object | ForEach-Object { "${_}:$($headers[$_])`n" }) -join ''
    $resource = "/$account$($uri.AbsolutePath)"
    if ($uri.Query) {
        foreach ($parameter in ($uri.Query.TrimStart('?').Split('&') | Sort-Object)) {
            $parts = $parameter.Split('=', 2)
            $resource += "`n$($parts[0].ToLowerInvariant()):$([Uri]::UnescapeDataString($parts[1]))"
        }
    }
    $stringToSign = @($Method, '', '', $contentLength, '', $contentType, '', '', '', '', '', $Range,
        ($canonicalHeaders + $resource)) -join "`n"
    $hmac = [Security.Cryptography.HMACSHA256]::new([Convert]::FromBase64String($accountKey))
    try { $signature = [Convert]::ToBase64String($hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($stringToSign))) }
    finally { $hmac.Dispose() }
    $request = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::new($Method), $uri)
    try {
        foreach ($name in $headers.Keys) { [void] $request.Headers.TryAddWithoutValidation($name, $headers[$name]) }
        [void] $request.Headers.TryAddWithoutValidation('Authorization', "SharedKey ${account}:$signature")
        if ($Range) { [void] $request.Headers.TryAddWithoutValidation('Range', $Range) }
        if ($null -ne $Body) {
            $request.Content = [Net.Http.ByteArrayContent]::new($Body)
            if ($contentType) { $request.Content.Headers.ContentType = [Net.Http.Headers.MediaTypeHeaderValue]::new($contentType) }
        }
        $response = $client.SendAsync($request).GetAwaiter().GetResult()
        try {
            return @{
                Status = [int] $response.StatusCode
                Body = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
            }
        }
        finally { $response.Dispose() }
    }
    finally { $request.Dispose() }
}

function Assert-Status($Response, [int] $Expected) {
    if ($Response.Status -ne $Expected) {
        throw "Storage request returned $($Response.Status), expected $Expected."
    }
}

function Get-ParquetFixture {
    # Independent three-row INT32 fixture also used by the official SDK smoke.
    $page = [Convert]::FromHexString('1500151815182c15061500150615060000')
    $values = [byte[]] (1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0)
    $footer = [byte[]] (
        [Convert]::FromHexString('1502192c4806') + [Text.Encoding]::UTF8.GetBytes('schema') +
        [Convert]::FromHexString('150200150225001802') + [Text.Encoding]::UTF8.GetBytes('id') +
        [Convert]::FromHexString('001606191c191c26081c150219250006191802') + [Text.Encoding]::UTF8.GetBytes('id') +
        [Convert]::FromHexString('15001606163a163a26080000163a16060000'))
    $length = [BitConverter]::GetBytes([int] $footer.Length)
    if (-not [BitConverter]::IsLittleEndian) { [Array]::Reverse($length) }
    return [byte[]] ([Text.Encoding]::ASCII.GetBytes('PAR1') + $page + $values + $footer + $length + [Text.Encoding]::ASCII.GetBytes('PAR1'))
}

function Assert-ParquetQuery([string] $Address) {
    $xml = '<QueryRequest><QueryType>SQL</QueryType><Expression>SELECT id AS id FROM BlobStorage;</Expression>' +
        '<InputSerialization><Format><Type>parquet</Type></Format></InputSerialization>' +
        '<OutputSerialization><Format><Type>json</Type><JsonTextConfiguration><RecordSeparator>&#10;</RecordSeparator>' +
        '</JsonTextConfiguration></Format></OutputSerialization></QueryRequest>'
    $response = Invoke-StorageRequest POST "$Address`?comp=query" ([Text.Encoding]::UTF8.GetBytes($xml))
    Assert-Status $response 200
    # The wire is an Avro container; uncompressed resultData must include the
    # exact JSON rows. Success alone could conceal a native-reader error frame.
    $text = [Text.Encoding]::UTF8.GetString($response.Body)
    if (-not $text.Contains("{`"id`":1}`n{`"id`":2}`n{`"id`":3}`n") -or $text.Contains('InvalidInput')) {
        throw 'Published native Parquet query did not produce the exact expected rows.'
    }
}

try {
    $ports = Get-TestPorts
    $applicationAddress = "http://127.0.0.1:$($ports[0])"
    $gatewayAddress = "http://127.0.0.1:$($ports[1])"
    $applicationEndpoint = "$applicationAddress/internal/application"
    $gateway = Start-TestHost Gateway $gatewayAddress $applicationEndpoint 'Gateway-alone'
    Wait-TestStatus "$gatewayAddress/health/live" 200 $gateway
    Wait-TestStatus "$gatewayAddress/health/ready" 503 $gateway
    if (Test-Path -LiteralPath $storageDirectory) { throw 'Gateway opened the durable root while Application was absent.' }
    Stop-TestHost $gateway
    $gateway = $null

    $application = Start-TestHost Application $applicationAddress $applicationEndpoint
    Wait-TestStatus "$applicationAddress/health/ready" 200 $application
    if ((Get-HttpStatus "$applicationAddress/$account") -ne 404) { throw 'Application exposes an Azure route.' }
    if ((Get-HttpStatus $applicationEndpoint POST) -notin 401, 403) { throw 'RPC is not protected before parsing.' }
    if ((Get-HttpStatus "$applicationEndpoint/control" POST) -notin 401, 403) { throw 'Control RPC is not protected before parsing.' }
    $gateway = Start-TestHost Gateway $gatewayAddress $applicationEndpoint
    Wait-TestStatus "$gatewayAddress/health/ready" 200 $gateway

    $containerAddress = "$gatewayAddress/$account/published-$([Guid]::NewGuid().ToString('N'))"
    $blobAddress = "$containerAddress/bytes.bin"
    Assert-Status (Invoke-StorageRequest PUT "$containerAddress`?restype=container" $null) 201
    $bytes = [Security.Cryptography.RandomNumberGenerator]::GetBytes(1048897)
    Assert-Status (Invoke-StorageRequest PUT $blobAddress $bytes) 201
    $full = Invoke-StorageRequest GET $blobAddress $null
    Assert-Status $full 200
    $expectedHash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes))
    if ([Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($full.Body)) -ne $expectedHash) {
        throw 'Published-host full read differs from the uploaded bytes.'
    }
    $range = Invoke-StorageRequest GET $blobAddress $null 'bytes=63-8257'
    Assert-Status $range 206
    if ($range.Body.Length -ne 8195 -or
        [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($range.Body)) -ne
        [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([byte[]] $bytes[63..8257]))) {
        throw 'Published-host range read differs from the uploaded bytes.'
    }
    $parquetAddress = "$containerAddress/rows.parquet"
    $parquet = Get-ParquetFixture
    Assert-Status (Invoke-StorageRequest PUT $parquetAddress $parquet) 201
    Assert-ParquetQuery $parquetAddress
    Stop-TestHost $application
    $application = $null
    Wait-TestStatus "$gatewayAddress/health/ready" 503 $gateway
    Wait-TestStatus "$gatewayAddress/health/live" 200 $gateway

    $backupDirectory = Join-Path $runtimeDirectory 'backup'
    $restoredDirectory = Join-Path $runtimeDirectory 'restored'
    Invoke-TestOperator 'backup-create' $backupDirectory $storageDirectory
    Invoke-TestOperator 'backup-validate' $backupDirectory $storageDirectory
    Invoke-TestOperator 'restore-from' $backupDirectory $restoredDirectory
    $storageDirectory = $restoredDirectory
    $application = Start-TestHost Application $applicationAddress $applicationEndpoint 'Application-restored'
    Wait-TestStatus "$applicationAddress/health/ready" 200 $application
    Wait-TestStatus "$gatewayAddress/health/ready" 200 $gateway
    $restored = Invoke-StorageRequest GET $blobAddress $null
    Assert-Status $restored 200
    if ([Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($restored.Body)) -ne $expectedHash) {
        throw 'Published Application restore changed acknowledged blob bytes.'
    }
    $restoredRange = Invoke-StorageRequest GET $blobAddress $null 'bytes=63-8257'
    Assert-Status $restoredRange 206
    if ($restoredRange.Body.Length -ne 8195 -or
        [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($restoredRange.Body)) -ne
        [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([byte[]] $bytes[63..8257]))) {
        throw 'Published Application restore changed acknowledged range bytes.'
    }
    Assert-ParquetQuery $parquetAddress
    Assert-Status (Invoke-StorageRequest DELETE $blobAddress $null) 202
    Assert-Status (Invoke-StorageRequest GET $blobAddress $null) 404
    Assert-Status (Invoke-StorageRequest DELETE "$containerAddress`?restype=container" $null) 202
    if (Test-Path -LiteralPath (Join-Path $runtimeDirectory 'gateway-unused')) { throw 'Gateway created a durable data directory.' }

    [ordered]@{
        schemaVersion = 1
        gatewayAloneLive = 200
        gatewayAloneReady = 503
        applicationAloneReady = 200
        applicationAzureRoute = 404
        protectedRpc = $true
        uploadBytes = $bytes.Length
        uploadSha256 = $expectedHash.ToLowerInvariant()
        fullReadExact = $true
        rangeBytes = 8195
        rangeReadExact = $true
        deletedObjectAbsent = $true
        gatewaySurvivedApplicationLoss = $true
        nativeParquetQueryExact = $true
        applicationOfflineBackupValidated = $true
        applicationOfflineRestoreExact = $true
        gatewayReconnectedWithoutRestart = $true
    } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $evidenceDirectory 'result.json') -Encoding utf8NoBOM
    Write-Output 'Published Gateway/Application independence, protected RPC, native query and exact backup/restore smoke passed.'
}
finally {
    try { Stop-TestHost $gateway }
    finally {
        try { Stop-TestHost $application }
        finally {
            $client.Dispose()
            [Array]::Clear($accessKeyBytes, 0, $accessKeyBytes.Length)
            if ([IO.Path]::GetFileName($runtimeDirectory).StartsWith('mk8-sava-published-')) {
                Remove-Item -LiteralPath $runtimeDirectory -Recurse -Force
            }
        }
    }
}
