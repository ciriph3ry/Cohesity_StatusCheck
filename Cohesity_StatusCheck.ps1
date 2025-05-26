[CmdletBinding()]
param (
    [Parameter()][string]$username = 'USERNAME_HERE', #Username
    [Parameter()][string]$domain = 'ADD_DOMAIN_HERE', #Domain
    [Parameter()][string]$tenant,
    [Parameter()][switch]$useApiKey,
    [Parameter()][SecureString]$password = $null,
    [Parameter()][switch]$noPrompt,
    [Parameter()][switch]$mcm,
    [Parameter()][string]$mfaCode = $null,
    [Parameter()][switch]$emailMfaCode,
    [Parameter()][string]$clusterName = $null,
    [Parameter()][string]$region = $null,
    [Parameter()][string]$alertType,
    [Parameter()][int]$daysBack = 2,
    [Parameter()][int]$maxLogBackupMinutes = 0,
    #Alert Severity to check, ie: kInfo, kWarning, kCritical
    [Parameter()][string]$severity = 'kWarning',
    [Parameter()][string]$matchString,
    [Parameter()][string]$startDate,
    [Parameter()][string]$endDate,
    #How many days back to check ALERTS?
    [Parameter()][int]$maxDays = 2
)

. $(Join-Path -Path $PSScriptRoot -ChildPath cohesity-api.ps1)

#get todays date...
$DATE = Get-Date
#where we are storing the log file...
$LOG = "C:\Users\USER\LOGS\$(get-date -Format yyyyddmm_hhmmtt)_CohesityCheck.txt"

Clear-Host
Write-Host "Domain: $domain - Username: $username`n" -ForegroundColor Yellow
Write-Host "1. NODE_1`n2. NODE_2`n3. NODE_3`n" -ForegroundColor Green
$yum = Read-Host "Please select a cluster: "

$vip = switch ($yum) {
    "1" { "NODE_1" }
    "2" { "NODE_2" }
    "3" { "NODE_3" }
    default {
        Write-Host "Invalid Choice. Exiting.`n"
        exit 5
    }
}

Write-Host "`nPlease Wait...`nAuthenticating...`n#############`n" -ForegroundColor Yellow
Start-Sleep 1

$usingHelios = $False
if (($mcm -or $vip -eq 'helios.cohesity.com') -and (!$clusterName)) {
    $usingHelios = $True
}

apiauth -vip $vip -username $username -domain $domain -apiKeyAuthentication $useApiKey -mfaCode $mfaCode -sendMfaCode $emailMfaCode -heliosAuthentication $mcm -regionid $region -tenant $tenant -noPromptForPassword $noPrompt

if (!$cohesity_api.authorized) {
    Write-Host "Not authenticated" -ForegroundColor Yellow
    exit
} 

#Stats/storage space etc

$cluster = api get cluster?fetchStats=true
$GiB = 1024 * 1024 * 1024

$version = ($cluster.clusterSoftwareVersion -split '_')
$status = $status.clusterConfig.proto
$nodeStatus = $status.nodeStatus

if($config){
    $chassisList = $config.chassisVec
    $hostName = $status.clusterConfig.proto.clusterPartitionVec[0].hostName
}else{
    $chassisList = (api get -v2 chassis).chassis
    $hostName = (api get clusterPartitions)[0].hostName
}

$nodes = api get nodes
$physicalCapacity = [math]::round($cluster.stats.usagePerfStats.physicalCapacityBytes / $GiB, 1)
$usedCapacity = [math]::round($cluster.stats.usagePerfStats.totalPhysicalUsageBytes / $GiB, 1)
$usedPct = [int][math]::round(100 * $usedCapacity / $physicalCapacity, 0)

# cluster info
Write-Host "`n-------------------------------------------------------"
Write-Output "-------------------------------------------------------" >> $LOG
Write-Host ("     Cluster Name: {0}" -f $hostName) | Tee-Object -FilePath $LOG -Append
Write-Output ("     Cluster Name: {0}" -f $hostName) >> $LOG
Write-Host ("  Product Version: {0}" -f $cluster.clusterSoftwareVersion)
Write-Output ("  Product Version: {0}" -f $cluster.clusterSoftwareVersion) >> $LOG
Write-Host ("       Cluster ID: {0}" -f $cluster.id)
Write-Output ("       Cluster ID: {0}" -f $cluster.id) >> $LOG
Write-Host ("Physical Capacity: {0} GiB" -f $physicalCapacity)
Write-Output ("Physical Capacity: {0} GiB" -f $physicalCapacity) >> $LOG
Write-Host ("    Used Capacity: {0} GiB" -f $usedCapacity)
Write-Output ("    Used Capacity: {0} GiB" -f $usedCapacity) >> $LOG
Write-Host ("     Used Percent: {0}%" -f $usedPct)
Write-Output ("     Used Percent: {0}%" -f $usedPct) >> $LOG
Write-Host ("  Number of nodes: {0}" -f @($nodes).Length)
Write-Output ("  Number of nodes: {0}" -f @($nodes).Length) >> $LOG
Write-Host ("-------------------------------------------------------")
Write-Output ("-------------------------------------------------------") >> $LOG

##ALERT CODE

Write-Host "`nChecking for critical alerts..." -ForegroundColor Green
Write-Output "`nCritical & Warning Alerts:`n" >> $LOG
Write-Host ("-------------------------------------------------------")

$alertQuery = "alerts?maxAlerts=1000&alertStateList=kOpen"
if($alertType){
    $alertQuery = $alertQuery + "&alertTypeList=$alertType"
}
if($startDate){
    $startDateUsecs = dateToUsecs $startDate
    $alertQuery = $alertQuery + "&startDateUsecs=$startDateUsecs"
}
if($endDate){
    $endDateUsecs = dateToUsecs $endDate
    $alertQuery = $alertQuery + "&endDateUsecs=$endDateUsecs"
}
if($maxDays -gt 0){
    $startDateUsecs = timeAgo $maxDays days
    $alertQuery = $alertQuery + "&startDateUsecs=$startDateUsecs"
}

if($usingHelios){
    if($region){
        $cohesity_api.header['regionid'] = $region
    }
    $alerts = api get -mcm $alertQuery | Where-Object alertState -ne 'kResolved'
    foreach($alert in $alerts){
        if(! $alert.PSObject.Properties['clusterName']){
            setApiProperty -object $alert -name clusterName -value $alert.regionId
        }
    }
}else{
    $alerts = api get $alertQuery | Where-Object alertState -ne 'kResolved'
}

# filter alerts
function filterAlerts($alerts, $filterOnCluster=$False){
    if($severity){
        $alerts = $alerts | Where-Object severity -eq $severity
    }
    if($usingHelios -and $clusterName -and $filterOnCluster){
        $alerts = $alerts | Where-Object clusterName -eq $clusterName
    }
    if($matchString){
        $alerts = $alerts | Where-Object {$_.alertDocument.alertDescription -match $matchString}
    }
	#filter alerts to exlude some things... kHardware, kMaintenance etc
    #SPECIFIC ALERTS USE : ($_.alertType -ne 10002) 
    $alerts = $alerts | Where-Object {($_.alertTypeBucket -eq 'kHardware')}
    return $alerts
}

$alerts = filterAlerts $alerts $True

if($alerts.Count -eq 0){
    Write-Host "`nNo critical alerts found!`n" -ForegroundColor Green | Write-Output "No critical alerts found!`n" >> $LOG
}

$alertsList = @()

$alerts | Sort-Object -Property {$_.latestTimestampUsecs} | Format-Table -Property @{l='Latest Occurrence'; e={usecsToDate ($_.latestTimestampUsecs)}}, alertType, alertTypeBucket, severity, @{l='Description'; e={$_.alertDocument.alertDescription}}
$alerts | Sort-Object -Property latestTimestampUsecs -Descending | ForEach-Object {
    $alertsList += [ordered] @{
        'Bucket' = $_.alertTypeBucket;
        'Latest Occurrence' = usecsToDate ($_.latestTimestampUsecs);
        'Alert Type' = $_.alertType;
        'Severity' = $_.severity;
        'Description' = $_.alertDocument.alertDescription;
    } | Tee-Object -FilePath $LOG -Append 
}

##SLA code

$nowUsecs = dateToUsecs (Get-Date)
$daysBackUsecs = timeAgo $daysBack days
$maxLogBackupUsecs = $maxLogBackupMinutes * 60000000

Write-Host ("-------------------------------------------------------")
Write-Output ("-------------------------------------------------------") >> $LOG
Write-Host "`nChecking for long running jobs...***1440 minutes = 1 day***`n" -ForegroundColor Blue
Start-Sleep 1

$missesRecorded = $false
$message = ""

$finishedStates = @('Succeeded', 'Canceled', 'Failed', 'Warning', 'SucceededWithWarning')

Write-Output "`nLong Running Jobs:`n" >> $LOG

$jobs = api get -v2 "data-protect/protection-groups?isDeleted=false&isActive=true&includeTenants=true"

if ($jobs -eq $null) {
    Write-Host "Failed to retrieve protection groups data. Exiting script." -ForegroundColor Red
    exit 1
	}

	foreach ($job in $jobs.protectionGroups | Sort-Object -Property name) {
    $jobId = $job.id
    $jobName = $job.name
    $slaPass = "Pass"
	# Check if the protection group has an SLA defined
    if ($job.sla -ne $null -and $job.sla.Count -gt 0) {
        $sla = $job.sla[0].slaMinutes
        $slaUsecs = $sla * 1400000000
    } else {
        # Handle the case where there is no SLA defined
        Write-Host "No SLA defined for protection group '$jobName'" -ForegroundColor Yellow
        continue  # Skip this protection group and move to the next one
    }
    $runs = api get -v2 "data-protect/protection-groups/$($job.id)/runs?numRuns=2&endTimeUsecs=$endUsecs&includeTenants=true"
    foreach($run in $runs.runs){
        if($run.PSObject.Properties['localBackupInfo']){
            $startTimeUsecs = $run.localBackupInfo.startTimeUsecs
            $status = $run.localBackupInfo.status
            if($run.localBackupInfo.PSObject.Properties['endTimeUsecs']){
                $endTimeUsecs = $run.localBackupInfo.endTimeUsecs
            }
        }else{
            $startTimeUsecs = $run.archivalInfo.archivalTargetResults[0].startTimeUsecs
            $status = $run.archivalInfo.archivalTargetResults[0].status
            if($run.archivalInfo.archivalTargetResults[0].PSObject.Properties['endTimeUsecs']){
                $endTimeUsecs = $run.archivalInfo.archivalTargetResults[0].endTimeUsecs
            } 
        }
        
        if($status -in $finishedStates){
            $runTimeUsecs = $endTimeUsecs - $startTimeUsecs
        }else{
            $runTimeUsecs = $nowUsecs - $startTimeUsecs
        }
        if(!($startTimeUsecs -le $daysBackUsecs -and $status -in $finishedStates)){
            if($status -ne 'Canceled'){
                if($runTimeUsecs -gt $slaUsecs){
                    $slaPass = "Miss"
                    $reason = ""
                }
                if($maxLogBackupMinutes -gt 0 -and $run.localBackupInfo.runType -eq 'kLog' -and $runTimeUsecs -ge $maxLogBackupUsecs){
                    $slaPass = "Miss"
                    $reason = "Log SLA: $maxLogBackupMinutes minutes"
                }
            }
        }

        $runTimeMinutes = [math]::Round(($runTimeUsecs / 60000000),0)
        if($slaPass -eq "Miss"){
            $missesRecorded = $True
            if($status -in $finishedStates){
                $verb = "ran"
            }else{
                $verb = "has been running"
            }
            $startTime = usecsToDate $startTimeUsecs
            $messageLine = "* {0} ({1}) {2} for {3} minutes" -f $jobName, $startTime, $verb, $runTimeMinutes, $reason | Tee-Object -FilePath $LOG -Append
            $messageLine
            $message += "$messageLine`n"
            break
        } 
    }
}

if($missesRecorded -eq $false){
    "`nNo SLA misses recorded!"
}

#Job Failure Code

Write-Host ("-------------------------------------------------------")
Write-Output ("-------------------------------------------------------") >> $LOG
Write-Host "`n`nJob Failure Report...If no failures are found the entry will be blank." -ForegroundColor Yellow
Write-Output "`nFailed Objects:`n" >> $LOG

$failureCount = 0
$consoleWidth = $Host.UI.RawUI.WindowSize.Width
$jobs = api get protectionJobs?environments=kSQL | Sort-Object -Property name

# Initialize a list to store processed errors
$processedErrors = @()

function Get-EnvironmentFailures($environment, $environmentDisplayName) {
    Write-Host "`n`n$environmentDisplayName Failures:`n" -ForegroundColor Green
    $jobs = api get protectionJobs?environments=$environment | Sort-Object -Property name

    foreach ($job in $jobs) {
        if ($job.isDeleted -ne $true -and $job.isPaused -ne $true -and $job.isActive -ne $false) {
            $runs = api get "protectionRuns?jobId=$($job.id)" | where-object {$_.backupRun.status -eq 'kFailure'}
            if ($runs.count -gt 0) {
                foreach ($run in $runs) {
                    $startTime = (usecsToDate $run.backupRun.stats.startTimeUsecs)
                    $dateKey = $startTime.Date.ToShortDateString()
                    $today = Get-Date -Format "yyyy-MM-dd"
                    $yesterday = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd")
                    if ($startTime -ge $yesterday -and $startTime -lt $today) {
                        $failureCount += 1
                        $msgType = if ($run.backupRun.status -eq 'kFailure') {'Failure'} else {'Warning'}
                        $link = "https://{0}/protection/job/{1}/run/{2}/{3}/protection" -f $vip, $job.id, $run.backupRun.jobRunId, $run.backupRun.stats.startTimeUsecs
                        "{0} ({1}) {2}" -f $job.name.ToUpper(), $job.environment.substring(1), $startTime | Tee-Object -FilePath $LOG -Append

                        foreach ($source in $run.backupRun.sourceBackupStatus) {
                            if ($source.status -eq 'kFailure' -or $source.PSObject.Properties['warnings']) {
                                if ($source.status -eq 'kFailure') {
                                    $msg = $source.error
                                    $objectReport = "* {0} ({1}): {2}" -f $source.source.name.ToUpper(), $msgType, $msg | Tee-Object -FilePath $LOG -Append
                                    if ($objectReport.ToString().length -gt ($consoleWidth - 5)) {
                                        $objectReport = "$($objectReport.substring(0,($consoleWidth-6)))..."
                                    }
                                    $objectReport
                                    $message += '<span>{0}</span><span class="info"> (<span class="{1}">{2}</span>)</span><div class="message">{3}</div>' -f $source.source.name.ToUpper(), $msgType, $msgType, $msgHTML
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

Get-EnvironmentFailures 'kSQL' 'kSQL'
Get-EnvironmentFailures 'kVMWare' 'kVMWare'
Get-EnvironmentFailures 'kPhysicalFiles' 'kPhysicalFiles'
Get-EnvironmentFailures 'kOracle' 'kOracle'
Get-EnvironmentFailures 'kHyperV' 'kHyperV'
Get-EnvironmentFailures 'kPhysical' 'kPhysical'
Write-Host "`nAnalysis Completed!`n" -ForegroundColor Yellow
Write-Host ("-------------------------------------------------------")
Write-Output ("-------------------------------------------------------") >> $LOG
