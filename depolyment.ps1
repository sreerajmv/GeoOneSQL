$source = ".\routes"
$destinationServer = "gipl@192.168.168.222"
$destinationPath = "/home/gipl/geooneMS"
$destination = "${destinationServer}:${destinationPath}"
$serviceName = "geooneMS"



# Verify source exists
if (-not (Test-Path $source)) {
    Write-Error "Source directory '$source' not found!"
    exit 1
}

# Execute SCP command
Write-Host "Starting file transfer..." -ForegroundColor Yellow
scp -r $source $destination
$scpExit = $LASTEXITCODE

if ($scpExit -ne 0) {
    Write-Error "Transfer failed with exit code: $scpExit"
    exit $scpExit
}
Write-Host "Transfer completed successfully!" -ForegroundColor Green

# SSH commands to restart services
Write-Host "Restarting services on remote server..." -ForegroundColor Yellow

ssh -t $destinationServer "echo 'Restarting ${serviceName}...'; sudo systemctl restart ${serviceName}.service; echo 'Restarting nginx...'; sudo systemctl restart nginx.service; echo 'Services restarted successfully!'"
$sshExit = $LASTEXITCODE

# Final check
if ($sshExit -eq 0) {
    Write-Host "All services restarted successfully!" -ForegroundColor Green
} else {
    Write-Error "Service restart failed with exit code: $sshExit"
    exit $sshExit
}


Write-Host "Deployment completed successfully!"