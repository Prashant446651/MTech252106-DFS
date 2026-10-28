# Four-Physical-Node DFS Deployment

Project: An Intelligent Distributed File System with Adaptive Replication and Fault Tolerance for Linux-based Clusters

## Target topology

- Metadata server: existing Ubuntu VirtualBox VM, TCP port 9000.
- Windows host address used for VirtualBox NAT port forwarding: `10.0.11.149:9000`.
- Node 1: `10.0.10.199:8001`.
- Node 2: `10.0.9.188:8002`.
- Node 3: `10.0.10.198:8003`.
- Node 4: `10.0.13.253:8004`.
- Desired replication factor: 3 total copies on distinct storage nodes. Four storage nodes are available; a file should not be intentionally replicated to all four when RF=3.

These IPs are the addresses supplied for this deployment. Confirm each address with `ip -br -4 addr` on its machine before starting. IPs may be DHCP-assigned and can change.

## Contents

- `metadata/main.go`: full metadata server, including the existing reconciliation and whole-file deletion paths, persistent replication queue, selected-replica deletion and exclusions. Adds `METADATA_DB` support so the new cluster can use a clean database while preserving the existing local-test database.
- `storage/main.go`: full storage server with upload/download/delete/health endpoints, metadata registration, SHA-256 calculation, persistent replication submission, and new `--advertise` support for real LAN IP addresses.
- `scripts/install-on-vm.sh`: timestamped backups and install/compile test on the Ubuntu VM.
- `scripts/start-metadata-vm.sh`: starts metadata on port 9000 and defaults to a new `metadata-cluster.db`.
- `scripts/start-node1.sh` through `start-node4.sh`: start each storage node at the supplied IP/port and configure the other three as peers.

## Important networking note

The physical nodes must be able to reach the Windows host at `10.0.11.149:9000` (the VirtualBox NAT port-forwarded metadata endpoint). The metadata server must also be able to reach all storage-node IPs on ports 8001-8004, and nodes should be able to reach their peer addresses for health checks, downloads and uploads. A successful `curl localhost:9000/health` inside the VM alone does not prove external reachability.

Use VirtualBox NAT port forwarding:

- Host IP: blank (listen on host interfaces; restrict with Windows Firewall)
- Host Port: 9000/TCP
- Guest IP: blank or current NAT guest address (current guest address has been `10.0.2.15`)
- Guest Port: 9000/TCP

Allow inbound TCP/9000 on Windows Firewall only from the four trusted node IPs. Keep this service on a trusted network; HTTP is unencrypted and this prototype does not include authentication.

## Step A — Preserve your existing project and install updated code

On the Ubuntu VM, extract the ZIP to a directory, such as `~/dfs_four_machine_deployment`. Then run:

```bash
cd ~/dfs_four_machine_deployment
bash scripts/install-on-vm.sh
```

The script backs up the current Go source, copies the updated source into `~/mtechproject`, formats it, runs `go test ./...`, and runs `go build ./...`. It does not replace `go.mod` or `go.sum` and does not delete `metadata.db`.

Because the old `metadata.db` may contain localhost-only node/file records, use the new `metadata-cluster.db` for this physical cluster. The previous database remains intact as a backup/reference.

## Step B — Check address and routing on each physical machine

Run on each physical node:

```bash
hostname -I
ip -br -4 addr
ip route
```

Make sure the reported IP matches the configured address above. Test the metadata endpoint FROM each physical node (not from the VM):

```bash
curl -i --connect-timeout 5 http://10.0.11.149:9000/health
```

Expected response body: `Metadata server is healthy`.

Also test physical node-to-node reachability after each target is started. Example from Node 1:

```bash
curl -i --connect-timeout 5 http://10.0.9.188:8002/health
curl -i --connect-timeout 5 http://10.0.10.198:8003/health
curl -i --connect-timeout 5 http://10.0.13.253:8004/health
```

If these fail, fix LAN routing, Wi-Fi/client isolation, host firewall, and node firewall before debugging Go code. The IPs show different third octets; do not assume the machines are in one subnet—check each machine's subnet mask and routing table.

## Firewall guidance

If UFW is active on a storage machine, allow its own port only from the metadata host and the other storage nodes. For example, on Node 1:

```bash
for source in 10.0.11.149 10.0.9.188 10.0.10.198 10.0.13.253; do
  sudo ufw allow from "$source" to any port 8001 proto tcp
 done
```

Use port `8002` on Node 2, `8003` on Node 3, and `8004` on Node 4. This assumes the source IP visible at the node for metadata-originated traffic is the Windows host IP; verify connectivity in your actual network. On Windows, run PowerShell as Administrator and restrict forwarded TCP/9000 to the four storage-node IPs:

```powershell
New-NetFirewallRule -DisplayName "DFS metadata 9000 from storage nodes" -Direction Inbound -Protocol TCP -LocalPort 9000 -RemoteAddress 10.0.10.199,10.0.9.188,10.0.10.198,10.0.13.253 -Action Allow -Profile Any
```

If a firewall already has a conflicting block rule, this allow rule may not be sufficient by itself. Keep the endpoint on a trusted LAN; this application currently has no authentication or TLS.

## Step C — Start metadata server on the Ubuntu VM

Either run:

```bash
cd ~/dfs_four_machine_deployment
bash scripts/start-metadata-vm.sh
```

or, once installed into `~/mtechproject`:

```bash
cd ~/mtechproject
METADATA_DB=./metadata-cluster.db METADATA_PORT=9000 go run metadata/main.go
```

Leave this terminal running. Confirm locally from another VM terminal:

```bash
curl -i http://localhost:9000/health
curl -s http://localhost:9000/nodes
```

## Step D — Copy the updated storage source to the four machines

Ensure Go is installed on each physical machine (`go version`). Each machine needs `~/mtechproject/storage/main.go` plus the project `go.mod` and `go.sum`. You can use `scp` from the VM if SSH server access is configured on the nodes; replace `NODE_USER` with the Linux username on each machine:

```bash
scp ~/mtechproject/go.mod ~/mtechproject/go.sum NODE_USER@10.0.10.199:~/mtechproject/
scp ~/mtechproject/storage/main.go NODE_USER@10.0.10.199:~/mtechproject/storage/main.go
```

Repeat with the relevant IP for Nodes 2-4. Alternatively copy the source with Git or a USB drive. Do not copy `metadata.db` or `metadata-cluster.db` to storage machines.

## Step E — Copy storage source and start scripts to each physical machine

On each physical machine, Go must be installed and SSH access should be enabled if you plan to use `scp`. On the physical machines, you can enable SSH with:

```bash
sudo apt update
sudo apt install -y openssh-server
sudo systemctl enable --now ssh
```

From the Ubuntu VM, replace `NODE_USER` with the Linux username on that physical machine and run the matching commands. Example for Node 1:

```bash
ssh NODE_USER@10.0.10.199 'mkdir -p ~/mtechproject/storage ~/mtechproject/data/node1 ~/dfs_four_machine_deployment/scripts'
scp ~/mtechproject/go.mod ~/mtechproject/go.sum NODE_USER@10.0.10.199:~/mtechproject/
scp ~/mtechproject/storage/main.go NODE_USER@10.0.10.199:~/mtechproject/storage/main.go
scp ~/dfs_four_machine_deployment/scripts/start-node1.sh NODE_USER@10.0.10.199:~/dfs_four_machine_deployment/scripts/
```

Repeat on the other three machines using the matching IP, `nodeN` directory, and `start-nodeN.sh` script. Copy only the storage code, `go.mod`, `go.sum`, and launch script. Do not copy the metadata source or metadata database onto a storage node.

If you do not want to enable SSH, copy `storage/main.go`, the module files, and the matching script to each physical machine by Git or removable media instead.

Start one node per physical machine:

- Node 1: `bash ~/dfs_four_machine_deployment/scripts/start-node1.sh`
- Node 2: `bash ~/dfs_four_machine_deployment/scripts/start-node2.sh`
- Node 3: `bash ~/dfs_four_machine_deployment/scripts/start-node3.sh`
- Node 4: `bash ~/dfs_four_machine_deployment/scripts/start-node4.sh`

If the Windows host address changes, supply the new address at launch, for example:

```bash
DFS_METADATA_URL=http://NEW_WINDOWS_WIFI_IP:9000 bash ~/dfs_four_machine_deployment/scripts/start-node1.sh
```

Note: a Windows IP change requires updating the metadata URL used by all storage nodes. Each node's `--advertise` IP must also be updated if that node's LAN IP changes.

## Step F — Verify the four-node cluster

From the Ubuntu VM:

```bash
curl -i http://localhost:9000/health
curl -s http://localhost:9000/nodes
```

Wait for the node monitor to check them (up to about 5-10 seconds) and confirm the four nodes show online. From the VM, test reverse reachability to each physical node:

```bash
curl -i http://10.0.10.199:8001/health
curl -i http://10.0.9.188:8002/health
curl -i http://10.0.10.198:8003/health
curl -i http://10.0.13.253:8004/health
```

If a node does not appear, check its terminal for `Registered with metadata server` and check that the advertised address is the LAN IP, not `localhost`.

## Step G — First cross-machine replication test

Only after all four nodes are online, use a fresh filename on a client or Node 1 machine:

```bash
echo "Four-machine DFS deployment test" > /tmp/dfs-four-node-test.txt
curl -i -X POST --data-binary @/tmp/dfs-four-node-test.txt \
  "http://10.0.10.199:8001/upload?name=dfs-four-node-test.txt"
```

Inspect the metadata queue:

```bash
curl -s http://localhost:9000/replication-queue
```

Check the file on each physical machine under `~/mtechproject/data/node1`, `node2`, `node3`, and `node4`. Compare the SHA-256 hashes of existing copies. With desired replication factor 3, exactly three verified copies should be the target under normal conditions; Node 4 is eligible but not guaranteed to be one of the three selected for each file.

The worker retries every five seconds; the reconciler runs every ten seconds. Allow some time, then verify queue completion and replica metadata.

## Existing endpoints retained

- Metadata: `/health`, `/nodes`, `/add`, `/get`, `/list`, `/register-node`, `/register-replica`, `/delete`, `/queue-replication`, `/replication-queue`, `/queue-status`, `/delete-replica`, `/replica-exclusions`.
- Storage: `/health`, `/upload`, `/download`, `/delete`.

## Backups and database safety

- The install script creates timestamped backups of `metadata/main.go` and `storage/main.go`.
- Existing `metadata.db` remains unchanged.
- The new cluster database defaults to `metadata-cluster.db`.
- Do not run two metadata-server processes against the same SQLite database concurrently.
- Do not copy a separate database to each storage node; the metadata DB belongs only to the metadata VM.

## Validation status

The source is based on the existing implementation and is formatted. In the available environment, the `modernc.org/sqlite` dependency could not be fetched from the network, so a real SQLite integration test was not possible here. Run `go test ./...` and `go build ./...` on your Ubuntu VM using your existing `go.mod`/`go.sum` before deploying. Then run the cross-machine tests above; compilation alone does not establish LAN connectivity or runtime correctness.
