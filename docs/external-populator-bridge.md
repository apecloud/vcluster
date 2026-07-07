# External populator restore bridge

This document specifies the apecloud fork's external populator restore bridge:
what it does, the identifiers it consumes from the populator implementation,
and the private materialization-request protocol it emits. None of these
mechanisms exist in upstream vcluster.

## Why this exists

A guest workload (KubeBlocks DataProtection) restores a backup by creating a
target PVC with a `dataSourceRef` pointing at a non-core object (e.g. a
`Backup`), plus a temporary populate helper PVC that provisions a real host
volume and fills it with the backup data. The populator then re-binds the
volume from the helper to the target inside the guest and deletes the helper.

The host cluster knows nothing about the guest-side re-bind. The bridge in the
PVC/PV syncers replays it on the host: it keeps the host helper PVC alive until
convergence, re-points the populated host PV's `claimRef` from the host helper
PVC to the host target PVC, and writes the target's `volumeName`. In fake PV
mode this requires the cluster role to allow `persistentvolumes get/list/patch`
(granted by the chart when PVC sync is enabled without PV sync).

## Consumed identifiers (populator → vcluster)

All identifiers the bridge reads from populator-produced objects are collected
in
[`pkg/controllers/resources/persistentvolumeclaims/external_populator_contract.go`](../pkg/controllers/resources/persistentvolumeclaims/external_populator_contract.go)
with per-token source-of-truth pointers, and pinned by
`TestExternalPopulatorContractTokens`.

| Token | Kind | Stability |
|---|---|---|
| `Restore`, `Populating` condition types | API identifier | stable |
| `Provisioned` reason (terminal no-data restore) | API identifier, exclusive to this state | stable |
| `Processing` reason | API identifier, shared by all in-flight states | stable but not distinguishing |
| `"Provisioning PVC without data restore"` message | prose | **version-pinned, weakest tier** — only in-flight signal for the no-data path |
| `dataprotection.kubeblocks.io/populate-from` PV annotation | legacy interface | stable key, forwarded verbatim |

Everything else the bridge uses is structural (PV/PVC binding relationships,
`dataSourceRef` kinds outside the core group) and does not depend on the
populator implementation.

## Materialization request protocol (vcluster → external materializer)

When the bridge needs a host PV that does not exist yet (the populated volume
is only visible in the guest), the PVC syncer emits a **materialization
request** and requeues until the host PV appears.

- **Writer**: vcluster PVC syncer
  (`upsertExternalPopulatorMaterializationRequest`).
- **Object**: `ConfigMap` in the vcluster's host namespace, named
  `external-populator-materialization-<sha256(hostNamespace/hostName)[:16]>`,
  labeled `vcluster.loft.sh/external-populator-materialization-request: "true"`.
- **Consumer**: an external, deployment-specific materializer watches
  ConfigMaps with that label, creates the host PV described by the payload,
  and is expected to either update `state` or simply let the bridge observe
  the host PV. vcluster never deletes another party's fields; it re-asserts
  `labels` and `data` when drift is detected.
- **Lifecycle**: created/patched while the target host PVC is waiting; becomes
  irrelevant once the host PV exists (the bridge stops updating it).

Payload (`data`):

| Field | Meaning |
|---|---|
| `state` | request state; vcluster writes `pending` |
| `hostPVCNamespace` / `hostPVCName` | host target PVC |
| `virtualPVCNamespace` / `virtualPVCName` / `virtualPVCUID` | guest target PVC |
| `virtualPVName` / `virtualPVUID` | guest-side populated PV |
| `dataSourceAPIGroup` / `dataSourceKind` / `dataSourceName` | the target PVC's `dataSourceRef` |
| `populateFrom` | value of the `populate-from` PV annotation (backup name) |

Compatibility rules for consumers: treat unknown fields as forward-compatible
additions; never rely on ConfigMap resourceVersion ordering; the request name
is deterministic per host PVC, so repeated writes are idempotent upserts.

## Related

- Deletion-convergence methodology (why the helper must outlive the bridge):
  kubeblocks-addon-docs `docs/vcluster/addon-vcluster-syncer-deletion-convergence-pattern-guide.md`
- Known limitation this bridge removes: kubeblocks-addon-docs
  `docs/test/addon-vcluster-fake-pv-restore-blocker-guide.md`
