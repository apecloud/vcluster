package persistentvolumeclaims

import corev1 "k8s.io/api/core/v1"

// This file is the single place where the external populator bridge depends on
// identifiers produced by an external populator implementation. Today the only
// producer is the KubeBlocks DataProtection VolumePopulator; the tokens below
// are version-pinned against apecloud/kubeblocks and MUST be re-verified when
// the deployed KubeBlocks version changes. See
// docs/external-populator-bridge.md for the full protocol description.
//
// Stability tiers:
//
//   - condition types and reasons are machine-readable API identifiers
//     (constants in apecloud/kubeblocks controllers/dataprotection/types.go).
//     They are the primary contract and are unlikely to change.
//   - the no-data-restore progress message is human-readable prose. KubeBlocks
//     reuses the "Processing" reason for every in-flight state, so the message
//     text is the only signal that distinguishes a ProvisionOnly (no data
//     restore) flow while it is still in progress. This is the weakest part of
//     the contract; TestExternalPopulatorContractTokens pins it so a token
//     change fails loudly here instead of silently breaking restores.
const (
	// legacyDataProtectionPopulateFromAnnotation is set by the KubeBlocks
	// populator on the populated PV and names the backup the volume was
	// restored from. It is forwarded verbatim in the materialization request.
	// Source of truth: apecloud/kubeblocks (dataprotection populator PV
	// annotations).
	legacyDataProtectionPopulateFromAnnotation = "dataprotection.kubeblocks.io/populate-from"

	// externalPopulatorRestoreConditionType is the PVC condition type the
	// populator uses for overall restore progress.
	// Source of truth: apecloud/kubeblocks apis/apps/v1 ConditionTypeRestore.
	externalPopulatorRestoreConditionType = corev1.PersistentVolumeClaimConditionType("Restore")

	// externalPopulatorPopulateConditionType is the PVC condition type the
	// populator uses for populate progress.
	// Source of truth: apecloud/kubeblocks controllers/dataprotection/types.go
	// PersistentVolumeClaimPopulating.
	externalPopulatorPopulateConditionType = corev1.PersistentVolumeClaimConditionType("Populating")

	// externalPopulatorRestoreConditionReasonProvisioned marks the terminal
	// "PVC provisioned without data restore" state. KubeBlocks uses this reason
	// exclusively for the no-data-restore outcome, so matching it requires no
	// message inspection.
	// Source of truth: apecloud/kubeblocks controllers/dataprotection/types.go
	// ReasonPopulatingProvisioned.
	externalPopulatorRestoreConditionReasonProvisioned = "Provisioned"

	// externalPopulatorRestoreConditionReasonProcessing is shared by every
	// in-flight populator state and therefore identifies nothing on its own.
	// Source of truth: apecloud/kubeblocks controllers/dataprotection/types.go
	// ReasonPopulatingProcessing.
	externalPopulatorRestoreConditionReasonProcessing = "Processing"

	// externalPopulatorNoDataRestoreMessage is the prose emitted with the
	// Processing reason while a ProvisionOnly flow is in progress. It is the
	// only in-flight signal for the no-data-restore path — version-pinned
	// prose, weakest contract tier (see file comment).
	// Source of truth: apecloud/kubeblocks
	// controllers/dataprotection/volumepopulator_controller.go ProvisionOnly.
	externalPopulatorNoDataRestoreMessage = "Provisioning PVC without data restore"
)
