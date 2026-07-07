package persistentvolumeclaims

import "testing"

// TestExternalPopulatorContractTokens pins the identifiers this fork consumes
// from the KubeBlocks DataProtection VolumePopulator. If this test needs a
// change, the deployed KubeBlocks version has (or is about to) change the
// producing side — re-verify every token against the source-of-truth pointers
// in external_populator_contract.go before updating, and check whether already
// deployed KubeBlocks versions still emit the old tokens (if so, the syncer
// must accept both).
func TestExternalPopulatorContractTokens(t *testing.T) {
	pins := map[string]string{
		"populate-from annotation":       legacyDataProtectionPopulateFromAnnotation,
		"restore condition type":         string(externalPopulatorRestoreConditionType),
		"populate condition type":        string(externalPopulatorPopulateConditionType),
		"terminal no-data reason":        externalPopulatorRestoreConditionReasonProvisioned,
		"in-flight reason":               externalPopulatorRestoreConditionReasonProcessing,
		"in-flight no-data prose (weak)": externalPopulatorNoDataRestoreMessage,
	}
	expected := map[string]string{
		"populate-from annotation":       "dataprotection.kubeblocks.io/populate-from",
		"restore condition type":         "Restore",
		"populate condition type":        "Populating",
		"terminal no-data reason":        "Provisioned",
		"in-flight reason":               "Processing",
		"in-flight no-data prose (weak)": "Provisioning PVC without data restore",
	}

	for name, want := range expected {
		if pins[name] != want {
			t.Errorf("external populator contract token %q changed: got %q, want %q — re-verify against KubeBlocks before updating", name, pins[name], want)
		}
	}
}
