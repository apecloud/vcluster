package backups

import (
	"testing"

	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestCopyNestedField(t *testing.T) {
	from := map[string]interface{}{
		"status": map[string]interface{}{"phase": "Completed"},
	}
	to := map[string]interface{}{
		"status": map[string]interface{}{"phase": "Running"},
	}

	copyNestedField(from, to, "status")

	phase, ok, err := unstructured.NestedString(to, "status", "phase")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.phase to be copied")
	} else if phase != "Completed" {
		t.Fatalf("expected phase Completed, got %q", phase)
	}
}

func TestCopyNestedFieldRemovesMissingField(t *testing.T) {
	to := map[string]interface{}{
		"status": map[string]interface{}{"phase": "Running"},
	}

	copyNestedField(map[string]interface{}{}, to, "status")

	_, ok, err := unstructured.NestedMap(to, "status")
	if err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected status to be removed")
	}
}

func TestMarkHostBackupReconciliationSkipped(t *testing.T) {
	host := NewObject()
	host.SetAnnotations(map[string]string{
		dataProtectionSkipReconciliation: "false",
		"keep":                           "me",
	})

	markHostBackupReconciliationSkipped(host)

	annotations := host.GetAnnotations()
	if annotations[dataProtectionSkipReconciliation] != "true" {
		t.Fatalf("expected skip reconciliation annotation true, got %q", annotations[dataProtectionSkipReconciliation])
	}
	if annotations["keep"] != "me" {
		t.Fatalf("expected unrelated annotation to be preserved, got %q", annotations["keep"])
	}
}

func TestHostBackupAnnotationsReappliesSkipAfterTranslation(t *testing.T) {
	virtual := NewObject()
	virtual.SetName("mysql-br-readback-xtrabackup-backup-64999")
	virtual.SetNamespace("mysql-backup-cr-readback")
	host := NewObject()
	host.SetName(translate.Default.HostName(&synccontext.SyncContext{}, virtual.GetName(), virtual.GetNamespace()).Name)
	host.SetAnnotations(map[string]string{
		dataProtectionSkipReconciliation: "false",
	})

	annotations := hostBackupAnnotations(virtual, host)

	if annotations[dataProtectionSkipReconciliation] != "true" {
		t.Fatalf("expected translated host annotations to force skip reconciliation true, got %q", annotations[dataProtectionSkipReconciliation])
	}
}

func TestVirtualBackupAnnotationsDropsHostSkipReconciliation(t *testing.T) {
	virtual := NewObject()
	virtual.SetAnnotations(map[string]string{
		dataProtectionSkipReconciliation: "true",
		"virtual":                        "keep",
	})
	host := NewObject()
	host.SetAnnotations(map[string]string{
		dataProtectionSkipReconciliation: "true",
		"host":                           "copy",
	})

	annotations := virtualBackupAnnotations(host, virtual)

	if _, ok := annotations[dataProtectionSkipReconciliation]; ok {
		t.Fatalf("expected virtual annotations to drop %s, got %#v", dataProtectionSkipReconciliation, annotations)
	}
	if annotations["host"] != "copy" {
		t.Fatalf("expected ordinary host annotation to copy, got %#v", annotations)
	}
}

func TestSyncBackupDesiredStateToSkippedHostPreservesVirtualRuntimeState(t *testing.T) {
	namespace := "mysql-backup-cr-readback"
	virtual := NewObject()
	virtual.SetName("mysql-br-readback-xtrabackup-backup-64999")
	virtual.SetNamespace(namespace)
	virtual.SetUID(types.UID("virtual-uid"))
	virtual.SetFinalizers([]string{"virtual-dp-finalizer"})
	_ = unstructured.SetNestedField(virtual.Object, "Completed", "status", "phase")
	_ = unstructured.SetNestedField(virtual.Object, "mysql-policy", "spec", "backupPolicyName")

	host := NewObject()
	host.SetName(translate.Default.HostName(&synccontext.SyncContext{}, virtual.GetName(), namespace).Name)
	host.SetNamespace(translate.Default.HostName(&synccontext.SyncContext{}, virtual.GetName(), namespace).Namespace)
	host.SetUID(types.UID("host-uid"))
	host.SetFinalizers([]string{"host-dp-finalizer"})
	_ = unstructured.SetNestedField(host.Object, "Running", "status", "phase")

	syncBackupDesiredStateToSkippedHost(&synccontext.SyncContext{}, virtual, host)

	phase, ok, err := unstructured.NestedString(virtual.Object, "status", "phase")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected virtual status.phase to remain set")
	} else if phase != "Completed" {
		t.Fatalf("expected virtual status.phase to remain Completed, got %q", phase)
	}
	if got := virtual.GetFinalizers(); len(got) != 1 || got[0] != "virtual-dp-finalizer" {
		t.Fatalf("expected virtual finalizers to remain virtual-owned, got %#v", got)
	}

	hostPhase, ok, err := unstructured.NestedString(host.Object, "status", "phase")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected host status.phase to remain set")
	} else if hostPhase != "Running" {
		t.Fatalf("expected host status.phase to remain Running, got %q", hostPhase)
	}
	if host.GetAnnotations()[dataProtectionSkipReconciliation] != "true" {
		t.Fatalf("expected host Backup to be marked skip reconciliation, got %#v", host.GetAnnotations())
	}
	if _, ok := virtual.GetAnnotations()[dataProtectionSkipReconciliation]; ok {
		t.Fatalf("expected virtual Backup to omit skip reconciliation annotation, got %#v", virtual.GetAnnotations())
	}

	hostPolicyName, ok, err := unstructured.NestedString(host.Object, "spec", "backupPolicyName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected host spec.backupPolicyName to be set")
	} else if hostPolicyName != translate.Default.HostName(&synccontext.SyncContext{}, "mysql-policy", namespace).Name {
		t.Fatalf("expected translated host backupPolicyName, got %q", hostPolicyName)
	}
}

func TestTranslateBackupPolicyName(t *testing.T) {
	from := map[string]interface{}{
		"spec": map[string]interface{}{
			"backupPolicyName": "mysql-policy",
		},
	}
	to := map[string]interface{}{
		"spec": map[string]interface{}{},
	}

	translateBackupPolicyName(nil, from, to, "mysql-ns")

	name, ok, err := unstructured.NestedString(to, "spec", "backupPolicyName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected spec.backupPolicyName to be set")
	} else if name != "mysql-policy-x-mysql-ns-x-suffix" {
		t.Fatalf("expected translated backupPolicyName, got %q", name)
	}
}

func TestTranslateBackupPolicyNameLeavesMissingField(t *testing.T) {
	to := map[string]interface{}{
		"spec": map[string]interface{}{},
	}

	translateBackupPolicyName(nil, map[string]interface{}{}, to, "mysql-ns")

	_, ok, err := unstructured.NestedString(to, "spec", "backupPolicyName")
	if err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected missing backupPolicyName to remain unset")
	}
}

func TestTranslateBackupRepoStatusToVirtualPreservesWhenMappingUnavailable(t *testing.T) {
	from := map[string]interface{}{
		"status": map[string]interface{}{"backupRepoName": "host-repo"},
	}
	to := map[string]interface{}{
		"status": map[string]interface{}{},
	}

	translateBackupRepoStatusToVirtual(nil, from, to)

	name, ok, err := unstructured.NestedString(to, "status", "backupRepoName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.backupRepoName to be set")
	} else if name != "host-repo" {
		t.Fatalf("nil context should preserve backupRepoName, got %q", name)
	}
}

func TestTranslateBackupTargetPodNameToVirtualPreservesWhenMappingUnavailable(t *testing.T) {
	from := map[string]interface{}{
		"status": map[string]interface{}{"targetPodName": "host-pod"},
	}
	to := map[string]interface{}{
		"status": map[string]interface{}{},
	}

	translateBackupTargetPodNameToVirtual(nil, from, to, "mysql-ns")

	name, ok, err := unstructured.NestedString(to, "status", "targetPodName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.targetPodName to be set")
	} else if name != "host-pod" {
		t.Fatalf("nil context should preserve targetPodName, got %q", name)
	}
}

func TestTranslateBackupTargetPodNameToVirtualFromPodMapper(t *testing.T) {
	from := map[string]interface{}{
		"status": map[string]interface{}{
			"targetPodName": "mysql-br-readback-mysql-1-x-mysql-backup-cr-readback-x-suffix",
			"target": map[string]interface{}{
				"selectedTargetPods": []interface{}{
					"mysql-br-readback-mysql-1-x-mysql-backup-cr-readback-x-suffix",
					"unmapped-host-pod",
				},
			},
		},
	}
	to := map[string]interface{}{
		"status": map[string]interface{}{},
	}
	registry := mappings.NewMappingsRegistry(nil)
	err := registry.AddMapper(staticPodMapperForTest{
		hostName:    "mysql-br-readback-mysql-1-x-mysql-backup-cr-readback-x-suffix",
		virtualName: "mysql-br-readback-mysql-1",
		namespace:   "mysql-backup-cr-readback",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := &synccontext.SyncContext{Mappings: registry}

	translateBackupTargetPodNameToVirtual(ctx, from, to, "mysql-backup-cr-readback")

	name, ok, err := unstructured.NestedString(to, "status", "targetPodName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.targetPodName to be set")
	} else if name != "mysql-br-readback-mysql-1" {
		t.Fatalf("expected translated targetPodName, got %q", name)
	}

	selectedTargetPods, ok, err := unstructured.NestedStringSlice(to, "status", "target", "selectedTargetPods")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.target.selectedTargetPods to be set")
	} else if len(selectedTargetPods) != 2 {
		t.Fatalf("expected two selected target pods, got %d", len(selectedTargetPods))
	} else if selectedTargetPods[0] != "mysql-br-readback-mysql-1" {
		t.Fatalf("expected translated selected target pod, got %q", selectedTargetPods[0])
	} else if selectedTargetPods[1] != "unmapped-host-pod" {
		t.Fatalf("expected unmapped selected target pod to be preserved, got %q", selectedTargetPods[1])
	}
}

func TestTranslateBackupTargetPodNameToVirtualFromSingleNamespaceHostName(t *testing.T) {
	virtualPodName := "mysql-br-readback-mysql-1"
	namespace := "mysql-backup-cr-readback"
	hostPodName := translate.Default.HostName(&synccontext.SyncContext{}, virtualPodName, namespace).Name
	from := map[string]interface{}{
		"status": map[string]interface{}{
			"targetPodName": hostPodName,
			"target": map[string]interface{}{
				"selectedTargetPods": []interface{}{hostPodName},
			},
		},
	}
	to := map[string]interface{}{
		"status": map[string]interface{}{},
	}

	translateBackupTargetPodNameToVirtual(&synccontext.SyncContext{}, from, to, namespace)

	name, ok, err := unstructured.NestedString(to, "status", "targetPodName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.targetPodName to be set")
	} else if name != virtualPodName {
		t.Fatalf("expected translated targetPodName, got %q", name)
	}

	selectedTargetPods, ok, err := unstructured.NestedStringSlice(to, "status", "target", "selectedTargetPods")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.target.selectedTargetPods to be set")
	} else if len(selectedTargetPods) != 1 || selectedTargetPods[0] != virtualPodName {
		t.Fatalf("expected translated selected target pods, got %#v", selectedTargetPods)
	}
}

func TestTranslateBackupTargetPodNameToVirtualPreservesUnverifiedSingleNamespaceName(t *testing.T) {
	from := map[string]interface{}{
		"status": map[string]interface{}{
			"targetPodName": "mysql-br-readback-mysql-1-x-mysql-backup-cr-readback-wronghash",
			"target": map[string]interface{}{
				"selectedTargetPods": []interface{}{"mysql-br-readback-mysql-1-x-mysql-backup-cr-readback-wronghash"},
			},
		},
	}
	to := map[string]interface{}{
		"status": map[string]interface{}{},
	}

	translateBackupTargetPodNameToVirtual(&synccontext.SyncContext{}, from, to, "mysql-backup-cr-readback")

	name, ok, err := unstructured.NestedString(to, "status", "targetPodName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.targetPodName to be set")
	} else if name != "mysql-br-readback-mysql-1-x-mysql-backup-cr-readback-wronghash" {
		t.Fatalf("expected unverified targetPodName to be preserved, got %q", name)
	}

	selectedTargetPods, ok, err := unstructured.NestedStringSlice(to, "status", "target", "selectedTargetPods")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.target.selectedTargetPods to be set")
	} else if len(selectedTargetPods) != 1 || selectedTargetPods[0] != "mysql-br-readback-mysql-1-x-mysql-backup-cr-readback-wronghash" {
		t.Fatalf("expected unverified selected target pods to be preserved, got %#v", selectedTargetPods)
	}
}

func TestTranslateBackupTargetConnectionCredentialSecretNameToHost(t *testing.T) {
	namespace := "mysql-backup-cr-readback"
	virtualSecretName := "mysql-br-readback-mysql-account-kbadmin"
	hostSecretName := translate.Default.HostName(&synccontext.SyncContext{}, virtualSecretName, namespace).Name
	if hostSecretName == virtualSecretName || len(hostSecretName) > 63 {
		t.Fatalf("expected a translated DNS-safe host secret name, got %q", hostSecretName)
	}
	from := map[string]interface{}{
		"status": map[string]interface{}{
			"target": map[string]interface{}{
				"connectionCredential": map[string]interface{}{
					"secretName": virtualSecretName,
				},
			},
		},
	}
	to := map[string]interface{}{}

	translateBackupTargetConnectionCredentialSecretNameToHost(&synccontext.SyncContext{}, from, to, namespace)

	secretName, ok, err := unstructured.NestedString(to, "status", "target", "connectionCredential", "secretName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.target.connectionCredential.secretName to be set")
	} else if secretName != hostSecretName {
		t.Fatalf("expected translated host secret name, got %q", secretName)
	}
}

func TestTranslateBackupTargetConnectionCredentialSecretNameToHostPreservesAlreadyHostName(t *testing.T) {
	namespace := "mysql-backup-cr-readback"
	virtualSecretName := "mysql-br-readback-mysql-account-kbadmin"
	hostSecretName := translate.Default.HostName(&synccontext.SyncContext{}, virtualSecretName, namespace).Name
	from := map[string]interface{}{
		"status": map[string]interface{}{
			"target": map[string]interface{}{
				"connectionCredential": map[string]interface{}{
					"secretName": hostSecretName,
				},
			},
		},
	}
	to := map[string]interface{}{}

	translateBackupTargetConnectionCredentialSecretNameToHost(&synccontext.SyncContext{}, from, to, namespace)

	secretName, ok, err := unstructured.NestedString(to, "status", "target", "connectionCredential", "secretName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.target.connectionCredential.secretName to be set")
	} else if secretName != hostSecretName {
		t.Fatalf("expected existing host secret name to be preserved, got %q", secretName)
	}
}

func TestTranslateBackupTargetConnectionCredentialSecretNameToHostRunsAfterLateStatusCopy(t *testing.T) {
	namespace := "mysql-backup-cr-readback"
	virtualSecretName := "mysql-br-readback-mysql-account-kbadmin"
	hostSecretName := translate.Default.HostName(&synccontext.SyncContext{}, virtualSecretName, namespace).Name
	virtual := map[string]interface{}{
		"status": map[string]interface{}{
			"target": map[string]interface{}{
				"connectionCredential": map[string]interface{}{
					"secretName": virtualSecretName,
				},
			},
		},
	}
	host := map[string]interface{}{}

	// Simulate any late host patch/copy restoring virtual status on the object
	// after the first create preparation steps.
	copyNestedField(virtual, host, "status")
	translateBackupTargetConnectionCredentialSecretNameToHost(&synccontext.SyncContext{}, virtual, host, namespace)

	secretName, ok, err := unstructured.NestedString(host, "status", "target", "connectionCredential", "secretName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.target.connectionCredential.secretName to be set")
	} else if secretName != hostSecretName {
		t.Fatalf("expected translated host secret name, got %q", secretName)
	}
}

func TestTranslateBackupTargetConnectionCredentialSecretNameToVirtual(t *testing.T) {
	namespace := "mysql-backup-cr-readback"
	virtualSecretName := "mysql-br-readback-mysql-account-kbadmin"
	hostSecretName := translate.Default.HostName(&synccontext.SyncContext{}, virtualSecretName, namespace).Name
	from := map[string]interface{}{
		"status": map[string]interface{}{
			"target": map[string]interface{}{
				"connectionCredential": map[string]interface{}{
					"secretName": hostSecretName,
				},
			},
		},
	}
	to := map[string]interface{}{
		"status": map[string]interface{}{},
	}

	translateBackupTargetConnectionCredentialSecretNameToVirtual(&synccontext.SyncContext{}, from, to, namespace)

	secretName, ok, err := unstructured.NestedString(to, "status", "target", "connectionCredential", "secretName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.target.connectionCredential.secretName to be set")
	} else if secretName != virtualSecretName {
		t.Fatalf("expected translated virtual secret name, got %q", secretName)
	}
}

func TestTranslateBackupTargetConnectionCredentialSecretNameToVirtualPreservesUnverifiedName(t *testing.T) {
	namespace := "mysql-backup-cr-readback"
	hostSecretName := "mysql-br-readback-mysql-account-kbadmin-x-mysql-back-wronghash"
	from := map[string]interface{}{
		"status": map[string]interface{}{
			"target": map[string]interface{}{
				"connectionCredential": map[string]interface{}{
					"secretName": hostSecretName,
				},
			},
		},
	}
	to := map[string]interface{}{
		"status": map[string]interface{}{},
	}

	translateBackupTargetConnectionCredentialSecretNameToVirtual(&synccontext.SyncContext{}, from, to, namespace)

	secretName, ok, err := unstructured.NestedString(to, "status", "target", "connectionCredential", "secretName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected status.target.connectionCredential.secretName to be set")
	} else if secretName != hostSecretName {
		t.Fatalf("expected unverified host secret name to be preserved, got %q", secretName)
	}
}

func TestTranslateBackupActionsToVirtual(t *testing.T) {
	namespace := "mysql-backup-cr-readback"
	virtualBackupName := "mysql-br-readback-xtrabackup-backup-60590"
	virtualBackupUID := types.UID("virtual1-2345-6789")
	hostBackupName := translate.Default.HostName(&synccontext.SyncContext{}, virtualBackupName, namespace).Name
	hostBackupUID := types.UID("hostuid1-2345-6789")
	virtualPodName := "mysql-br-readback-mysql-1"
	hostPodName := translate.Default.HostName(&synccontext.SyncContext{}, virtualPodName, namespace).Name
	actionName := "dp-backup-0"
	hostJobName := generateBackupJobNameForStatus(hostBackupName, hostBackupUID, actionName)
	if hostJobName != "dp-backup-0-mysql-br-readback-xtrabackup-backup-60590-x-mysql-b" {
		t.Fatalf("expected host job name to match the v7 truncated form, got %q", hostJobName)
	}
	to := map[string]interface{}{
		"status": map[string]interface{}{
			"actions": []interface{}{
				map[string]interface{}{
					"actionType":    "Job",
					"name":          actionName,
					"targetPodName": hostPodName,
					"objectRef": map[string]interface{}{
						"apiVersion": "batch/v1",
						"kind":       "Job",
						"name":       hostJobName,
						"namespace":  translate.Default.HostName(&synccontext.SyncContext{}, virtualBackupName, namespace).Namespace,
					},
				},
			},
		},
	}

	translateBackupActionsToVirtual(&synccontext.SyncContext{}, to, namespace, hostBackupName, hostBackupUID, virtualBackupName, virtualBackupUID)

	actions, ok, err := unstructured.NestedSlice(to, "status", "actions")
	if err != nil {
		t.Fatal(err)
	} else if !ok || len(actions) != 1 {
		t.Fatalf("expected one status action, got ok=%v len=%d", ok, len(actions))
	}
	action := actions[0].(map[string]interface{})
	podName, ok, err := unstructured.NestedString(action, "targetPodName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected action targetPodName to be set")
	} else if podName != virtualPodName {
		t.Fatalf("expected translated action targetPodName, got %q", podName)
	}

	objectRefName, ok, err := unstructured.NestedString(action, "objectRef", "name")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected objectRef.name to be set")
	} else if objectRefName != generateBackupJobNameForStatus(virtualBackupName, virtualBackupUID, actionName) {
		t.Fatalf("expected translated objectRef.name, got %q", objectRefName)
	}

	objectRefNamespace, ok, err := unstructured.NestedString(action, "objectRef", "namespace")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected objectRef.namespace to be set")
	} else if objectRefNamespace != namespace {
		t.Fatalf("expected translated objectRef.namespace, got %q", objectRefNamespace)
	}
}

func TestTranslateBackupActionsToVirtualPreservesUnexpectedJobRefName(t *testing.T) {
	namespace := "mysql-backup-cr-readback"
	virtualBackupName := "mysql-br-readback-xtrabackup-backup-51929"
	virtualBackupUID := types.UID("virtual1-2345-6789")
	hostBackupName := translate.Default.HostName(&synccontext.SyncContext{}, virtualBackupName, namespace).Name
	hostBackupUID := types.UID("hostuid1-2345-6789")
	unexpectedJobName := "different-host-job"
	to := map[string]interface{}{
		"status": map[string]interface{}{
			"actions": []interface{}{
				map[string]interface{}{
					"actionType": "Job",
					"name":       "dp-backup-0",
					"objectRef": map[string]interface{}{
						"kind":      "Job",
						"name":      unexpectedJobName,
						"namespace": translate.Default.HostName(&synccontext.SyncContext{}, virtualBackupName, namespace).Namespace,
					},
				},
			},
		},
	}

	translateBackupActionsToVirtual(&synccontext.SyncContext{}, to, namespace, hostBackupName, hostBackupUID, virtualBackupName, virtualBackupUID)

	actions, ok, err := unstructured.NestedSlice(to, "status", "actions")
	if err != nil {
		t.Fatal(err)
	} else if !ok || len(actions) != 1 {
		t.Fatalf("expected one status action, got ok=%v len=%d", ok, len(actions))
	}
	action := actions[0].(map[string]interface{})
	objectRefName, ok, err := unstructured.NestedString(action, "objectRef", "name")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected objectRef.name to be set")
	} else if objectRefName != unexpectedJobName {
		t.Fatalf("expected unexpected objectRef.name to be preserved, got %q", objectRefName)
	}

	objectRefNamespace, ok, err := unstructured.NestedString(action, "objectRef", "namespace")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected objectRef.namespace to be set")
	} else if objectRefNamespace != namespace {
		t.Fatalf("expected host objectRef.namespace to be translated, got %q", objectRefNamespace)
	}
}

func TestTranslateBackupRepoLabelToVirtualFromDefaultRepo(t *testing.T) {
	backupRepos := &unstructured.UnstructuredList{
		Items: []unstructured.Unstructured{
			newBackupRepoForTest("virtual-repo", true),
		},
	}

	got := translateBackupRepoNameToVirtualFromList("host-repo", backupRepos)
	if got != "virtual-repo" {
		t.Fatalf("expected host repo to map to virtual default repo, got %q", got)
	}
}

func TestTranslateBackupRepoNameToVirtualPreservesSameName(t *testing.T) {
	backupRepos := &unstructured.UnstructuredList{
		Items: []unstructured.Unstructured{
			newBackupRepoForTest("host-repo", true),
		},
	}

	got := translateBackupRepoNameToVirtualFromList("host-repo", backupRepos)
	if got != "host-repo" {
		t.Fatalf("expected same-name repo to be preserved, got %q", got)
	}
}

func TestTranslateBackupRepoNameToVirtualLeavesAmbiguousRepos(t *testing.T) {
	backupRepos := &unstructured.UnstructuredList{
		Items: []unstructured.Unstructured{
			newBackupRepoForTest("repo-a", false),
			newBackupRepoForTest("repo-b", false),
		},
	}

	got := translateBackupRepoNameToVirtualFromList("host-repo", backupRepos)
	if got != "host-repo" {
		t.Fatalf("expected ambiguous repo list to preserve host repo, got %q", got)
	}
}

func newBackupRepoForTest(name string, defaultRepo bool) unstructured.Unstructured {
	repo := unstructured.Unstructured{}
	repo.SetName(name)
	if defaultRepo {
		repo.SetAnnotations(map[string]string{dataProtectionDefaultRepoAnnotation: "true"})
	}
	return repo
}

type staticPodMapperForTest struct {
	hostName    string
	virtualName string
	namespace   string
}

func (s staticPodMapperForTest) GroupVersionKind() schema.GroupVersionKind {
	return mappings.Pods()
}

func (s staticPodMapperForTest) Migrate(_ *synccontext.RegisterContext, _ synccontext.Mapper) error {
	return nil
}

func (s staticPodMapperForTest) VirtualToHost(_ *synccontext.SyncContext, req types.NamespacedName, _ client.Object) types.NamespacedName {
	return req
}

func (s staticPodMapperForTest) HostToVirtual(_ *synccontext.SyncContext, req types.NamespacedName, _ client.Object) types.NamespacedName {
	if req.Name != s.hostName {
		return types.NamespacedName{}
	}

	return types.NamespacedName{Name: s.virtualName, Namespace: s.namespace}
}

func (s staticPodMapperForTest) IsManaged(_ *synccontext.SyncContext, _ client.Object) (bool, error) {
	return false, nil
}
