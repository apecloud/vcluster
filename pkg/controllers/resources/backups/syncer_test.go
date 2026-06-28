package backups

import (
	"testing"

	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
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
