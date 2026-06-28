package backups

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
