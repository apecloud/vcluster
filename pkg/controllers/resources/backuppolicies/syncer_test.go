package backuppolicies

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestCopyNestedField(t *testing.T) {
	from := map[string]interface{}{
		"spec": map[string]interface{}{"backupRepoName": "repo"},
	}
	to := map[string]interface{}{
		"spec": map[string]interface{}{"backupRepoName": "old"},
	}

	copyNestedField(from, to, "spec")

	name, ok, err := unstructured.NestedString(to, "spec", "backupRepoName")
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected spec.backupRepoName to be copied")
	} else if name != "repo" {
		t.Fatalf("expected repo, got %q", name)
	}
}

func TestCopyNestedFieldRemovesMissingField(t *testing.T) {
	to := map[string]interface{}{
		"spec": map[string]interface{}{"backupRepoName": "old"},
	}

	copyNestedField(map[string]interface{}{}, to, "spec")

	_, ok, err := unstructured.NestedMap(to, "spec")
	if err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected spec to be removed")
	}
}
