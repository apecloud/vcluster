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
