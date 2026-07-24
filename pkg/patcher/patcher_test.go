package patcher

import (
	"context"
	"strings"
	"testing"

	"github.com/loft-sh/vcluster/pkg/scheme"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	testingutil "github.com/loft-sh/vcluster/pkg/util/testing"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSyncerPatcherRebaseHostFailClosed(t *testing.T) {
	host := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "host",
			Namespace:       "test",
			ResourceVersion: "1",
		},
	}
	virtual := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "virtual",
			Namespace:       "test",
			ResourceVersion: "1",
		},
	}
	ctx := &synccontext.SyncContext{
		Context:       context.Background(),
		HostClient:    testingutil.NewFakeClient(scheme.Scheme),
		VirtualClient: testingutil.NewFakeClient(scheme.Scheme),
	}
	syncerPatcher, err := NewSyncerPatcher(ctx, host, virtual)
	if err != nil {
		t.Fatalf("new syncer patcher: %v", err)
	}

	tests := []struct {
		name string
		obj  *corev1.PersistentVolumeClaim
		want string
	}{
		{
			name: "nil committed object",
			obj:  nil,
			want: "expected non-nil object",
		},
		{
			name: "missing committed resource version",
			obj: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "host", Namespace: "test"},
			},
			want: "committed resourceVersion is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := syncerPatcher.RebaseHost(tt.obj)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("RebaseHost() error = %v, want substring %q", err, tt.want)
			}
		})
	}

	committed := host.DeepCopy()
	committed.ResourceVersion = "2"
	if err := syncerPatcher.RebaseHost(committed); err != nil {
		t.Fatalf("RebaseHost() committed object: %v", err)
	}
}

func TestSyncerPatcherFailedHostRebaseDisablesOnlyHostPatch(t *testing.T) {
	hostClient := testingutil.NewFakeClient(scheme.Scheme, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "host",
			Namespace: "test",
			Annotations: map[string]string{
				"example.test/external-writer": "preserve",
			},
		},
	})
	virtualClient := testingutil.NewFakeClient(scheme.Scheme, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "virtual",
			Namespace: "test",
		},
	})
	ctx := &synccontext.SyncContext{
		Context:       context.Background(),
		HostClient:    hostClient,
		VirtualClient: virtualClient,
	}
	host := &corev1.PersistentVolumeClaim{}
	if err := hostClient.Get(ctx, client.ObjectKey{Namespace: "test", Name: "host"}, host); err != nil {
		t.Fatalf("get host: %v", err)
	}
	virtual := &corev1.PersistentVolumeClaim{}
	if err := virtualClient.Get(ctx, client.ObjectKey{Namespace: "test", Name: "virtual"}, virtual); err != nil {
		t.Fatalf("get virtual: %v", err)
	}
	syncerPatcher, err := NewSyncerPatcher(ctx, host, virtual)
	if err != nil {
		t.Fatalf("new syncer patcher: %v", err)
	}

	host.Annotations["example.test/stale-controller-diff"] = "must-not-apply"
	virtual.Annotations = map[string]string{
		"example.test/virtual-diff": "apply",
	}
	freshWithoutResourceVersion := host.DeepCopy()
	freshWithoutResourceVersion.ResourceVersion = ""
	if err := syncerPatcher.RebaseHost(freshWithoutResourceVersion); err == nil {
		t.Fatal("RebaseHost() succeeded with empty resourceVersion")
	}
	if err := syncerPatcher.Patch(ctx, host, virtual); err != nil {
		t.Fatalf("Patch() after failed host rebase: %v", err)
	}

	actualHost := &corev1.PersistentVolumeClaim{}
	if err := hostClient.Get(ctx, client.ObjectKey{Namespace: "test", Name: "host"}, actualHost); err != nil {
		t.Fatalf("get actual host: %v", err)
	}
	if actualHost.Annotations["example.test/external-writer"] != "preserve" ||
		actualHost.Annotations["example.test/stale-controller-diff"] != "" {
		t.Fatalf("host patch was not disabled after failed rebase: annotations=%v", actualHost.Annotations)
	}
	actualVirtual := &corev1.PersistentVolumeClaim{}
	if err := virtualClient.Get(ctx, client.ObjectKey{Namespace: "test", Name: "virtual"}, actualVirtual); err != nil {
		t.Fatalf("get actual virtual: %v", err)
	}
	if actualVirtual.Annotations["example.test/virtual-diff"] != "apply" {
		t.Fatalf("virtual patch was disabled with host patch: annotations=%v", actualVirtual.Annotations)
	}
}
