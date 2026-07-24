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
