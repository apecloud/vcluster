package storageclasses

import (
	"testing"

	vclusterconfig "github.com/loft-sh/vcluster/config"
	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	syncertesting "github.com/loft-sh/vcluster/pkg/syncer/testing"
	testingutil "github.com/loft-sh/vcluster/pkg/util/testing"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	"gotest.tools/assert"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestFromHostSyncManagedCollisionSelectorMismatch pins the live collision
// contract: a foreign vCluster's managed host object carries
// translate.NameAnnotation pointing at a short virtual name, so the reconciler
// pairs it with the guest's own storage class of that name. When that managed
// host object also fails the fromHost selector, the guest's storage class must
// be preserved — owned or not — and the foreign host object must not be
// touched.
func TestFromHostSyncManagedCollisionSelectorMismatch(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)
	const shortName = "test-storageclass"

	foreignManagedHost := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:            shortName + "-x-other-vcluster",
			ResourceVersion: syncertesting.FakeClientResourceVersion,
			Labels: map[string]string{
				translate.MarkerLabel: "other-vcluster",
			},
			Annotations: map[string]string{
				translate.NameAnnotation: shortName,
			},
		},
		Provisioner: "foreign-provisioner",
	}
	ownedShortVirtual := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:            shortName,
			ResourceVersion: syncertesting.FakeClientResourceVersion,
			Annotations: map[string]string{
				translate.ControllerLabel: "host-storageclass",
			},
		},
		Provisioner: "my-provisioner",
	}
	unownedShortVirtual := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:            shortName,
			ResourceVersion: syncertesting.FakeClientResourceVersion,
		},
		Provisioner: "my-provisioner",
	}
	requireSelector := func(vConfig *config.VirtualClusterConfig) {
		vConfig.Sync.FromHost.StorageClasses.Selector = vclusterconfig.StandardLabelSelector{
			MatchLabels: map[string]string{"sync": "true"},
		}
	}

	syncertesting.RunTests(t, []*syncertesting.SyncTest{
		{
			Name:                 "Preserve owned annotation-mapped virtual for foreign managed host failing selector",
			InitialPhysicalState: []runtime.Object{foreignManagedHost.DeepCopy()},
			InitialVirtualState:  []runtime.Object{ownedShortVirtual.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {foreignManagedHost},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {ownedShortVirtual},
			},
			AdjustConfig: requireSelector,
			Sync: func(ctx *synccontext.RegisterContext) {
				syncerCtx, syncer := newFakeSyncer(t, ctx)
				_, err := syncer.Sync(syncerCtx, synccontext.NewSyncEvent(foreignManagedHost.DeepCopy(), ownedShortVirtual.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Preserve unowned annotation-mapped virtual for foreign managed host failing selector",
			InitialPhysicalState: []runtime.Object{foreignManagedHost.DeepCopy()},
			InitialVirtualState:  []runtime.Object{unownedShortVirtual.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {foreignManagedHost},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {unownedShortVirtual},
			},
			AdjustConfig: requireSelector,
			Sync: func(ctx *synccontext.RegisterContext) {
				syncerCtx, syncer := newFakeSyncer(t, ctx)
				_, err := syncer.Sync(syncerCtx, synccontext.NewSyncEvent(foreignManagedHost.DeepCopy(), unownedShortVirtual.DeepCopy()))
				assert.NilError(t, err)
			},
		},
	})
}
