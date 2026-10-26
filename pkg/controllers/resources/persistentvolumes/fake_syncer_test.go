package persistentvolumes

import (
	"testing"

	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	syncertesting "github.com/loft-sh/vcluster/pkg/syncer/testing"
	"gotest.tools/assert"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/loft-sh/vcluster/pkg/constants"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

func newFakeFakeSyncer(t *testing.T, ctx *synccontext.RegisterContext) (*synccontext.SyncContext, *fakePersistentVolumeSyncer) {
	err := ctx.VirtualManager.GetFieldIndexer().IndexField(ctx, &corev1.PersistentVolumeClaim{}, constants.IndexByAssigned, func(rawObj client.Object) []string {
		pod := rawObj.(*corev1.PersistentVolumeClaim)
		return []string{pod.Spec.VolumeName}
	})
	assert.NilError(t, err)

	syncContext, object := syncertesting.FakeStartSyncer(t, ctx, NewFakeSyncer)
	return syncContext, object.(*fakePersistentVolumeSyncer)
}

func TestFakeSync(t *testing.T) {
	basePvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "testpvc",
			Namespace:       "testns",
			ResourceVersion: syncertesting.FakeClientResourceVersion,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "mypv",
			StorageClassName: stringPointer("mystorageclass"),
		},
	}
	basePvName := types.NamespacedName{
		Name:      "mypv",
		Namespace: "testns",
	}
	basePv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: basePvName.Name,
			Labels: map[string]string{
				"vcluster.loft.sh/fake-pv": "true",
			},
			Annotations: map[string]string{
				"kubernetes.io/createdby":              "fake-pv-provisioner",
				"pv.kubernetes.io/bound-by-controller": "true",
				"pv.kubernetes.io/provisioned-by":      "fake-pv-provisioner",
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				FlexVolume: &corev1.FlexPersistentVolumeSource{
					Driver: "fake",
				},
			},
			Capacity:    basePvc.Spec.Resources.Requests,
			AccessModes: basePvc.Spec.AccessModes,
			ClaimRef: &corev1.ObjectReference{
				Kind:            "PersistentVolumeClaim",
				Namespace:       basePvc.Namespace,
				Name:            basePvc.Name,
				UID:             basePvc.UID,
				APIVersion:      corev1.SchemeGroupVersion.Version,
				ResourceVersion: basePvc.ResourceVersion,
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              *basePvc.Spec.StorageClassName,
			VolumeMode:                    basePvc.Spec.VolumeMode,
		},
		Status: corev1.PersistentVolumeStatus{
			Phase: corev1.VolumeBound,
		},
	}
	pvWithFinalizers := basePv.DeepCopy()
	pvWithFinalizers.Finalizers = []string{"myfinalizer"}
	// a restore target PVC whose volumeName is not derived yet, while the fake
	// PV's claimRef was already re-pointed to it by an external populator
	rePointedTargetPvc := basePvc.DeepCopy()
	rePointedTargetPvc.Spec.VolumeName = ""

	smallStorage := corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("20Gi")}
	largeStorage := corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("21Gi")}
	pv20Gi := basePv.DeepCopy()
	pv20Gi.Spec.Capacity = smallStorage.DeepCopy()
	pv21Gi := basePv.DeepCopy()
	pv21Gi.Spec.Capacity = largeStorage.DeepCopy()
	expandedPvc := basePvc.DeepCopy()
	expandedPvc.Spec.Resources.Requests = largeStorage.DeepCopy()
	expandedPvc.Status.Phase = corev1.ClaimBound
	expandedPvc.Status.Capacity = largeStorage.DeepCopy()
	expandingPvc := expandedPvc.DeepCopy()
	expandingPvc.Status.Capacity = smallStorage.DeepCopy()
	shrunkStatusPvc := expandedPvc.DeepCopy()
	shrunkStatusPvc.Status.Capacity = smallStorage.DeepCopy()
	realPv20Gi := pv20Gi.DeepCopy()
	delete(realPv20Gi.Labels, fakePVLabel)

	syncertesting.RunTests(t, []*syncertesting.SyncTest{
		{
			Name:                "Grow fake pv capacity after bound pvc expansion completes",
			InitialVirtualState: []runtime.Object{pv20Gi, expandedPvc},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {pv21Gi},
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {expandedPvc},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncContext, syncer := newFakeFakeSyncer(t, ctx)
				_, err := syncer.FakeSync(syncContext, pv20Gi.DeepCopy())
				assert.NilError(t, err)
			},
		},
		{
			Name:                "Keep fake pv capacity while pvc expansion is only requested",
			InitialVirtualState: []runtime.Object{pv20Gi, expandingPvc},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {pv20Gi},
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {expandingPvc},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncContext, syncer := newFakeFakeSyncer(t, ctx)
				_, err := syncer.FakeSync(syncContext, pv20Gi.DeepCopy())
				assert.NilError(t, err)
			},
		},
		{
			Name:                "Never shrink fake pv capacity",
			InitialVirtualState: []runtime.Object{pv21Gi, shrunkStatusPvc},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {pv21Gi},
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {shrunkStatusPvc},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncContext, syncer := newFakeFakeSyncer(t, ctx)
				_, err := syncer.FakeSync(syncContext, pv21Gi.DeepCopy())
				assert.NilError(t, err)
			},
		},
		{
			Name:                "Leave capacity of pv without fake label untouched",
			InitialVirtualState: []runtime.Object{realPv20Gi, expandedPvc},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {realPv20Gi},
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {expandedPvc},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncContext, syncer := newFakeFakeSyncer(t, ctx)
				_, err := syncer.FakeSync(syncContext, realPv20Gi.DeepCopy())
				assert.NilError(t, err)
			},
		},
		{
			Name:                "Keep pv while claim ref references live pvc without volume name",
			InitialVirtualState: []runtime.Object{basePv, rePointedTargetPvc},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {basePv},
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {rePointedTargetPvc},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncContext, syncer := newFakeFakeSyncer(t, ctx)
				_, err := syncer.FakeSync(syncContext, basePv.DeepCopy())
				assert.NilError(t, err)
			},
		},
		{
			Name:                "Create",
			InitialVirtualState: []runtime.Object{basePvc},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {basePv},
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {basePvc},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncContext, syncer := newFakeFakeSyncer(t, ctx)
				_, err := syncer.FakeSyncToVirtual(syncContext, basePvName)
				assert.NilError(t, err)
			},
		},
		{
			Name:                "Create not needed",
			InitialVirtualState: []runtime.Object{},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {},
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncContext, syncer := newFakeFakeSyncer(t, ctx)
				_, err := syncer.FakeSyncToVirtual(syncContext, basePvName)
				assert.NilError(t, err)
			},
		},
		{
			Name:                "Delete",
			InitialVirtualState: []runtime.Object{pvWithFinalizers},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {},
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncContext, syncer := newFakeFakeSyncer(t, ctx)
				_, err := syncer.FakeSync(syncContext, pvWithFinalizers)
				assert.NilError(t, err)
			},
		},
		{
			Name:                "Delete not existent pv",
			InitialVirtualState: []runtime.Object{},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {},
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncContext, syncer := newFakeFakeSyncer(t, ctx)
				_, err := syncer.FakeSync(syncContext, basePv)
				assert.NilError(t, err)
			},
		},
	})
}

func stringPointer(str string) *string {
	return &str
}
