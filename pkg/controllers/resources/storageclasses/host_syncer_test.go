package storageclasses

import (
	"context"
	"errors"
	"testing"

	vclusterconfig "github.com/loft-sh/vcluster/config"
	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/pro"
	"github.com/loft-sh/vcluster/pkg/scheme"
	coresyncer "github.com/loft-sh/vcluster/pkg/syncer"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	syncertesting "github.com/loft-sh/vcluster/pkg/syncer/testing"
	testingutil "github.com/loft-sh/vcluster/pkg/util/testing"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	"gotest.tools/assert"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestFromHostSync(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)
	const storageClassName = "test-storageclass"
	const staleStorageClassName = "test-storageclass-x-other-vcluster"

	pObject := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:            storageClassName,
			ResourceVersion: syncertesting.FakeClientResourceVersion,
			Labels: map[string]string{
				"example.com/label-a": "test-1",
				"example.com/label-b": "test-2",
			},
			Annotations: map[string]string{
				"example.com/annotation-a": "test-1",
				"example.com/annotation-b": "test-2",
			},
		},
		Provisioner: "my-provisioner",
	}
	vObject := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:            storageClassName,
			ResourceVersion: syncertesting.FakeClientResourceVersion,
			Labels: map[string]string{
				"example.com/label-a": "test-1",
				"example.com/label-b": "test-2",
			},
			Annotations: map[string]string{
				"example.com/annotation-a": "test-1",
				"example.com/annotation-b": "test-2",
			},
		},
		Provisioner: "my-provisioner",
	}
	guestOnlyObject := vObject.DeepCopy()
	vObject.Annotations[translate.ControllerLabel] = "host-storageclass"
	nonMatchingHostObject := pObject.DeepCopy()
	nonMatchingHostObject.Labels = map[string]string{"sync": "false"}
	pObjectUpdated := pObject.DeepCopy()
	pObjectUpdated.Labels["example.com/label-c"] = "test-3"
	pObjectUpdated.Annotations["example.com/annotation-c"] = "test-3"
	pObjectUpdated.Parameters = map[string]string{
		"test": "value",
	}
	vObjectUpdated := vObject.DeepCopy()
	vObjectUpdated.Labels["example.com/label-c"] = "test-3"
	vObjectUpdated.Annotations["example.com/annotation-c"] = "test-3"
	vObjectUpdated.Parameters = map[string]string{
		"test": "value",
	}
	managedHostObject := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:            staleStorageClassName,
			UID:             types.UID("managed-host-storageclass-uid"),
			ResourceVersion: syncertesting.FakeClientResourceVersion,
			Labels: map[string]string{
				translate.MarkerLabel: "other-vcluster",
			},
			Annotations: map[string]string{
				translate.NameAnnotation: storageClassName,
			},
		},
		Provisioner: "stale-provisioner",
	}
	staleVirtualObject := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:            staleStorageClassName,
			ResourceVersion: syncertesting.FakeClientResourceVersion,
		},
		Provisioner: "stale-provisioner",
	}
	ownedStaleVirtualObject := staleVirtualObject.DeepCopy()
	markOwnedVirtual(ownedStaleVirtualObject, "host-storageclass")

	syncertesting.RunTests(t, []*syncertesting.SyncTest{
		{
			Name:                 "Ignore managed host resource before creating a virtual resource",
			InitialPhysicalState: []runtime.Object{managedHostObject.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {managedHostObject},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncerCtx, syncer := newFakeSyncer(t, ctx)
				_, err := syncer.SyncToVirtual(syncerCtx, synccontext.NewSyncToVirtualEvent(managedHostObject.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Preserve unowned same-name virtual resource for managed host object",
			InitialPhysicalState: []runtime.Object{managedHostObject.DeepCopy()},
			InitialVirtualState:  []runtime.Object{staleVirtualObject.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {managedHostObject},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {staleVirtualObject},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncerCtx, syncer := newFakeSyncer(t, ctx)
				_, err := syncer.Sync(syncerCtx, synccontext.NewSyncEvent(managedHostObject.DeepCopy(), staleVirtualObject.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Delete owned same-name virtual mirror without deleting managed host resource",
			InitialPhysicalState: []runtime.Object{managedHostObject.DeepCopy()},
			InitialVirtualState:  []runtime.Object{ownedStaleVirtualObject.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {managedHostObject},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncerCtx, syncer := newFakeSyncer(t, ctx)
				_, err := syncer.Sync(syncerCtx, synccontext.NewSyncEvent(managedHostObject.DeepCopy(), ownedStaleVirtualObject.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Do not delete or overwrite annotation-mapped virtual collision",
			InitialPhysicalState: []runtime.Object{managedHostObject.DeepCopy()},
			InitialVirtualState:  []runtime.Object{vObject.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {managedHostObject},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {vObject},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncerCtx, syncer := newFakeSyncer(t, ctx)
				_, err := syncer.Sync(syncerCtx, synccontext.NewSyncEvent(managedHostObject.DeepCopy(), vObject.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Sync new host resource to virtual",
			InitialPhysicalState: []runtime.Object{pObject.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {pObject},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {vObject},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncerCtx, syncer := newFakeSyncer(t, ctx)
				_, err := syncer.SyncToVirtual(syncerCtx, synccontext.NewSyncToVirtualEvent(pObject))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Sync host changes to virtual",
			InitialPhysicalState: []runtime.Object{pObjectUpdated.DeepCopy()}, // host resource has been updated
			InitialVirtualState:  []runtime.Object{vObject.DeepCopy()},        // virtual resource has old values
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {pObjectUpdated}, // host resource did not change
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {vObjectUpdated}, // virtual resource has been updated after syncing
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncerCtx, syncer := newFakeSyncer(t, ctx)
				_, err := syncer.Sync(syncerCtx, synccontext.NewSyncEvent(pObjectUpdated, vObject.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Preserve unowned virtual resource after host stops matching selector",
			InitialPhysicalState: []runtime.Object{nonMatchingHostObject.DeepCopy()},
			InitialVirtualState:  []runtime.Object{guestOnlyObject.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {nonMatchingHostObject},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {guestOnlyObject},
			},
			AdjustConfig: requireSyncLabel,
			Sync: func(ctx *synccontext.RegisterContext) {
				syncerCtx, syncer := newFakeSyncer(t, ctx)
				_, err := syncer.Sync(syncerCtx, synccontext.NewSyncEvent(nonMatchingHostObject.DeepCopy(), guestOnlyObject.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Delete owned virtual resource after host stops matching selector",
			InitialPhysicalState: []runtime.Object{nonMatchingHostObject.DeepCopy()},
			InitialVirtualState:  []runtime.Object{vObject.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {nonMatchingHostObject},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {},
			},
			AdjustConfig: requireSyncLabel,
			Sync: func(ctx *synccontext.RegisterContext) {
				syncerCtx, syncer := newFakeSyncer(t, ctx)
				_, err := syncer.Sync(syncerCtx, synccontext.NewSyncEvent(nonMatchingHostObject.DeepCopy(), vObject.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Preserve guest-only storage class when host resource is absent",
			InitialPhysicalState: []runtime.Object{},
			InitialVirtualState:  []runtime.Object{guestOnlyObject.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {guestOnlyObject},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncerCtx, syncer := newFakeSyncer(t, ctx)
				_, err := syncer.SyncToHost(syncerCtx, synccontext.NewSyncToHostEvent(guestOnlyObject.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Delete virtual resources after host resource has been deleted",
			InitialPhysicalState: []runtime.Object{},                   // host resource has been deleted
			InitialVirtualState:  []runtime.Object{vObject.DeepCopy()}, // virtual resource exists, since it was previously synced
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {},
			}, // virtual resource has been deleted after syncing
			Sync: func(ctx *synccontext.RegisterContext) {
				syncerCtx, syncer := newFakeSyncer(t, ctx)
				_, err := syncer.SyncToHost(syncerCtx, synccontext.NewSyncToHostEvent(vObject.DeepCopy()))
				assert.NilError(t, err)
			},
		},
	})
}

func TestHostStorageClassAdmission(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)
	ctx := syncertesting.NewFakeRegisterContext(testingutil.NewFakeConfig(), testingutil.NewFakeClient(scheme.Scheme), testingutil.NewFakeClient(scheme.Scheme))
	syncerCtx, syncer := newFakeSyncer(t, ctx)

	baseline := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "baseline"}}
	managed := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{
		Name: "baseline-x-other-vcluster",
		Labels: map[string]string{
			translate.MarkerLabel: "other-vcluster",
		},
		Annotations: map[string]string{
			translate.NameAnnotation: "baseline",
		},
	}}

	got, err := syncer.IsManaged(syncerCtx, baseline)
	assert.NilError(t, err)
	assert.Equal(t, got, true)

	got, err = syncer.IsManaged(syncerCtx, managed)
	assert.NilError(t, err)
	assert.Equal(t, got, true)

	baselineName := syncer.HostToVirtual(syncerCtx, types.NamespacedName{Name: baseline.Name}, baseline)
	assert.Equal(t, baselineName.Name, baseline.Name)

	managedName := syncer.HostToVirtual(syncerCtx, types.NamespacedName{Name: managed.Name}, managed)
	assert.Equal(t, managedName.Name, "")
}

func TestHostStorageClassReconcile(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)
	const baselineName = "baseline"
	const managedName = "baseline-x-other-vcluster"

	managedHost := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:            managedName,
			UID:             types.UID("managed-host-uid"),
			ResourceVersion: syncertesting.FakeClientResourceVersion,
			Labels: map[string]string{
				translate.MarkerLabel: "other-vcluster",
			},
			Annotations: map[string]string{
				translate.NameAnnotation: baselineName,
				translate.UIDAnnotation:  "old-guest-uid",
			},
		},
		Provisioner: "stale-provisioner",
	}
	staleVirtual := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:            managedName,
			UID:             types.UID("current-guest-uid"),
			ResourceVersion: syncertesting.FakeClientResourceVersion,
		},
		Provisioner: "stale-provisioner",
	}
	ownedStaleVirtual := staleVirtual.DeepCopy()
	markOwnedVirtual(ownedStaleVirtual, "host-storageclass")
	baselineHost := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:            baselineName,
			UID:             types.UID("baseline-host-uid"),
			ResourceVersion: syncertesting.FakeClientResourceVersion,
		},
		Provisioner: "baseline-provisioner",
		Parameters:  map[string]string{"type": "baseline"},
	}
	baselineVirtual := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:            baselineName,
			UID:             types.UID("baseline-guest-uid"),
			ResourceVersion: syncertesting.FakeClientResourceVersion,
		},
		Provisioner: "old-provisioner",
	}
	baselineVirtualUpdated := baselineHost.DeepCopy()
	baselineVirtualUpdated.UID = baselineVirtual.UID
	markOwnedVirtual(baselineVirtualUpdated, "host-storageclass")
	baselineVirtualCreated := baselineHost.DeepCopy()
	markOwnedVirtual(baselineVirtualCreated, "host-storageclass")

	syncertesting.RunTests(t, []*syncertesting.SyncTest{
		{
			Name:                 "Managed host same-name preserves unowned virtual object",
			InitialPhysicalState: []runtime.Object{managedHost.DeepCopy()},
			InitialVirtualState:  []runtime.Object{staleVirtual.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {managedHost},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {staleVirtual},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				reconcileHostStorageClass(t, ctx, managedName)
			},
		},
		{
			Name:                 "Managed host same-name deletes owned stale virtual object",
			InitialPhysicalState: []runtime.Object{managedHost.DeepCopy()},
			InitialVirtualState:  []runtime.Object{ownedStaleVirtual.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {managedHost},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				reconcileHostStorageClass(t, ctx, managedName)
			},
		},
		{
			Name:                 "Baseline host resource is created in virtual cluster",
			InitialPhysicalState: []runtime.Object{baselineHost.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {baselineHost},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {baselineVirtualCreated},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				reconcileHostStorageClass(t, ctx, baselineName)
			},
		},
		{
			Name:                 "Baseline host resource updates virtual cluster without deleting host",
			InitialPhysicalState: []runtime.Object{baselineHost.DeepCopy()},
			InitialVirtualState:  []runtime.Object{baselineVirtual.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {baselineHost},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {baselineVirtualUpdated},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				reconcileHostStorageClass(t, ctx, baselineName)
			},
		},
		{
			Name:                "Missing baseline host resource deletes virtual mirror",
			InitialVirtualState: []runtime.Object{baselineVirtualUpdated.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				reconcileHostStorageClass(t, ctx, baselineName)
			},
		},
	})
}

func TestFromHostReconcileAdoptsThenDeletes(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)
	const storageClassName = "legacy-unmarked-storageclass"
	host := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: storageClassName},
		Provisioner: "host-provisioner",
	}
	legacyVirtual := host.DeepCopy()

	syncertesting.RunTests(t, []*syncertesting.SyncTest{
		{
			Name:                 "Adopt unmarked host-backed virtual resource and preserve deletion propagation",
			InitialPhysicalState: []runtime.Object{host.DeepCopy()},
			InitialVirtualState:  []runtime.Object{legacyVirtual.DeepCopy()},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				storagev1.SchemeGroupVersion.WithKind("StorageClass"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				_, object := newFakeSyncer(t, ctx)
				controller, err := coresyncer.NewSyncController(ctx, object)
				assert.NilError(t, err)
				request := ctrl.Request{NamespacedName: types.NamespacedName{Name: storageClassName}}

				_, err = controller.Reconcile(ctx, request)
				assert.NilError(t, err)
				adopted := &storagev1.StorageClass{}
				err = ctx.VirtualManager.GetClient().Get(context.Background(), client.ObjectKey{Name: storageClassName}, adopted)
				assert.NilError(t, err)
				assert.Equal(t, adopted.Annotations[translate.ControllerLabel], object.Name())

				err = ctx.HostManager.GetClient().Delete(context.Background(), host.DeepCopy())
				assert.NilError(t, err)
				_, err = controller.Reconcile(ctx, request)
				assert.NilError(t, err)
			},
		},
	})
}

func reconcileHostStorageClass(t *testing.T, ctx *synccontext.RegisterContext, name string) {
	_, object := newFakeSyncer(t, ctx)
	controller, err := coresyncer.NewSyncController(ctx, object)
	assert.NilError(t, err)

	_, err = controller.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
	assert.NilError(t, err)
}

func TestFromHostSyncReassertsOwnershipAfterPatches(t *testing.T) {
	originalPatch := pro.ApplyPatchesVirtualObject
	pro.ApplyPatchesVirtualObject = func(_ *synccontext.SyncContext, _, obj, _ client.Object, _ []vclusterconfig.TranslatePatch, _ bool) error {
		storageClass := obj.(*storagev1.StorageClass)
		delete(storageClass.Annotations, translate.ControllerLabel)
		return nil
	}
	t.Cleanup(func() { pro.ApplyPatchesVirtualObject = originalPatch })

	host := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "patch-order-storageclass"},
		Provisioner: "host-provisioner",
	}
	virtual := host.DeepCopy()
	virtual.Provisioner = "old-provisioner"
	pClient := testingutil.NewFakeClient(scheme.Scheme, host.DeepCopy())
	vClient := testingutil.NewFakeClient(scheme.Scheme, virtual.DeepCopy())
	registerCtx := syncertesting.NewFakeRegisterContext(testingutil.NewFakeConfig(), pClient, vClient)
	syncCtx, syncer := newFakeSyncer(t, registerCtx)
	current := &storagev1.StorageClass{}
	assert.NilError(t, vClient.Get(context.Background(), client.ObjectKey{Name: virtual.Name}, current))

	_, err := syncer.Sync(syncCtx, synccontext.NewSyncEvent(host, current))
	assert.NilError(t, err)
	got := &storagev1.StorageClass{}
	assert.NilError(t, vClient.Get(context.Background(), client.ObjectKey{Name: virtual.Name}, got))
	assert.Equal(t, got.Annotations[translate.ControllerLabel], syncer.Name())
}

func TestFromHostSyncPatchErrorDoesNotPersistPartialState(t *testing.T) {
	injectedError := errors.New("injected patch failure")
	originalPatch := pro.ApplyPatchesVirtualObject
	pro.ApplyPatchesVirtualObject = func(_ *synccontext.SyncContext, _, obj, _ client.Object, _ []vclusterconfig.TranslatePatch, _ bool) error {
		storageClass := obj.(*storagev1.StorageClass)
		delete(storageClass.Annotations, translate.ControllerLabel)
		return injectedError
	}
	t.Cleanup(func() { pro.ApplyPatchesVirtualObject = originalPatch })

	host := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "patch-error-storageclass"},
		Provisioner: "new-host-provisioner",
	}
	virtual := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: host.Name,
			Annotations: map[string]string{
				translate.ControllerLabel: "host-storageclass",
			},
		},
		Provisioner: "old-virtual-provisioner",
	}
	pClient := testingutil.NewFakeClient(scheme.Scheme, host.DeepCopy())
	vClient := testingutil.NewFakeClient(scheme.Scheme, virtual.DeepCopy())
	registerCtx := syncertesting.NewFakeRegisterContext(testingutil.NewFakeConfig(), pClient, vClient)
	syncCtx, syncer := newFakeSyncer(t, registerCtx)
	current := &storagev1.StorageClass{}
	assert.NilError(t, vClient.Get(context.Background(), client.ObjectKey{Name: virtual.Name}, current))

	_, err := syncer.Sync(syncCtx, synccontext.NewSyncEvent(host, current))
	assert.ErrorContains(t, err, injectedError.Error())
	got := &storagev1.StorageClass{}
	assert.NilError(t, vClient.Get(context.Background(), client.ObjectKey{Name: virtual.Name}, got))
	assert.Equal(t, got.Annotations[translate.ControllerLabel], syncer.Name())
	assert.Equal(t, got.Provisioner, virtual.Provisioner)
}

func TestFromHostSyncPatchErrorDoesNotMutateAliasedHostFields(t *testing.T) {
	injectedError := errors.New("injected alias patch failure")
	originalPatch := pro.ApplyPatchesVirtualObject
	pro.ApplyPatchesVirtualObject = func(_ *synccontext.SyncContext, _, obj, _ client.Object, _ []vclusterconfig.TranslatePatch, _ bool) error {
		storageClass := obj.(*storagev1.StorageClass)
		storageClass.MountOptions[0] = "mutated-mount-option"
		*storageClass.ReclaimPolicy = corev1.PersistentVolumeReclaimRetain
		storageClass.AllowedTopologies[0].MatchLabelExpressions[0].Values[0] = "mutated-zone"
		return injectedError
	}
	t.Cleanup(func() { pro.ApplyPatchesVirtualObject = originalPatch })

	reclaimPolicy := corev1.PersistentVolumeReclaimDelete
	host := &storagev1.StorageClass{
		ObjectMeta:    metav1.ObjectMeta{Name: "patch-error-alias-storageclass"},
		Provisioner:   "host-provisioner",
		ReclaimPolicy: &reclaimPolicy,
		MountOptions:  []string{"original-mount-option"},
		AllowedTopologies: []corev1.TopologySelectorTerm{{
			MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{{
				Key:    "topology.kubernetes.io/zone",
				Values: []string{"original-zone"},
			}},
		}},
	}
	virtual := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        host.Name,
			Annotations: map[string]string{translate.ControllerLabel: "host-storageclass"},
		},
		Provisioner: "old-virtual-provisioner",
	}
	pClient := testingutil.NewFakeClient(scheme.Scheme, host.DeepCopy())
	vClient := testingutil.NewFakeClient(scheme.Scheme, virtual.DeepCopy())
	registerCtx := syncertesting.NewFakeRegisterContext(testingutil.NewFakeConfig(), pClient, vClient)
	syncCtx, syncer := newFakeSyncer(t, registerCtx)
	currentHost := &storagev1.StorageClass{}
	assert.NilError(t, pClient.Get(context.Background(), client.ObjectKey{Name: host.Name}, currentHost))
	currentVirtual := &storagev1.StorageClass{}
	assert.NilError(t, vClient.Get(context.Background(), client.ObjectKey{Name: virtual.Name}, currentVirtual))

	_, err := syncer.Sync(syncCtx, synccontext.NewSyncEvent(currentHost, currentVirtual))
	assert.ErrorContains(t, err, injectedError.Error())
	assert.Equal(t, currentHost.MountOptions[0], "original-mount-option")
	assert.Equal(t, *currentHost.ReclaimPolicy, corev1.PersistentVolumeReclaimDelete)
	assert.Equal(t, currentHost.AllowedTopologies[0].MatchLabelExpressions[0].Values[0], "original-zone")

	gotHost := &storagev1.StorageClass{}
	assert.NilError(t, pClient.Get(context.Background(), client.ObjectKey{Name: host.Name}, gotHost))
	assert.Equal(t, gotHost.MountOptions[0], "original-mount-option")
	assert.Equal(t, *gotHost.ReclaimPolicy, corev1.PersistentVolumeReclaimDelete)
	assert.Equal(t, gotHost.AllowedTopologies[0].MatchLabelExpressions[0].Values[0], "original-zone")
	gotVirtual := &storagev1.StorageClass{}
	assert.NilError(t, vClient.Get(context.Background(), client.ObjectKey{Name: virtual.Name}, gotVirtual))
	assert.Equal(t, gotVirtual.Provisioner, virtual.Provisioner)
	assert.DeepEqual(t, gotVirtual.MountOptions, virtual.MountOptions)
	assert.DeepEqual(t, gotVirtual.ReclaimPolicy, virtual.ReclaimPolicy)
	assert.DeepEqual(t, gotVirtual.AllowedTopologies, virtual.AllowedTopologies)
}

func TestFromHostSyncToHostPreservesReplacedVirtualStorageClass(t *testing.T) {
	tests := []struct {
		name         string
		currentUID   types.UID
		currentOwner string
	}{
		{
			name:         "owner removed from current object",
			currentUID:   types.UID("old-owned-uid"),
			currentOwner: "",
		},
		{
			name:         "same-name owned object was recreated with a new UID",
			currentUID:   types.UID("new-owned-uid"),
			currentOwner: "host-storageclass",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stale := &storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "replaced-storageclass",
					UID:         types.UID("old-owned-uid"),
					Annotations: map[string]string{translate.ControllerLabel: "host-storageclass"},
				},
			}
			current := stale.DeepCopy()
			current.UID = tt.currentUID
			if tt.currentOwner == "" {
				current.Annotations = nil
			} else {
				current.Annotations[translate.ControllerLabel] = tt.currentOwner
			}

			pClient := testingutil.NewFakeClient(scheme.Scheme)
			vClient := testingutil.NewFakeClient(scheme.Scheme, current.DeepCopy())
			registerCtx := syncertesting.NewFakeRegisterContext(testingutil.NewFakeConfig(), pClient, vClient)
			syncCtx, syncer := newFakeSyncer(t, registerCtx)

			_, err := syncer.SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(stale))
			assert.NilError(t, err)
			got := &storagev1.StorageClass{}
			assert.NilError(t, vClient.Get(context.Background(), client.ObjectKey{Name: current.Name}, got))
			assert.Equal(t, got.UID, current.UID)
			assert.Equal(t, got.Annotations[translate.ControllerLabel], tt.currentOwner)
		})
	}
}

func TestManagedHostSyncPreservesReplacedVirtualStorageClass(t *testing.T) {
	managedHost := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "managed-replaced-storageclass",
			Labels: map[string]string{translate.MarkerLabel: "other-vcluster"},
		},
	}
	staleOwned := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        managedHost.Name,
			UID:         types.UID("old-owned-uid"),
			Annotations: map[string]string{translate.ControllerLabel: "host-storageclass"},
		},
	}
	currentUnowned := staleOwned.DeepCopy()
	currentUnowned.UID = types.UID("new-unowned-uid")
	currentUnowned.Annotations = nil

	pClient := testingutil.NewFakeClient(scheme.Scheme, managedHost.DeepCopy())
	vClient := testingutil.NewFakeClient(scheme.Scheme, currentUnowned.DeepCopy())
	registerCtx := syncertesting.NewFakeRegisterContext(testingutil.NewFakeConfig(), pClient, vClient)
	syncCtx, syncer := newFakeSyncer(t, registerCtx)

	_, err := syncer.Sync(syncCtx, synccontext.NewSyncEvent(managedHost, staleOwned))
	assert.NilError(t, err)
	got := &storagev1.StorageClass{}
	assert.NilError(t, vClient.Get(context.Background(), client.ObjectKey{Name: currentUnowned.Name}, got))
	assert.Equal(t, got.UID, currentUnowned.UID)
	assert.Equal(t, got.Annotations[translate.ControllerLabel], "")
}

func requireSyncLabel(vConfig *config.VirtualClusterConfig) {
	vConfig.Sync.FromHost.StorageClasses.Selector = vclusterconfig.StandardLabelSelector{
		MatchLabels: map[string]string{"sync": "true"},
	}
}

func newFakeSyncer(t *testing.T, ctx *synccontext.RegisterContext) (*synccontext.SyncContext, *hostStorageClassSyncer) {
	syncContext, object := syncertesting.FakeStartSyncer(t, ctx, NewHostStorageClassSyncer)
	return syncContext, object.(*hostStorageClassSyncer)
}
