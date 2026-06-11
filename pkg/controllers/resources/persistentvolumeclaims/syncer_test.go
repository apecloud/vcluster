package persistentvolumeclaims

import (
	"testing"
	"time"

	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	syncertesting "github.com/loft-sh/vcluster/pkg/syncer/testing"
	testingutil "github.com/loft-sh/vcluster/pkg/util/testing"
	"gotest.tools/assert"
	"k8s.io/apimachinery/pkg/types"

	"github.com/loft-sh/vcluster/pkg/util/translate"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestSync(t *testing.T) {
	vObjectMeta := metav1.ObjectMeta{
		Name:      "testpvc",
		Namespace: "testns",
	}
	pObjectMeta := metav1.ObjectMeta{
		Name:      translate.Default.HostName(nil, "testpvc", "testns").Name,
		Namespace: "test",
		Annotations: map[string]string{
			translate.NameAnnotation:          vObjectMeta.Name,
			translate.NamespaceAnnotation:     vObjectMeta.Namespace,
			translate.UIDAnnotation:           "",
			translate.KindAnnotation:          corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim").String(),
			translate.HostNamespaceAnnotation: "test",
			translate.HostNameAnnotation:      translate.Default.HostName(nil, "testpvc", "testns").Name,
		},
		Labels: map[string]string{
			translate.MarkerLabel:    translate.VClusterName,
			translate.NamespaceLabel: vObjectMeta.Namespace,
		},
	}
	changedResources := corev1.VolumeResourceRequirements{
		Requests: map[corev1.ResourceName]resource.Quantity{
			"storage": {
				Format: "teststoragerequest",
			},
		},
	}
	basePvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: vObjectMeta,
	}
	createdPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: pObjectMeta,
	}
	deletePvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:              vObjectMeta.Name,
			Namespace:         vObjectMeta.Namespace,
			Finalizers:        []string{"kubernetes"},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
	}
	updatePvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vObjectMeta.Name,
			Namespace: vObjectMeta.Namespace,
			Annotations: map[string]string{
				"otherAnnotationKey": "update this",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: changedResources,
		},
	}
	updatedPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pObjectMeta.Name,
			Namespace: pObjectMeta.Namespace,
			Annotations: map[string]string{
				translate.NameAnnotation:          vObjectMeta.Name,
				translate.NamespaceAnnotation:     vObjectMeta.Namespace,
				translate.UIDAnnotation:           "",
				translate.KindAnnotation:          corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim").String(),
				translate.HostNamespaceAnnotation: pObjectMeta.Namespace,
				translate.HostNameAnnotation:      pObjectMeta.Name,
				"otherAnnotationKey":              "update this",
			},
			Labels: pObjectMeta.Labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: changedResources,
		},
	}
	backwardUpdateAnnotationsPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pObjectMeta.Name,
			Namespace: pObjectMeta.Namespace,
			Annotations: map[string]string{
				translate.NameAnnotation:          vObjectMeta.Name,
				translate.NamespaceAnnotation:     vObjectMeta.Namespace,
				translate.UIDAnnotation:           "",
				translate.KindAnnotation:          corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim").String(),
				translate.HostNameAnnotation:      pObjectMeta.Name,
				translate.HostNamespaceAnnotation: pObjectMeta.Namespace,
				bindCompletedAnnotation:           "testannotation",
				boundByControllerAnnotation:       "testannotation2",
				storageProvisionerAnnotation:      "testannotation3",
			},
			Labels: pObjectMeta.Labels,
		},
	}
	backwardUpdatedAnnotationsPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vObjectMeta.Name,
			Namespace: vObjectMeta.Namespace,
			Annotations: map[string]string{
				bindCompletedAnnotation:      "testannotation",
				boundByControllerAnnotation:  "testannotation2",
				storageProvisionerAnnotation: "testannotation3",
			},
		},
	}
	backwardUpdateStatusPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: pObjectMeta,
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "myvolume",
		},
		Status: corev1.PersistentVolumeClaimStatus{
			AccessModes: []corev1.PersistentVolumeAccessMode{"testmode"},
		},
	}
	backwardUpdatedStatusPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: vObjectMeta,
		Spec:       backwardUpdateStatusPvc.Spec,
		Status:     backwardUpdateStatusPvc.Status,
	}
	backwardUpdateVolumeNameOnlyPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: vObjectMeta,
		Spec:       backwardUpdateStatusPvc.Spec,
	}
	dataProtectionGroup := dataProtectionAPIGroup
	dataProtectionBackupPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vObjectMeta.Name,
			Namespace: vObjectMeta.Namespace,
			UID:       types.UID("target-pvc-uid"),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "restore-populated-pv",
			DataSourceRef: &corev1.TypedObjectReference{
				APIGroup: &dataProtectionGroup,
				Kind:     dataProtectionBackupKind,
				Name:     "backup-1",
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
		},
	}
	dataProtectionBackupPendingPvc := dataProtectionBackupPvc.DeepCopy()
	dataProtectionBackupPendingPvc.Spec.VolumeName = ""
	dataProtectionBackupPendingPvc.Status = corev1.PersistentVolumeClaimStatus{
		Phase: corev1.ClaimPending,
	}
	dataProtectionBackupPendingPvcWithVolumeName := dataProtectionBackupPendingPvc.DeepCopy()
	dataProtectionBackupPendingPvcWithVolumeName.Spec.VolumeName = "restore-populated-pv"
	dataProtectionBackupPendingPvcWithVolumeNameBoundStatus := dataProtectionBackupPendingPvcWithVolumeName.DeepCopy()
	dataProtectionBackupPendingPvcWithVolumeNameBoundStatus.Status = corev1.PersistentVolumeClaimStatus{
		Phase:       corev1.ClaimBound,
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		Capacity: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("1Gi"),
		},
	}
	dataProtectionHostPendingPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: pObjectMeta,
		Status: corev1.PersistentVolumeClaimStatus{
			Phase:    corev1.ClaimPending,
			Capacity: corev1.ResourceList{},
		},
	}
	dataProtectionHostPendingPvcWithUID := dataProtectionHostPendingPvc.DeepCopy()
	dataProtectionHostPendingPvcWithUID.Annotations[translate.UIDAnnotation] = string(dataProtectionBackupPvc.UID)
	dataProtectionPopulatedPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "restore-populated-pv",
			Annotations: map[string]string{
				dataProtectionPopulateFromAnnotation: "backup-1",
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			ClaimRef: &corev1.ObjectReference{
				Namespace: dataProtectionBackupPvc.Namespace,
				Name:      dataProtectionBackupPvc.Name,
				UID:       dataProtectionBackupPvc.UID,
			},
		},
		Status: corev1.PersistentVolumeStatus{
			Phase: corev1.VolumeBound,
		},
	}
	dataProtectionStaleUIDPvc := dataProtectionBackupPvc.DeepCopy()
	dataProtectionStaleUIDPvc.Status = *dataProtectionHostPendingPvc.Status.DeepCopy()
	dataProtectionStaleUIDPV := dataProtectionPopulatedPV.DeepCopy()
	dataProtectionStaleUIDPV.Spec.ClaimRef.UID = types.UID("stale-pvc-uid")
	dataProtectionStaleUIDPendingPvc := dataProtectionBackupPendingPvc.DeepCopy()
	dataProtectionMaterializationRequestCM := dataProtectionMaterializationRequest("test", dataProtectionHostPendingPvc, dataProtectionBackupPvc, dataProtectionPopulatedPV)
	dataProtectionNoDataRestorePvc := dataProtectionBackupPvc.DeepCopy()
	dataProtectionNoDataRestorePvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
		{
			Type:   corev1.PersistentVolumeClaimConditionType("Restore"),
			Status: corev1.ConditionTrue,
			Reason: dataProtectionRestoreConditionReasonProvisioned,
		},
	}
	dataProtectionNoDataHostPvc := dataProtectionHostPendingPvcWithUID.DeepCopy()
	dataProtectionNoDataHostPvc.Spec = corev1.PersistentVolumeClaimSpec{}
	dataProtectionNoDataHostPvc.Status = dataProtectionNoDataRestorePvc.Status

	dataProtectionPopulateHelperPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kb-populate-target-pvc-uid",
			Namespace: dataProtectionBackupPvc.Namespace,
			UID:       types.UID("populate-helper-pvc-uid"),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: dataProtectionPopulatedPV.Name,
		},
	}
	dataProtectionHostPopulateHelperPvcName := translate.Default.HostName(nil, dataProtectionPopulateHelperPvc.Name, dataProtectionPopulateHelperPvc.Namespace)
	dataProtectionHostPopulateHelperPvcName.Namespace = pObjectMeta.Namespace
	dataProtectionHostPopulateHelperPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dataProtectionHostPopulateHelperPvcName.Name,
			Namespace: dataProtectionHostPopulateHelperPvcName.Namespace,
			UID:       types.UID("host-populate-helper-pvc-uid"),
			Annotations: map[string]string{
				translate.NameAnnotation:          dataProtectionPopulateHelperPvc.Name,
				translate.NamespaceAnnotation:     dataProtectionPopulateHelperPvc.Namespace,
				translate.UIDAnnotation:           string(dataProtectionPopulateHelperPvc.UID),
				translate.KindAnnotation:          corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim").String(),
				translate.HostNamespaceAnnotation: dataProtectionHostPopulateHelperPvcName.Namespace,
				translate.HostNameAnnotation:      dataProtectionHostPopulateHelperPvcName.Name,
			},
			Labels: map[string]string{
				translate.MarkerLabel:    translate.VClusterName,
				translate.NamespaceLabel: dataProtectionPopulateHelperPvc.Namespace,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: dataProtectionPopulatedPV.Name,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
		},
	}
	dataProtectionHostPVBoundToHelper := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: dataProtectionPopulatedPV.Name,
		},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				APIVersion: corev1.SchemeGroupVersion.Version,
				Kind:       "PersistentVolumeClaim",
				Namespace:  dataProtectionHostPopulateHelperPvc.Namespace,
				Name:       dataProtectionHostPopulateHelperPvc.Name,
				UID:        dataProtectionHostPopulateHelperPvc.UID,
			},
		},
		Status: corev1.PersistentVolumeStatus{
			Phase: corev1.VolumeBound,
		},
	}
	dataProtectionHostPVBoundToTarget := dataProtectionHostPVBoundToHelper.DeepCopy()
	dataProtectionHostPVBoundToTarget.Spec.ClaimRef = &corev1.ObjectReference{
		APIVersion: corev1.SchemeGroupVersion.Version,
		Kind:       "PersistentVolumeClaim",
		Namespace:  dataProtectionHostPendingPvcWithUID.Namespace,
		Name:       dataProtectionHostPendingPvcWithUID.Name,
	}
	dataProtectionHostMaterializedTargetPvc := dataProtectionHostPendingPvcWithUID.DeepCopy()
	dataProtectionHostMaterializedTargetPvc.Spec.VolumeName = dataProtectionPopulatedPV.Name

	syncertesting.RunTestsWithContext(t, func(vConfig *config.VirtualClusterConfig, pClient *testingutil.FakeIndexClient, vClient *testingutil.FakeIndexClient) *synccontext.RegisterContext {
		ctx := syncertesting.NewFakeRegisterContext(vConfig, pClient, vClient)
		ctx.Config.Sync.ToHost.StorageClasses.Enabled = false
		return ctx
	}, []*syncertesting.SyncTest{
		{
			Name:                "Create forward",
			InitialVirtualState: []runtime.Object{basePvc},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {basePvc},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {createdPvc},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*persistentVolumeClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(basePvc.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                "Create data protection no-data restore forward without host backup data source",
			InitialVirtualState: []runtime.Object{dataProtectionNoDataRestorePvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestorePvc.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataHostPvc.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(dataProtectionNoDataRestorePvc.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Delete forward with create function",
			InitialVirtualState:  []runtime.Object{basePvc},
			InitialPhysicalState: []runtime.Object{createdPvc},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {createdPvc},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*persistentVolumeClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(deletePvc.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Update forward",
			InitialVirtualState:  []runtime.Object{updatePvc},
			InitialPhysicalState: []runtime.Object{createdPvc},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {updatePvc},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {updatedPvc},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)

				pObjOld := createdPvc.DeepCopy()
				pObj := createdPvc.DeepCopy()

				vObjOld := updatePvc.DeepCopy()
				vObjOld.ObjectMeta.SetAnnotations(nil)
				vObj := updatePvc.DeepCopy()

				_, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					pObjOld,
					pObj,
					vObjOld,
					vObj,
				))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Update forward not needed",
			InitialVirtualState:  []runtime.Object{basePvc},
			InitialPhysicalState: []runtime.Object{createdPvc},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {basePvc},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {createdPvc},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					createdPvc,
					createdPvc.DeepCopy(),
					basePvc,
					basePvc.DeepCopy(),
				))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Delete forward with update function",
			InitialVirtualState:  []runtime.Object{basePvc},
			InitialPhysicalState: []runtime.Object{createdPvc},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {basePvc},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEvent(createdPvc.DeepCopy(), deletePvc.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Update backwards new annotations",
			InitialVirtualState:  []runtime.Object{basePvc},
			InitialPhysicalState: []runtime.Object{backwardUpdateAnnotationsPvc},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {backwardUpdatedAnnotationsPvc},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {backwardUpdateAnnotationsPvc},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				pObjOld := backwardUpdateAnnotationsPvc
				pObj := backwardUpdateAnnotationsPvc.DeepCopy()

				vObjOld := basePvc
				vObj := basePvc.DeepCopy()

				_, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					pObjOld,
					pObj,
					vObjOld,
					vObj,
				))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Requeue after updating virtual pvc volume name from host",
			InitialVirtualState:  []runtime.Object{basePvc.DeepCopy()},
			InitialPhysicalState: []runtime.Object{backwardUpdateStatusPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {backwardUpdateVolumeNameOnlyPvc.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {backwardUpdateStatusPvc.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					backwardUpdateStatusPvc.DeepCopy(),
					backwardUpdateStatusPvc.DeepCopy(),
					basePvc.DeepCopy(),
					basePvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.Requeue)
			},
		},
		{
			Name:                 "Update backwards new status",
			InitialVirtualState:  []runtime.Object{backwardUpdateVolumeNameOnlyPvc.DeepCopy()},
			InitialPhysicalState: []runtime.Object{backwardUpdateStatusPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {backwardUpdatedStatusPvc.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {backwardUpdateStatusPvc.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				pObjOld := backwardUpdateStatusPvc.DeepCopy()
				pObj := backwardUpdateStatusPvc.DeepCopy()
				vObjOld := backwardUpdateVolumeNameOnlyPvc.DeepCopy()
				vObj := backwardUpdateVolumeNameOnlyPvc.DeepCopy()

				_, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					pObjOld,
					pObj,
					vObjOld,
					vObj,
				))
				assert.NilError(t, err)
			},
		},
		{
			Name: "Preserve data protection populated virtual status while host pvc waits for volume",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPvc.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{dataProtectionHostPendingPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionBackupPvc.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionHostPendingPvcWithUID.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("ConfigMap"):             {dataProtectionMaterializationRequestCM.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionBackupPvc.DeepCopy(),
					dataProtectionBackupPvc.DeepCopy(),
				))
				assert.NilError(t, err)
			},
		},
		{
			Name: "Bridge data protection populated host pv from helper pvc to target pvc",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPvc.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
				dataProtectionPopulateHelperPvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostPendingPvc.DeepCopy(),
				dataProtectionHostPopulateHelperPvc.DeepCopy(),
				dataProtectionHostPVBoundToHelper.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionBackupPvc.DeepCopy(),
					dataProtectionPopulateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostMaterializedTargetPvc.DeepCopy(),
					dataProtectionHostPopulateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionHostPVBoundToTarget.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionBackupPvc.DeepCopy(),
					dataProtectionBackupPvc.DeepCopy(),
				))
				assert.NilError(t, err)
			},
		},
		{
			Name: "Requeue after deriving data protection pvc volume name from populated pv claim ref",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvc.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{dataProtectionHostPendingPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionBackupPendingPvcWithVolumeName.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionHostPendingPvc.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionBackupPendingPvc.DeepCopy(),
					dataProtectionBackupPendingPvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.Requeue)
			},
		},
		{
			Name: "Derive data protection populated virtual status from bound populated pv while host pvc waits",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{dataProtectionHostPendingPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionBackupPendingPvcWithVolumeNameBoundStatus.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionHostPendingPvcWithUID.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("ConfigMap"):             {dataProtectionMaterializationRequestCM.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				))
				assert.NilError(t, err)
			},
		},
		{
			Name: "Do not derive data protection pvc volume name from stale same-name claim ref uid",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvc.DeepCopy(),
				dataProtectionStaleUIDPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{dataProtectionHostPendingPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionStaleUIDPendingPvc.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionStaleUIDPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionHostPendingPvcWithUID.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionBackupPendingPvc.DeepCopy(),
					dataProtectionBackupPendingPvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, !result.Requeue)
			},
		},
		{
			Name: "Do not preserve data protection populated virtual status for stale same-name claim ref uid",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPvc.DeepCopy(),
				dataProtectionStaleUIDPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{dataProtectionHostPendingPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionStaleUIDPvc.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionStaleUIDPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionHostPendingPvcWithUID.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionBackupPvc.DeepCopy(),
					dataProtectionBackupPvc.DeepCopy(),
				))
				assert.NilError(t, err)
			},
		},
		{
			Name: "Recreate pvc if volume name is different",
			InitialVirtualState: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: basePvc.ObjectMeta,
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName: "test",
					},
				},
			},
			InitialPhysicalState: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: pObjectMeta,
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName: "test2",
					},
				},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					&corev1.PersistentVolumeClaim{
						ObjectMeta: basePvc.ObjectMeta,
						Spec: corev1.PersistentVolumeClaimSpec{
							VolumeName: "test2",
						},
					},
				},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					&corev1.PersistentVolumeClaim{
						ObjectMeta: pObjectMeta,
						Spec: corev1.PersistentVolumeClaimSpec{
							VolumeName: "test2",
						},
					},
				},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				vPVC := &corev1.PersistentVolumeClaim{}
				err := syncCtx.VirtualClient.Get(syncCtx, types.NamespacedName{
					Namespace: basePvc.Namespace,
					Name:      basePvc.Name,
				}, vPVC)
				assert.NilError(t, err)

				pPVC := &corev1.PersistentVolumeClaim{}
				err = syncCtx.HostClient.Get(syncCtx, types.NamespacedName{
					Namespace: pObjectMeta.Namespace,
					Name:      pObjectMeta.Name,
				}, pPVC)
				assert.NilError(t, err)

				_, err = syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEvent(pPVC.DeepCopy(), vPVC.DeepCopy()))
				assert.NilError(t, err)
			},
		},
	})
}
