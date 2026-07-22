package persistentvolumeclaims

import (
	"strings"
	"testing"
	"time"

	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	syncertesting "github.com/loft-sh/vcluster/pkg/syncer/testing"
	syncertypes "github.com/loft-sh/vcluster/pkg/syncer/types"
	testingutil "github.com/loft-sh/vcluster/pkg/util/testing"
	"gotest.tools/assert"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"

	"github.com/loft-sh/vcluster/pkg/util/translate"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type eventRecordingTranslator struct {
	syncertypes.GenericTranslator
	recorder events.EventRecorder
}

func (t *eventRecordingTranslator) EventRecorder() events.EventRecorder {
	return t.recorder
}

func installPVCEventRecorder(syncer *persistentVolumeClaimSyncer) *events.FakeRecorder {
	recorder := events.NewFakeRecorder(4)
	syncer.GenericTranslator = &eventRecordingTranslator{
		GenericTranslator: syncer.GenericTranslator,
		recorder:          recorder,
	}
	return recorder
}

func assertSinglePVCEvent(t *testing.T, recorder *events.FakeRecorder, fragments ...string) {
	t.Helper()

	var event string
	select {
	case event = <-recorder.Events:
	default:
		t.Error("expected one PVC event, got none")
		return
	}
	for _, fragment := range fragments {
		assert.Check(t, strings.Contains(event, fragment), "event %q does not contain %q", event, fragment)
	}
	select {
	case extra := <-recorder.Events:
		t.Errorf("expected one PVC event, got extra event %q", extra)
	default:
	}
}

func assertNoPVCEvent(t *testing.T, recorder *events.FakeRecorder) {
	t.Helper()

	select {
	case event := <-recorder.Events:
		t.Errorf("expected no PVC event, got %q", event)
	default:
	}
}

func checkExternalPopulatorHandoffPending(
	t *testing.T,
	ctx *synccontext.SyncContext,
	hostTarget, virtualTarget, expectedClaimRef types.NamespacedName,
	hostPVName string,
) {
	t.Helper()

	actualHostTarget := &corev1.PersistentVolumeClaim{}
	if err := ctx.HostClient.Get(ctx.Context, hostTarget, actualHostTarget); err != nil {
		t.Errorf("get host target PVC after handoff: %v", err)
	} else if actualHostTarget.Spec.VolumeName != "" {
		t.Errorf("host target PVC volumeName changed before topology convergence: %q", actualHostTarget.Spec.VolumeName)
	}

	actualHostPV := &corev1.PersistentVolume{}
	if err := ctx.HostClient.Get(ctx.Context, types.NamespacedName{Name: hostPVName}, actualHostPV); err != nil {
		t.Errorf("get host PV after handoff: %v", err)
	} else if actualHostPV.Spec.ClaimRef == nil {
		t.Error("host PV claimRef was cleared before topology convergence")
	} else if actualHostPV.Spec.ClaimRef.Namespace != expectedClaimRef.Namespace || actualHostPV.Spec.ClaimRef.Name != expectedClaimRef.Name {
		t.Errorf(
			"host PV claimRef changed before topology convergence: got %s/%s, want %s/%s",
			actualHostPV.Spec.ClaimRef.Namespace,
			actualHostPV.Spec.ClaimRef.Name,
			expectedClaimRef.Namespace,
			expectedClaimRef.Name,
		)
	}

	actualVirtualTarget := &corev1.PersistentVolumeClaim{}
	if err := ctx.VirtualClient.Get(ctx.Context, virtualTarget, actualVirtualTarget); err != nil {
		t.Errorf("get virtual target PVC after handoff: %v", err)
	} else if actualVirtualTarget.Status.Phase != corev1.ClaimPending {
		t.Errorf("virtual target PVC phase changed before topology convergence: %q", actualVirtualTarget.Status.Phase)
	}
}

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
				selectedNodeAnnotation:            "node1",
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
				selectedNodeAnnotation:       "node1",
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
	dataProtectionGroup := "dataprotection.kubeblocks.io"
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
				Kind:     "Backup",
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
	waitForFirstConsumerMode := storagev1.VolumeBindingWaitForFirstConsumer
	waitForFirstConsumerStorageClassName := "wffc-sc"
	waitForFirstConsumerStorageClass := &storagev1.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: waitForFirstConsumerStorageClassName},
		VolumeBindingMode: &waitForFirstConsumerMode,
	}
	dataProtectionBackupPendingWaitForFirstConsumerPvc := dataProtectionBackupPendingPvc.DeepCopy()
	dataProtectionBackupPendingWaitForFirstConsumerPvc.Spec.StorageClassName = &waitForFirstConsumerStorageClassName
	dataProtectionBackupPendingWaitForFirstConsumerSelectedNodePvc := dataProtectionBackupPendingWaitForFirstConsumerPvc.DeepCopy()
	dataProtectionBackupPendingWaitForFirstConsumerSelectedNodePvc.Annotations = map[string]string{
		selectedNodeAnnotation: "node1",
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
	dataProtectionHostPendingPvcWithFakeVolumeName := dataProtectionHostPendingPvc.DeepCopy()
	dataProtectionHostPendingPvcWithFakeVolumeName.Spec.VolumeName = "restore-populated-pv"
	dataProtectionHostPendingPvcWithUID := dataProtectionHostPendingPvc.DeepCopy()
	dataProtectionHostPendingPvcWithUID.Annotations[translate.UIDAnnotation] = string(dataProtectionBackupPvc.UID)
	dataProtectionHostPendingPvcWithFakeVolumeNameAndUID := dataProtectionHostPendingPvcWithFakeVolumeName.DeepCopy()
	dataProtectionHostPendingPvcWithFakeVolumeNameAndUID.Annotations[translate.UIDAnnotation] = string(dataProtectionBackupPvc.UID)
	dataProtectionPopulatedPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "restore-populated-pv",
			Annotations: map[string]string{
				"dataprotection.kubeblocks.io/populate-from": "backup-1",
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
	customPopulatorGroup := "example.io"
	customExternalPopulatorPvc := dataProtectionBackupPvc.DeepCopy()
	customExternalPopulatorPvc.UID = types.UID("custom-target-pvc-uid")
	customExternalPopulatorPvc.Spec.DataSourceRef.APIGroup = &customPopulatorGroup
	customExternalPopulatorPvc.Spec.DataSourceRef.Kind = "Dataset"
	customExternalPopulatorPvc.Spec.DataSourceRef.Name = "dataset-1"
	customExternalPopulatorPV := dataProtectionPopulatedPV.DeepCopy()
	customExternalPopulatorPV.Annotations = nil
	customExternalPopulatorPV.Spec.ClaimRef.UID = customExternalPopulatorPvc.UID
	customExternalPopulatorHostPendingPvcWithUID := dataProtectionHostPendingPvc.DeepCopy()
	customExternalPopulatorHostPendingPvcWithUID.Annotations[translate.UIDAnnotation] = string(customExternalPopulatorPvc.UID)
	dataProtectionNoDataRestorePvc := dataProtectionBackupPvc.DeepCopy()
	dataProtectionNoDataRestorePvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
		{
			Type:   externalPopulatorRestoreConditionType,
			Status: corev1.ConditionTrue,
			Reason: externalPopulatorRestoreConditionReasonProvisioned,
		},
	}
	dataProtectionNoDataRestorePendingPvc := dataProtectionBackupPendingPvc.DeepCopy()
	dataProtectionNoDataRestorePendingPvc.Status = dataProtectionNoDataRestorePvc.Status
	dataProtectionNoDataRestorePvcWithVolumeName := dataProtectionNoDataRestorePvc.DeepCopy()
	dataProtectionNoDataRestorePvcWithVolumeName.Spec.VolumeName = dataProtectionPopulatedPV.Name
	dataProtectionNoDataRestorePvcWithVolumeNameBoundStatus := dataProtectionNoDataRestorePvcWithVolumeName.DeepCopy()
	dataProtectionNoDataRestorePvcWithVolumeNameBoundStatus.Status.Phase = corev1.ClaimBound
	dataProtectionNoDataRestorePvcWithVolumeNameBoundStatus.Status.Capacity = dataProtectionPopulatedPV.Spec.Capacity.DeepCopy()
	dataProtectionNoDataRestoreProcessingPvc := dataProtectionBackupPendingPvc.DeepCopy()
	dataProtectionNoDataRestoreProcessingPvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
		{
			Type:    externalPopulatorPopulateConditionType,
			Status:  corev1.ConditionTrue,
			Reason:  externalPopulatorRestoreConditionReasonProcessing,
			Message: externalPopulatorNoDataRestoreMessage,
		},
		{
			Type:    externalPopulatorRestoreConditionType,
			Status:  corev1.ConditionUnknown,
			Reason:  externalPopulatorRestoreConditionReasonProcessing,
			Message: externalPopulatorNoDataRestoreMessage,
		},
	}
	dataProtectionNoDataHostPvc := dataProtectionHostPendingPvcWithUID.DeepCopy()
	dataProtectionNoDataHostPvc.Spec = corev1.PersistentVolumeClaimSpec{
		DataSourceRef: dataProtectionBackupPvc.Spec.DataSourceRef.DeepCopy(),
	}
	dataProtectionNoDataHostPvc.Status = dataProtectionNoDataRestorePvc.Status
	dataProtectionNoDataHostProcessingPvc := dataProtectionHostPendingPvcWithUID.DeepCopy()
	dataProtectionNoDataHostProcessingPvc.Spec = corev1.PersistentVolumeClaimSpec{
		DataSourceRef: dataProtectionBackupPvc.Spec.DataSourceRef.DeepCopy(),
	}
	dataProtectionNoDataHostProcessingPvc.Status = dataProtectionNoDataRestoreProcessingPvc.Status
	dataProtectionNoDataHostPendingWithBackupSource := dataProtectionHostPendingPvcWithUID.DeepCopy()
	dataProtectionNoDataHostPendingWithBackupSource.Spec = corev1.PersistentVolumeClaimSpec{
		DataSource: &corev1.TypedLocalObjectReference{
			APIGroup: &dataProtectionGroup,
			Kind:     "Backup",
			Name:     "backup-1",
		},
		DataSourceRef: &corev1.TypedObjectReference{
			APIGroup: &dataProtectionGroup,
			Kind:     "Backup",
			Name:     "backup-1",
		},
	}
	dataProtectionNoDataHostPendingWithBackupSource.ResourceVersion = "1"
	dataProtectionHostPendingWithBackupSource := dataProtectionNoDataHostPendingWithBackupSource.DeepCopy()
	dataProtectionHostPendingWithBackupSource.Spec.DataSource = nil
	dataProtectionHostPendingWithBackupSource.ResourceVersion = ""
	dataProtectionHostPendingWaitForFirstConsumerWithBackupSource := dataProtectionNoDataHostPendingWithBackupSource.DeepCopy()
	dataProtectionHostPendingWaitForFirstConsumerWithBackupSource.Spec.DataSource = nil
	dataProtectionHostPendingWaitForFirstConsumerWithBackupSource.Spec.StorageClassName = &waitForFirstConsumerStorageClassName
	dataProtectionHostPendingWaitForFirstConsumerSelectedNodeWithBackupSource := dataProtectionHostPendingWaitForFirstConsumerWithBackupSource.DeepCopy()
	dataProtectionHostPendingWaitForFirstConsumerSelectedNodeWithBackupSource.Annotations[translate.ManagedAnnotationsAnnotation] = selectedNodeAnnotation
	dataProtectionHostPendingWaitForFirstConsumerSelectedNodeWithBackupSource.Annotations[selectedNodeAnnotation] = "node1"
	dataProtectionNoDataHostProcessingWithBackupSource := dataProtectionNoDataHostPendingWithBackupSource.DeepCopy()
	dataProtectionNoDataHostProcessingWithBackupSource.Status = dataProtectionNoDataRestoreProcessingPvc.Status
	dataProtectionNoDataHostPendingWithBackupSourceAndFakeVolumeName := dataProtectionNoDataHostPendingWithBackupSource.DeepCopy()
	dataProtectionNoDataHostPendingWithBackupSourceAndFakeVolumeName.Spec.VolumeName = dataProtectionPopulatedPV.Name
	dataProtectionNoDataHostPendingAfterGuestMaterialized := dataProtectionNoDataHostPendingWithBackupSource.DeepCopy()
	dataProtectionDataRestoreHostPvc := dataProtectionHostPendingPvcWithUID.DeepCopy()
	dataProtectionDataRestoreHostPvc.Spec = corev1.PersistentVolumeClaimSpec{
		VolumeName: dataProtectionPopulatedPV.Name,
	}
	dataProtectionDataRestoreHostPvc.Status = *dataProtectionBackupPvc.Status.DeepCopy()
	dataProtectionNoDataHostDeletingWithBackupSource := dataProtectionNoDataHostPendingWithBackupSource.DeepCopy()
	dataProtectionNoDataHostDeletingWithBackupSource.Finalizers = []string{"kubernetes.io/pvc-protection"}
	dataProtectionNoDataHostDeletingWithBackupSource.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	dataProtectionNoDataHostBoundWithBackupSource := dataProtectionNoDataHostPendingWithBackupSource.DeepCopy()
	dataProtectionNoDataHostBoundWithBackupSource.ResourceVersion = "2"
	dataProtectionNoDataHostBoundWithBackupSource.Spec.VolumeName = "restore-populated-pv"
	dataProtectionNoDataHostBoundWithBackupSource.Status = corev1.PersistentVolumeClaimStatus{
		Phase: corev1.ClaimBound,
		Capacity: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("1Gi"),
		},
	}
	dataProtectionNoDataRestorePvcWithHostBoundStatus := dataProtectionNoDataRestorePvc.DeepCopy()
	dataProtectionNoDataRestorePvcWithHostBoundStatus.Status = *dataProtectionNoDataHostBoundWithBackupSource.Status.DeepCopy()
	dataProtectionNoDataHostPendingWithoutBackupSource := dataProtectionHostPendingPvcWithUID.DeepCopy()
	dataProtectionNoDataHostPendingWithoutBackupSource.Spec = corev1.PersistentVolumeClaimSpec{}
	dataProtectionNoDataHostDeletingWithoutBackupSource := dataProtectionNoDataHostPendingWithoutBackupSource.DeepCopy()
	dataProtectionNoDataHostDeletingWithoutBackupSource.Finalizers = []string{"kubernetes.io/pvc-protection"}
	dataProtectionNoDataHostDeletingWithoutBackupSource.DeletionTimestamp = &metav1.Time{Time: time.Now()}

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
	dataProtectionHostPendingPopulateHelperPvc := dataProtectionHostPopulateHelperPvc.DeepCopy()
	dataProtectionHostPendingPopulateHelperPvc.Spec.VolumeName = ""
	dataProtectionHostPendingPopulateHelperPvc.Status = corev1.PersistentVolumeClaimStatus{}
	dataProtectionPopulateHelperPvcWithHostBoundStatus := dataProtectionPopulateHelperPvc.DeepCopy()
	dataProtectionPopulateHelperPvcWithHostBoundStatus.Status = *dataProtectionHostPopulateHelperPvc.Status.DeepCopy()
	dataProtectionDeletingPopulateHelperPvc := dataProtectionPopulateHelperPvc.DeepCopy()
	dataProtectionDeletingPopulateHelperPvc.Finalizers = []string{"kubernetes.io/pvc-protection"}
	dataProtectionDeletingPopulateHelperPvc.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	dataProtectionPopulatedPVBoundToHelper := dataProtectionPopulatedPV.DeepCopy()
	dataProtectionPopulatedPVBoundToHelper.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: dataProtectionPopulateHelperPvc.Namespace,
		Name:      dataProtectionPopulateHelperPvc.Name,
		UID:       dataProtectionPopulateHelperPvc.UID,
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
	dataProtectionHostPendingPvcWithObjectUID := dataProtectionHostPendingPvcWithUID.DeepCopy()
	dataProtectionHostPendingPvcWithObjectUID.UID = types.UID("host-target-pvc-uid")
	dataProtectionHostMaterializedTargetPvcWithObjectUID := dataProtectionHostMaterializedTargetPvc.DeepCopy()
	dataProtectionHostMaterializedTargetPvcWithObjectUID.UID = dataProtectionHostPendingPvcWithObjectUID.UID
	dataProtectionHostPVBoundToTargetStaleUID := dataProtectionHostPVBoundToTarget.DeepCopy()
	dataProtectionHostPVBoundToTargetStaleUID.Spec.ClaimRef.UID = types.UID("stale-host-target-pvc-uid")
	dataProtectionHostPVBoundToTargetFreshUID := dataProtectionHostPVBoundToTarget.DeepCopy()
	dataProtectionHostPVBoundToTargetFreshUID.Spec.ClaimRef.UID = dataProtectionHostPendingPvcWithObjectUID.UID

	withStorageClassAndSelectedNode := func(pvc *corev1.PersistentVolumeClaim, storageClassName, selectedNode string, hostObject bool) *corev1.PersistentVolumeClaim {
		updated := pvc.DeepCopy()
		updated.Spec.StorageClassName = &storageClassName
		if selectedNode == "" {
			return updated
		}
		if updated.Annotations == nil {
			updated.Annotations = map[string]string{}
		}
		updated.Annotations[selectedNodeAnnotation] = selectedNode
		if hostObject {
			updated.Annotations[translate.ManagedAnnotationsAnnotation] = selectedNodeAnnotation
		}
		return updated
	}
	withRequiredNodeAffinity := func(pv *corev1.PersistentVolume, hostname string) *corev1.PersistentVolume {
		updated := pv.DeepCopy()
		updated.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{
			Required: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      corev1.LabelHostname,
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{hostname},
							},
						},
					},
				},
			},
		}
		return updated
	}
	node2 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node2",
			Labels: map[string]string{corev1.LabelHostname: "node2"},
		},
	}
	node4 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node4",
			Labels: map[string]string{corev1.LabelHostname: "node4"},
		},
	}

	dataProtectionWFFCTargetNode4Pvc := withStorageClassAndSelectedNode(dataProtectionBackupPendingPvcWithVolumeName, waitForFirstConsumerStorageClassName, "node4", false)
	dataProtectionWFFCTargetNode4BoundPvc := dataProtectionWFFCTargetNode4Pvc.DeepCopy()
	dataProtectionWFFCTargetNode4BoundPvc.Status = *dataProtectionBackupPendingPvcWithVolumeNameBoundStatus.Status.DeepCopy()
	dataProtectionHostWFFCTargetNode4Pvc := withStorageClassAndSelectedNode(dataProtectionHostPendingPvcWithUID, waitForFirstConsumerStorageClassName, "node4", true)
	dataProtectionHostWFFCMaterializedTargetNode4Pvc := dataProtectionHostWFFCTargetNode4Pvc.DeepCopy()
	dataProtectionHostWFFCMaterializedTargetNode4Pvc.Spec.VolumeName = dataProtectionPopulatedPV.Name
	dataProtectionHostWFFCTargetNode2Pvc := withStorageClassAndSelectedNode(dataProtectionHostPendingPvcWithUID, waitForFirstConsumerStorageClassName, "node2", true)

	dataProtectionWFFCHelperNode2Pvc := withStorageClassAndSelectedNode(dataProtectionPopulateHelperPvc, waitForFirstConsumerStorageClassName, "node2", false)
	dataProtectionHostWFFCHelperNode2Pvc := withStorageClassAndSelectedNode(dataProtectionHostPopulateHelperPvc, waitForFirstConsumerStorageClassName, "node2", true)
	dataProtectionWFFCHelperNode4Pvc := withStorageClassAndSelectedNode(dataProtectionPopulateHelperPvc, waitForFirstConsumerStorageClassName, "node4", false)
	dataProtectionHostWFFCHelperNode4Pvc := withStorageClassAndSelectedNode(dataProtectionHostPopulateHelperPvc, waitForFirstConsumerStorageClassName, "node4", true)

	dataProtectionWFFCVirtualPVNode2BoundToHelper := withRequiredNodeAffinity(dataProtectionPopulatedPVBoundToHelper, "node2")
	dataProtectionWFFCHostPVNode2BoundToHelper := withRequiredNodeAffinity(dataProtectionHostPVBoundToHelper, "node2")
	dataProtectionWFFCVirtualPVNode4BoundToHelper := withRequiredNodeAffinity(dataProtectionPopulatedPVBoundToHelper, "node4")
	dataProtectionWFFCVirtualPVNode4BoundToTarget := withRequiredNodeAffinity(dataProtectionPopulatedPV, "node4")
	dataProtectionWFFCHostPVNode4BoundToHelper := withRequiredNodeAffinity(dataProtectionHostPVBoundToHelper, "node4")
	dataProtectionWFFCHostPVNode4BoundToTarget := dataProtectionWFFCHostPVNode4BoundToHelper.DeepCopy()
	dataProtectionWFFCHostPVNode4BoundToTarget.Spec.ClaimRef = dataProtectionHostPVBoundToTarget.Spec.ClaimRef.DeepCopy()

	dataProtectionWFFCTargetWithoutSelectedNodePvc := withStorageClassAndSelectedNode(dataProtectionBackupPendingPvcWithVolumeName, waitForFirstConsumerStorageClassName, "", false)
	dataProtectionHostWFFCTargetWithoutSelectedNodePvc := withStorageClassAndSelectedNode(dataProtectionHostPendingPvcWithUID, waitForFirstConsumerStorageClassName, "", true)
	dataProtectionWFFCHelperWithoutSelectedNodePvc := withStorageClassAndSelectedNode(dataProtectionPopulateHelperPvc, waitForFirstConsumerStorageClassName, "", false)
	dataProtectionHostWFFCHelperWithoutSelectedNodePvc := withStorageClassAndSelectedNode(dataProtectionHostPopulateHelperPvc, waitForFirstConsumerStorageClassName, "", true)

	immediateMode := storagev1.VolumeBindingImmediate
	immediateStorageClassName := "immediate-sc"
	immediateStorageClass := &storagev1.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: immediateStorageClassName},
		VolumeBindingMode: &immediateMode,
	}
	dataProtectionImmediateTargetPvc := withStorageClassAndSelectedNode(dataProtectionBackupPendingPvcWithVolumeName, immediateStorageClassName, "", false)
	dataProtectionImmediateTargetBoundPvc := dataProtectionImmediateTargetPvc.DeepCopy()
	dataProtectionImmediateTargetBoundPvc.Status = *dataProtectionBackupPendingPvcWithVolumeNameBoundStatus.Status.DeepCopy()
	dataProtectionHostImmediateTargetPvc := withStorageClassAndSelectedNode(dataProtectionHostPendingPvcWithUID, immediateStorageClassName, "", true)
	dataProtectionHostImmediateMaterializedTargetPvc := dataProtectionHostImmediateTargetPvc.DeepCopy()
	dataProtectionHostImmediateMaterializedTargetPvc.Spec.VolumeName = dataProtectionPopulatedPV.Name
	dataProtectionImmediateHelperPvc := withStorageClassAndSelectedNode(dataProtectionPopulateHelperPvc, immediateStorageClassName, "", false)
	dataProtectionHostImmediateHelperPvc := withStorageClassAndSelectedNode(dataProtectionHostPopulateHelperPvc, immediateStorageClassName, "", true)

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
			Name:                "Create data protection restore forward while guest backup materializes",
			InitialVirtualState: []runtime.Object{dataProtectionBackupPendingPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionBackupPendingPvc.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionHostPendingWithBackupSource.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(dataProtectionBackupPendingPvc.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name: "Create data protection WFFC restore forward to obtain selected-node before guest backup materializes",
			InitialVirtualState: []runtime.Object{
				waitForFirstConsumerStorageClass.DeepCopy(),
				dataProtectionBackupPendingWaitForFirstConsumerPvc.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionBackupPendingWaitForFirstConsumerPvc.DeepCopy()},
				storagev1.SchemeGroupVersion.WithKind("StorageClass"):       {waitForFirstConsumerStorageClass.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionHostPendingWaitForFirstConsumerWithBackupSource.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(dataProtectionBackupPendingWaitForFirstConsumerPvc.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name: "Create data protection WFFC restore forward after selected-node while guest backup materializes",
			InitialVirtualState: []runtime.Object{
				waitForFirstConsumerStorageClass.DeepCopy(),
				dataProtectionBackupPendingWaitForFirstConsumerSelectedNodePvc.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionBackupPendingWaitForFirstConsumerSelectedNodePvc.DeepCopy()},
				storagev1.SchemeGroupVersion.WithKind("StorageClass"):       {waitForFirstConsumerStorageClass.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionHostPendingWaitForFirstConsumerSelectedNodeWithBackupSource.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(dataProtectionBackupPendingWaitForFirstConsumerSelectedNodePvc.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                "Create data protection restore forward from guest materialized volume without host backup data source",
			InitialVirtualState: []runtime.Object{dataProtectionBackupPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionBackupPvc.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionDataRestoreHostPvc.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(dataProtectionBackupPvc.DeepCopy()))
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
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(dataProtectionNoDataRestorePvc.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                "Create data protection no-data restore processing forward without host backup data source",
			InitialVirtualState: []runtime.Object{dataProtectionNoDataRestoreProcessingPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestoreProcessingPvc.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(dataProtectionNoDataRestoreProcessingPvc.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                "Recreate data protection no-data host pvc without deleting virtual after stale host pvc was deleted",
			InitialVirtualState: []runtime.Object{dataProtectionNoDataRestorePvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestorePvc.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).SyncToHost(syncCtx, &synccontext.SyncToHostEvent[*corev1.PersistentVolumeClaim]{
					HostOld: dataProtectionNoDataHostDeletingWithBackupSource.DeepCopy(),
					Virtual: dataProtectionNoDataRestorePvc.DeepCopy(),
				})
				assert.NilError(t, err)
			},
		},
		{
			Name:                "Recreate data protection no-data host pvc without deleting virtual after cleared host pvc was deleted",
			InitialVirtualState: []runtime.Object{dataProtectionNoDataRestorePvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestorePvc.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).SyncToHost(syncCtx, &synccontext.SyncToHostEvent[*corev1.PersistentVolumeClaim]{
					HostOld: dataProtectionNoDataHostDeletingWithoutBackupSource.DeepCopy(),
					Virtual: dataProtectionNoDataRestorePvc.DeepCopy(),
				})
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
			Name:                 "Do not delete virtual data protection no-data pvc while stale host pvc is deleting",
			InitialVirtualState:  []runtime.Object{dataProtectionNoDataRestorePvc.DeepCopy()},
			InitialPhysicalState: []runtime.Object{dataProtectionNoDataHostDeletingWithBackupSource.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestorePvc.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataHostDeletingWithBackupSource.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionNoDataHostDeletingWithBackupSource.DeepCopy(),
					dataProtectionNoDataHostDeletingWithBackupSource.DeepCopy(),
					dataProtectionNoDataRestorePvc.DeepCopy(),
					dataProtectionNoDataRestorePvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.RequeueAfter > 0)
			},
		},
		{
			Name:                 "Do not delete virtual data protection no-data pvc while cleared host pvc is deleting",
			InitialVirtualState:  []runtime.Object{dataProtectionNoDataRestorePvc.DeepCopy()},
			InitialPhysicalState: []runtime.Object{dataProtectionNoDataHostDeletingWithoutBackupSource.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestorePvc.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataHostDeletingWithoutBackupSource.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionNoDataHostDeletingWithoutBackupSource.DeepCopy(),
					dataProtectionNoDataHostDeletingWithoutBackupSource.DeepCopy(),
					dataProtectionNoDataRestorePvc.DeepCopy(),
					dataProtectionNoDataRestorePvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.RequeueAfter > 0)
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
			Name:                 "Back off existing host backup data source pvc after no-data restore is provisioned",
			InitialVirtualState:  []runtime.Object{dataProtectionNoDataRestorePendingPvc.DeepCopy()},
			InitialPhysicalState: []runtime.Object{dataProtectionNoDataHostPendingWithBackupSource.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestorePendingPvc.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataHostPendingWithBackupSource.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
					dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
					dataProtectionNoDataRestorePendingPvc.DeepCopy(),
					dataProtectionNoDataRestorePendingPvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, externalPopulatorNoDataRestoreBackoff)
			},
		},
		{
			Name:                 "Back off existing host backup data source pvc while no-data restore is processing",
			InitialVirtualState:  []runtime.Object{dataProtectionNoDataRestoreProcessingPvc.DeepCopy()},
			InitialPhysicalState: []runtime.Object{dataProtectionNoDataHostProcessingWithBackupSource.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestoreProcessingPvc.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataHostProcessingWithBackupSource.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionNoDataHostProcessingWithBackupSource.DeepCopy(),
					dataProtectionNoDataHostProcessingWithBackupSource.DeepCopy(),
					dataProtectionNoDataRestoreProcessingPvc.DeepCopy(),
					dataProtectionNoDataRestoreProcessingPvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, externalPopulatorNoDataRestoreBackoff)
			},
		},
		{
			Name: "Keep immutable host backup data source and retry while populated host pv is missing",
			InitialVirtualState: []runtime.Object{
				dataProtectionNoDataRestorePvcWithVolumeName.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{dataProtectionNoDataHostPendingWithBackupSource.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestorePvcWithVolumeName.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataHostPendingAfterGuestMaterialized.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
					dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
					dataProtectionNoDataRestorePvcWithVolumeName.DeepCopy(),
					dataProtectionNoDataRestorePvcWithVolumeName.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, 2*time.Second)
			},
		},
		{
			Name: "Switch host pv from deleted populate helper to target after no-data guest materializes",
			InitialVirtualState: []runtime.Object{
				dataProtectionNoDataRestorePvcWithVolumeName.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
				dataProtectionHostPVBoundToHelper.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestorePvcWithVolumeNameBoundStatus.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataHostPendingAfterGuestMaterialized.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionHostPVBoundToTarget.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
					dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
					dataProtectionNoDataRestorePvcWithVolumeName.DeepCopy(),
					dataProtectionNoDataRestorePvcWithVolumeName.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.RequeueAfter == 0)
			},
		},
		{
			Name: "Preserve existing data restore host backup data source pvc with stale fake volume name while populated host pv is missing",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{dataProtectionNoDataHostPendingWithBackupSourceAndFakeVolumeName.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionBackupPendingPvcWithVolumeName.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataHostPendingWithBackupSourceAndFakeVolumeName.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionNoDataHostPendingWithBackupSourceAndFakeVolumeName.DeepCopy(),
					dataProtectionNoDataHostPendingWithBackupSourceAndFakeVolumeName.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, 2*time.Second)
			},
		},
		{
			Name:                 "Do not delete host backup data source pvc from stale pending snapshot after it is bound",
			InitialVirtualState:  []runtime.Object{dataProtectionNoDataRestorePvc.DeepCopy()},
			InitialPhysicalState: []runtime.Object{dataProtectionNoDataHostBoundWithBackupSource.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestorePvc.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataHostBoundWithBackupSource.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
					dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
					dataProtectionNoDataRestorePvc.DeepCopy(),
					dataProtectionNoDataRestorePvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, externalPopulatorNoDataRestoreBackoff)
			},
		},
		{
			Name:                 "Do not delete current bound host backup data source pvc",
			InitialVirtualState:  []runtime.Object{dataProtectionNoDataRestorePvc.DeepCopy()},
			InitialPhysicalState: []runtime.Object{dataProtectionNoDataHostBoundWithBackupSource.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestorePvcWithHostBoundStatus.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataHostBoundWithBackupSource.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionNoDataHostBoundWithBackupSource.DeepCopy(),
					dataProtectionNoDataHostBoundWithBackupSource.DeepCopy(),
					dataProtectionNoDataRestorePvc.DeepCopy(),
					dataProtectionNoDataRestorePvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, !result.Requeue)
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
			Name: "Preserve custom external populator virtual status while host pvc waits for volume",
			InitialVirtualState: []runtime.Object{
				customExternalPopulatorPvc.DeepCopy(),
				customExternalPopulatorPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{dataProtectionHostPendingPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {customExternalPopulatorPvc.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {customExternalPopulatorPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {customExternalPopulatorHostPendingPvcWithUID.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionHostPendingPvc.DeepCopy(),
					customExternalPopulatorPvc.DeepCopy(),
					customExternalPopulatorPvc.DeepCopy(),
				))
				assert.NilError(t, err)
			},
		},
		{
			Name: "Preserve host populate helper pvc while helper volume name has not converged",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				dataProtectionDeletingPopulateHelperPvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostPendingPvcWithFakeVolumeNameAndUID.DeepCopy(),
				dataProtectionHostPendingPopulateHelperPvc.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
					dataProtectionDeletingPopulateHelperPvc.DeepCopy(),
				},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostPendingPvcWithFakeVolumeNameAndUID.DeepCopy(),
					dataProtectionHostPendingPopulateHelperPvc.DeepCopy(),
				},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingPopulateHelperPvc.DeepCopy(),
					dataProtectionHostPendingPopulateHelperPvc.DeepCopy(),
					dataProtectionDeletingPopulateHelperPvc.DeepCopy(),
					dataProtectionDeletingPopulateHelperPvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.RequeueAfter == 2*time.Second)
			},
		},
		{
			Name: "Do not derive data protection pvc volume name from populated helper pvc",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvc.DeepCopy(),
				dataProtectionPopulateHelperPvc.DeepCopy(),
				dataProtectionPopulatedPVBoundToHelper.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{dataProtectionHostPendingPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionBackupPendingPvc.DeepCopy(),
					dataProtectionPopulateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionPopulatedPVBoundToHelper.DeepCopy()},
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
				assert.Check(t, result.IsZero())
			},
		},
		{
			Name: "Do not wake target data protection pvc after populated helper pvc gets volume name",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvc.DeepCopy(),
				dataProtectionPopulateHelperPvc.DeepCopy(),
				dataProtectionPopulatedPVBoundToHelper.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{dataProtectionHostPopulateHelperPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionBackupPendingPvc.DeepCopy(),
					dataProtectionPopulateHelperPvcWithHostBoundStatus.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionPopulatedPVBoundToHelper.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionHostPopulateHelperPvc.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPopulateHelperPvc.DeepCopy(),
					dataProtectionHostPopulateHelperPvc.DeepCopy(),
					dataProtectionPopulateHelperPvc.DeepCopy(),
					dataProtectionPopulateHelperPvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.IsZero())
			},
		},
		{
			Name: "Keep WFFC external-populator handoff pending when target node4 disagrees with helper and PV node2",
			InitialVirtualState: []runtime.Object{
				waitForFirstConsumerStorageClass.DeepCopy(),
				dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionWFFCVirtualPVNode2BoundToHelper.DeepCopy(),
				dataProtectionWFFCHelperNode2Pvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionHostWFFCHelperNode2Pvc.DeepCopy(),
				dataProtectionWFFCHostPVNode2BoundToHelper.DeepCopy(),
				node2.DeepCopy(),
				node4.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCHelperNode2Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCVirtualPVNode2BoundToHelper.DeepCopy()},
				storagev1.SchemeGroupVersion.WithKind("StorageClass"):  {waitForFirstConsumerStorageClass.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCHelperNode2Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCHostPVNode2BoundToHelper.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("Node"):             {node2.DeepCopy(), node4.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, ctx, New)
				pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
				pvcSyncer.useFakePersistentVolumes = true
				recorder := installPVCEventRecorder(pvcSyncer)

				result, err := pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.RequeueAfter == 2*time.Second, "expected a 2s topology requeue, got %s", result.RequeueAfter)
				assertSinglePVCEvent(t, recorder, "Warning ExternalPopulatorTopologyMismatch", "node4", "node2")
				checkExternalPopulatorHandoffPending(
					t,
					syncCtx,
					types.NamespacedName{Namespace: dataProtectionHostWFFCTargetNode4Pvc.Namespace, Name: dataProtectionHostWFFCTargetNode4Pvc.Name},
					types.NamespacedName{Namespace: dataProtectionWFFCTargetNode4Pvc.Namespace, Name: dataProtectionWFFCTargetNode4Pvc.Name},
					types.NamespacedName{Namespace: dataProtectionHostWFFCHelperNode2Pvc.Namespace, Name: dataProtectionHostWFFCHelperNode2Pvc.Name},
					dataProtectionWFFCHostPVNode2BoundToHelper.Name,
				)
			},
		},
		{
			Name: "Keep WFFC external-populator handoff pending when target and helper node4 disagree with PV node2",
			InitialVirtualState: []runtime.Object{
				waitForFirstConsumerStorageClass.DeepCopy(),
				dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionWFFCVirtualPVNode2BoundToHelper.DeepCopy(),
				dataProtectionWFFCHelperNode4Pvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				dataProtectionWFFCHostPVNode2BoundToHelper.DeepCopy(),
				node2.DeepCopy(),
				node4.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCHelperNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCVirtualPVNode2BoundToHelper.DeepCopy()},
				storagev1.SchemeGroupVersion.WithKind("StorageClass"):  {waitForFirstConsumerStorageClass.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCHostPVNode2BoundToHelper.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("Node"):             {node2.DeepCopy(), node4.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, ctx, New)
				pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
				pvcSyncer.useFakePersistentVolumes = true
				recorder := installPVCEventRecorder(pvcSyncer)

				result, err := pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.RequeueAfter == 2*time.Second, "expected a 2s topology requeue, got %s", result.RequeueAfter)
				assertSinglePVCEvent(t, recorder, "Warning ExternalPopulatorTopologyMismatch", dataProtectionPopulatedPV.Name, "node4", "node2")
				checkExternalPopulatorHandoffPending(
					t,
					syncCtx,
					types.NamespacedName{Namespace: dataProtectionHostWFFCTargetNode4Pvc.Namespace, Name: dataProtectionHostWFFCTargetNode4Pvc.Name},
					types.NamespacedName{Namespace: dataProtectionWFFCTargetNode4Pvc.Namespace, Name: dataProtectionWFFCTargetNode4Pvc.Name},
					types.NamespacedName{Namespace: dataProtectionHostWFFCHelperNode4Pvc.Namespace, Name: dataProtectionHostWFFCHelperNode4Pvc.Name},
					dataProtectionWFFCHostPVNode2BoundToHelper.Name,
				)
			},
		},
		{
			Name: "Keep WFFC external-populator handoff pending when selected host Node is missing",
			InitialVirtualState: []runtime.Object{
				waitForFirstConsumerStorageClass.DeepCopy(),
				dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionWFFCVirtualPVNode4BoundToHelper.DeepCopy(),
				dataProtectionWFFCHelperNode4Pvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				dataProtectionWFFCHostPVNode4BoundToHelper.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCHelperNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCVirtualPVNode4BoundToHelper.DeepCopy()},
				storagev1.SchemeGroupVersion.WithKind("StorageClass"):  {waitForFirstConsumerStorageClass.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCHostPVNode4BoundToHelper.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, ctx, New)
				pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
				pvcSyncer.useFakePersistentVolumes = true
				recorder := installPVCEventRecorder(pvcSyncer)

				result, err := pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.RequeueAfter == 2*time.Second, "expected a 2s topology requeue, got %s", result.RequeueAfter)
				assertSinglePVCEvent(t, recorder, "Warning ExternalPopulatorTopologyNotReady", "node4", "host Node")
				checkExternalPopulatorHandoffPending(
					t,
					syncCtx,
					types.NamespacedName{Namespace: dataProtectionHostWFFCTargetNode4Pvc.Namespace, Name: dataProtectionHostWFFCTargetNode4Pvc.Name},
					types.NamespacedName{Namespace: dataProtectionWFFCTargetNode4Pvc.Namespace, Name: dataProtectionWFFCTargetNode4Pvc.Name},
					types.NamespacedName{Namespace: dataProtectionHostWFFCHelperNode4Pvc.Namespace, Name: dataProtectionHostWFFCHelperNode4Pvc.Name},
					dataProtectionWFFCHostPVNode4BoundToHelper.Name,
				)
			},
		},
		{
			Name: "Complete WFFC target volumeName after claimRef handoff when virtual helper is gone",
			InitialVirtualState: []runtime.Object{
				waitForFirstConsumerStorageClass.DeepCopy(),
				dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				dataProtectionWFFCHostPVNode4BoundToTarget.DeepCopy(),
				node4.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionWFFCTargetNode4BoundPvc.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy()},
				storagev1.SchemeGroupVersion.WithKind("StorageClass"):       {waitForFirstConsumerStorageClass.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostWFFCMaterializedTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCHostPVNode4BoundToTarget.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("Node"):             {node4.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, ctx, New)
				pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
				pvcSyncer.useFakePersistentVolumes = true
				recorder := installPVCEventRecorder(pvcSyncer)

				result, err := pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.IsZero())
				assertNoPVCEvent(t, recorder)
			},
		},
		{
			Name: "Keep WFFC target pending when cached selected-node changes before volumeName commit",
			InitialVirtualState: []runtime.Object{
				waitForFirstConsumerStorageClass.DeepCopy(),
				dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionWFFCVirtualPVNode4BoundToHelper.DeepCopy(),
				dataProtectionWFFCHelperNode4Pvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostWFFCTargetNode2Pvc.DeepCopy(),
				dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				dataProtectionWFFCHostPVNode4BoundToHelper.DeepCopy(),
				node2.DeepCopy(),
				node4.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCHelperNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCVirtualPVNode4BoundToHelper.DeepCopy()},
				storagev1.SchemeGroupVersion.WithKind("StorageClass"):  {waitForFirstConsumerStorageClass.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostWFFCTargetNode2Pvc.DeepCopy(),
					dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCHostPVNode4BoundToHelper.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("Node"):             {node2.DeepCopy(), node4.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, ctx, New)
				pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
				pvcSyncer.useFakePersistentVolumes = true
				recorder := installPVCEventRecorder(pvcSyncer)

				result, err := pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.RequeueAfter == 2*time.Second, "expected a 2s topology requeue, got %s", result.RequeueAfter)
				assertSinglePVCEvent(t, recorder, "Warning ExternalPopulatorTopologyMismatch", "node4", "node2")
				checkExternalPopulatorHandoffPending(
					t,
					syncCtx,
					types.NamespacedName{Namespace: dataProtectionHostWFFCTargetNode2Pvc.Namespace, Name: dataProtectionHostWFFCTargetNode2Pvc.Name},
					types.NamespacedName{Namespace: dataProtectionWFFCTargetNode4Pvc.Namespace, Name: dataProtectionWFFCTargetNode4Pvc.Name},
					types.NamespacedName{Namespace: dataProtectionHostWFFCHelperNode4Pvc.Namespace, Name: dataProtectionHostWFFCHelperNode4Pvc.Name},
					dataProtectionWFFCHostPVNode4BoundToHelper.Name,
				)
			},
		},
		{
			Name: "Bridge WFFC external-populator handoff when target helper and PV all select node4",
			InitialVirtualState: []runtime.Object{
				waitForFirstConsumerStorageClass.DeepCopy(),
				dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionWFFCVirtualPVNode4BoundToHelper.DeepCopy(),
				dataProtectionWFFCHelperNode4Pvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				dataProtectionWFFCHostPVNode4BoundToHelper.DeepCopy(),
				node2.DeepCopy(),
				node4.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionWFFCTargetNode4BoundPvc.DeepCopy(),
					dataProtectionWFFCHelperNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCVirtualPVNode4BoundToHelper.DeepCopy()},
				storagev1.SchemeGroupVersion.WithKind("StorageClass"):  {waitForFirstConsumerStorageClass.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostWFFCMaterializedTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCHostPVNode4BoundToTarget.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("Node"):             {node2.DeepCopy(), node4.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, ctx, New)
				pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
				pvcSyncer.useFakePersistentVolumes = true
				recorder := installPVCEventRecorder(pvcSyncer)

				result, err := pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.IsZero())
				assertNoPVCEvent(t, recorder)
			},
		},
		{
			Name: "Keep WFFC external-populator handoff pending until target selected-node exists",
			InitialVirtualState: []runtime.Object{
				waitForFirstConsumerStorageClass.DeepCopy(),
				dataProtectionWFFCTargetWithoutSelectedNodePvc.DeepCopy(),
				dataProtectionPopulatedPVBoundToHelper.DeepCopy(),
				dataProtectionWFFCHelperWithoutSelectedNodePvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostWFFCTargetWithoutSelectedNodePvc.DeepCopy(),
				dataProtectionHostWFFCHelperWithoutSelectedNodePvc.DeepCopy(),
				dataProtectionHostPVBoundToHelper.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionWFFCTargetWithoutSelectedNodePvc.DeepCopy(),
					dataProtectionWFFCHelperWithoutSelectedNodePvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionPopulatedPVBoundToHelper.DeepCopy()},
				storagev1.SchemeGroupVersion.WithKind("StorageClass"):  {waitForFirstConsumerStorageClass.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostWFFCTargetWithoutSelectedNodePvc.DeepCopy(),
					dataProtectionHostWFFCHelperWithoutSelectedNodePvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionHostPVBoundToHelper.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, ctx, New)
				pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
				pvcSyncer.useFakePersistentVolumes = true
				recorder := installPVCEventRecorder(pvcSyncer)

				result, err := pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostWFFCTargetWithoutSelectedNodePvc.DeepCopy(),
					dataProtectionHostWFFCTargetWithoutSelectedNodePvc.DeepCopy(),
					dataProtectionWFFCTargetWithoutSelectedNodePvc.DeepCopy(),
					dataProtectionWFFCTargetWithoutSelectedNodePvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.RequeueAfter == 2*time.Second, "expected a 2s topology requeue, got %s", result.RequeueAfter)
				assertSinglePVCEvent(t, recorder, "Warning ExternalPopulatorTopologyNotReady", waitForFirstConsumerStorageClassName, selectedNodeAnnotation)
				checkExternalPopulatorHandoffPending(
					t,
					syncCtx,
					types.NamespacedName{Namespace: dataProtectionHostWFFCTargetWithoutSelectedNodePvc.Namespace, Name: dataProtectionHostWFFCTargetWithoutSelectedNodePvc.Name},
					types.NamespacedName{Namespace: dataProtectionWFFCTargetWithoutSelectedNodePvc.Namespace, Name: dataProtectionWFFCTargetWithoutSelectedNodePvc.Name},
					types.NamespacedName{Namespace: dataProtectionHostWFFCHelperWithoutSelectedNodePvc.Namespace, Name: dataProtectionHostWFFCHelperWithoutSelectedNodePvc.Name},
					dataProtectionHostPVBoundToHelper.Name,
				)
			},
		},
		{
			Name: "Bridge Immediate external-populator handoff without selected-node or PV node affinity",
			InitialVirtualState: []runtime.Object{
				immediateStorageClass.DeepCopy(),
				dataProtectionImmediateTargetPvc.DeepCopy(),
				dataProtectionPopulatedPVBoundToHelper.DeepCopy(),
				dataProtectionImmediateHelperPvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostImmediateTargetPvc.DeepCopy(),
				dataProtectionHostImmediateHelperPvc.DeepCopy(),
				dataProtectionHostPVBoundToHelper.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionImmediateTargetBoundPvc.DeepCopy(),
					dataProtectionImmediateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionPopulatedPVBoundToHelper.DeepCopy()},
				storagev1.SchemeGroupVersion.WithKind("StorageClass"):  {immediateStorageClass.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostImmediateMaterializedTargetPvc.DeepCopy(),
					dataProtectionHostImmediateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionHostPVBoundToTarget.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, ctx, New)
				pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
				pvcSyncer.useFakePersistentVolumes = true
				recorder := installPVCEventRecorder(pvcSyncer)

				result, err := pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostImmediateTargetPvc.DeepCopy(),
					dataProtectionHostImmediateTargetPvc.DeepCopy(),
					dataProtectionImmediateTargetPvc.DeepCopy(),
					dataProtectionImmediateTargetPvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.IsZero())
				assertNoPVCEvent(t, recorder)
			},
		},
		{
			Name: "Bridge shared external-populator handoff without selected-node or PV node affinity",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				dataProtectionPopulatedPVBoundToHelper.DeepCopy(),
				dataProtectionPopulateHelperPvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostPendingPvc.DeepCopy(),
				dataProtectionHostPopulateHelperPvc.DeepCopy(),
				dataProtectionHostPVBoundToHelper.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionBackupPendingPvcWithVolumeNameBoundStatus.DeepCopy(),
					dataProtectionPopulateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionPopulatedPVBoundToHelper.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostMaterializedTargetPvc.DeepCopy(),
					dataProtectionHostPopulateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionHostPVBoundToTarget.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, ctx, New)
				pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
				pvcSyncer.useFakePersistentVolumes = true
				recorder := installPVCEventRecorder(pvcSyncer)

				result, err := pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.IsZero())
				assertNoPVCEvent(t, recorder)
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
			Name: "Refresh stale data protection host pv target claim ref uid",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPvc.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
				dataProtectionPopulateHelperPvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostPendingPvcWithObjectUID.DeepCopy(),
				dataProtectionHostPVBoundToTargetStaleUID.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionBackupPvc.DeepCopy(),
					dataProtectionPopulateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionHostMaterializedTargetPvcWithObjectUID.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionHostPVBoundToTargetFreshUID.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				_, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingPvcWithObjectUID.DeepCopy(),
					dataProtectionHostPendingPvcWithObjectUID.DeepCopy(),
					dataProtectionBackupPvc.DeepCopy(),
					dataProtectionBackupPvc.DeepCopy(),
				))
				assert.NilError(t, err)
			},
		},
		{
			Name: "Do not derive data protection pvc volume name from populated pv claim ref",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvc.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{dataProtectionHostPendingPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionBackupPendingPvc.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionPopulatedPV.DeepCopy()},
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
				assert.Check(t, result.IsZero())
			},
		},
		{
			Name: "Keep virtual pvc pending instead of deriving bound status while populated host pv is missing",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{dataProtectionHostPendingPvc.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionBackupPendingPvcWithVolumeName.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionHostPendingPvcWithUID.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true
				recorder := installPVCEventRecorder(syncer.(*persistentVolumeClaimSyncer))

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionHostPendingPvc.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, 2*time.Second)
				assertSinglePVCEvent(t, recorder, "Warning ExternalPopulatorTopologyNotReady", dataProtectionPopulatedPV.Name)
			},
		},
		{
			Name: "Keep virtual pvc pending with stale fake volume name while populated host pv is missing",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{dataProtectionHostPendingPvcWithFakeVolumeName.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionBackupPendingPvcWithVolumeName.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionHostPendingPvcWithFakeVolumeNameAndUID.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingPvcWithFakeVolumeName.DeepCopy(),
					dataProtectionHostPendingPvcWithFakeVolumeName.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, 2*time.Second)
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
		{
			Name: "Preserve orphaned host populate helper pvc in sync to virtual while target handoff is pending",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostPopulateHelperPvc.DeepCopy(),
				dataProtectionHostPendingPvcWithUID.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostPopulateHelperPvc.DeepCopy(),
					dataProtectionHostPendingPvcWithUID.DeepCopy(),
				},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).SyncToVirtual(syncCtx, synccontext.NewSyncToVirtualEvent(
					dataProtectionHostPopulateHelperPvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, 2*time.Second)
			},
		},
		{
			Name: "Delete orphaned host populate helper pvc in sync to virtual after target handoff converged",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostPopulateHelperPvc.DeepCopy(),
				dataProtectionDataRestoreHostPvc.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionBackupPvc.DeepCopy(),
				},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionDataRestoreHostPvc.DeepCopy(),
				},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).SyncToVirtual(syncCtx, synccontext.NewSyncToVirtualEvent(
					dataProtectionHostPopulateHelperPvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, time.Duration(0))
			},
		},
	})
}

func TestSync_ExternalPopulatorStatusNotOverwritten(t *testing.T) {
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
	apiGroup := "dataprotection.kubeblocks.io"

	syncertesting.RunTestsWithContext(t, func(vConfig *config.VirtualClusterConfig, pClient *testingutil.FakeIndexClient, vClient *testingutil.FakeIndexClient) *synccontext.RegisterContext {
		ctx := syncertesting.NewFakeRegisterContext(vConfig, pClient, vClient)
		ctx.Config.Sync.ToHost.StorageClasses.Enabled = false
		return ctx
	}, []*syncertesting.SyncTest{
		{
			Name: "External populator PVC keeps virtual status on sync",
			InitialVirtualState: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: vObjectMeta,
					Spec: corev1.PersistentVolumeClaimSpec{
						DataSourceRef: &corev1.TypedObjectReference{
							APIGroup: &apiGroup,
							Kind:     "Backup",
							Name:     "my-backup",
						},
						VolumeName: "pvc-restored-vol",
					},
					Status: corev1.PersistentVolumeClaimStatus{
						Phase:       corev1.ClaimBound,
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Capacity: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("10Gi"),
						},
					},
				},
			},
			InitialPhysicalState: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: pObjectMeta,
					Spec: corev1.PersistentVolumeClaimSpec{
						DataSourceRef: &corev1.TypedObjectReference{
							APIGroup: &apiGroup,
							Kind:     "Backup",
							Name:     "my-backup",
						},
					},
					Status: corev1.PersistentVolumeClaimStatus{
						Phase: corev1.ClaimPending,
					},
				},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					&corev1.PersistentVolumeClaim{
						ObjectMeta: vObjectMeta,
						Spec: corev1.PersistentVolumeClaimSpec{
							DataSourceRef: &corev1.TypedObjectReference{
								APIGroup: &apiGroup,
								Kind:     "Backup",
								Name:     "my-backup",
							},
							VolumeName: "pvc-restored-vol",
						},
						Status: corev1.PersistentVolumeClaimStatus{
							Phase:       corev1.ClaimBound,
							AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
							Capacity: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("10Gi"),
							},
						},
					},
				},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					&corev1.PersistentVolumeClaim{
						ObjectMeta: pObjectMeta,
						Spec: corev1.PersistentVolumeClaimSpec{
							DataSourceRef: &corev1.TypedObjectReference{
								APIGroup: &apiGroup,
								Kind:     "Backup",
								Name:     "my-backup",
							},
						},
						Status: corev1.PersistentVolumeClaimStatus{
							Phase: corev1.ClaimPending,
						},
					},
				},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)

				vPVC := &corev1.PersistentVolumeClaim{}
				err := syncCtx.VirtualClient.Get(syncCtx, types.NamespacedName{
					Namespace: vObjectMeta.Namespace,
					Name:      vObjectMeta.Name,
				}, vPVC)
				assert.NilError(t, err)

				pPVC := &corev1.PersistentVolumeClaim{}
				err = syncCtx.HostClient.Get(syncCtx, types.NamespacedName{
					Namespace: pObjectMeta.Namespace,
					Name:      pObjectMeta.Name,
				}, pPVC)
				assert.NilError(t, err)

				_, err = syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					pPVC.DeepCopy(),
					pPVC.DeepCopy(),
					vPVC.DeepCopy(),
					vPVC.DeepCopy(),
				))
				assert.NilError(t, err)
			},
		},
		{
			Name: "VolumeSnapshot PVC still gets host status overwrite",
			InitialVirtualState: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "snapshot-pvc",
						Namespace: "testns",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						DataSourceRef: &corev1.TypedObjectReference{
							APIGroup: func() *string { s := "snapshot.storage.k8s.io"; return &s }(),
							Kind:     "VolumeSnapshot",
							Name:     "my-snapshot",
						},
					},
					Status: corev1.PersistentVolumeClaimStatus{
						Phase: corev1.ClaimPending,
					},
				},
			},
			InitialPhysicalState: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      translate.Default.HostName(nil, "snapshot-pvc", "testns").Name,
						Namespace: "test",
						Annotations: map[string]string{
							translate.NameAnnotation:          "snapshot-pvc",
							translate.NamespaceAnnotation:     "testns",
							translate.UIDAnnotation:           "",
							translate.KindAnnotation:          corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim").String(),
							translate.HostNamespaceAnnotation: "test",
							translate.HostNameAnnotation:      translate.Default.HostName(nil, "snapshot-pvc", "testns").Name,
						},
						Labels: map[string]string{
							translate.MarkerLabel:    translate.VClusterName,
							translate.NamespaceLabel: "testns",
						},
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						DataSourceRef: &corev1.TypedObjectReference{
							APIGroup: func() *string { s := "snapshot.storage.k8s.io"; return &s }(),
							Kind:     "VolumeSnapshot",
							Name:     "my-snapshot",
						},
					},
					Status: corev1.PersistentVolumeClaimStatus{
						Phase:       corev1.ClaimBound,
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					},
				},
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					&corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "snapshot-pvc",
							Namespace: "testns",
						},
						Spec: corev1.PersistentVolumeClaimSpec{
							DataSourceRef: &corev1.TypedObjectReference{
								APIGroup: func() *string { s := "snapshot.storage.k8s.io"; return &s }(),
								Kind:     "VolumeSnapshot",
								Name:     "my-snapshot",
							},
						},
						Status: corev1.PersistentVolumeClaimStatus{
							Phase:       corev1.ClaimBound,
							AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						},
					},
				},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					&corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name:      translate.Default.HostName(nil, "snapshot-pvc", "testns").Name,
							Namespace: "test",
							Annotations: map[string]string{
								translate.NameAnnotation:          "snapshot-pvc",
								translate.NamespaceAnnotation:     "testns",
								translate.UIDAnnotation:           "",
								translate.KindAnnotation:          corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim").String(),
								translate.HostNamespaceAnnotation: "test",
								translate.HostNameAnnotation:      translate.Default.HostName(nil, "snapshot-pvc", "testns").Name,
							},
							Labels: map[string]string{
								translate.MarkerLabel:    translate.VClusterName,
								translate.NamespaceLabel: "testns",
							},
						},
						Spec: corev1.PersistentVolumeClaimSpec{
							DataSourceRef: &corev1.TypedObjectReference{
								APIGroup: func() *string { s := "snapshot.storage.k8s.io"; return &s }(),
								Kind:     "VolumeSnapshot",
								Name:     "my-snapshot",
							},
						},
						Status: corev1.PersistentVolumeClaimStatus{
							Phase:       corev1.ClaimBound,
							AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						},
					},
				},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)

				vPVC := &corev1.PersistentVolumeClaim{}
				err := syncCtx.VirtualClient.Get(syncCtx, types.NamespacedName{
					Namespace: "testns",
					Name:      "snapshot-pvc",
				}, vPVC)
				assert.NilError(t, err)

				pPVC := &corev1.PersistentVolumeClaim{}
				err = syncCtx.HostClient.Get(syncCtx, types.NamespacedName{
					Namespace: "test",
					Name:      translate.Default.HostName(nil, "snapshot-pvc", "testns").Name,
				}, pPVC)
				assert.NilError(t, err)

				_, err = syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					pPVC.DeepCopy(),
					pPVC.DeepCopy(),
					vPVC.DeepCopy(),
					vPVC.DeepCopy(),
				))
				assert.NilError(t, err)
			},
		},
	})
}

func TestHasExternalPopulatorDataSource(t *testing.T) {
	apiGroup := "dataprotection.kubeblocks.io"
	snapshotGroup := "snapshot.storage.k8s.io"

	tests := []struct {
		name     string
		pvc      *corev1.PersistentVolumeClaim
		expected bool
	}{
		{
			name:     "nil dataSourceRef",
			pvc:      &corev1.PersistentVolumeClaim{},
			expected: false,
		},
		{
			name: "VolumeSnapshot kind",
			pvc: &corev1.PersistentVolumeClaim{
				Spec: corev1.PersistentVolumeClaimSpec{
					DataSourceRef: &corev1.TypedObjectReference{
						APIGroup: &snapshotGroup,
						Kind:     "VolumeSnapshot",
						Name:     "snap",
					},
				},
			},
			expected: false,
		},
		{
			name: "PersistentVolumeClaim kind",
			pvc: &corev1.PersistentVolumeClaim{
				Spec: corev1.PersistentVolumeClaimSpec{
					DataSourceRef: &corev1.TypedObjectReference{
						Kind: "PersistentVolumeClaim",
						Name: "source-pvc",
					},
				},
			},
			expected: false,
		},
		{
			name: "Backup kind",
			pvc: &corev1.PersistentVolumeClaim{
				Spec: corev1.PersistentVolumeClaimSpec{
					DataSourceRef: &corev1.TypedObjectReference{
						APIGroup: &apiGroup,
						Kind:     "Backup",
						Name:     "my-backup",
					},
				},
			},
			expected: true,
		},
		{
			name: "custom external populator kind",
			pvc: &corev1.PersistentVolumeClaim{
				Spec: corev1.PersistentVolumeClaimSpec{
					DataSourceRef: &corev1.TypedObjectReference{
						APIGroup: &apiGroup,
						Kind:     "CustomPopulator",
						Name:     "custom",
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, hasExternalPopulatorDataSource(tt.pvc), tt.expected)
		})
	}
}
