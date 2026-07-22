package persistentvolumeclaims

import (
	"fmt"
	"strings"
	"time"

	storagev1 "k8s.io/api/storage/v1"

	"github.com/pkg/errors"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"

	"github.com/loft-sh/vcluster/pkg/constants"
	"github.com/loft-sh/vcluster/pkg/controllers/resources/persistentvolumes"
	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/patcher"
	"github.com/loft-sh/vcluster/pkg/pro"
	"github.com/loft-sh/vcluster/pkg/syncer"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"github.com/loft-sh/vcluster/pkg/syncer/translator"
	syncertypes "github.com/loft-sh/vcluster/pkg/syncer/types"
	"github.com/loft-sh/vcluster/pkg/util/translate"

	"github.com/loft-sh/vcluster/pkg/snapshot"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/loft-sh/vcluster/pkg/util/loghelper"
)

var (
	// Default grace period in seconds
	minimumGracePeriodInSeconds int64 = 30
	zero                              = int64(0)
)

const (
	bindCompletedAnnotation      = "pv.kubernetes.io/bind-completed"
	boundByControllerAnnotation  = "pv.kubernetes.io/bound-by-controller"
	storageProvisionerAnnotation = "volume.beta.kubernetes.io/storage-provisioner"
	selectedNodeAnnotation       = "volume.kubernetes.io/selected-node"

	dataProtectionAPIGroup   = "dataprotection.kubeblocks.io"
	dataProtectionBackupKind = "Backup"

	externalPopulatorPopulateHelperPrefix = "kb-populate-"

	externalPopulatorRestoreConditionType              = corev1.PersistentVolumeClaimConditionType("Restore")
	externalPopulatorPopulateConditionType             = corev1.PersistentVolumeClaimConditionType("Populating")
	externalPopulatorRestoreConditionReasonProvisioned = "Provisioned"
	externalPopulatorRestoreConditionReasonSucceeded   = "Succeed"
	externalPopulatorRestoreConditionReasonProcessing  = "Processing"
	externalPopulatorNoDataRestoreMessage              = "Provisioning PVC without data restore"
	externalPopulatorNoDataRestoreBackoff              = 15 * time.Second

	externalPopulatorTopologyMismatchEventReason = "ExternalPopulatorTopologyMismatch"
	externalPopulatorTopologyNotReadyEventReason = "ExternalPopulatorTopologyNotReady"
	externalPopulatorTopologyEventAction         = "SyncPersistentVolumeClaim"
)

func New(ctx *synccontext.RegisterContext) (syncertypes.Object, error) {
	mapper, err := ctx.Mappings.ByGVK(mappings.PersistentVolumeClaims())
	if err != nil {
		return nil, err
	}

	return &persistentVolumeClaimSyncer{
		GenericTranslator: translator.NewGenericTranslator(ctx, "persistent-volume-claim", &corev1.PersistentVolumeClaim{}, mapper),
		Importer:          pro.NewImporter(mapper),

		excludedAnnotations: []string{bindCompletedAnnotation, boundByControllerAnnotation, storageProvisionerAnnotation},

		storageClassesEnabled:    ctx.Config.Sync.ToHost.StorageClasses.Enabled,
		schedulerEnabled:         ctx.Config.SchedulingInVirtualClusterEnabled(),
		useFakePersistentVolumes: !ctx.Config.Sync.ToHost.PersistentVolumes.Enabled,
	}, nil
}

type persistentVolumeClaimSyncer struct {
	syncertypes.GenericTranslator
	syncertypes.Importer

	excludedAnnotations []string

	storageClassesEnabled    bool
	schedulerEnabled         bool
	useFakePersistentVolumes bool
}

var _ syncertypes.OptionsProvider = &persistentVolumeClaimSyncer{}

func (s *persistentVolumeClaimSyncer) Options() *syncertypes.Options {
	return &syncertypes.Options{
		DisableUIDDeletion: true,
		ObjectCaching:      true,
	}
}

var _ syncertypes.Syncer = &persistentVolumeClaimSyncer{}

func (s *persistentVolumeClaimSyncer) Syncer() syncertypes.Sync[client.Object] {
	return syncer.ToGenericSyncer(s)
}

func (s *persistentVolumeClaimSyncer) SyncToHost(ctx *synccontext.SyncContext, event *synccontext.SyncToHostEvent[*corev1.PersistentVolumeClaim]) (ctrl.Result, error) {
	// check if host PVC is currently being restored
	pObjName := s.VirtualToHost(ctx, types.NamespacedName{Name: event.Virtual.GetName(), Namespace: event.Virtual.GetNamespace()}, event.Virtual)
	restoreInProgress, err := s.isHostVolumeRestoreInProgress(ctx, pObjName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check if host volume restore is in progress: %w", err)
	}
	if restoreInProgress {
		return ctrl.Result{
			RequeueAfter: 15 * time.Second,
		}, nil
	}

	if s.applyLimitByClass(ctx, event.Virtual) {
		return ctrl.Result{}, nil
	}

	preserveDeletingHostPVC, err := s.shouldPreserveExternalPopulatorNoDataRestorePVCWhileHostDeleting(ctx, event.Virtual, event.HostOld)
	if err != nil {
		return ctrl.Result{}, err
	}
	if event.HostOld != nil && preserveDeletingHostPVC && event.Virtual.DeletionTimestamp == nil {
		// The host PVC was intentionally deleted so it can be recreated without
		// the external dataSource. Keep the virtual restore PVC and continue into
		// the create path below.
	} else if event.HostOld != nil || event.Virtual.DeletionTimestamp != nil {
		return patcher.DeleteVirtualObjectWithOptions(ctx, event.Virtual, event.HostOld, "host object was deleted", &client.DeleteOptions{
			GracePeriodSeconds: &zero,
		})
	}

	pObj, handled, err := s.translateExternalPopulatorNoDataRestoreToHost(ctx, event.Virtual)
	if err != nil {
		s.EventRecorder().Eventf(
			event.Virtual,
			nil,
			"Warning",
			"SyncError",
			fmt.Sprintf("Sync%s", event.Virtual.GetObjectKind().GroupVersionKind().Kind),
			err.Error(),
		)
		return ctrl.Result{}, err
	}
	if handled {
		err = pro.ApplyPatchesHostObject(ctx, nil, pObj, event.Virtual, ctx.Config.Sync.ToHost.PersistentVolumeClaims.Patches, false)
		if err != nil {
			return ctrl.Result{}, err
		}
		blocked, err := s.shouldBlockExternalPopulatorHostPVCUntilGuestBackupMaterialized(ctx, event.Virtual, pObj)
		if err != nil || blocked {
			return ctrl.Result{RequeueAfter: externalPopulatorNoDataRestoreBackoff}, err
		}

		return patcher.CreateHostObject(ctx, event.Virtual, pObj, s.EventRecorder(), true)
	}

	pObj, err = s.translate(ctx, event.Virtual)
	if err != nil {
		s.EventRecorder().Eventf(
			event.Virtual,
			nil,
			"Warning",
			"SyncError",
			fmt.Sprintf("Sync%s", event.Virtual.GetObjectKind().GroupVersionKind().Kind),
			err.Error(),
		)
		return ctrl.Result{}, err
	}

	err = pro.ApplyPatchesHostObject(ctx, nil, pObj, event.Virtual, ctx.Config.Sync.ToHost.PersistentVolumeClaims.Patches, false)
	if err != nil {
		return ctrl.Result{}, err
	}
	blocked, err := s.shouldBlockExternalPopulatorHostPVCUntilGuestBackupMaterialized(ctx, event.Virtual, pObj)
	if err != nil || blocked {
		return ctrl.Result{RequeueAfter: externalPopulatorNoDataRestoreBackoff}, err
	}

	return patcher.CreateHostObject(ctx, event.Virtual, pObj, s.EventRecorder(), true)
}

func (s *persistentVolumeClaimSyncer) Sync(ctx *synccontext.SyncContext, event *synccontext.SyncEvent[*corev1.PersistentVolumeClaim]) (result ctrl.Result, retErr error) {
	if s.applyLimitByClass(ctx, event.Virtual) {
		return ctrl.Result{}, nil
	}

	// check if host PVC is currently being restored
	hostObjName := types.NamespacedName{
		Name:      event.Host.GetName(),
		Namespace: event.Host.GetNamespace(),
	}
	restoreInProgress, err := s.isHostVolumeRestoreInProgress(ctx, hostObjName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check if host volume restore is in progress: %w", err)
	}
	if restoreInProgress {
		return ctrl.Result{
			RequeueAfter: 15 * time.Second,
		}, nil
	}

	// if pvs are deleted check the corresponding pvc is deleted as well
	if event.Host.DeletionTimestamp != nil {
		preserveDeletingHostPVC, err := s.shouldPreserveExternalPopulatorNoDataRestorePVCWhileHostDeleting(ctx, event.Virtual, event.Host)
		if err != nil {
			return ctrl.Result{}, err
		}
		if preserveDeletingHostPVC {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		if event.Virtual.DeletionTimestamp == nil {
			return patcher.DeleteVirtualObjectWithOptions(ctx, event.Virtual, event.Host, "host persistent volume claim is being deleted", &client.DeleteOptions{GracePeriodSeconds: &minimumGracePeriodInSeconds})
		} else if *event.Virtual.DeletionGracePeriodSeconds != *event.Host.DeletionGracePeriodSeconds {
			return patcher.DeleteVirtualObjectWithOptions(ctx, event.Virtual, event.Host, fmt.Sprintf("with grace period seconds %v", *event.Host.DeletionGracePeriodSeconds), &client.DeleteOptions{GracePeriodSeconds: event.Host.DeletionGracePeriodSeconds, Preconditions: metav1.NewUIDPreconditions(string(event.Virtual.UID))})
		}

		return ctrl.Result{}, nil
	} else if event.Virtual.DeletionTimestamp != nil {
		preserveDeletingHostPVC, err := s.shouldPreserveDeletingExternalPopulatorPopulateHelper(ctx, event.Virtual, event.Host)
		if err != nil {
			return ctrl.Result{}, err
		}
		if preserveDeletingHostPVC {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}

		return patcher.DeleteHostObjectWithOptions(ctx, event.Host, event.Virtual, "virtual persistent volume claim is being deleted", &client.DeleteOptions{
			GracePeriodSeconds: event.Virtual.DeletionGracePeriodSeconds,
			Preconditions:      metav1.NewUIDPreconditions(string(event.Host.UID)),
		})
	}
	backoffHostPVC, err := s.shouldBackoffExternalPopulatorHostNoDataRestorePVC(ctx, event.Host, event.Virtual)
	if err != nil {
		return ctrl.Result{}, err
	}
	if backoffHostPVC {
		logExternalPopulatorNoDataRestoreBackoff(ctx.Log, event.Host, event.Virtual)
		return ctrl.Result{RequeueAfter: externalPopulatorNoDataRestoreBackoff}, nil
	}
	waitForPopulateHelper, err := s.shouldWaitForExternalPopulatorHelperAbsence(ctx, event.Virtual)
	if err != nil {
		return ctrl.Result{}, err
	}
	if waitForPopulateHelper {
		ctx.Log.Infof("wait for virtual populate helper to be absent before reconciling host target pvc: guestPVC=%s/%s guestUID=%s hostPVC=%s/%s", event.Virtual.Namespace, event.Virtual.Name, event.Virtual.UID, event.Host.Namespace, event.Host.Name)
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// make sure the persistent volume is synced / faked
	if event.Host.Spec.VolumeName != "" {
		requeue, err := s.ensurePersistentVolume(ctx, event.Host, event.Virtual, ctx.Log)
		if err != nil {
			return ctrl.Result{}, err
		} else if requeue {
			return ctrl.Result{Requeue: true}, nil
		}
	}
	// patch objects
	patch, err := patcher.NewSyncerPatcher(ctx, event.Host, event.Virtual, patcher.TranslatePatches(ctx.Config.Sync.ToHost.PersistentVolumeClaims.Patches, false))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("new syncer patcher: %w", err)
	}
	defer func() {
		if err := patch.Patch(ctx, event.Host, event.Virtual); err != nil {
			retErr = utilerrors.NewAggregate([]error{retErr, err})
		}

		if kerrors.IsConflict(retErr) {
			result = ctrl.Result{RequeueAfter: time.Second}
			retErr = nil
			return
		}

		if retErr != nil {
			s.EventRecorder().Eventf(
				event.Virtual,
				nil,
				"Warning",
				"SyncError",
				fmt.Sprintf("Sync%s", event.Virtual.GetObjectKind().GroupVersionKind().Kind),
				"Error syncing: %v",
				retErr,
			)
		}
	}()

	// check backwards update
	s.translateUpdateBackwards(event.Host, event.Virtual)

	// copy host status
	vPV, preserveVirtualStatus, err := s.externalPopulatorPersistentVolume(ctx, event.Host, event.Virtual)
	if err != nil {
		return ctrl.Result{}, err
	}
	if preserveVirtualStatus {
		hostConverged, err := s.ensureExternalPopulatorHostMaterialization(ctx, event.Host, event.Virtual, vPV)
		if err != nil {
			return ctrl.Result{}, err
		}
		if hostConverged {
			ensureExternalPopulatorVirtualPopulateStatus(event.Virtual, vPV)
		} else {
			// the populated host PV is missing or its claimRef has not been handed off
			// to the target PVC yet; do NOT report the virtual PVC as Bound, keep it
			// pending so the real state stays visible and retry
			result = ctrl.Result{RequeueAfter: 2 * time.Second}
		}
	} else {
		preserveExternalPopulatorStatus, err := s.shouldPreserveExternalPopulatorVirtualStatus(ctx, event.Host, event.Virtual)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !preserveExternalPopulatorStatus {
			event.Virtual.Status = *event.Host.Status.DeepCopy()
		}
	}

	// allow storage size to be increased
	event.Host.Spec.Resources.Requests = event.Virtual.Spec.Resources.Requests

	// bi-directional sync of annotations and labels
	event.Virtual.Annotations, event.Host.Annotations = translate.AnnotationsBidirectionalUpdate(event, s.excludedAnnotations...)
	event.Virtual.Labels, event.Host.Labels = translate.LabelsBidirectionalUpdate(event)

	return result, nil
}

func (s *persistentVolumeClaimSyncer) SyncToVirtual(ctx *synccontext.SyncContext, event *synccontext.SyncToVirtualEvent[*corev1.PersistentVolumeClaim]) (_ ctrl.Result, retErr error) {
	if event.VirtualOld != nil || translate.ShouldDeleteHostObject(event.Host) {
		// the virtual populate helper PVC can be fully gone (finalizer removed) while
		// the populated host PV still has to be handed off to the target PVC; deleting
		// the host helper PVC in that window releases and possibly reclaims the PV
		preserveOrphanedHelper, err := s.shouldPreserveOrphanedExternalPopulatorPopulateHelperHost(ctx, event.Host)
		if err != nil {
			return ctrl.Result{}, err
		}
		if preserveOrphanedHelper {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}

		// virtual object is not here anymore, so we delete
		return patcher.DeleteHostObject(ctx, event.Host, event.VirtualOld, "virtual object was deleted")
	}

	vPvc := translate.VirtualMetadata(event.Host, s.HostToVirtual(ctx, types.NamespacedName{Name: event.Host.Name, Namespace: event.Host.Namespace}, event.Host), s.excludedAnnotations...)
	err := pro.ApplyPatchesVirtualObject(ctx, nil, vPvc, event.Host, ctx.Config.Sync.ToHost.PersistentVolumeClaims.Patches, false)
	if err != nil {
		return ctrl.Result{}, err
	}

	return patcher.CreateVirtualObject(ctx, event.Host, vPvc, s.EventRecorder(), true)
}

func (s *persistentVolumeClaimSyncer) translateExternalPopulatorNoDataRestoreToHost(ctx *synccontext.SyncContext, vObj *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, bool, error) {
	noDataRestore, err := s.isExternalPopulatorNoDataRestorePVC(ctx, vObj)
	if err != nil {
		return nil, true, err
	}
	if !noDataRestore {
		return nil, false, nil
	}

	pObj, err := s.translate(ctx, vObj)
	if err != nil {
		return nil, true, err
	}
	pObj.Spec.VolumeName = ""
	return pObj, true, nil
}

func (s *persistentVolumeClaimSyncer) ensurePersistentVolume(ctx *synccontext.SyncContext, pObj *corev1.PersistentVolumeClaim, vObj *corev1.PersistentVolumeClaim, log loghelper.Logger) (bool, error) {
	// ensure the persistent volume is available in the virtual cluster
	vPV := &corev1.PersistentVolume{}
	err := ctx.VirtualClient.Get(ctx, types.NamespacedName{Name: pObj.Spec.VolumeName}, vPV)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			log.Infof("error retrieving virtual pv %s: %v", pObj.Spec.VolumeName, err)
			return false, err
		}
	}

	if pObj.Spec.VolumeName != "" && vObj.Spec.VolumeName != pObj.Spec.VolumeName {
		newVolumeName := pObj.Spec.VolumeName
		if !s.useFakePersistentVolumes {
			vName := mappings.HostToVirtual(ctx, pObj.Spec.VolumeName, "", nil, mappings.PersistentVolumes())
			if vName.Name == "" {
				log.Infof("error retrieving virtual persistent volume %s: not found", pObj.Spec.VolumeName)
				return false, fmt.Errorf("error retrieving virtual persistent volume %s: not found", pObj.Spec.VolumeName)
			}

			newVolumeName = vName.Name
		}

		if newVolumeName != vObj.Spec.VolumeName {
			if vObj.Spec.VolumeName != "" {
				log.Infof("recreate persistent volume claim because volumeName differs between physical and virtual pvc: %s != %s", vObj.Spec.VolumeName, newVolumeName)
				s.EventRecorder().Eventf(
					vObj,
					nil,
					corev1.EventTypeWarning,
					"VolumeNameDiffers",
					fmt.Sprintf("Sync%s", vObj.GetObjectKind().GroupVersionKind().Kind),
					"recreate persistent volume claim because volumeName differs between physical and virtual pvc: %s != %s",
					vObj.Spec.VolumeName,
					newVolumeName,
				)
				_, err = recreatePersistentVolumeClaim(ctx, ctx.VirtualClient, vPV, vObj, newVolumeName, log)
				if err != nil {
					log.Infof("error recreating virtual persistent volume claim: %v", err)
					return false, err
				}

				return true, nil
			}

			log.Infof("update virtual pvc %s/%s volume name to %s", vObj.Namespace, vObj.Name, newVolumeName)
			vObj.Spec.VolumeName = newVolumeName
			err = ctx.VirtualClient.Update(ctx, vObj)
			if err != nil {
				return false, err
			}

			// The direct update changes the virtual PVC resourceVersion. Stop this
			// reconcile here so the following status patch uses a fresh object.
			return true, nil
		}
	}

	return false, nil
}

// ensureExternalPopulatorHostMaterialization returns true only when the populated host
// PV exists and its claimRef references the host target PVC. Callers must not report the
// virtual PVC as Bound before that: a missing host PV means there is no volume behind
// the claim (e.g. the populate helper was released and the PV got reclaimed), and there
// is no host-side consumer that could re-materialize it on demand.
func (s *persistentVolumeClaimSyncer) ensureExternalPopulatorHostMaterialization(ctx *synccontext.SyncContext, pObj, vObj *corev1.PersistentVolumeClaim, vPV *corev1.PersistentVolume) (bool, error) {
	hostPVName := s.externalPopulatorHostPersistentVolumeName(ctx, vPV)
	hostPV := &corev1.PersistentVolume{}
	err := ctx.HostClient.Get(ctx.Context, types.NamespacedName{Name: hostPVName}, hostPV)
	if err != nil {
		if kerrors.IsNotFound(err) {
			ctx.Log.Infof("populated host pv %s for external populator pvc %s/%s not found, keeping virtual pvc pending", hostPVName, vObj.Namespace, vObj.Name)
			s.recordExternalPopulatorTopologyEvent(
				vObj,
				externalPopulatorTopologyNotReadyEventReason,
				"External-populator handoff is waiting because populated host PV %q is not available",
				hostPVName,
			)
			return false, nil
		}
		return false, err
	}
	currentTarget, targetReady, err := s.externalPopulatorCurrentHostTarget(ctx, pObj, vObj, hostPVName)
	if err != nil || !targetReady {
		return false, err
	}
	*pObj = *currentTarget
	if pObj.Spec.VolumeName == hostPVName && claimRefMatchesPersistentVolumeClaim(hostPV.Spec.ClaimRef, pObj) {
		return true, nil
	}

	helperPVC, helperFound, err := s.findExternalPopulatorHelperPVC(ctx, vObj, vPV)
	if err != nil {
		return false, err
	}
	topologyReady, err := s.externalPopulatorHandoffTopologyReady(ctx, hostPV, pObj, vObj, helperPVC, helperFound)
	if err != nil || !topologyReady {
		return false, err
	}
	currentTarget, targetReady, err = s.externalPopulatorCurrentHostTarget(ctx, pObj, vObj, hostPVName)
	if err != nil || !targetReady {
		return false, err
	}
	*pObj = *currentTarget

	claimRefReady, err := s.ensureExternalPopulatorHostPVClaimRef(ctx, hostPV, pObj, vObj, helperPVC, helperFound)
	if err != nil || !claimRefReady {
		return false, err
	}

	// A helper-to-target claimRef patch and a target PVC patch cannot be one
	// Kubernetes transaction. Re-read and re-run the full topology gate after
	// the claimRef commit so a stale/changed PV, helper, or Node snapshot cannot
	// be carried into the target volumeName commit.
	currentHostPV := &corev1.PersistentVolume{}
	err = ctx.HostClient.Get(ctx.Context, types.NamespacedName{Name: hostPVName}, currentHostPV)
	if err != nil {
		if kerrors.IsNotFound(err) {
			s.recordExternalPopulatorTopologyEvent(
				vObj,
				externalPopulatorTopologyNotReadyEventReason,
				"External-populator handoff is waiting because populated host PV %q disappeared before target materialization",
				hostPVName,
			)
			return false, nil
		}
		return false, err
	}
	if !claimRefMatchesPersistentVolumeClaim(currentHostPV.Spec.ClaimRef, pObj) {
		s.recordExternalPopulatorTopologyEvent(
			vObj,
			externalPopulatorTopologyNotReadyEventReason,
			"External-populator handoff is waiting because host PV %q claimRef changed before target materialization",
			hostPVName,
		)
		return false, nil
	}
	topologyReady, err = s.externalPopulatorHandoffTopologyReady(ctx, currentHostPV, pObj, vObj, helperPVC, helperFound)
	if err != nil || !topologyReady {
		return false, err
	}

	if shouldKeepExternalPopulatorHostPVCSpecImmutable(pObj, vObj) {
		return true, nil
	}
	return s.ensureExternalPopulatorHostTargetVolumeName(ctx, pObj, vObj, hostPVName)
}

func (s *persistentVolumeClaimSyncer) externalPopulatorHostPersistentVolumeName(ctx *synccontext.SyncContext, vPV *corev1.PersistentVolume) string {
	if s.useFakePersistentVolumes {
		return vPV.Name
	}

	return mappings.VirtualToHostName(ctx, vPV.Name, "", mappings.PersistentVolumes())
}

func (s *persistentVolumeClaimSyncer) findExternalPopulatorHelperPVC(ctx *synccontext.SyncContext, vObj *corev1.PersistentVolumeClaim, vPV *corev1.PersistentVolume) (*corev1.PersistentVolumeClaim, bool, error) {
	pvcList := &corev1.PersistentVolumeClaimList{}
	err := ctx.VirtualClient.List(ctx.Context, pvcList, client.InNamespace(vObj.Namespace))
	if err != nil {
		return nil, false, err
	}

	var match *corev1.PersistentVolumeClaim
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if pvc.Name == vObj.Name || pvc.Spec.VolumeName != vPV.Name {
			continue
		}
		if match != nil && match.Name != pvc.Name {
			return nil, false, fmt.Errorf("multiple external populator helper persistent volume claims match pv %s for pvc %s/%s", vPV.Name, vObj.Namespace, vObj.Name)
		}
		match = pvc.DeepCopy()
	}
	if match == nil {
		return nil, false, nil
	}

	return match, true, nil
}

func (s *persistentVolumeClaimSyncer) externalPopulatorHandoffTopologyReady(
	ctx *synccontext.SyncContext,
	hostPV *corev1.PersistentVolume,
	pObj, vObj, helperPVC *corev1.PersistentVolumeClaim,
	helperFound bool,
) (bool, error) {
	targetSelectedNode := pObj.Annotations[selectedNodeAnnotation]
	if targetSelectedNode == "" {
		waitForFirstConsumer, storageClassName, storageClassFound, err := s.externalPopulatorTargetWaitForFirstConsumer(ctx, vObj)
		if err != nil {
			return false, err
		}
		if !storageClassFound {
			s.recordExternalPopulatorTopologyEvent(
				vObj,
				externalPopulatorTopologyNotReadyEventReason,
				"External-populator handoff is waiting because guest StorageClass %q is not available to classify an empty %s annotation",
				storageClassName,
				selectedNodeAnnotation,
			)
			return false, nil
		}
		if waitForFirstConsumer {
			s.recordExternalPopulatorTopologyEvent(
				vObj,
				externalPopulatorTopologyNotReadyEventReason,
				"External-populator handoff is waiting because guest StorageClass %q uses WaitForFirstConsumer and host target PVC %s/%s has no %s annotation",
				storageClassName,
				pObj.Namespace,
				pObj.Name,
				selectedNodeAnnotation,
			)
			return false, nil
		}

		// An empty selected-node is legal for Immediate/static and unconstrained
		// shared volumes. Do not reinterpret this sentinel as WFFC-not-ready.
		return true, nil
	}

	hostHelperPVC, hostHelperFound, err := s.externalPopulatorHostHelperPVC(ctx, hostPV, pObj, vObj, helperPVC, helperFound)
	if err != nil {
		return false, err
	}
	if !hostHelperFound {
		s.recordExternalPopulatorTopologyEvent(
			vObj,
			externalPopulatorTopologyNotReadyEventReason,
			"External-populator handoff to target selected-node %q is waiting because the host populate helper PVC is not available",
			targetSelectedNode,
		)
		return false, nil
	}

	helperSelectedNode := hostHelperPVC.Annotations[selectedNodeAnnotation]
	if helperSelectedNode == "" {
		s.recordExternalPopulatorTopologyEvent(
			vObj,
			externalPopulatorTopologyNotReadyEventReason,
			"External-populator handoff to target selected-node %q is waiting because host helper PVC %s/%s has no %s annotation",
			targetSelectedNode,
			hostHelperPVC.Namespace,
			hostHelperPVC.Name,
			selectedNodeAnnotation,
		)
		return false, nil
	}
	if helperSelectedNode != targetSelectedNode {
		s.recordExternalPopulatorTopologyEvent(
			vObj,
			externalPopulatorTopologyMismatchEventReason,
			"External-populator handoff rejected: host target selected-node %q does not match host helper PVC %s/%s selected-node %q",
			targetSelectedNode,
			hostHelperPVC.Namespace,
			hostHelperPVC.Name,
			helperSelectedNode,
		)
		return false, nil
	}

	if hostPV.Spec.NodeAffinity == nil || hostPV.Spec.NodeAffinity.Required == nil {
		return true, nil
	}

	hostNode := &corev1.Node{}
	err = ctx.HostClient.Get(ctx.Context, types.NamespacedName{Name: targetSelectedNode}, hostNode)
	if err != nil {
		if kerrors.IsNotFound(err) {
			s.recordExternalPopulatorTopologyEvent(
				vObj,
				externalPopulatorTopologyNotReadyEventReason,
				"External-populator handoff to target selected-node %q is waiting because that host Node is not available",
				targetSelectedNode,
			)
			return false, nil
		}
		return false, fmt.Errorf("get host target Node %q for external-populator handoff: %w", targetSelectedNode, err)
	}

	matches, err := nodeaffinity.NewLazyErrorNodeSelector(hostPV.Spec.NodeAffinity.Required).Match(hostNode)
	if err != nil {
		s.recordExternalPopulatorTopologyEvent(
			vObj,
			externalPopulatorTopologyMismatchEventReason,
			"External-populator handoff rejected because host PV %s has invalid required node affinity: %v",
			hostPV.Name,
			err,
		)
		return false, nil
	}
	if !matches {
		s.recordExternalPopulatorTopologyEvent(
			vObj,
			externalPopulatorTopologyMismatchEventReason,
			"External-populator handoff rejected: host PV %s required node affinity %v does not accept target selected-node %q",
			hostPV.Name,
			hostPV.Spec.NodeAffinity.Required,
			targetSelectedNode,
		)
		return false, nil
	}

	return true, nil
}

func (s *persistentVolumeClaimSyncer) externalPopulatorTargetWaitForFirstConsumer(ctx *synccontext.SyncContext, vObj *corev1.PersistentVolumeClaim) (bool, string, bool, error) {
	storageClassName := ""
	if vObj.Spec.StorageClassName != nil {
		storageClassName = *vObj.Spec.StorageClassName
	}
	if storageClassName == "" {
		storageClassName = vObj.Annotations[deprecatedStorageClassAnnotation]
	}
	if storageClassName == "" {
		return false, "", true, nil
	}

	storageClass := &storagev1.StorageClass{}
	err := ctx.VirtualClient.Get(ctx.Context, types.NamespacedName{Name: storageClassName}, storageClass)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return false, storageClassName, false, nil
		}
		return false, storageClassName, false, fmt.Errorf("get guest StorageClass %q for external-populator handoff: %w", storageClassName, err)
	}
	if storageClass.DeletionTimestamp != nil {
		return false, storageClassName, false, nil
	}
	if storageClass.VolumeBindingMode == nil || *storageClass.VolumeBindingMode == storagev1.VolumeBindingImmediate {
		return false, storageClassName, true, nil
	}
	if *storageClass.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
		return true, storageClassName, true, nil
	}

	return false, storageClassName, true, fmt.Errorf("guest StorageClass %q has unsupported volumeBindingMode %q", storageClassName, *storageClass.VolumeBindingMode)
}

func (s *persistentVolumeClaimSyncer) externalPopulatorHostHelperPVC(
	ctx *synccontext.SyncContext,
	hostPV *corev1.PersistentVolume,
	pObj, vObj, helperPVC *corev1.PersistentVolumeClaim,
	helperFound bool,
) (*corev1.PersistentVolumeClaim, bool, error) {
	ref := hostPV.Spec.ClaimRef
	hostHelperName := types.NamespacedName{}
	if helperFound {
		hostHelperName = s.VirtualToHost(ctx, types.NamespacedName{Name: helperPVC.Name, Namespace: helperPVC.Namespace}, helperPVC)
		if ref != nil && !claimRefReferencesPersistentVolumeClaim(ref, pObj) && (ref.Namespace != hostHelperName.Namespace || ref.Name != hostHelperName.Name) {
			return nil, false, fmt.Errorf("host pv %s claimRef %s/%s does not match target pvc %s/%s or expected populate helper pvc %s/%s", hostPV.Name, ref.Namespace, ref.Name, pObj.Namespace, pObj.Name, helperPVC.Namespace, helperPVC.Name)
		}
	} else {
		expectedHostHelperName, expectedHelperFound := s.externalPopulatorExpectedHostHelperName(ctx, vObj)
		if !expectedHelperFound {
			return nil, false, nil
		}
		if ref != nil && !claimRefReferencesPersistentVolumeClaim(ref, pObj) && (ref.Namespace != expectedHostHelperName.Namespace || ref.Name != expectedHostHelperName.Name) {
			return nil, false, fmt.Errorf("host pv %s is bound to %s/%s, but no virtual populate helper pvc was found for target pvc %s/%s", hostPV.Name, ref.Namespace, ref.Name, pObj.Namespace, pObj.Name)
		}
		hostHelperName = expectedHostHelperName
	}

	hostHelperPVC := &corev1.PersistentVolumeClaim{}
	err := ctx.HostClient.Get(ctx.Context, hostHelperName, hostHelperPVC)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if ref != nil && !claimRefReferencesPersistentVolumeClaim(ref, pObj) && ref.UID != "" && ref.UID != hostHelperPVC.UID {
		return nil, false, fmt.Errorf("host pv %s claimRef UID %s does not match host populate helper pvc %s/%s UID %s", hostPV.Name, ref.UID, hostHelperPVC.Namespace, hostHelperPVC.Name, hostHelperPVC.UID)
	}

	return hostHelperPVC, true, nil
}

func (s *persistentVolumeClaimSyncer) recordExternalPopulatorTopologyEvent(vObj *corev1.PersistentVolumeClaim, reason, note string, args ...interface{}) {
	s.EventRecorder().Eventf(
		vObj,
		nil,
		"Warning",
		reason,
		externalPopulatorTopologyEventAction,
		note,
		args...,
	)
}

func (s *persistentVolumeClaimSyncer) externalPopulatorCurrentHostTarget(
	ctx *synccontext.SyncContext,
	pObj, vObj *corev1.PersistentVolumeClaim,
	hostPVName string,
) (*corev1.PersistentVolumeClaim, bool, error) {
	current := &corev1.PersistentVolumeClaim{}
	err := ctx.HostClient.Get(ctx.Context, types.NamespacedName{Namespace: pObj.Namespace, Name: pObj.Name}, current)
	if err != nil {
		if kerrors.IsNotFound(err) {
			s.recordExternalPopulatorTopologyEvent(
				vObj,
				externalPopulatorTopologyNotReadyEventReason,
				"External-populator handoff is waiting because host target PVC %s/%s is not available",
				pObj.Namespace,
				pObj.Name,
			)
			return nil, false, nil
		}
		return nil, false, err
	}
	if current.UID != pObj.UID || current.DeletionTimestamp != nil {
		s.recordExternalPopulatorTopologyEvent(
			vObj,
			externalPopulatorTopologyNotReadyEventReason,
			"External-populator handoff is waiting because host target PVC %s/%s was replaced or is terminating",
			pObj.Namespace,
			pObj.Name,
		)
		return nil, false, nil
	}

	gatedSelectedNode := pObj.Annotations[selectedNodeAnnotation]
	currentSelectedNode := current.Annotations[selectedNodeAnnotation]
	if currentSelectedNode != gatedSelectedNode {
		s.recordExternalPopulatorTopologyEvent(
			vObj,
			externalPopulatorTopologyMismatchEventReason,
			"External-populator handoff rejected because host target PVC selected-node changed from %q to %q before target materialization",
			gatedSelectedNode,
			currentSelectedNode,
		)
		return nil, false, nil
	}
	if current.Spec.VolumeName != "" && current.Spec.VolumeName != hostPVName {
		s.recordExternalPopulatorTopologyEvent(
			vObj,
			externalPopulatorTopologyMismatchEventReason,
			"External-populator handoff rejected because host target PVC %s/%s already references volume %q instead of %q",
			current.Namespace,
			current.Name,
			current.Spec.VolumeName,
			hostPVName,
		)
		return nil, false, nil
	}
	if current.Spec.VolumeName == "" && !isHostPVCWaitingForVolume(current) {
		s.recordExternalPopulatorTopologyEvent(
			vObj,
			externalPopulatorTopologyNotReadyEventReason,
			"External-populator handoff is waiting because host target PVC %s/%s is no longer waiting for a volume",
			current.Namespace,
			current.Name,
		)
		return nil, false, nil
	}

	return current, true, nil
}

// ensureExternalPopulatorHostTargetVolumeName commits the safety-critical
// target binding against a fresh host PVC snapshot. The explicit selected-node
// comparison catches a changed cached object before the patch, and the
// optimistic lock catches a change between the GET and PATCH.
func (s *persistentVolumeClaimSyncer) ensureExternalPopulatorHostTargetVolumeName(
	ctx *synccontext.SyncContext,
	pObj, vObj *corev1.PersistentVolumeClaim,
	hostPVName string,
) (bool, error) {
	current, ready, err := s.externalPopulatorCurrentHostTarget(ctx, pObj, vObj, hostPVName)
	if err != nil || !ready {
		return false, err
	}
	if current.Spec.VolumeName == hostPVName {
		*pObj = *current
		return true, nil
	}

	updated := current.DeepCopy()
	updated.Spec.VolumeName = hostPVName
	err = ctx.HostClient.Patch(ctx.Context, updated, client.MergeFromWithOptions(current, client.MergeFromWithOptimisticLock{}))
	if err != nil {
		return false, err
	}
	*pObj = *updated
	return true, nil
}

// ensureExternalPopulatorHostPVClaimRef returns true when the host PV claimRef
// references the host target PVC, either already or after a successful handoff patch.
// The caller passes the same PV snapshot used by the topology gate so an intervening
// update is rejected by the optimistic-lock patch instead of bypassing validation.
func (s *persistentVolumeClaimSyncer) ensureExternalPopulatorHostPVClaimRef(ctx *synccontext.SyncContext, hostPV *corev1.PersistentVolume, pObj, vObj *corev1.PersistentVolumeClaim, helperPVC *corev1.PersistentVolumeClaim, helperFound bool) (bool, error) {
	hostPVName := hostPV.Name
	targetRef := &corev1.ObjectReference{
		APIVersion: corev1.SchemeGroupVersion.Version,
		Kind:       "PersistentVolumeClaim",
		Namespace:  pObj.Namespace,
		Name:       pObj.Name,
		UID:        pObj.UID,
	}
	if helperFound {
		if hostPV.Spec.ClaimRef == nil {
			return false, fmt.Errorf("host pv %s has no claimRef while virtual populate helper pvc %s/%s exists", hostPVName, helperPVC.Namespace, helperPVC.Name)
		}
		if claimRefReferencesPersistentVolumeClaim(hostPV.Spec.ClaimRef, pObj) && hostPV.Spec.ClaimRef.UID == pObj.UID {
			ctx.Log.Infof("wait for virtual populate helper to disappear before accepting host pv target handoff: hostPV=%s targetPVC=%s/%s helperPVC=%s/%s", hostPVName, pObj.Namespace, pObj.Name, helperPVC.Namespace, helperPVC.Name)
			return false, nil
		}

		ok, err := s.hostPVClaimRefMatchesVirtualPVC(ctx, hostPV.Spec.ClaimRef, helperPVC)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, fmt.Errorf("host pv %s claimRef %s/%s does not match target pvc %s/%s or expected populate helper pvc %s/%s", hostPVName, hostPV.Spec.ClaimRef.Namespace, hostPV.Spec.ClaimRef.Name, pObj.Namespace, pObj.Name, helperPVC.Namespace, helperPVC.Name)
		}

		return false, nil
	}

	exactTargetClaimRef := claimRefReferencesPersistentVolumeClaim(hostPV.Spec.ClaimRef, pObj) && hostPV.Spec.ClaimRef.UID == pObj.UID
	if !claimRefReferencesPersistentVolumeClaim(hostPV.Spec.ClaimRef, pObj) && hostPV.Spec.ClaimRef != nil {
		if !s.hostPVClaimRefMatchesExpectedPopulateHelper(ctx, hostPV.Spec.ClaimRef, vObj) {
			return false, fmt.Errorf("host pv %s is bound to %s/%s, but no virtual populate helper pvc was found for target pvc %s/%s", hostPVName, hostPV.Spec.ClaimRef.Namespace, hostPV.Spec.ClaimRef.Name, pObj.Namespace, pObj.Name)
		}
	}

	if ctx.VirtualAPIReader == nil {
		return false, fmt.Errorf("virtual API reader is required to revalidate populate helper absence before patching host pv %s", hostPVName)
	}
	helperCreationClosed, err := externalPopulatorHelperCreationClosed(ctx, ctx.VirtualAPIReader, vObj)
	if err != nil {
		return false, err
	}
	if !helperCreationClosed {
		ctx.Log.Infof("wait for virtual target pvc to reach a terminal populator condition before patching host pv claimRef: hostPV=%s targetPVC=%s/%s guestPVC=%s/%s", hostPVName, pObj.Namespace, pObj.Name, vObj.Namespace, vObj.Name)
		return false, nil
	}
	helperFound, err = externalPopulatorPopulateHelperExistsWithReader(ctx, ctx.VirtualAPIReader, vObj)
	if err != nil {
		return false, err
	}
	if helperFound {
		ctx.Log.Infof("wait for newly observed virtual populate helper to disappear before patching host pv claimRef: hostPV=%s targetPVC=%s/%s", hostPVName, pObj.Namespace, pObj.Name)
		return false, nil
	}
	if exactTargetClaimRef {
		return true, nil
	}

	updated := hostPV.DeepCopy()
	updated.Spec.ClaimRef = targetRef
	err := ctx.HostClient.Patch(ctx.Context, updated, client.MergeFromWithOptions(hostPV, client.MergeFromWithOptimisticLock{}))
	if err != nil {
		return false, err
	}

	return true, nil
}

func externalPopulatorHelperCreationClosed(ctx *synccontext.SyncContext, reader client.Reader, targetPVC *corev1.PersistentVolumeClaim) (bool, error) {
	if reader == nil || targetPVC == nil || targetPVC.UID == "" || targetPVC.Spec.VolumeName == "" {
		return false, nil
	}

	latest := &corev1.PersistentVolumeClaim{}
	err := reader.Get(ctx.Context, types.NamespacedName{Namespace: targetPVC.Namespace, Name: targetPVC.Name}, latest)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if latest.UID != targetPVC.UID || latest.Spec.VolumeName != targetPVC.Spec.VolumeName || !latest.DeletionTimestamp.IsZero() {
		return false, nil
	}

	for _, condition := range latest.Status.Conditions {
		if condition.Type != externalPopulatorPopulateConditionType || condition.Status != corev1.ConditionTrue {
			continue
		}
		if condition.Reason == externalPopulatorRestoreConditionReasonSucceeded || condition.Reason == externalPopulatorRestoreConditionReasonProvisioned {
			return true, nil
		}
	}
	return false, nil
}

func shouldKeepExternalPopulatorHostPVCSpecImmutable(pObj, vObj *corev1.PersistentVolumeClaim) bool {
	return pObj != nil &&
		vObj != nil &&
		pObj.Spec.VolumeName == "" &&
		isHostPVCWaitingForVolume(pObj) &&
		isDataProtectionBackupDataSourceRef(pObj.Spec.DataSourceRef) &&
		vObj.Spec.VolumeName != "" &&
		externalPopulatorNoDataRestoreCondition(vObj) != nil
}

func (s *persistentVolumeClaimSyncer) hostPVClaimRefMatchesExpectedPopulateHelper(ctx *synccontext.SyncContext, ref *corev1.ObjectReference, targetPVC *corev1.PersistentVolumeClaim) bool {
	if ref == nil {
		return false
	}
	hostName, ok := s.externalPopulatorExpectedHostHelperName(ctx, targetPVC)
	return ok && ref.Namespace == hostName.Namespace && ref.Name == hostName.Name
}

func (s *persistentVolumeClaimSyncer) externalPopulatorExpectedHostHelperName(ctx *synccontext.SyncContext, targetPVC *corev1.PersistentVolumeClaim) (types.NamespacedName, bool) {
	if targetPVC == nil || targetPVC.UID == "" {
		return types.NamespacedName{}, false
	}

	helperPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: targetPVC.Namespace,
			Name:      externalPopulatorPopulateHelperPrefix + string(targetPVC.UID),
		},
	}
	hostName := s.VirtualToHost(ctx, types.NamespacedName{
		Namespace: helperPVC.Namespace,
		Name:      helperPVC.Name,
	}, helperPVC)

	return hostName, true
}

func (s *persistentVolumeClaimSyncer) hostPVClaimRefMatchesVirtualPVC(ctx *synccontext.SyncContext, ref *corev1.ObjectReference, vPVC *corev1.PersistentVolumeClaim) (bool, error) {
	if ref == nil || vPVC == nil {
		return false, nil
	}

	hostPVCName := s.VirtualToHost(ctx, types.NamespacedName{Name: vPVC.Name, Namespace: vPVC.Namespace}, vPVC)
	if ref.Namespace != hostPVCName.Namespace || ref.Name != hostPVCName.Name {
		return false, nil
	}

	hostPVC := &corev1.PersistentVolumeClaim{}
	err := ctx.HostClient.Get(ctx.Context, hostPVCName, hostPVC)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	return ref.UID == "" || ref.UID == hostPVC.UID, nil
}

func claimRefMatchesPersistentVolumeClaim(ref *corev1.ObjectReference, pvc *corev1.PersistentVolumeClaim) bool {
	if ref == nil || pvc == nil {
		return false
	}

	return claimRefReferencesPersistentVolumeClaim(ref, pvc) &&
		(ref.UID == "" || ref.UID == pvc.UID)
}

func claimRefReferencesPersistentVolumeClaim(ref *corev1.ObjectReference, pvc *corev1.PersistentVolumeClaim) bool {
	if ref == nil || pvc == nil {
		return false
	}

	return ref.Namespace == pvc.Namespace && ref.Name == pvc.Name
}

func (s *persistentVolumeClaimSyncer) externalPopulatorPersistentVolume(ctx *synccontext.SyncContext, pObj, vObj *corev1.PersistentVolumeClaim) (*corev1.PersistentVolume, bool, error) {
	if !isExternalPopulatorPVC(vObj) || vObj.Spec.VolumeName == "" || !isHostPVCWaitingForVolume(pObj) {
		return nil, false, nil
	}

	vPV := &corev1.PersistentVolume{}
	err := ctx.VirtualClient.Get(ctx, types.NamespacedName{Name: vObj.Spec.VolumeName}, vPV)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	if !isExternalPopulatorPersistentVolumeForPVC(vPV, vObj, false) {
		helperPVC, ok, err := s.findExternalPopulatorHelperPVC(ctx, vObj, vPV)
		if err != nil || !ok {
			return nil, false, err
		}
		if !isExternalPopulatorPopulateHelperPVCForTarget(helperPVC, vObj) ||
			!isExternalPopulatorPersistentVolumeForPVC(vPV, helperPVC, false) {
			return nil, false, nil
		}
	}
	if !isVirtualPVCBound(vObj) && vPV.Status.Phase != corev1.VolumeBound {
		return nil, false, nil
	}

	return vPV, true, nil
}

func (s *persistentVolumeClaimSyncer) shouldPreserveExternalPopulatorVirtualStatus(ctx *synccontext.SyncContext, pObj, vObj *corev1.PersistentVolumeClaim) (bool, error) {
	if !hasExternalPopulatorDataSource(vObj) {
		return false, nil
	}
	if !isHostPVCWaitingForVolume(pObj) {
		return false, nil
	}
	if vObj.Spec.VolumeName == "" {
		return true, nil
	}

	vPV := &corev1.PersistentVolume{}
	err := ctx.VirtualClient.Get(ctx, types.NamespacedName{Name: vObj.Spec.VolumeName}, vPV)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	return isExternalPopulatorPersistentVolumeForPVC(vPV, vObj, false), nil
}

func ensureExternalPopulatorVirtualPopulateStatus(vObj *corev1.PersistentVolumeClaim, vPV *corev1.PersistentVolume) {
	if isVirtualPVCBound(vObj) {
		return
	}

	if vPV.Status.Phase != corev1.VolumeBound {
		return
	}

	vObj.Status.Phase = corev1.ClaimBound
	if len(vObj.Status.AccessModes) == 0 {
		vObj.Status.AccessModes = append([]corev1.PersistentVolumeAccessMode(nil), vPV.Spec.AccessModes...)
	}
	storage, ok := vObj.Status.Capacity[corev1.ResourceStorage]
	if vObj.Status.Capacity == nil || !ok || storage.IsZero() {
		vObj.Status.Capacity = vPV.Spec.Capacity.DeepCopy()
	}
}

func isExternalPopulatorPersistentVolumeForPVC(vPV *corev1.PersistentVolume, vObj *corev1.PersistentVolumeClaim, requireBoundPV bool) bool {
	if requireBoundPV && vPV.Status.Phase != corev1.VolumeBound {
		return false
	}
	if vPV.Spec.ClaimRef == nil ||
		vPV.Spec.ClaimRef.Namespace != vObj.Namespace ||
		vPV.Spec.ClaimRef.Name != vObj.Name {
		return false
	}
	if vPV.Spec.ClaimRef.UID != "" && vPV.Spec.ClaimRef.UID != vObj.UID {
		return false
	}

	return true
}

func isExternalPopulatorPopulateHelperPVCForTarget(helperPVC, targetPVC *corev1.PersistentVolumeClaim) bool {
	if helperPVC == nil || targetPVC == nil || targetPVC.UID == "" {
		return false
	}

	return helperPVC.Namespace == targetPVC.Namespace &&
		helperPVC.Name == externalPopulatorPopulateHelperPrefix+string(targetPVC.UID) &&
		helperPVC.DeletionTimestamp == nil
}

func isExternalPopulatorPVC(pvc *corev1.PersistentVolumeClaim) bool {
	return hasExternalPopulatorDataSource(pvc)
}

func hasExternalPopulatorDataSource(pvc *corev1.PersistentVolumeClaim) bool {
	if pvc.Spec.DataSourceRef == nil || pvc.Spec.DataSourceRef.Name == "" {
		return false
	}

	switch pvc.Spec.DataSourceRef.Kind {
	case "VolumeSnapshot", "PersistentVolumeClaim":
		return false
	default:
		return true
	}
}

func isVirtualPVCBound(pvc *corev1.PersistentVolumeClaim) bool {
	if pvc.Spec.VolumeName == "" || pvc.Status.Phase != corev1.ClaimBound {
		return false
	}

	storage, ok := pvc.Status.Capacity[corev1.ResourceStorage]
	return ok && !storage.IsZero()
}

func isHostPVCWaitingForVolume(pvc *corev1.PersistentVolumeClaim) bool {
	if pvc.Status.Phase == corev1.ClaimBound {
		return false
	}

	storage, ok := pvc.Status.Capacity[corev1.ResourceStorage]
	return !ok || storage.IsZero()
}

func isExternalPopulatorRestoreProvisionedWithoutDataRestore(pvc *corev1.PersistentVolumeClaim) bool {
	for _, condition := range pvc.Status.Conditions {
		if condition.Type == externalPopulatorRestoreConditionType &&
			condition.Status == corev1.ConditionTrue &&
			condition.Reason == externalPopulatorRestoreConditionReasonProvisioned {
			return true
		}
	}

	return false
}

func externalPopulatorNoDataRestoreCondition(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaimCondition {
	for i := range pvc.Status.Conditions {
		condition := &pvc.Status.Conditions[i]
		if condition.Type == externalPopulatorRestoreConditionType &&
			condition.Status == corev1.ConditionTrue &&
			condition.Reason == externalPopulatorRestoreConditionReasonProvisioned {
			return condition
		}
		if condition.Type != externalPopulatorRestoreConditionType &&
			condition.Type != externalPopulatorPopulateConditionType {
			continue
		}
		if condition.Status == corev1.ConditionFalse ||
			condition.Reason != externalPopulatorRestoreConditionReasonProcessing ||
			!strings.Contains(condition.Message, externalPopulatorNoDataRestoreMessage) {
			continue
		}

		return condition
	}

	return nil
}

func isExternalPopulatorRestoreProvisioningWithoutDataRestore(pvc *corev1.PersistentVolumeClaim) bool {
	for _, condition := range pvc.Status.Conditions {
		if condition.Type != externalPopulatorRestoreConditionType &&
			condition.Type != externalPopulatorPopulateConditionType {
			continue
		}

		if condition.Status == corev1.ConditionFalse ||
			condition.Reason != externalPopulatorRestoreConditionReasonProcessing ||
			!strings.Contains(condition.Message, externalPopulatorNoDataRestoreMessage) {
			continue
		}

		return true
	}

	return false
}

func (s *persistentVolumeClaimSyncer) shouldBlockExternalPopulatorHostPVCUntilGuestBackupMaterialized(ctx *synccontext.SyncContext, vObj, pObj *corev1.PersistentVolumeClaim) (bool, error) {
	ref := pObj.Spec.DataSourceRef
	if !isDataProtectionBackupDataSourceRef(ref) {
		return false, nil
	}

	noDataRestore, err := s.isExternalPopulatorNoDataRestorePVC(ctx, vObj)
	if err != nil {
		return false, err
	}
	if noDataRestore {
		ctx.Log.Infof("block host external populator pvc because guest backup materialized without restored data: guestPVC=%s/%s guestUID=%s hostPVC=%s/%s dataSourceRef=%s",
			vObj.Namespace,
			vObj.Name,
			vObj.UID,
			pObj.Namespace,
			pObj.Name,
			externalPopulatorDataSourceRefDescription(ref),
		)
		return true, nil
	}

	if vObj.Spec.VolumeName != "" {
		helperExists, err := s.externalPopulatorPopulateHelperExists(ctx, vObj)
		if err != nil {
			return false, err
		}
		if helperExists {
			ctx.Log.Infof("block host external populator target pvc while virtual populate helper exists: guestPVC=%s/%s guestUID=%s hostPVC=%s/%s",
				vObj.Namespace,
				vObj.Name,
				vObj.UID,
				pObj.Namespace,
				pObj.Name,
			)
			return true, nil
		}

		clearExternalPopulatorHostDataSource(pObj)
		return false, nil
	}
	if pObj.Spec.VolumeName != "" {
		clearExternalPopulatorHostDataSource(pObj)
		return false, nil
	}

	ctx.Log.Infof("allow host external populator pvc to wait for guest backup materialization: guestPVC=%s/%s guestUID=%s hostPVC=%s/%s dataSourceRef=%s",
		vObj.Namespace,
		vObj.Name,
		vObj.UID,
		pObj.Namespace,
		pObj.Name,
		externalPopulatorDataSourceRefDescription(ref),
	)
	return false, nil
}

func (s *persistentVolumeClaimSyncer) externalPopulatorPopulateHelperExists(ctx *synccontext.SyncContext, targetPVC *corev1.PersistentVolumeClaim) (bool, error) {
	return externalPopulatorPopulateHelperExistsWithReader(ctx, ctx.VirtualClient, targetPVC)
}

func externalPopulatorPopulateHelperExistsWithReader(ctx *synccontext.SyncContext, reader client.Reader, targetPVC *corev1.PersistentVolumeClaim) (bool, error) {
	if targetPVC.UID == "" {
		return false, nil
	}
	if reader == nil {
		return false, fmt.Errorf("virtual reader is required to check external populator helper for pvc %s/%s", targetPVC.Namespace, targetPVC.Name)
	}

	helperPVC := &corev1.PersistentVolumeClaim{}
	err := reader.Get(ctx.Context, types.NamespacedName{
		Namespace: targetPVC.Namespace,
		Name:      externalPopulatorPopulateHelperPrefix + string(targetPVC.UID),
	}, helperPVC)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (s *persistentVolumeClaimSyncer) shouldWaitForExternalPopulatorHelperAbsence(ctx *synccontext.SyncContext, targetPVC *corev1.PersistentVolumeClaim) (bool, error) {
	if targetPVC.Spec.VolumeName == "" || !isDataProtectionBackupDataSourceRef(targetPVC.Spec.DataSourceRef) {
		return false, nil
	}

	return s.externalPopulatorPopulateHelperExists(ctx, targetPVC)
}

func isDataProtectionBackupDataSourceRef(ref *corev1.TypedObjectReference) bool {
	return ref != nil &&
		ref.Name != "" &&
		ref.APIGroup != nil &&
		*ref.APIGroup == dataProtectionAPIGroup &&
		ref.Kind == dataProtectionBackupKind
}

func clearExternalPopulatorHostDataSource(pvc *corev1.PersistentVolumeClaim) {
	pvc.Spec.DataSource = nil
	pvc.Spec.DataSourceRef = nil
}

func clearExternalPopulatorHostDataSourceAfterGuestMaterialized(pObj, vObj *corev1.PersistentVolumeClaim) {
	if pObj == nil || vObj == nil || !isDataProtectionBackupDataSourceRef(pObj.Spec.DataSourceRef) {
		return
	}
	if vObj.Spec.VolumeName == "" || externalPopulatorNoDataRestoreCondition(vObj) == nil {
		return
	}

	clearExternalPopulatorHostDataSource(pObj)
}

func (s *persistentVolumeClaimSyncer) shouldBackoffExternalPopulatorHostNoDataRestorePVC(ctx *synccontext.SyncContext, pObj, vObj *corev1.PersistentVolumeClaim) (bool, error) {
	if !isHostPVCWaitingForVolume(pObj) ||
		(!isExternalPopulatorDataSource(pObj.Spec.DataSource) && !isExternalPopulatorDataSourceRef(pObj.Spec.DataSourceRef)) {
		return false, nil
	}

	noDataRestore, err := s.isExternalPopulatorNoDataRestorePVC(ctx, vObj)
	if err != nil || !noDataRestore {
		return false, err
	}
	if vObj.Spec.VolumeName != "" {
		currentHost := &corev1.PersistentVolumeClaim{}
		err := ctx.HostClient.Get(ctx.Context, types.NamespacedName{
			Namespace: pObj.Namespace,
			Name:      pObj.Name,
		}, currentHost)
		if err != nil {
			if kerrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if currentHost.Spec.VolumeName != "" || currentHost.Status.Phase == corev1.ClaimBound {
			return true, nil
		}

		return false, nil
	}

	return true, nil
}

func logExternalPopulatorNoDataRestoreBackoff(log loghelper.Logger, pObj, vObj *corev1.PersistentVolumeClaim) {
	condition := externalPopulatorNoDataRestoreCondition(vObj)
	conditionType, conditionStatus, conditionReason, conditionMessage, transitionTime := "", "", "", "", ""
	if condition != nil {
		conditionType = string(condition.Type)
		conditionStatus = string(condition.Status)
		conditionReason = condition.Reason
		conditionMessage = condition.Message
		transitionTime = condition.LastTransitionTime.String()
	}

	hostDeletionTimestamp := ""
	if pObj.DeletionTimestamp != nil {
		hostDeletionTimestamp = pObj.DeletionTimestamp.String()
	}

	log.Infof("preserve host external populator pvc after no-data restore condition: guestPVC=%s/%s guestUID=%s hostPVC=%s/%s hostUID=%s hostResourceVersion=%s hostPhase=%s hostDeletionTimestamp=%s dataSourceRef=%s noDataConditionType=%s noDataConditionStatus=%s noDataConditionReason=%s noDataConditionMessage=%q noDataConditionLastTransitionTime=%s",
		vObj.Namespace,
		vObj.Name,
		vObj.UID,
		pObj.Namespace,
		pObj.Name,
		pObj.UID,
		pObj.ResourceVersion,
		pObj.Status.Phase,
		hostDeletionTimestamp,
		externalPopulatorDataSourceRefDescription(pObj.Spec.DataSourceRef),
		conditionType,
		conditionStatus,
		conditionReason,
		conditionMessage,
		transitionTime,
	)
}

func (s *persistentVolumeClaimSyncer) shouldPreserveExternalPopulatorNoDataRestorePVCWhileHostDeleting(ctx *synccontext.SyncContext, vObj, pObj *corev1.PersistentVolumeClaim) (bool, error) {
	noDataRestore, err := s.isExternalPopulatorNoDataRestorePVC(ctx, vObj)
	if err != nil || noDataRestore {
		return noDataRestore, err
	}

	return s.shouldPreserveDeletingExternalPopulatorPopulateHelper(ctx, vObj, pObj)
}

func (s *persistentVolumeClaimSyncer) shouldPreserveDeletingExternalPopulatorPopulateHelper(ctx *synccontext.SyncContext, helperVirtual, helperHost *corev1.PersistentVolumeClaim) (bool, error) {
	if helperVirtual == nil || helperHost == nil || helperVirtual.DeletionTimestamp == nil || !strings.HasPrefix(helperVirtual.Name, externalPopulatorPopulateHelperPrefix) {
		return false, nil
	}

	targetUID := strings.TrimPrefix(helperVirtual.Name, externalPopulatorPopulateHelperPrefix)
	if targetUID == "" {
		return false, nil
	}

	targetVirtual, ok, err := s.findExternalPopulatorTargetPVCByUID(ctx, helperVirtual.Namespace, types.UID(targetUID))
	if err != nil || !ok {
		return false, err
	}

	return s.externalPopulatorHandoffPendingForTarget(ctx, targetVirtual, helperHost)
}

// shouldPreserveOrphanedExternalPopulatorPopulateHelperHost guards the SyncToVirtual
// deletion path: the virtual helper PVC no longer exists at all, so the helper can only
// be identified through the host object's virtual name mapping.
func (s *persistentVolumeClaimSyncer) shouldPreserveOrphanedExternalPopulatorPopulateHelperHost(ctx *synccontext.SyncContext, helperHost *corev1.PersistentVolumeClaim) (bool, error) {
	if helperHost == nil {
		return false, nil
	}

	helperVirtualName := s.HostToVirtual(ctx, types.NamespacedName{Name: helperHost.Name, Namespace: helperHost.Namespace}, helperHost)
	if helperVirtualName.Name == "" || !strings.HasPrefix(helperVirtualName.Name, externalPopulatorPopulateHelperPrefix) {
		return false, nil
	}

	targetUID := strings.TrimPrefix(helperVirtualName.Name, externalPopulatorPopulateHelperPrefix)
	if targetUID == "" {
		return false, nil
	}

	targetVirtual, ok, err := s.findExternalPopulatorTargetPVCByUID(ctx, helperVirtualName.Namespace, types.UID(targetUID))
	if err != nil || !ok {
		return false, err
	}

	return s.externalPopulatorHandoffPendingForTarget(ctx, targetVirtual, helperHost)
}

func (s *persistentVolumeClaimSyncer) externalPopulatorHandoffPendingForTarget(ctx *synccontext.SyncContext, targetVirtual, helperHost *corev1.PersistentVolumeClaim) (bool, error) {
	targetHostName := s.VirtualToHost(ctx, types.NamespacedName{Name: targetVirtual.Name, Namespace: targetVirtual.Namespace}, targetVirtual)
	targetHost := &corev1.PersistentVolumeClaim{}
	err := ctx.HostClient.Get(ctx.Context, targetHostName, targetHost)
	if err != nil {
		if kerrors.IsNotFound(err) {
			// the host target PVC does not exist yet; releasing the helper now would
			// reclaim the populated PV before the handoff can happen
			return true, nil
		}
		return false, err
	}

	if !isHostPVCWaitingForVolume(targetHost) {
		return false, nil
	}
	if targetHost.Spec.VolumeName == "" || helperHost.Spec.VolumeName == "" {
		return true, nil
	}

	return targetHost.Spec.VolumeName == helperHost.Spec.VolumeName, nil
}

func (s *persistentVolumeClaimSyncer) findExternalPopulatorTargetPVCByUID(ctx *synccontext.SyncContext, namespace string, uid types.UID) (*corev1.PersistentVolumeClaim, bool, error) {
	if uid == "" {
		return nil, false, nil
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	err := ctx.VirtualClient.List(ctx.Context, pvcList, client.InNamespace(namespace))
	if err != nil {
		return nil, false, err
	}

	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if pvc.UID == uid && isExternalPopulatorPVC(pvc) && pvc.DeletionTimestamp == nil {
			return pvc.DeepCopy(), true, nil
		}
	}

	return nil, false, nil
}

func (s *persistentVolumeClaimSyncer) isExternalPopulatorNoDataRestorePVC(ctx *synccontext.SyncContext, vObj *corev1.PersistentVolumeClaim) (bool, error) {
	if !isExternalPopulatorPVC(vObj) {
		return false, nil
	}

	return isExternalPopulatorRestoreProvisionedWithoutDataRestore(vObj) ||
		isExternalPopulatorRestoreProvisioningWithoutDataRestore(vObj), nil
}

func deleteExternalPopulatorNoDataRestoreHostPVC(ctx *synccontext.SyncContext, pObj, vObj *corev1.PersistentVolumeClaim) (ctrl.Result, error) {
	result, err := patcher.DeleteHostObjectWithOptions(ctx, pObj, vObj, "external populator restore pvc was provisioned without data restore", &client.DeleteOptions{
		GracePeriodSeconds: &zero,
		Preconditions:      hostDeletePreconditions(pObj),
	})
	if kerrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}

	return result, err
}

func hostDeletePreconditions(obj client.Object) *metav1.Preconditions {
	preconditions := &metav1.Preconditions{}
	if obj.GetUID() != "" {
		uid := obj.GetUID()
		preconditions.UID = &uid
	}
	if obj.GetResourceVersion() != "" {
		resourceVersion := obj.GetResourceVersion()
		preconditions.ResourceVersion = &resourceVersion
	}
	if preconditions.UID == nil && preconditions.ResourceVersion == nil {
		return nil
	}

	return preconditions
}

func isExternalPopulatorDataSource(ref *corev1.TypedLocalObjectReference) bool {
	if ref == nil || ref.Name == "" {
		return false
	}

	switch ref.Kind {
	case "VolumeSnapshot", "PersistentVolumeClaim":
		return false
	default:
		return true
	}
}

func isExternalPopulatorDataSourceRef(ref *corev1.TypedObjectReference) bool {
	if ref == nil || ref.Name == "" {
		return false
	}

	switch ref.Kind {
	case "VolumeSnapshot", "PersistentVolumeClaim":
		return false
	default:
		return true
	}
}

func externalPopulatorDataSourceAPIGroup(ref *corev1.TypedObjectReference) string {
	if ref == nil || ref.APIGroup == nil {
		return ""
	}

	return *ref.APIGroup
}

func externalPopulatorDataSourceRefDescription(ref *corev1.TypedObjectReference) string {
	if ref == nil {
		return "<nil>"
	}

	return fmt.Sprintf("%s/%s/%s", externalPopulatorDataSourceAPIGroup(ref), ref.Kind, ref.Name)
}

func (s *persistentVolumeClaimSyncer) isHostVolumeRestoreInProgress(ctx *synccontext.SyncContext, pObj types.NamespacedName) (bool, error) {
	configMaps := &corev1.ConfigMapList{}
	err := ctx.HostClient.List(ctx.Context, configMaps, client.InNamespace(ctx.Config.HostNamespace), client.MatchingLabels{
		constants.RestoreRequestLabel: "",
	})
	if err != nil {
		return false, err
	}

	pvcName := types.NamespacedName{
		Namespace: pObj.Namespace,
		Name:      pObj.Name,
	}.String()
	for _, configMap := range configMaps.Items {
		restoreRequest, err := snapshot.UnmarshalRestoreRequest(&configMap)
		if err != nil {
			return false, fmt.Errorf("unmarshal restore request: %w", err)
		}
		volumeRestore, ok := restoreRequest.Status.VolumesRestore.PersistentVolumeClaims[pvcName]
		if !ok {
			continue
		}
		if !(volumeRestore.CleaningUp() || volumeRestore.Done()) {
			return true, nil
		}
	}
	return false, nil
}

func recreatePersistentVolumeClaim(ctx *synccontext.SyncContext, virtualClient client.Client, vPV *corev1.PersistentVolume, vPVC *corev1.PersistentVolumeClaim, volumeName string, log loghelper.Logger) (*corev1.PersistentVolumeClaim, error) {
	// check if we should lock the pv from deletion
	if vPV != nil && vPV.Name != "" {
		// lock pv
		before := vPV.DeepCopy()
		if vPV.Annotations == nil {
			vPV.Annotations = map[string]string{}
		}
		timestamp, err := metav1.Now().MarshalText()
		if err != nil {
			return nil, errors.Wrap(err, "marshal time")
		}
		vPV.Annotations[persistentvolumes.LockPersistentVolume] = string(timestamp)
		err = virtualClient.Patch(ctx, vPV, client.MergeFrom(before))
		if err != nil {
			return nil, errors.Wrap(err, "patch persistent volume")
		}

		// reset pv
		defer func() {
			before := vPV.DeepCopy()
			delete(vPV.Annotations, persistentvolumes.LockPersistentVolume)

			err := virtualClient.Patch(ctx, vPV, client.MergeFrom(before))
			if err != nil {
				log.Errorf("error resetting pv %s: %v", vPV.Name, err)
			}
		}()
	}

	// remove finalizers & delete
	if len(vPVC.Finalizers) > 0 {
		vPVC.Finalizers = []string{}
		err := virtualClient.Update(ctx, vPVC)
		if err != nil {
			return nil, errors.Wrap(err, "remove finalizers")
		}
	}

	// delete & create with correct volume name
	err := virtualClient.Delete(ctx, vPVC)
	if err != nil && !kerrors.IsNotFound(err) {
		return nil, errors.Wrap(err, "delete pvc")
	}

	// make sure we don't set the resource version during create
	vPVC = vPVC.DeepCopy()
	vPVC.ResourceVersion = ""
	vPVC.UID = ""
	vPVC.DeletionTimestamp = nil
	vPVC.Generation = 0
	vPVC.Spec.VolumeName = volumeName

	// create the new service with the correct volume name
	err = virtualClient.Create(ctx, vPVC)
	if err != nil {
		klog.Errorf("error recreating virtual pvc: %s/%s: %v", vPVC.Namespace, vPVC.Name, err)
		return nil, errors.Wrap(err, "create pvc")
	}

	return vPVC, nil
}

func (s *persistentVolumeClaimSyncer) applyLimitByClass(ctx *synccontext.SyncContext, virtual *corev1.PersistentVolumeClaim) bool {
	if !ctx.Config.Sync.FromHost.StorageClasses.Enabled.Bool() ||
		ctx.Config.Sync.FromHost.StorageClasses.Selector.Empty() ||
		virtual.Spec.StorageClassName == nil ||
		*virtual.Spec.StorageClassName == "" {
		return false
	}

	pStorageClass := &storagev1.StorageClass{}
	err := ctx.HostClient.Get(ctx.Context, types.NamespacedName{Name: *virtual.Spec.StorageClassName}, pStorageClass)
	if err != nil || pStorageClass.GetDeletionTimestamp() != nil {
		s.EventRecorder().Eventf(
			virtual,
			nil,
			"Warning",
			"SyncWarning",
			fmt.Sprintf("Sync%s", virtual.GetObjectKind().GroupVersionKind().Kind),
			"did not sync persistent volume claim %q to host because the storage class %q couldn't be reached in the host: %s",
			virtual.GetName(),
			*virtual.Spec.StorageClassName,
			err,
		)
		return true
	}
	matches, err := ctx.Config.Sync.FromHost.StorageClasses.Selector.Matches(pStorageClass)
	if err != nil {
		s.EventRecorder().Eventf(
			virtual,
			nil,
			"Warning",
			"SyncWarning",
			fmt.Sprintf("Sync%s", virtual.GetObjectKind().GroupVersionKind().Kind),
			"did not sync persistent volume claim %q to host because the storage class %q in the host could not be checked against the selector under 'sync.fromHost.storageClasses.selector': %s",
			virtual.GetName(),
			pStorageClass.GetName(),
			err,
		)
		return true
	}
	if !matches {
		s.EventRecorder().Eventf(
			virtual,
			nil,
			"Warning",
			"SyncWarning",
			fmt.Sprintf("Sync%s", virtual.GetObjectKind().GroupVersionKind().Kind),
			"did not sync persistent volume claim %q to host because the storage class %q in the host does not match the selector under 'sync.fromHost.storageClasses.selector'",
			virtual.GetName(),
			pStorageClass.GetName(),
		)
		return true
	}

	return false
}
