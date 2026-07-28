package storageclasses

import (
	"fmt"
	"maps"

	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/patcher"
	"github.com/loft-sh/vcluster/pkg/pro"
	"github.com/loft-sh/vcluster/pkg/syncer"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"github.com/loft-sh/vcluster/pkg/syncer/translator"
	syncertypes "github.com/loft-sh/vcluster/pkg/syncer/types"
	"github.com/loft-sh/vcluster/pkg/util/translate"
)

func NewHostStorageClassSyncer(ctx *synccontext.RegisterContext) (syncertypes.Object, error) {
	mapper, err := ctx.Mappings.ByGVK(mappings.StorageClasses())
	if err != nil {
		return nil, err
	}

	return &hostStorageClassSyncer{
		GenericTranslator: translator.NewGenericTranslator(ctx, "storageclass", &storagev1.StorageClass{}, mapper),
	}, nil
}

type hostStorageClassSyncer struct {
	syncertypes.GenericTranslator
}

func (s *hostStorageClassSyncer) UseUncachedPhysicalClient() bool {
	return false
}

func (s *hostStorageClassSyncer) Name() string {
	return "host-storageclass"
}

func (s *hostStorageClassSyncer) Resource() client.Object {
	return &storagev1.StorageClass{}
}

var _ syncertypes.Syncer = &hostStorageClassSyncer{}

func (s *hostStorageClassSyncer) Syncer() syncertypes.Sync[client.Object] {
	return syncer.ToGenericSyncer(s)
}

func (s *hostStorageClassSyncer) SyncToVirtual(ctx *synccontext.SyncContext, event *synccontext.SyncToVirtualEvent[*storagev1.StorageClass]) (ctrl.Result, error) {
	matches, err := ctx.Config.Sync.FromHost.StorageClasses.Selector.Matches(event.Host)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check storage class selector: %w", err)
	}
	if !matches {
		ctx.Log.Infof("Warning: did not sync storage class %q because it does not match the selector under 'sync.fromHost.storageClasses.selector'", event.Host.Name)
		return ctrl.Result{}, nil
	}

	vObj := translate.CopyObjectWithName(event.Host, types.NamespacedName{Name: event.Host.Name}, false)

	// Apply pro patches
	err = pro.ApplyPatchesVirtualObject(ctx, nil, vObj, event.Host, ctx.Config.Sync.FromHost.StorageClasses.Patches, true)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error applying patches: %w", err)
	}
	markOwnedVirtual(vObj, s.Name())

	ctx.Log.Infof("create storage class %s, because it does not exist in virtual cluster", vObj.Name)
	return ctrl.Result{}, ctx.VirtualClient.Create(ctx, vObj)
}

func (s *hostStorageClassSyncer) Sync(ctx *synccontext.SyncContext, event *synccontext.SyncEvent[*storagev1.StorageClass]) (_ ctrl.Result, retErr error) {
	matches, err := ctx.Config.Sync.FromHost.StorageClasses.Selector.Matches(event.Host)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check storage class selector: %w", err)
	}
	if !matches {
		if !isOwnedVirtual(event.Virtual, s.Name()) {
			ctx.Log.Infof("preserve virtual storage class %q because it is not owned by %s", event.Virtual.Name, s.Name())
			return ctrl.Result{}, nil
		}
		return s.deleteOwnedVirtual(ctx, event.Virtual, event.Host, fmt.Sprintf("did not sync storage class %q because it does not match the selector under 'sync.fromHost.storageClasses.selector'", event.Host.Name))
	}

	// Build the desired object on a copy so a transform error cannot be
	// persisted by the patcher or leak into the host object through aliases.
	hostDesired := event.Host.DeepCopy()
	desired := event.Virtual.DeepCopy()
	desired.Annotations = maps.Clone(hostDesired.Annotations)
	desired.Labels = maps.Clone(hostDesired.Labels)
	desired.Provisioner = hostDesired.Provisioner
	desired.Parameters = maps.Clone(hostDesired.Parameters)
	desired.ReclaimPolicy = hostDesired.ReclaimPolicy
	desired.MountOptions = hostDesired.MountOptions
	desired.AllowVolumeExpansion = hostDesired.AllowVolumeExpansion
	desired.VolumeBindingMode = hostDesired.VolumeBindingMode
	desired.AllowedTopologies = hostDesired.AllowedTopologies
	if err := pro.ApplyPatchesVirtualObject(ctx, nil, desired, event.Host, ctx.Config.Sync.FromHost.StorageClasses.Patches, true); err != nil {
		return ctrl.Result{}, fmt.Errorf("error applying patches: %w", err)
	}
	markOwnedVirtual(desired, s.Name())

	patch, err := patcher.NewSyncerPatcher(ctx, event.Host, event.Virtual)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("new syncer patcher: %w", err)
	}
	defer func() {
		if err := patch.Patch(ctx, event.Host, event.Virtual); err != nil {
			retErr = utilerrors.NewAggregate([]error{retErr, err})
		}
	}()

	*event.Virtual = *desired
	return ctrl.Result{}, nil
}

func (s *hostStorageClassSyncer) SyncToHost(ctx *synccontext.SyncContext, event *synccontext.SyncToHostEvent[*storagev1.StorageClass]) (ctrl.Result, error) {
	if !isOwnedVirtual(event.Virtual, s.Name()) {
		ctx.Log.Infof("preserve virtual storage class %q because physical object is missing and it is not owned by %s", event.Virtual.Name, s.Name())
		return ctrl.Result{}, nil
	}
	return s.deleteOwnedVirtual(ctx, event.Virtual, nil, "physical object is missing")
}

func (s *hostStorageClassSyncer) deleteOwnedVirtual(ctx *synccontext.SyncContext, eventVirtual, eventHost *storagev1.StorageClass, reason string) (ctrl.Result, error) {
	current := &storagev1.StorageClass{}
	if err := ctx.VirtualClient.Get(ctx, client.ObjectKeyFromObject(eventVirtual), current); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get current virtual storage class %q before delete: %w", eventVirtual.Name, err)
	}

	if !isOwnedVirtual(current, s.Name()) {
		ctx.Log.Infof("preserve current virtual storage class %q because it is not owned by %s", current.Name, s.Name())
		return ctrl.Result{}, nil
	}
	if eventVirtual.UID != "" && current.UID != eventVirtual.UID {
		ctx.Log.Infof("preserve current virtual storage class %q because UID changed from %q to %q", current.Name, eventVirtual.UID, current.UID)
		return ctrl.Result{}, nil
	}

	deleteOptions := &client.DeleteOptions{}
	if current.UID != "" {
		deleteOptions.Preconditions = metav1.NewUIDPreconditions(string(current.UID))
	}
	ctx.Log.Infof("delete owned virtual storage class %q, because %s", current.Name, reason)
	return patcher.DeleteVirtualObjectWithOptions(ctx, current, eventHost, reason, deleteOptions)
}

func markOwnedVirtual(obj *storagev1.StorageClass, owner string) {
	if obj.Annotations == nil {
		obj.Annotations = map[string]string{}
	}
	obj.Annotations[translate.ControllerLabel] = owner
}

func isOwnedVirtual(obj *storagev1.StorageClass, owner string) bool {
	return obj.Annotations != nil && obj.Annotations[translate.ControllerLabel] == owner
}
