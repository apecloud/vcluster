package backups

import (
	"fmt"

	"github.com/loft-sh/vcluster/config"
	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/patcher"
	"github.com/loft-sh/vcluster/pkg/pro"
	"github.com/loft-sh/vcluster/pkg/syncer"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	translator2 "github.com/loft-sh/vcluster/pkg/syncer/translator"
	syncertypes "github.com/loft-sh/vcluster/pkg/syncer/types"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func New(ctx *synccontext.RegisterContext) (syncertypes.Object, error) {
	cfg, ok := Config(ctx.Config.Sync.ToHost.CustomResources)
	if !ok {
		return nil, nil
	}

	mapper, err := ctx.Mappings.ByGVK(mappings.DataProtectionBackups())
	if err != nil {
		return nil, err
	}

	return &backupSyncer{
		GenericTranslator: translator2.NewGenericTranslator(ctx, "dataprotection-backup", NewObject(), mapper),
		patches:           cfg.Patches,
	}, nil
}

type backupSyncer struct {
	syncertypes.GenericTranslator

	patches []config.TranslatePatch
}

var _ syncertypes.OptionsProvider = &backupSyncer{}

func (s *backupSyncer) Options() *syncertypes.Options {
	return &syncertypes.Options{
		ObjectCaching: true,
	}
}

var _ syncertypes.Syncer = &backupSyncer{}

func (s *backupSyncer) Syncer() syncertypes.Sync[client.Object] {
	return syncer.ToGenericSyncer(s)
}

func (s *backupSyncer) SyncToHost(ctx *synccontext.SyncContext, event *synccontext.SyncToHostEvent[*unstructured.Unstructured]) (ctrl.Result, error) {
	if event.Virtual.GetDeletionTimestamp() != nil {
		if event.HostOld != nil {
			return patcher.DeleteHostObject(ctx, event.HostOld, event.Virtual, "virtual dataprotection backup is being deleted")
		}

		return ctrl.Result{}, nil
	}

	pObj := translate.HostMetadata(event.Virtual, s.VirtualToHost(ctx, client.ObjectKeyFromObject(event.Virtual), event.Virtual))
	unstructured.RemoveNestedField(pObj.Object, "status")

	err := pro.ApplyPatchesHostObject(ctx, nil, pObj, event.Virtual, s.patches, false)
	if err != nil {
		return ctrl.Result{}, err
	}

	return patcher.CreateHostObject(ctx, event.Virtual, pObj, s.EventRecorder(), false)
}

func (s *backupSyncer) Sync(ctx *synccontext.SyncContext, event *synccontext.SyncEvent[*unstructured.Unstructured]) (_ ctrl.Result, retErr error) {
	if event.Host.GetDeletionTimestamp() != nil {
		if event.Virtual.GetDeletionTimestamp() == nil {
			return patcher.DeleteVirtualObject(ctx, event.Virtual, event.Host, "host dataprotection backup is being deleted")
		}

		return ctrl.Result{}, nil
	} else if event.Virtual.GetDeletionTimestamp() != nil {
		return patcher.DeleteHostObject(ctx, event.Host, event.Virtual, "virtual dataprotection backup is being deleted")
	}

	patch, err := patcher.NewSyncerPatcher(ctx, event.Host, event.Virtual, patcher.TranslatePatches(s.patches, false))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("new syncer patcher: %w", err)
	}
	defer func() {
		if err := patch.Patch(ctx, event.Host, event.Virtual); err != nil {
			retErr = utilerrors.NewAggregate([]error{retErr, err})
		}
	}()

	// Host DP owns runtime state; reflect it back so virtual callers can wait on Backup phase.
	copyNestedField(event.Host.Object, event.Virtual.Object, "status")
	event.Virtual.SetFinalizers(event.Host.GetFinalizers())

	// Virtual users own the desired spec; host DP owns status/finalizers.
	copyNestedField(event.Virtual.Object, event.Host.Object, "spec")

	event.Virtual.SetAnnotations(translate.VirtualAnnotations(event.Host, event.Virtual))
	event.Host.SetAnnotations(translate.HostAnnotations(event.Virtual, event.Host))
	event.Virtual.SetLabels(translate.VirtualLabels(event.Host, event.Virtual))
	event.Host.SetLabels(translate.HostLabels(event.Virtual, event.Host))

	return ctrl.Result{}, nil
}

func (s *backupSyncer) SyncToVirtual(ctx *synccontext.SyncContext, event *synccontext.SyncToVirtualEvent[*unstructured.Unstructured]) (ctrl.Result, error) {
	return patcher.DeleteHostObject(ctx, event.Host, event.VirtualOld, "virtual dataprotection backup was deleted")
}

func copyNestedField(from, to map[string]interface{}, fields ...string) {
	value, ok, _ := unstructured.NestedFieldCopy(from, fields...)
	if !ok {
		unstructured.RemoveNestedField(to, fields...)
		return
	}

	_ = unstructured.SetNestedField(to, value, fields...)
}
