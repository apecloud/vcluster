package backups

import (
	"fmt"
	"strings"

	"github.com/loft-sh/vcluster/config"
	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/patcher"
	"github.com/loft-sh/vcluster/pkg/pro"
	"github.com/loft-sh/vcluster/pkg/syncer"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	translator2 "github.com/loft-sh/vcluster/pkg/syncer/translator"
	syncertypes "github.com/loft-sh/vcluster/pkg/syncer/types"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	dataProtectionBackupRepoLabel       = "dataprotection.kubeblocks.io/backup-repo-name"
	dataProtectionDefaultRepoAnnotation = "dataprotection.kubeblocks.io/is-default-repo"
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
	translateBackupPolicyName(ctx, event.Virtual.Object, pObj.Object, event.Virtual.GetNamespace())

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
	translateBackupRepoStatusToVirtual(ctx, event.Host.Object, event.Virtual.Object)
	translateBackupTargetPodNameToVirtual(ctx, event.Host.Object, event.Virtual.Object, event.Virtual.GetNamespace())
	translateBackupActionsToVirtual(ctx, event.Virtual.Object, event.Virtual.GetNamespace(), event.Host.GetName(), event.Host.GetUID(), event.Virtual.GetName(), event.Virtual.GetUID())
	event.Virtual.SetFinalizers(event.Host.GetFinalizers())

	// Virtual users own the desired spec; host DP owns status/finalizers.
	copyNestedField(event.Virtual.Object, event.Host.Object, "spec")
	translateBackupPolicyName(ctx, event.Virtual.Object, event.Host.Object, event.Virtual.GetNamespace())

	event.Virtual.SetAnnotations(translate.VirtualAnnotations(event.Host, event.Virtual))
	event.Host.SetAnnotations(translate.HostAnnotations(event.Virtual, event.Host))
	virtualLabels := translate.VirtualLabels(event.Host, event.Virtual)
	translateBackupRepoLabelToVirtual(ctx, event.Host.GetLabels(), virtualLabels)
	event.Virtual.SetLabels(virtualLabels)

	hostLabels := translate.HostLabels(event.Virtual, event.Host)
	preserveHostBackupRepoLabel(event.Host.GetLabels(), hostLabels)
	event.Host.SetLabels(hostLabels)

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

func translateBackupPolicyName(ctx *synccontext.SyncContext, from, to map[string]interface{}, namespace string) {
	backupPolicyName, ok, _ := unstructured.NestedString(from, "spec", "backupPolicyName")
	if !ok || backupPolicyName == "" {
		return
	}

	hostName := translate.Default.HostName(ctx, backupPolicyName, namespace).Name
	if hostName == "" {
		return
	}

	_ = unstructured.SetNestedField(to, hostName, "spec", "backupPolicyName")
}

func translateBackupRepoStatusToVirtual(ctx *synccontext.SyncContext, from, to map[string]interface{}) {
	backupRepoName, ok, _ := unstructured.NestedString(from, "status", "backupRepoName")
	if !ok || backupRepoName == "" {
		return
	}

	_ = unstructured.SetNestedField(to, translateBackupRepoNameToVirtual(ctx, backupRepoName), "status", "backupRepoName")
}

func translateBackupTargetPodNameToVirtual(ctx *synccontext.SyncContext, from, to map[string]interface{}, namespace string) {
	targetPodName, ok, _ := unstructured.NestedString(from, "status", "targetPodName")
	if !ok || targetPodName == "" {
		return
	}

	_ = unstructured.SetNestedField(to, translateHostPodNameToVirtual(ctx, targetPodName, namespace), "status", "targetPodName")
}

func translateBackupActionsToVirtual(ctx *synccontext.SyncContext, to map[string]interface{}, namespace, hostBackupName string, hostBackupUID types.UID, virtualBackupName string, virtualBackupUID types.UID) {
	actions, ok, _ := unstructured.NestedSlice(to, "status", "actions")
	if !ok || len(actions) == 0 {
		return
	}

	hostNamespace := translate.Default.HostName(ctx, virtualBackupName, namespace).Namespace
	for i := range actions {
		action, ok := actions[i].(map[string]interface{})
		if !ok {
			continue
		}

		targetPodName, ok, _ := unstructured.NestedString(action, "targetPodName")
		if ok && targetPodName != "" {
			_ = unstructured.SetNestedField(action, translateHostPodNameToVirtual(ctx, targetPodName, namespace), "targetPodName")
		}

		translateBackupActionObjectRefToVirtual(action, namespace, hostNamespace, hostBackupName, hostBackupUID, virtualBackupName, virtualBackupUID)
	}

	_ = unstructured.SetNestedSlice(to, actions, "status", "actions")
}

func translateBackupActionObjectRefToVirtual(action map[string]interface{}, namespace, hostNamespace, hostBackupName string, hostBackupUID types.UID, virtualBackupName string, virtualBackupUID types.UID) {
	objectRef, ok, _ := unstructured.NestedMap(action, "objectRef")
	if !ok {
		return
	}

	objectRefNamespace, ok, _ := unstructured.NestedString(action, "objectRef", "namespace")
	if ok && objectRefNamespace == hostNamespace {
		objectRef["namespace"] = namespace
	}

	kind, _, _ := unstructured.NestedString(action, "objectRef", "kind")
	objectRefName, ok, _ := unstructured.NestedString(action, "objectRef", "name")
	if !ok || kind != "Job" || objectRefName == "" || hostBackupName == "" || virtualBackupName == "" || len(hostBackupUID) < 8 || len(virtualBackupUID) < 8 {
		_ = unstructured.SetNestedMap(action, objectRef, "objectRef")
		return
	}

	actionName, ok, _ := unstructured.NestedString(action, "name")
	if !ok || actionName == "" {
		_ = unstructured.SetNestedMap(action, objectRef, "objectRef")
		return
	}

	hostJobName := generateBackupJobNameForStatus(hostBackupName, hostBackupUID, actionName)
	if objectRefName == hostJobName {
		objectRef["name"] = generateBackupJobNameForStatus(virtualBackupName, virtualBackupUID, actionName)
	}

	_ = unstructured.SetNestedMap(action, objectRef, "objectRef")
}

func generateBackupJobNameForStatus(backupName string, backupUID types.UID, prefix string) string {
	if len(backupUID) < 8 {
		return ""
	}

	name := fmt.Sprintf("%s-%s-%s", prefix, backupName, string(backupUID)[:8])
	if len(name) > 63 {
		return strings.TrimSuffix(name[:63], "-")
	}
	return name
}

func translateHostPodNameToVirtual(ctx *synccontext.SyncContext, hostName, namespace string) string {
	if hostName == "" || namespace == "" || ctx == nil {
		return hostName
	}

	if ctx.Mappings == nil {
		return translateSingleNamespaceHostPodNameToVirtual(ctx, hostName, namespace)
	}

	podMapper, err := ctx.Mappings.ByGVK(mappings.Pods())
	if err != nil {
		return translateSingleNamespaceHostPodNameToVirtual(ctx, hostName, namespace)
	}

	hostNamespace := translate.Default.HostName(ctx, hostName, namespace).Namespace
	virtualName := podMapper.HostToVirtual(ctx, types.NamespacedName{Name: hostName, Namespace: hostNamespace}, nil)
	if virtualName.Name == "" {
		return translateSingleNamespaceHostPodNameToVirtual(ctx, hostName, namespace)
	}

	return virtualName.Name
}

func translateSingleNamespaceHostPodNameToVirtual(ctx *synccontext.SyncContext, hostName, namespace string) string {
	separator := "-x-" + namespace
	index := strings.LastIndex(hostName, separator)
	if index <= 0 {
		return hostName
	}

	virtualName := hostName[:index]
	if translate.Default.HostName(ctx, virtualName, namespace).Name != hostName {
		return hostName
	}

	return virtualName
}

func translateBackupRepoLabelToVirtual(ctx *synccontext.SyncContext, from, to map[string]string) {
	if to == nil {
		return
	}

	backupRepoName := from[dataProtectionBackupRepoLabel]
	if backupRepoName == "" {
		return
	}

	to[dataProtectionBackupRepoLabel] = translateBackupRepoNameToVirtual(ctx, backupRepoName)
}

func preserveHostBackupRepoLabel(from, to map[string]string) {
	if to == nil {
		return
	}

	backupRepoName := from[dataProtectionBackupRepoLabel]
	if backupRepoName == "" {
		return
	}

	to[dataProtectionBackupRepoLabel] = backupRepoName
}

func translateBackupRepoNameToVirtual(ctx *synccontext.SyncContext, hostName string) string {
	if hostName == "" || ctx == nil || ctx.VirtualClient == nil {
		return hostName
	}

	backupRepos := &unstructured.UnstructuredList{}
	backupRepos.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "dataprotection.kubeblocks.io",
		Version: "v1alpha1",
		Kind:    "BackupRepoList",
	})
	err := ctx.VirtualClient.List(ctx, backupRepos)
	if err != nil {
		if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			return hostName
		}
		return hostName
	}

	return translateBackupRepoNameToVirtualFromList(hostName, backupRepos)
}

func translateBackupRepoNameToVirtualFromList(hostName string, backupRepos *unstructured.UnstructuredList) string {
	if backupRepos == nil || len(backupRepos.Items) == 0 {
		return hostName
	}

	for i := range backupRepos.Items {
		if backupRepos.Items[i].GetName() == hostName {
			return hostName
		}
	}

	defaultRepo := ""
	for i := range backupRepos.Items {
		if isDefaultBackupRepo(&backupRepos.Items[i]) {
			if defaultRepo != "" {
				return hostName
			}
			defaultRepo = backupRepos.Items[i].GetName()
		}
	}
	if defaultRepo != "" {
		return defaultRepo
	}

	if len(backupRepos.Items) == 1 {
		return backupRepos.Items[0].GetName()
	}

	return hostName
}

func isDefaultBackupRepo(repo *unstructured.Unstructured) bool {
	if repo == nil {
		return false
	}

	if repo.GetAnnotations()[dataProtectionDefaultRepoAnnotation] == "true" {
		return true
	}

	isDefault, ok, _ := unstructured.NestedBool(repo.Object, "status", "isDefault")
	return ok && isDefault
}
