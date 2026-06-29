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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	dataProtectionBackupRepoLabel       = "dataprotection.kubeblocks.io/backup-repo-name"
	dataProtectionDefaultRepoAnnotation = "dataprotection.kubeblocks.io/is-default-repo"
	dataProtectionSkipReconciliation    = "dataprotection.kubeblocks.io/skip-reconciliation"
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
	markHostBackupReconciliationSkipped(pObj)
	unstructured.RemoveNestedField(pObj.Object, "status")
	translateBackupPolicyName(ctx, event.Virtual.Object, pObj.Object, event.Virtual.GetNamespace())

	err := pro.ApplyPatchesHostObject(ctx, nil, pObj, event.Virtual, s.patches, false)
	if err != nil {
		return ctrl.Result{}, err
	}

	markHostBackupReconciliationSkipped(pObj)
	translateBackupTargetConnectionCredentialSecretNameToHost(ctx, event.Virtual.Object, pObj.Object, event.Virtual.GetNamespace())

	result, err := patcher.CreateHostObject(ctx, event.Virtual, pObj, s.EventRecorder(), false)
	if err != nil {
		return result, err
	}

	return result, ensureHostBackupRestoreStatus(ctx, event.Virtual, pObj)
}

func (s *backupSyncer) Sync(ctx *synccontext.SyncContext, event *synccontext.SyncEvent[*unstructured.Unstructured]) (ctrl.Result, error) {
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

	syncBackupDesiredStateToSkippedHost(ctx, event.Virtual, event.Host)

	if err := patch.Patch(ctx, event.Host, event.Virtual); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, ensureHostBackupRestoreStatus(ctx, event.Virtual, event.Host)
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

func markHostBackupReconciliationSkipped(hostBackup *unstructured.Unstructured) {
	if hostBackup == nil {
		return
	}

	hostBackup.SetAnnotations(skipHostBackupReconciliation(hostBackup.GetAnnotations()))
}

func hostBackupAnnotations(virtualBackup, hostBackup client.Object) map[string]string {
	return skipHostBackupReconciliation(translate.HostAnnotations(virtualBackup, hostBackup))
}

func virtualBackupAnnotations(hostBackup, virtualBackup client.Object) map[string]string {
	annotations := translate.VirtualAnnotations(hostBackup, virtualBackup)
	delete(annotations, dataProtectionSkipReconciliation)
	return annotations
}

func skipHostBackupReconciliation(annotations map[string]string) map[string]string {
	if annotations == nil {
		annotations = map[string]string{}
	}

	annotations[dataProtectionSkipReconciliation] = "true"
	return annotations
}

func syncBackupDesiredStateToSkippedHost(ctx *synccontext.SyncContext, virtualBackup, hostBackup *unstructured.Unstructured) {
	// The in-vcluster DP controller owns runtime state. The host Backup is only a
	// translated mirror so the host DP controller must not launch a second Job.
	// Host-side restore consumers still read Backup status, so mirror translated
	// status while keeping host Backup reconciliation disabled.
	copyNestedField(virtualBackup.Object, hostBackup.Object, "spec")
	copyNestedField(virtualBackup.Object, hostBackup.Object, "status")
	translateBackupPolicyName(ctx, virtualBackup.Object, hostBackup.Object, virtualBackup.GetNamespace())
	translateBackupRestoreStatusToHost(ctx, hostBackup.Object, virtualBackup.GetNamespace())

	virtualBackup.SetAnnotations(virtualBackupAnnotations(hostBackup, virtualBackup))
	hostBackup.SetAnnotations(hostBackupAnnotations(virtualBackup, hostBackup))
	virtualLabels := translate.VirtualLabels(hostBackup, virtualBackup)
	translateBackupRepoLabelToVirtual(ctx, hostBackup.GetLabels(), virtualLabels)
	virtualBackup.SetLabels(virtualLabels)

	hostLabels := translate.HostLabels(virtualBackup, hostBackup)
	preserveHostBackupRepoLabel(hostBackup.GetLabels(), hostLabels)
	hostBackup.SetLabels(hostLabels)
}

func ensureHostBackupRestoreStatus(ctx *synccontext.SyncContext, virtualBackup, hostBackup *unstructured.Unstructured) error {
	if ctx == nil || ctx.HostClient == nil || virtualBackup == nil || hostBackup == nil {
		return nil
	}

	desiredStatus, ok, err := desiredHostBackupRestoreStatus(ctx, virtualBackup)
	if err != nil || !ok || len(desiredStatus) == 0 {
		return err
	}

	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		latest := NewObject()
		if err := ctx.HostClient.Get(ctx, client.ObjectKeyFromObject(hostBackup), latest); err != nil {
			return fmt.Errorf("get host backup before status mirror: %w", err)
		}

		if !hostBackupRestoreStatusNeedsMirror(latest, desiredStatus) {
			return nil
		}

		if err := unstructured.SetNestedMap(latest.Object, desiredStatus, "status"); err != nil {
			return fmt.Errorf("set translated host backup status: %w", err)
		}
		return ctx.HostClient.Status().Update(ctx, latest)
	}); err != nil {
		return fmt.Errorf("mirror host backup status: %w", err)
	}

	return nil
}

func desiredHostBackupRestoreStatus(ctx *synccontext.SyncContext, virtualBackup *unstructured.Unstructured) (map[string]interface{}, bool, error) {
	status, ok, err := unstructured.NestedMap(virtualBackup.Object, "status")
	if err != nil || !ok || len(status) == 0 {
		return nil, ok, err
	}

	desired := map[string]interface{}{"status": status}
	translateBackupRestoreStatusToHost(ctx, desired, virtualBackup.GetNamespace())
	return desired["status"].(map[string]interface{}), true, nil
}

func hostBackupRestoreStatusNeedsMirror(hostBackup *unstructured.Unstructured, desiredStatus map[string]interface{}) bool {
	currentStatus, ok, err := unstructured.NestedMap(hostBackup.Object, "status")
	if err != nil || !ok {
		return true
	}

	return !apiequality.Semantic.DeepEqual(currentStatus, desiredStatus)
}

func translateBackupRestoreStatusToHost(ctx *synccontext.SyncContext, to map[string]interface{}, namespace string) {
	translateBackupRepoStatusToHost(ctx, to)
	translateBackupTargetPodNameToHost(ctx, to, namespace)
	translateBackupTargetConnectionCredentialSecretNameToHost(ctx, to, to, namespace)
	translateBackupTargetsToHost(ctx, to, namespace)
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

func translateBackupRepoStatusToHost(ctx *synccontext.SyncContext, to map[string]interface{}) {
	backupRepoName, ok, _ := unstructured.NestedString(to, "status", "backupRepoName")
	if !ok || backupRepoName == "" {
		return
	}

	_ = unstructured.SetNestedField(to, translateBackupRepoNameToHost(ctx, backupRepoName), "status", "backupRepoName")
}

func translateBackupTargetPodNameToHost(ctx *synccontext.SyncContext, to map[string]interface{}, namespace string) {
	targetPodName, ok, _ := unstructured.NestedString(to, "status", "targetPodName")
	if ok && targetPodName != "" {
		_ = unstructured.SetNestedField(to, translateVirtualPodNameToHost(ctx, targetPodName, namespace), "status", "targetPodName")
	}

	translateSelectedTargetPodsToHost(ctx, to, namespace, "status", "target", "selectedTargetPods")
}

func translateBackupTargetPodNameToVirtual(ctx *synccontext.SyncContext, from, to map[string]interface{}, namespace string) {
	targetPodName, ok, _ := unstructured.NestedString(from, "status", "targetPodName")
	if ok && targetPodName != "" {
		_ = unstructured.SetNestedField(to, translateHostPodNameToVirtual(ctx, targetPodName, namespace), "status", "targetPodName")
	}

	selectedTargetPods, ok, _ := unstructured.NestedSlice(from, "status", "target", "selectedTargetPods")
	if !ok || len(selectedTargetPods) == 0 {
		return
	}

	for i := range selectedTargetPods {
		podName, ok := selectedTargetPods[i].(string)
		if !ok || podName == "" {
			continue
		}

		selectedTargetPods[i] = translateHostPodNameToVirtual(ctx, podName, namespace)
	}
	_ = unstructured.SetNestedSlice(to, selectedTargetPods, "status", "target", "selectedTargetPods")
}

func translateBackupTargetConnectionCredentialSecretNameToHost(ctx *synccontext.SyncContext, from, to map[string]interface{}, namespace string) {
	secretName, ok, _ := unstructured.NestedString(from, "status", "target", "connectionCredential", "secretName")
	if !ok || secretName == "" {
		return
	}

	_ = unstructured.SetNestedField(to, translateVirtualSecretNameToHost(ctx, secretName, namespace), "status", "target", "connectionCredential", "secretName")
}

func translateBackupTargetsToHost(ctx *synccontext.SyncContext, to map[string]interface{}, namespace string) {
	targets, ok, _ := unstructured.NestedSlice(to, "status", "targets")
	if !ok || len(targets) == 0 {
		return
	}

	for i := range targets {
		target, ok := targets[i].(map[string]interface{})
		if !ok {
			continue
		}

		translateSelectedTargetPodsToHost(ctx, target, namespace, "selectedTargetPods")
		secretName, ok, _ := unstructured.NestedString(target, "connectionCredential", "secretName")
		if ok && secretName != "" {
			_ = unstructured.SetNestedField(target, translateVirtualSecretNameToHost(ctx, secretName, namespace), "connectionCredential", "secretName")
		}
	}

	_ = unstructured.SetNestedSlice(to, targets, "status", "targets")
}

func translateSelectedTargetPodsToHost(ctx *synccontext.SyncContext, to map[string]interface{}, namespace string, fields ...string) {
	selectedTargetPods, ok, _ := unstructured.NestedSlice(to, fields...)
	if !ok || len(selectedTargetPods) == 0 {
		return
	}

	for i := range selectedTargetPods {
		podName, ok := selectedTargetPods[i].(string)
		if !ok || podName == "" {
			continue
		}

		selectedTargetPods[i] = translateVirtualPodNameToHost(ctx, podName, namespace)
	}
	_ = unstructured.SetNestedSlice(to, selectedTargetPods, fields...)
}

func translateBackupTargetConnectionCredentialSecretNameToVirtual(ctx *synccontext.SyncContext, from, to map[string]interface{}, namespace string) {
	secretName, ok, _ := unstructured.NestedString(from, "status", "target", "connectionCredential", "secretName")
	if !ok || secretName == "" {
		return
	}

	_ = unstructured.SetNestedField(to, translateHostSecretNameToVirtual(ctx, secretName, namespace), "status", "target", "connectionCredential", "secretName")
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

func translateVirtualPodNameToHost(ctx *synccontext.SyncContext, virtualName, namespace string) string {
	if virtualName == "" || namespace == "" {
		return virtualName
	}

	if translateHostPodNameToVirtual(ctx, virtualName, namespace) != virtualName {
		return virtualName
	}

	if ctx != nil && ctx.Mappings != nil {
		podMapper, err := ctx.Mappings.ByGVK(mappings.Pods())
		if err == nil {
			hostName := podMapper.VirtualToHost(ctx, types.NamespacedName{Name: virtualName, Namespace: namespace}, nil)
			if hostName.Name != "" {
				return hostName.Name
			}
		}
	}

	return translate.Default.HostName(ctx, virtualName, namespace).Name
}

func translateSingleNamespaceHostPodNameToVirtual(ctx *synccontext.SyncContext, hostName, namespace string) string {
	return translateSingleNamespaceHostNameToVirtual(ctx, hostName, namespace)
}

func translateVirtualSecretNameToHost(ctx *synccontext.SyncContext, virtualName, namespace string) string {
	if virtualName == "" || namespace == "" {
		return virtualName
	}

	if translateHostSecretNameToVirtual(ctx, virtualName, namespace) != virtualName {
		return virtualName
	}

	if ctx != nil && ctx.Mappings != nil {
		secretMapper, err := ctx.Mappings.ByGVK(mappings.Secrets())
		if err == nil {
			hostName := secretMapper.VirtualToHost(ctx, types.NamespacedName{Name: virtualName, Namespace: namespace}, nil)
			if hostName.Name != "" {
				return hostName.Name
			}
		}
	}

	return translate.Default.HostName(ctx, virtualName, namespace).Name
}

func translateHostSecretNameToVirtual(ctx *synccontext.SyncContext, hostName, namespace string) string {
	if hostName == "" || namespace == "" || ctx == nil {
		return hostName
	}

	if ctx.Mappings != nil {
		secretMapper, err := ctx.Mappings.ByGVK(mappings.Secrets())
		if err == nil {
			hostNamespace := translate.Default.HostName(ctx, hostName, namespace).Namespace
			virtualName := secretMapper.HostToVirtual(ctx, types.NamespacedName{Name: hostName, Namespace: hostNamespace}, nil)
			if virtualName.Name != "" {
				return virtualName.Name
			}
		}
	}

	return translateSingleNamespaceHostNameToVirtual(ctx, hostName, namespace)
}

func translateSingleNamespaceHostNameToVirtual(ctx *synccontext.SyncContext, hostName, namespace string) string {
	separator := "-x-"
	for start := 0; start < len(hostName); {
		index := strings.Index(hostName[start:], separator)
		if index < 0 {
			break
		}

		index += start
		if index > 0 {
			virtualName := hostName[:index]
			if translate.Default.HostName(ctx, virtualName, namespace).Name == hostName {
				return virtualName
			}
		}

		start = index + len(separator)
	}

	return hostName
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

func translateBackupRepoNameToHost(ctx *synccontext.SyncContext, virtualName string) string {
	if virtualName == "" || ctx == nil || ctx.HostClient == nil {
		return virtualName
	}

	backupRepos := &unstructured.UnstructuredList{}
	backupRepos.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "dataprotection.kubeblocks.io",
		Version: "v1alpha1",
		Kind:    "BackupRepoList",
	})
	err := ctx.HostClient.List(ctx, backupRepos)
	if err != nil {
		if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			return virtualName
		}
		return virtualName
	}

	return translateBackupRepoNameToHostFromList(virtualName, backupRepos)
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

func translateBackupRepoNameToHostFromList(virtualName string, backupRepos *unstructured.UnstructuredList) string {
	if backupRepos == nil || len(backupRepos.Items) == 0 {
		return virtualName
	}

	for i := range backupRepos.Items {
		if backupRepos.Items[i].GetName() == virtualName {
			return virtualName
		}
	}

	defaultRepo := ""
	for i := range backupRepos.Items {
		if isDefaultBackupRepo(&backupRepos.Items[i]) {
			if defaultRepo != "" {
				return virtualName
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

	return virtualName
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
