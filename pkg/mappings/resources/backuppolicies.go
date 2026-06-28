package resources

import (
	"github.com/loft-sh/vcluster/pkg/controllers/resources/backuppolicies"
	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/mappings/generic"
	"github.com/loft-sh/vcluster/pkg/mappings/store/verify"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func CreateDataProtectionBackupPoliciesMapper(ctx *synccontext.RegisterContext) (synccontext.Mapper, error) {
	if _, ok := backuppolicies.Config(ctx.Config.Sync.ToHost.CustomResources); !ok {
		return nil, nil
	}

	return generic.WithRecorder(&dataProtectionBackupPoliciesMapper{}), nil
}

type dataProtectionBackupPoliciesMapper struct{}

func (m *dataProtectionBackupPoliciesMapper) GroupVersionKind() schema.GroupVersionKind {
	return mappings.DataProtectionBackupPolicies()
}

func (m *dataProtectionBackupPoliciesMapper) Migrate(_ *synccontext.RegisterContext, _ synccontext.Mapper) error {
	return nil
}

func (m *dataProtectionBackupPoliciesMapper) VirtualToHost(ctx *synccontext.SyncContext, req types.NamespacedName, _ client.Object) types.NamespacedName {
	return translate.Default.HostName(ctx, req.Name, req.Namespace)
}

func (m *dataProtectionBackupPoliciesMapper) HostToVirtual(ctx *synccontext.SyncContext, req types.NamespacedName, pObj client.Object) types.NamespacedName {
	vName := generic.TryToTranslateBackByAnnotations(ctx, req, pObj, mappings.DataProtectionBackupPolicies())
	if vName.Name != "" {
		return vName
	}

	return generic.TryToTranslateBackByName(ctx, req, mappings.DataProtectionBackupPolicies())
}

func (m *dataProtectionBackupPoliciesMapper) IsManaged(ctx *synccontext.SyncContext, pObj client.Object) (bool, error) {
	if !verify.CheckHostObject(ctx, synccontext.Object{
		GroupVersionKind: mappings.DataProtectionBackupPolicies(),
		NamespacedName:   client.ObjectKeyFromObject(pObj),
	}) {
		return false, nil
	}

	return translate.Default.IsManaged(ctx, pObj), nil
}
