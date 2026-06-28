package resources

import (
	"github.com/loft-sh/vcluster/pkg/controllers/resources/backups"
	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/mappings/generic"
	"github.com/loft-sh/vcluster/pkg/mappings/store/verify"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func CreateDataProtectionBackupsMapper(ctx *synccontext.RegisterContext) (synccontext.Mapper, error) {
	if _, ok := backups.Config(ctx.Config.Sync.ToHost.CustomResources); !ok {
		return nil, nil
	}

	return generic.WithRecorder(&dataProtectionBackupsMapper{}), nil
}

type dataProtectionBackupsMapper struct{}

func (m *dataProtectionBackupsMapper) GroupVersionKind() schema.GroupVersionKind {
	return mappings.DataProtectionBackups()
}

func (m *dataProtectionBackupsMapper) Migrate(_ *synccontext.RegisterContext, _ synccontext.Mapper) error {
	return nil
}

func (m *dataProtectionBackupsMapper) VirtualToHost(ctx *synccontext.SyncContext, req types.NamespacedName, _ client.Object) types.NamespacedName {
	return translate.Default.HostName(ctx, req.Name, req.Namespace)
}

func (m *dataProtectionBackupsMapper) HostToVirtual(ctx *synccontext.SyncContext, req types.NamespacedName, pObj client.Object) types.NamespacedName {
	vName := generic.TryToTranslateBackByAnnotations(ctx, req, pObj, mappings.DataProtectionBackups())
	if vName.Name != "" {
		return vName
	}

	return generic.TryToTranslateBackByName(ctx, req, mappings.DataProtectionBackups())
}

func (m *dataProtectionBackupsMapper) IsManaged(ctx *synccontext.SyncContext, pObj client.Object) (bool, error) {
	if !verify.CheckHostObject(ctx, synccontext.Object{
		GroupVersionKind: mappings.DataProtectionBackups(),
		NamespacedName:   client.ObjectKeyFromObject(pObj),
	}) {
		return false, nil
	}

	return translate.Default.IsManaged(ctx, pObj), nil
}
