package backups

import (
	"github.com/loft-sh/vcluster/config"
	"github.com/loft-sh/vcluster/pkg/mappings"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const ResourcePath = "backups.dataprotection.kubeblocks.io/v1alpha1"

func Config(customResources map[string]config.SyncToHostCustomResource) (config.SyncToHostCustomResource, bool) {
	cfg, ok := customResources[ResourcePath]
	if !ok || !cfg.Enabled {
		return config.SyncToHostCustomResource{}, false
	}

	return cfg, true
}

func NewObject() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(mappings.DataProtectionBackups())
	return obj
}
