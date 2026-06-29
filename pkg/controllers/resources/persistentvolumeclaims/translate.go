package persistentvolumeclaims

import (
	"github.com/loft-sh/vcluster/pkg/constants"
	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/mappings/generic"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

var deprecatedStorageClassAnnotation = "volume.beta.kubernetes.io/storage-class"

func (s *persistentVolumeClaimSyncer) translate(ctx *synccontext.SyncContext, vPvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error) {
	pPVC := translate.HostMetadata(vPvc, s.VirtualToHost(ctx, types.NamespacedName{Name: vPvc.GetName(), Namespace: vPvc.GetNamespace()}, vPvc), s.excludedAnnotations...)
	s.translateSelector(ctx, pPVC)

	if vPvc.Annotations[constants.SkipTranslationAnnotation] != "true" {
		if pPVC.Spec.DataSource != nil {
			switch pPVC.Spec.DataSource.Kind {
			case "VolumeSnapshot":
				pPVC.Spec.DataSource.Name = mappings.VirtualToHostName(ctx, pPVC.Spec.DataSource.Name, vPvc.Namespace, mappings.VolumeSnapshots())
			case "PersistentVolumeClaim":
				pPVC.Spec.DataSource.Name = mappings.VirtualToHostName(ctx, pPVC.Spec.DataSource.Name, vPvc.Namespace, mappings.PersistentVolumeClaims())
			case dataProtectionBackupKind:
				if pPVC.Spec.DataSource.APIGroup != nil && *pPVC.Spec.DataSource.APIGroup == dataProtectionAPIGroup {
					pPVC.Spec.DataSource.Name = translateDataProtectionBackupNameToHost(ctx, pPVC.Spec.DataSource.Name, vPvc.Namespace)
				}
			}
		}

		if pPVC.Spec.DataSourceRef != nil {
			namespace := vPvc.Namespace
			if pPVC.Spec.DataSourceRef.Namespace != nil {
				namespace = *pPVC.Spec.DataSourceRef.Namespace
			}

			switch pPVC.Spec.DataSourceRef.Kind {
			case "VolumeSnapshot":
				pPVC.Spec.DataSourceRef.Name = mappings.VirtualToHostName(ctx, pPVC.Spec.DataSourceRef.Name, namespace, mappings.VolumeSnapshots())
			case "PersistentVolumeClaim":
				pPVC.Spec.DataSourceRef.Name = mappings.VirtualToHostName(ctx, pPVC.Spec.DataSourceRef.Name, namespace, mappings.PersistentVolumeClaims())
			case dataProtectionBackupKind:
				if pPVC.Spec.DataSourceRef.APIGroup != nil && *pPVC.Spec.DataSourceRef.APIGroup == dataProtectionAPIGroup {
					pPVC.Spec.DataSourceRef.Name = translateDataProtectionBackupNameToHost(ctx, pPVC.Spec.DataSourceRef.Name, namespace)
				}
			}
		}
	}

	return pPVC, nil
}

func translateDataProtectionRestoreSourceAnnotationsToHost(ctx *synccontext.SyncContext, pPVC *corev1.PersistentVolumeClaim, virtualNamespace string) {
	if !isDataProtectionRestoreSourceAnnotationBackup(pPVC.Annotations) {
		return
	}

	sourceName := pPVC.Annotations[kubeBlocksRestoreSourceNameAnnotation]
	if sourceName == "" {
		return
	}
	sourceNamespace := pPVC.Annotations[kubeBlocksRestoreSourceNamespaceAnnotation]
	if sourceNamespace == "" {
		sourceNamespace = virtualNamespace
	}
	if sourceNamespace == pPVC.Namespace && sourceNamespace != virtualNamespace {
		return
	}

	hostName := translateDataProtectionBackupNameToHostName(ctx, sourceName, sourceNamespace)
	if hostName.Name == "" {
		return
	}
	pPVC.Annotations[kubeBlocksRestoreSourceNameAnnotation] = hostName.Name
	if hostName.Namespace != "" {
		pPVC.Annotations[kubeBlocksRestoreSourceNamespaceAnnotation] = hostName.Namespace
	}
}

func translateDataProtectionRestoreSourceAnnotationsToVirtual(ctx *synccontext.SyncContext, vPVC *corev1.PersistentVolumeClaim) {
	if !isDataProtectionRestoreSourceAnnotationBackup(vPVC.Annotations) {
		return
	}

	sourceName := vPVC.Annotations[kubeBlocksRestoreSourceNameAnnotation]
	sourceNamespace := vPVC.Annotations[kubeBlocksRestoreSourceNamespaceAnnotation]
	if sourceName == "" || sourceNamespace == "" || ctx == nil || ctx.Mappings == nil {
		return
	}

	hostName := types.NamespacedName{Name: sourceName, Namespace: sourceNamespace}
	virtualName, ok := generic.HostToVirtualFromStore(ctx, hostName, mappings.DataProtectionBackups())
	if ok && virtualName.Name != "" {
		vPVC.Annotations[kubeBlocksRestoreSourceNameAnnotation] = virtualName.Name
		if virtualName.Namespace != "" {
			vPVC.Annotations[kubeBlocksRestoreSourceNamespaceAnnotation] = virtualName.Namespace
		}
		return
	}

	mapper, err := ctx.Mappings.ByGVK(mappings.DataProtectionBackups())
	if err != nil {
		return
	}
	virtualName = mapper.HostToVirtual(ctx, hostName, nil)
	if virtualName.Name == "" {
		return
	}
	vPVC.Annotations[kubeBlocksRestoreSourceNameAnnotation] = virtualName.Name
	if virtualName.Namespace != "" {
		vPVC.Annotations[kubeBlocksRestoreSourceNamespaceAnnotation] = virtualName.Namespace
	}
}

func isDataProtectionRestoreSourceAnnotationBackup(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}

	return annotations[kubeBlocksRestoreSourceAPIGroupAnnotation] == dataProtectionAPIGroup &&
		annotations[kubeBlocksRestoreSourceKindAnnotation] == dataProtectionBackupKind
}

func translateDataProtectionBackupNameToHost(ctx *synccontext.SyncContext, name, namespace string) string {
	return translateDataProtectionBackupNameToHostName(ctx, name, namespace).Name
}

func translateDataProtectionBackupNameToHostName(ctx *synccontext.SyncContext, name, namespace string) types.NamespacedName {
	if name == "" || namespace == "" {
		return types.NamespacedName{Name: name}
	}

	if ctx != nil && ctx.Mappings != nil {
		mapper, err := ctx.Mappings.ByGVK(mappings.DataProtectionBackups())
		if err == nil {
			hostName := mapper.VirtualToHost(ctx, types.NamespacedName{Name: name, Namespace: namespace}, nil)
			if hostName.Name != "" {
				return hostName
			}
		}
	}

	return translate.Default.HostName(ctx, name, namespace)
}

func (s *persistentVolumeClaimSyncer) translateSelector(ctx *synccontext.SyncContext, vPvc *corev1.PersistentVolumeClaim) {
	storageClassName := ""
	if vPvc.Spec.StorageClassName != nil && *vPvc.Spec.StorageClassName != "" {
		storageClassName = *vPvc.Spec.StorageClassName
	} else if vPvc.Annotations != nil && vPvc.Annotations[deprecatedStorageClassAnnotation] != "" {
		storageClassName = vPvc.Annotations[deprecatedStorageClassAnnotation]
	}

	// translate storage class if we manage those in vcluster
	if s.storageClassesEnabled && storageClassName != "" {
		translated := translate.Default.HostNameCluster(storageClassName)
		delete(vPvc.Annotations, deprecatedStorageClassAnnotation)
		vPvc.Spec.StorageClassName = &translated
	}

	// translate selector & volume name
	if !s.useFakePersistentVolumes {
		if vPvc.Annotations == nil || vPvc.Annotations[constants.SkipTranslationAnnotation] != "true" {
			if vPvc.Spec.Selector != nil {
				vPvc.Spec.Selector = translate.HostLabelSelector(vPvc.Spec.Selector)
			}
			if vPvc.Spec.VolumeName != "" {
				vPvc.Spec.VolumeName = translate.Default.HostNameCluster(vPvc.Spec.VolumeName)
			}
			// check if the storage class exists in the physical cluster
			if !s.storageClassesEnabled && storageClassName != "" {
				// Should the PVC be dynamically provisioned or not?
				if vPvc.Spec.Selector == nil && vPvc.Spec.VolumeName == "" {
					err := ctx.HostClient.Get(ctx, types.NamespacedName{Name: storageClassName}, &storagev1.StorageClass{})
					if err != nil && kerrors.IsNotFound(err) {
						translated := translate.Default.HostNameCluster(storageClassName)
						delete(vPvc.Annotations, deprecatedStorageClassAnnotation)
						vPvc.Spec.StorageClassName = &translated
					}
				} else {
					translated := translate.Default.HostNameCluster(storageClassName)
					delete(vPvc.Annotations, deprecatedStorageClassAnnotation)
					vPvc.Spec.StorageClassName = &translated
				}
			}
		}
	}
}

func (s *persistentVolumeClaimSyncer) translateUpdateBackwards(pObj, vObj *corev1.PersistentVolumeClaim) {
	if vObj.Annotations[bindCompletedAnnotation] != pObj.Annotations[bindCompletedAnnotation] {
		if vObj.Annotations == nil {
			vObj.Annotations = map[string]string{}
		}
		vObj.Annotations[bindCompletedAnnotation] = pObj.Annotations[bindCompletedAnnotation]
	}
	if vObj.Annotations[boundByControllerAnnotation] != pObj.Annotations[boundByControllerAnnotation] {
		if vObj.Annotations == nil {
			vObj.Annotations = map[string]string{}
		}
		vObj.Annotations[boundByControllerAnnotation] = pObj.Annotations[boundByControllerAnnotation]
	}
	if vObj.Annotations[storageProvisionerAnnotation] != pObj.Annotations[storageProvisionerAnnotation] {
		if vObj.Annotations == nil {
			vObj.Annotations = map[string]string{}
		}
		vObj.Annotations[storageProvisionerAnnotation] = pObj.Annotations[storageProvisionerAnnotation]
	}
	if vObj.Annotations[selectedNodeAnnotation] != pObj.Annotations[selectedNodeAnnotation] {
		if vObj.Annotations == nil {
			vObj.Annotations = map[string]string{}
		}
		vObj.Annotations[selectedNodeAnnotation] = pObj.Annotations[selectedNodeAnnotation]
	}
}
