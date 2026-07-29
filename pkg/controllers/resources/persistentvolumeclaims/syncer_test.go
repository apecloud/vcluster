package persistentvolumeclaims

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/scheme"
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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type injectHelperAfterHostPVGetClient struct {
	client.Client
	virtualClient client.Client
	helper        *corev1.PersistentVolumeClaim
	hostPVName    string
	hostPVGets    int
	patchCalls    int
	injected      bool
}

func (c *injectHelperAfterHostPVGetClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := c.Client.Get(ctx, key, obj, opts...)
	if err != nil {
		return err
	}

	if _, ok := obj.(*corev1.PersistentVolume); ok && key.Name == c.hostPVName {
		c.hostPVGets++
		if c.hostPVGets == 2 && !c.injected {
			c.injected = true
			return c.virtualClient.Create(ctx, c.helper.DeepCopy())
		}
	}

	return nil
}

func (c *injectHelperAfterHostPVGetClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if _, ok := obj.(*corev1.PersistentVolume); ok {
		c.patchCalls++
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

var externalPopulatorControllerTestID atomic.Uint64

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

type externalPopulatorDependencyFixture struct {
	target *corev1.PersistentVolumeClaim
	helper *corev1.PersistentVolumeClaim
	pv     *corev1.PersistentVolume
}

func newExternalPopulatorDependencyFixture() *externalPopulatorDependencyFixture {
	const (
		namespace = "testns"
		pvName    = "restore-populated-pv"
	)
	targetUID := types.UID("target-pvc-uid")
	helperUID := types.UID("populate-helper-pvc-uid")
	dataProtectionGroup := dataProtectionAPIGroup
	target := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "target",
			Namespace: namespace,
			UID:       targetUID,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: pvName,
			DataSourceRef: &corev1.TypedObjectReference{
				APIGroup: &dataProtectionGroup,
				Kind:     dataProtectionBackupKind,
				Name:     "backup",
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimPending,
			Conditions: []corev1.PersistentVolumeClaimCondition{
				{
					Type:   externalPopulatorPopulateConditionType,
					Status: corev1.ConditionTrue,
					Reason: externalPopulatorRestoreConditionReasonSucceeded,
				},
			},
		},
	}
	helper := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      externalPopulatorPopulateHelperPrefix + string(targetUID),
			Namespace: namespace,
			UID:       helperUID,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: pvName,
		},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				Namespace: namespace,
				Name:      helper.Name,
				UID:       helperUID,
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	return &externalPopulatorDependencyFixture{
		target: target,
		helper: helper,
		pv:     pv,
	}
}

func newExternalPopulatorHostHandoffObjects(
	f *externalPopulatorDependencyFixture,
) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	hostTranslator := translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)
	hostTargetName := hostTranslator.HostName(nil, f.target.Name, f.target.Namespace)
	hostTarget := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hostTargetName.Name,
			Namespace: testingutil.DefaultTestTargetNamespace,
			UID:       types.UID("host-target-pvc-uid"),
			Annotations: map[string]string{
				translate.NameAnnotation:          f.target.Name,
				translate.NamespaceAnnotation:     f.target.Namespace,
				translate.UIDAnnotation:           string(f.target.UID),
				translate.KindAnnotation:          corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim").String(),
				translate.HostNamespaceAnnotation: testingutil.DefaultTestTargetNamespace,
				translate.HostNameAnnotation:      hostTargetName.Name,
			},
			Labels: map[string]string{
				translate.MarkerLabel:    translate.VClusterName,
				translate.NamespaceLabel: f.target.Namespace,
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}
	hostHelperName := hostTranslator.HostName(nil, f.helper.Name, f.helper.Namespace)
	hostHelper := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hostHelperName.Name,
			Namespace: testingutil.DefaultTestTargetNamespace,
			UID:       types.UID("host-helper-pvc-uid"),
		},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: f.pv.Name},
	}
	hostPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: f.pv.Name},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				APIVersion: corev1.SchemeGroupVersion.Version,
				Kind:       "PersistentVolumeClaim",
				Namespace:  hostHelper.Namespace,
				Name:       hostHelper.Name,
				UID:        hostHelper.UID,
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	return hostTarget, hostHelper, hostPV
}

func newExternalPopulatorMapperTestSyncer(
	t *testing.T,
	virtualObjects ...runtime.Object,
) (*synccontext.SyncContext, *persistentVolumeClaimSyncer) {
	t.Helper()

	hostClient := testingutil.NewFakeClient(scheme.Scheme)
	virtualClient := testingutil.NewFakeClient(scheme.Scheme, virtualObjects...)
	registerCtx := syncertesting.NewFakeRegisterContext(testingutil.NewFakeConfig(), hostClient, virtualClient)
	syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, registerCtx, New)
	return syncCtx, objectSyncer.(*persistentVolumeClaimSyncer)
}

type externalPopulatorMapperErrorClient struct {
	client.Client
	getErr  error
	listErr error
}

func (c *externalPopulatorMapperErrorClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if c.getErr != nil {
		return c.getErr
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *externalPopulatorMapperErrorClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if c.listErr != nil {
		return c.listErr
	}
	return c.Client.List(ctx, list, opts...)
}

type externalPopulatorFlakyMapperClient struct {
	client.Client

	mu                 sync.Mutex
	remainingGetErrors int
	injectedGetErrors  int
	getErr             error
}

func (c *externalPopulatorFlakyMapperClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	c.mu.Lock()
	if c.remainingGetErrors > 0 {
		c.remainingGetErrors--
		c.injectedGetErrors++
		err := c.getErr
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()

	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *externalPopulatorFlakyMapperClient) injectedErrors() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.injectedGetErrors
}

type externalPopulatorTopologyListErrorAfterWriterClient struct {
	client.Client
	hostClient  client.Client
	hostTarget  types.NamespacedName
	topologyErr error
	updateErr   error

	mu                   sync.Mutex
	pvcListCalls         int
	injectedUpdateErrors int
	writerErr            error
}

func (c *externalPopulatorTopologyListErrorAfterWriterClient) List(
	ctx context.Context,
	list client.ObjectList,
	opts ...client.ListOption,
) error {
	if _, ok := list.(*corev1.PersistentVolumeClaimList); !ok {
		return c.Client.List(ctx, list, opts...)
	}

	c.mu.Lock()
	c.pvcListCalls++
	trigger := c.pvcListCalls == 2
	c.mu.Unlock()
	if !trigger {
		return c.Client.List(ctx, list, opts...)
	}

	current := &corev1.PersistentVolumeClaim{}
	err := c.hostClient.Get(ctx, c.hostTarget, current)
	if err == nil {
		if current.Annotations == nil {
			current.Annotations = map[string]string{}
		}
		current.Annotations["example.test/external-writer"] = "after-fresh-read"
		current.Spec.Resources.Requests = corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("3Gi"),
		}
		err = c.hostClient.Update(ctx, current)
	}

	c.mu.Lock()
	c.writerErr = err
	c.mu.Unlock()
	return c.topologyErr
}

func (c *externalPopulatorTopologyListErrorAfterWriterClient) Update(
	ctx context.Context,
	obj client.Object,
	opts ...client.UpdateOption,
) error {
	if c.updateErr != nil {
		c.mu.Lock()
		c.injectedUpdateErrors++
		c.mu.Unlock()
		return c.updateErr
	}
	return c.Client.Update(ctx, obj, opts...)
}

func (c *externalPopulatorTopologyListErrorAfterWriterClient) state() (int, int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pvcListCalls, c.injectedUpdateErrors, c.writerErr
}

type externalPopulatorConcurrentWriterClient struct {
	client.Client
	target types.NamespacedName

	once          sync.Once
	directRV      string
	concurrentRV  string
	concurrentErr error
}

func (c *externalPopulatorConcurrentWriterClient) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.PatchOption,
) error {
	if err := c.Client.Patch(ctx, obj, patch, opts...); err != nil {
		return err
	}

	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok ||
		pvc.Namespace != c.target.Namespace ||
		pvc.Name != c.target.Name ||
		pvc.Spec.VolumeName == "" {
		return nil
	}

	c.once.Do(func() {
		c.directRV = pvc.ResourceVersion
		current := &corev1.PersistentVolumeClaim{}
		c.concurrentErr = c.Client.Get(ctx, c.target, current)
		if c.concurrentErr != nil {
			return
		}
		if current.Annotations == nil {
			current.Annotations = map[string]string{}
		}
		current.Annotations["example.test/concurrent-writer"] = "preserve"
		current.Spec.Resources.Requests = corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("2Gi"),
		}
		c.concurrentErr = c.Client.Update(ctx, current)
		if c.concurrentErr == nil {
			c.concurrentRV = current.ResourceVersion
		}
	})

	return nil
}

type externalPopulatorRecordingManager struct {
	ctrl.Manager
	recordingCache cache.Cache

	mu        sync.Mutex
	runnables []manager.Runnable
}

func (m *externalPopulatorRecordingManager) GetCache() cache.Cache {
	return m.recordingCache
}

func (m *externalPopulatorRecordingManager) Add(runnable manager.Runnable) error {
	m.mu.Lock()
	m.runnables = append(m.runnables, runnable)
	m.mu.Unlock()
	return m.Manager.Add(runnable)
}

func (m *externalPopulatorRecordingManager) registeredRunnables() []manager.Runnable {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]manager.Runnable(nil), m.runnables...)
}

type externalPopulatorRecordingCache struct {
	cache.Cache

	mu       sync.Mutex
	handlers map[reflect.Type][]toolscache.ResourceEventHandler
}

func newExternalPopulatorRecordingCache(delegate cache.Cache) *externalPopulatorRecordingCache {
	return &externalPopulatorRecordingCache{
		Cache:    delegate,
		handlers: map[reflect.Type][]toolscache.ResourceEventHandler{},
	}
}

func (c *externalPopulatorRecordingCache) GetInformer(
	ctx context.Context,
	object client.Object,
	opts ...cache.InformerGetOption,
) (cache.Informer, error) {
	informer, err := c.Cache.GetInformer(ctx, object, opts...)
	if err != nil {
		return nil, err
	}

	objectType := reflect.TypeOf(object)
	return &externalPopulatorRecordingInformer{
		Informer: informer,
		record: func(handler toolscache.ResourceEventHandler) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.handlers[objectType] = append(c.handlers[objectType], handler)
		},
	}, nil
}

func (c *externalPopulatorRecordingCache) handlersFor(object client.Object) []toolscache.ResourceEventHandler {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]toolscache.ResourceEventHandler(nil), c.handlers[reflect.TypeOf(object)]...)
}

type externalPopulatorRecordingInformer struct {
	cache.Informer
	record func(toolscache.ResourceEventHandler)
}

func (i *externalPopulatorRecordingInformer) AddEventHandler(handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	i.record(handler)
	return i.Informer.AddEventHandler(handler)
}

func (i *externalPopulatorRecordingInformer) AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, resyncPeriod time.Duration) (toolscache.ResourceEventHandlerRegistration, error) {
	i.record(handler)
	return i.Informer.AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
}

func (i *externalPopulatorRecordingInformer) AddEventHandlerWithOptions(handler toolscache.ResourceEventHandler, options toolscache.HandlerOptions) (toolscache.ResourceEventHandlerRegistration, error) {
	i.record(handler)
	return i.Informer.AddEventHandlerWithOptions(handler, options)
}

func TestSyncExternalPopulatorPreGateTransientSchedulesTarget(t *testing.T) {
	type dependencyRequestMapper interface {
		externalPopulatorDependencyRequests(context.Context, client.Object) ([]ctrl.Request, error)
	}
	type fixture struct {
		virtualTarget *corev1.PersistentVolumeClaim
		virtualPV     *corev1.PersistentVolume
		virtualHelper *corev1.PersistentVolumeClaim
		hostTarget    *corev1.PersistentVolumeClaim
		hostPV        *corev1.PersistentVolume
	}

	newFixture := func() *fixture {
		const (
			virtualNamespace = "testns"
			virtualTargetUID = types.UID("target-pvc-uid")
			virtualPVName    = "restore-populated-pv"
		)

		dataProtectionGroup := "dataprotection.kubeblocks.io"
		virtualTarget := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "testpvc",
				Namespace: virtualNamespace,
				UID:       virtualTargetUID,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName: virtualPVName,
				DataSourceRef: &corev1.TypedObjectReference{
					APIGroup: &dataProtectionGroup,
					Kind:     "Backup",
					Name:     "backup-1",
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{
				Phase: corev1.ClaimPending,
				Conditions: []corev1.PersistentVolumeClaimCondition{
					{
						Type:   externalPopulatorPopulateConditionType,
						Status: corev1.ConditionTrue,
						Reason: externalPopulatorRestoreConditionReasonSucceeded,
					},
				},
			},
		}
		virtualPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: virtualPVName},
			Spec: corev1.PersistentVolumeSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
				ClaimRef: &corev1.ObjectReference{
					Namespace: virtualNamespace,
					Name:      virtualTarget.Name,
					UID:       virtualTargetUID,
				},
			},
			Status: corev1.PersistentVolumeStatus{
				Phase: corev1.VolumeBound,
			},
		}
		virtualHelper := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      externalPopulatorPopulateHelperPrefix + string(virtualTargetUID),
				Namespace: virtualNamespace,
				UID:       types.UID("populate-helper-pvc-uid"),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName: virtualPVName,
			},
		}

		hostTranslator := translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)
		hostTargetName := hostTranslator.HostName(
			nil,
			virtualTarget.Name,
			virtualTarget.Namespace,
		)
		hostTarget := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      hostTargetName.Name,
				Namespace: testingutil.DefaultTestTargetNamespace,
				UID:       types.UID("host-target-pvc-uid"),
				Annotations: map[string]string{
					translate.NameAnnotation:          virtualTarget.Name,
					translate.NamespaceAnnotation:     virtualTarget.Namespace,
					translate.UIDAnnotation:           string(virtualTarget.UID),
					translate.KindAnnotation:          corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim").String(),
					translate.HostNamespaceAnnotation: testingutil.DefaultTestTargetNamespace,
					translate.HostNameAnnotation:      hostTargetName.Name,
				},
				Labels: map[string]string{
					translate.MarkerLabel:    translate.VClusterName,
					translate.NamespaceLabel: virtualTarget.Namespace,
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{
				Phase:    corev1.ClaimPending,
				Capacity: corev1.ResourceList{},
			},
		}
		hostPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: virtualPVName},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					APIVersion: corev1.SchemeGroupVersion.Version,
					Kind:       "PersistentVolumeClaim",
					Namespace:  hostTarget.Namespace,
					Name:       hostTarget.Name,
					UID:        hostTarget.UID,
				},
			},
			Status: corev1.PersistentVolumeStatus{
				Phase: corev1.VolumeBound,
			},
		}

		return &fixture{
			virtualTarget: virtualTarget,
			virtualPV:     virtualPV,
			virtualHelper: virtualHelper,
			hostTarget:    hostTarget,
			hostPV:        hostPV,
		}
	}

	tests := []struct {
		name           string
		trigger        string
		initialVirtual func(*fixture) []runtime.Object
		converge       func(*testing.T, *synccontext.SyncContext, *fixture) client.Object
	}{
		{
			name:    "guest fake pv cache not found",
			trigger: "guest PV create",
			initialVirtual: func(f *fixture) []runtime.Object {
				return []runtime.Object{f.virtualTarget.DeepCopy()}
			},
			converge: func(t *testing.T, ctx *synccontext.SyncContext, f *fixture) client.Object {
				t.Helper()
				converged := f.virtualPV.DeepCopy()
				assert.NilError(t, ctx.VirtualClient.Create(ctx.Context, converged))
				return converged
			},
		},
		{
			name:    "guest PV claimRef handoff closes missing helper identity",
			trigger: "guest PV claimRef update",
			initialVirtual: func(f *fixture) []runtime.Object {
				pvBoundToHelper := f.virtualPV.DeepCopy()
				pvBoundToHelper.Spec.ClaimRef = &corev1.ObjectReference{
					Namespace: f.virtualHelper.Namespace,
					Name:      f.virtualHelper.Name,
					UID:       f.virtualHelper.UID,
				}
				return []runtime.Object{f.virtualTarget.DeepCopy(), pvBoundToHelper}
			},
			converge: func(t *testing.T, ctx *synccontext.SyncContext, f *fixture) client.Object {
				t.Helper()
				convergedPV := &corev1.PersistentVolume{}
				assert.NilError(t, ctx.VirtualClient.Get(
					ctx.Context,
					types.NamespacedName{Name: f.virtualPV.Name},
					convergedPV,
				))
				convergedPV.Spec.ClaimRef = &corev1.ObjectReference{
					Namespace: f.virtualTarget.Namespace,
					Name:      f.virtualTarget.Name,
					UID:       f.virtualTarget.UID,
				}
				assert.NilError(t, ctx.VirtualClient.Update(ctx.Context, convergedPV))
				return convergedPV
			},
		},
		{
			name:    "pvc pv bound visibility mismatch",
			trigger: "guest PV phase update",
			initialVirtual: func(f *fixture) []runtime.Object {
				pendingPV := f.virtualPV.DeepCopy()
				pendingPV.Status.Phase = corev1.VolumePending
				return []runtime.Object{f.virtualTarget.DeepCopy(), pendingPV}
			},
			converge: func(t *testing.T, ctx *synccontext.SyncContext, f *fixture) client.Object {
				t.Helper()
				converged := &corev1.PersistentVolume{}
				assert.NilError(t, ctx.VirtualClient.Get(
					ctx.Context,
					types.NamespacedName{Name: f.virtualPV.Name},
					converged,
				))
				converged.Status.Phase = corev1.VolumeBound
				assert.NilError(t, ctx.VirtualClient.Status().Update(ctx.Context, converged))
				return converged
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture()
			test := &syncertesting.SyncTest{
				Name:                 tt.name,
				InitialVirtualState:  tt.initialVirtual(f),
				InitialPhysicalState: []runtime.Object{f.hostTarget.DeepCopy(), f.hostPV.DeepCopy()},
				Sync: func(registerCtx *synccontext.RegisterContext) {
					syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, registerCtx, New)
					pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
					pvcSyncer.useFakePersistentVolumes = true

					targetKey := types.NamespacedName{
						Namespace: f.virtualTarget.Namespace,
						Name:      f.virtualTarget.Name,
					}
					hostTargetKey := types.NamespacedName{
						Namespace: f.hostTarget.Namespace,
						Name:      f.hostTarget.Name,
					}
					targetReconciles := 0
					timerRequeues := 0
					mappedTargetRequests := 0

					reconcileTarget := func() ctrl.Result {
						t.Helper()
						currentVirtual := &corev1.PersistentVolumeClaim{}
						assert.NilError(t, syncCtx.VirtualClient.Get(syncCtx.Context, targetKey, currentVirtual))
						currentHost := &corev1.PersistentVolumeClaim{}
						assert.NilError(t, syncCtx.HostClient.Get(syncCtx.Context, hostTargetKey, currentHost))
						result, err := pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
							currentHost.DeepCopy(),
							currentHost.DeepCopy(),
							currentVirtual.DeepCopy(),
							currentVirtual.DeepCopy(),
						))
						assert.NilError(t, err)
						targetReconciles++
						if result.Requeue || result.RequeueAfter > 0 {
							timerRequeues++
						}
						return result
					}

					// Pin the exact pre-gate return before exercising the full Sync
					// boundary: every fixture must be a transient (nil, false, nil),
					// rather than the genuine non-applicable entry gate.
					currentVirtual := &corev1.PersistentVolumeClaim{}
					assert.NilError(t, syncCtx.VirtualClient.Get(syncCtx.Context, targetKey, currentVirtual))
					currentHost := &corev1.PersistentVolumeClaim{}
					assert.NilError(t, syncCtx.HostClient.Get(syncCtx.Context, hostTargetKey, currentHost))
					preGatePV, preGateReady, err := pvcSyncer.externalPopulatorPersistentVolume(
						syncCtx,
						currentHost,
						currentVirtual,
					)
					assert.NilError(t, err)
					if preGatePV != nil || preGateReady {
						t.Fatalf(
							"%s did not enter a transient (nil, false, nil) pre-gate: pv=%#v ready=%t",
							tt.trigger,
							preGatePV,
							preGateReady,
						)
					}

					// The first target reconcile observes that transient and
					// intentionally schedules no polling timer.
					firstResult := reconcileTarget()
					assert.Check(t, firstResult.IsZero(), "%s first result was %#v", tt.trigger, firstResult)

					virtualBeforeDependency := &corev1.PersistentVolumeClaim{}
					assert.NilError(t, syncCtx.VirtualClient.Get(syncCtx.Context, targetKey, virtualBeforeDependency))
					helperCreationClosed, err := externalPopulatorHelperCreationClosed(
						syncCtx,
						syncCtx.VirtualAPIReader,
						virtualBeforeDependency,
					)
					assert.NilError(t, err)
					if !helperCreationClosed {
						t.Fatalf(
							"%s first host-Pending sync erased the target's terminal populator condition",
							tt.trigger,
						)
					}
					convergedDependency := tt.converge(t, syncCtx, f)
					virtualAfterDependency := &corev1.PersistentVolumeClaim{}
					assert.NilError(t, syncCtx.VirtualClient.Get(syncCtx.Context, targetKey, virtualAfterDependency))
					assert.Assert(
						t,
						apiequality.Semantic.DeepEqual(virtualAfterDependency, virtualBeforeDependency),
						"dependency convergence changed the target PVC before its mapped reconcile",
					)

					// Model only requests produced by the dependency event. A PVC's
					// primary watch would enqueue the helper key, not this target key.
					mapper, hasDependencyMapper := any(pvcSyncer).(dependencyRequestMapper)
					_, hasControllerModifier := any(pvcSyncer).(syncertypes.ControllerModifier)
					var requests []ctrl.Request
					if hasDependencyMapper {
						var err error
						requests, err = mapper.externalPopulatorDependencyRequests(syncCtx.Context, convergedDependency)
						assert.NilError(t, err)
					}
					for _, request := range requests {
						if request.NamespacedName != targetKey {
							t.Fatalf(
								"%s mapped unexpected request %s/%s; want target %s/%s",
								tt.trigger,
								request.Namespace,
								request.Name,
								targetKey.Namespace,
								targetKey.Name,
							)
						}
						mappedTargetRequests++
						reconcileTarget()
					}

					actualHostTarget := &corev1.PersistentVolumeClaim{}
					assert.NilError(t, syncCtx.HostClient.Get(syncCtx.Context, hostTargetKey, actualHostTarget))
					actualHostPV := &corev1.PersistentVolume{}
					assert.NilError(t, syncCtx.HostClient.Get(
						syncCtx.Context,
						types.NamespacedName{Name: f.hostPV.Name},
						actualHostPV,
					))
					actualVirtualTarget := &corev1.PersistentVolumeClaim{}
					assert.NilError(t, syncCtx.VirtualClient.Get(syncCtx.Context, targetKey, actualVirtualTarget))

					if timerRequeues != 0 ||
						!hasControllerModifier ||
						mappedTargetRequests != 1 ||
						targetReconciles != 2 ||
						actualHostTarget.Spec.VolumeName != f.virtualPV.Name ||
						!claimRefMatchesPersistentVolumeClaim(actualHostPV.Spec.ClaimRef, actualHostTarget) ||
						actualVirtualTarget.Status.Phase != corev1.ClaimBound {
						t.Fatalf(
							"%s convergence contract: timer_requeues=%d controller_modifier=%t mapped_target_requests=%d target_reconciles=%d host_volume=%q host_claim_ref=%#v virtual_phase=%q; want 0/true/1/2/%q/target/%q",
							tt.trigger,
							timerRequeues,
							hasControllerModifier,
							mappedTargetRequests,
							targetReconciles,
							actualHostTarget.Spec.VolumeName,
							actualHostPV.Spec.ClaimRef,
							actualVirtualTarget.Status.Phase,
							f.virtualPV.Name,
							corev1.ClaimBound,
						)
					}
				},
			}
			test.Run(t, syncertesting.NewFakeRegisterContext)
		})
	}
}

func TestExternalPopulatorDependencyMapperRejectsInvalidDependencies(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*externalPopulatorDependencyFixture) client.Object
	}{
		{
			name: "non-applicable target",
			prepare: func(f *externalPopulatorDependencyFixture) client.Object {
				f.target.Spec.DataSourceRef.Kind = "PersistentVolumeClaim"
				f.pv.Spec.ClaimRef = &corev1.ObjectReference{
					Namespace: f.target.Namespace,
					Name:      f.target.Name,
					UID:       f.target.UID,
				}
				return f.pv
			},
		},
		{
			name: "terminating helper",
			prepare: func(f *externalPopulatorDependencyFixture) client.Object {
				now := metav1.Now()
				f.helper.DeletionTimestamp = &now
				f.helper.Finalizers = []string{"test"}
				return f.helper
			},
		},
		{
			name: "invalid helper identity",
			prepare: func(f *externalPopulatorDependencyFixture) client.Object {
				f.helper.Name = externalPopulatorPopulateHelperPrefix + "other-target"
				f.pv.Spec.ClaimRef.Name = f.helper.Name
				return f.helper
			},
		},
		{
			name: "helper claimRef UID mismatch",
			prepare: func(f *externalPopulatorDependencyFixture) client.Object {
				f.pv.Spec.ClaimRef.UID = types.UID("stale-helper-uid")
				return f.helper
			},
		},
		{
			name: "helper claimRef UID absent",
			prepare: func(f *externalPopulatorDependencyFixture) client.Object {
				f.pv.Spec.ClaimRef.UID = ""
				return f.helper
			},
		},
		{
			name: "target claimRef UID mismatch",
			prepare: func(f *externalPopulatorDependencyFixture) client.Object {
				f.pv.Spec.ClaimRef = &corev1.ObjectReference{
					Namespace: f.target.Namespace,
					Name:      f.target.Name,
					UID:       types.UID("stale-target-uid"),
				}
				return f.pv
			},
		},
		{
			name: "target claimRef UID absent",
			prepare: func(f *externalPopulatorDependencyFixture) client.Object {
				f.pv.Spec.ClaimRef = &corev1.ObjectReference{
					Namespace: f.target.Namespace,
					Name:      f.target.Name,
				}
				return f.pv
			},
		},
		{
			name: "helper absent",
			prepare: func(f *externalPopulatorDependencyFixture) client.Object {
				return f.pv
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newExternalPopulatorDependencyFixture()
			event := tt.prepare(f)
			virtualObjects := []runtime.Object{f.target.DeepCopy(), f.pv.DeepCopy()}
			if tt.name != "helper absent" {
				virtualObjects = append(virtualObjects, f.helper.DeepCopy())
			}
			syncCtx, pvcSyncer := newExternalPopulatorMapperTestSyncer(t, virtualObjects...)

			requests, err := pvcSyncer.externalPopulatorDependencyRequests(syncCtx.Context, event)
			assert.NilError(t, err)
			assert.Equal(t, len(requests), 0)
		})
	}
}

func TestExternalPopulatorDependencyMapperPropagatesLookupErrors(t *testing.T) {
	tests := []struct {
		name    string
		getErr  error
		listErr error
	}{
		{
			name:   "persistent volume lookup",
			getErr: errors.New("injected persistent volume lookup failure"),
		},
		{
			name:    "target index lookup",
			listErr: errors.New("injected target index lookup failure"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newExternalPopulatorDependencyFixture()
			syncCtx, pvcSyncer := newExternalPopulatorMapperTestSyncer(
				t,
				f.target.DeepCopy(),
				f.helper.DeepCopy(),
				f.pv.DeepCopy(),
			)
			pvcSyncer.virtualClient = &externalPopulatorMapperErrorClient{
				Client:  syncCtx.VirtualClient,
				getErr:  tt.getErr,
				listErr: tt.listErr,
			}

			requests, err := pvcSyncer.externalPopulatorDependencyRequests(syncCtx.Context, f.helper)
			assert.Equal(t, len(requests), 0)
			expectedErr := tt.getErr
			if expectedErr == nil {
				expectedErr = tt.listErr
			}
			if !errors.Is(err, expectedErr) {
				t.Fatalf("mapper error = %v, want wrapped %v", err, expectedErr)
			}
		})
	}
}

func TestExternalPopulatorDependencyMapperIgnoresUnrelatedOrIncompleteEventsBeforeLookup(t *testing.T) {
	f := newExternalPopulatorDependencyFixture()
	syncCtx, pvcSyncer := newExternalPopulatorMapperTestSyncer(
		t,
		f.target.DeepCopy(),
		f.helper.DeepCopy(),
		f.pv.DeepCopy(),
	)
	pvcSyncer.virtualClient = &externalPopulatorMapperErrorClient{
		Client: syncCtx.VirtualClient,
		getErr: errors.New("ordinary pvc must not trigger a dependency lookup"),
	}
	ordinaryPVC := f.target.DeepCopy()
	ordinaryPVC.Name = "ordinary"
	ordinaryPVC.Spec.DataSourceRef = nil

	incompletePV := f.pv.DeepCopy()
	incompletePV.Spec.ClaimRef.UID = ""

	for _, tt := range []struct {
		name   string
		object client.Object
	}{
		{name: "ordinary pvc", object: ordinaryPVC},
		{name: "persistent volume missing claim UID", object: incompletePV},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requests, err := pvcSyncer.externalPopulatorDependencyRequests(syncCtx.Context, tt.object)
			assert.NilError(t, err)
			assert.Equal(t, len(requests), 0)
		})
	}
}

func TestExternalPopulatorDependencyModifyControllerWiring(t *testing.T) {
	f := newExternalPopulatorDependencyFixture()
	hostClient := testingutil.NewFakeClient(scheme.Scheme)
	virtualClient := testingutil.NewFakeClient(
		scheme.Scheme,
		f.target.DeepCopy(),
		f.helper.DeepCopy(),
		f.pv.DeepCopy(),
	)
	registerCtx := syncertesting.NewFakeRegisterContext(testingutil.NewFakeConfig(), hostClient, virtualClient)
	baseManager := testingutil.NewFakeManager(virtualClient)
	recordingCache := newExternalPopulatorRecordingCache(baseManager.GetCache())
	recordingManager := &externalPopulatorRecordingManager{
		Manager:        baseManager,
		recordingCache: recordingCache,
	}
	registerCtx.VirtualManager = recordingManager

	objectSyncer, err := New(registerCtx)
	assert.NilError(t, err)
	pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
	assert.NilError(t, pvcSyncer.RegisterIndices(registerCtx))

	requests := make(chan ctrl.Request, 32)
	controllerBuilder := ctrl.NewControllerManagedBy(recordingManager).
		Named("external-populator-dependency-wiring-test").
		For(&corev1.PersistentVolumeClaim{})
	controllerBuilder, err = pvcSyncer.ModifyController(registerCtx, controllerBuilder)
	assert.NilError(t, err)
	controller, err := controllerBuilder.Build(reconcile.Func(func(_ context.Context, request ctrl.Request) (ctrl.Result, error) {
		requests <- request
		return ctrl.Result{}, nil
	}))
	assert.NilError(t, err)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan error, 1)
	go func() {
		started <- controller.Start(runCtx)
	}()

	waitDeadline := time.NewTimer(2 * time.Second)
	defer waitDeadline.Stop()
	waitTicker := time.NewTicker(5 * time.Millisecond)
	defer waitTicker.Stop()
	for {
		if len(recordingCache.handlersFor(&corev1.PersistentVolume{})) == 1 &&
			len(recordingCache.handlersFor(&corev1.PersistentVolumeClaim{})) == 2 {
			break
		}
		select {
		case err := <-started:
			t.Fatalf("controller stopped before watch registration: %v", err)
		case <-waitDeadline.C:
			t.Fatalf(
				"watch registration timed out: pv handlers=%d pvc handlers=%d",
				len(recordingCache.handlersFor(&corev1.PersistentVolume{})),
				len(recordingCache.handlersFor(&corev1.PersistentVolumeClaim{})),
			)
		case <-waitTicker.C:
		}
	}

	targetKey := types.NamespacedName{Namespace: f.target.Namespace, Name: f.target.Name}
	drainRequests := func() {
		for {
			select {
			case <-requests:
			default:
				return
			}
		}
	}
	expectTarget := func(label string) {
		t.Helper()
		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		for {
			select {
			case request := <-requests:
				if request.NamespacedName == targetKey {
					return
				}
			case <-deadline.C:
				t.Fatalf("%s did not enqueue exact target %s", label, targetKey)
			}
		}
	}
	expectNoTarget := func(label string) {
		t.Helper()
		deadline := time.NewTimer(150 * time.Millisecond)
		defer deadline.Stop()
		for {
			select {
			case request := <-requests:
				if request.NamespacedName == targetKey {
					t.Fatalf("%s unexpectedly enqueued exact target %s", label, targetKey)
				}
			case <-deadline.C:
				return
			}
		}
	}
	fireAdd := func(object client.Object) {
		t.Helper()
		handlers := recordingCache.handlersFor(object)
		if len(handlers) == 0 {
			t.Fatalf("no registered handler for %T", object)
		}
		for _, eventHandler := range handlers {
			eventHandler.OnAdd(object, false)
		}
	}
	fireUpdate := func(oldObject, newObject client.Object) {
		t.Helper()
		handlers := recordingCache.handlersFor(newObject)
		if len(handlers) == 0 {
			t.Fatalf("no registered handler for %T", newObject)
		}
		for _, eventHandler := range handlers {
			eventHandler.OnUpdate(oldObject, newObject)
		}
	}
	fireDelete := func(object client.Object) {
		t.Helper()
		handlers := recordingCache.handlersFor(object)
		if len(handlers) == 0 {
			t.Fatalf("no registered handler for %T", object)
		}
		for _, eventHandler := range handlers {
			eventHandler.OnDelete(object)
		}
	}

	directPV := f.pv.DeepCopy()
	directPV.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: f.target.Namespace,
		Name:      f.target.Name,
		UID:       f.target.UID,
	}
	fireAdd(directPV)
	expectTarget("PV create")
	drainRequests()
	pendingDirectPV := directPV.DeepCopy()
	pendingDirectPV.Status.Phase = corev1.VolumePending
	fireUpdate(pendingDirectPV, directPV)
	expectTarget("PV update")
	drainRequests()
	fireDelete(directPV)
	expectTarget("PV delete")
	drainRequests()

	fireAdd(f.helper.DeepCopy())
	expectTarget("helper create")
	drainRequests()
	updatedHelper := f.helper.DeepCopy()
	updatedHelper.Labels = map[string]string{"generation": "2"}
	fireUpdate(f.helper.DeepCopy(), updatedHelper)
	expectTarget("helper update")
	drainRequests()

	assert.NilError(t, virtualClient.Delete(runCtx, f.helper.DeepCopy()))
	deletedHelper := f.helper.DeepCopy()
	now := metav1.Now()
	deletedHelper.DeletionTimestamp = &now
	fireDelete(deletedHelper)
	expectTarget("helper delete")
	drainRequests()
	fireUpdate(f.pv.DeepCopy(), f.pv.DeepCopy())
	expectNoTarget("PV update while helper absent")
	drainRequests()

	recreatedHelper := f.helper.DeepCopy()
	recreatedHelper.ResourceVersion = ""
	assert.NilError(t, virtualClient.Create(runCtx, recreatedHelper))
	fireAdd(recreatedHelper.DeepCopy())
	expectTarget("helper recreate")
	drainRequests()

	terminatingHelper := recreatedHelper.DeepCopy()
	terminatingHelper.DeletionTimestamp = &now
	fireAdd(terminatingHelper)
	expectNoTarget("terminating helper")
	drainRequests()

	assert.NilError(t, virtualClient.Delete(runCtx, f.pv.DeepCopy()))
	fireUpdate(recreatedHelper.DeepCopy(), recreatedHelper.DeepCopy())
	expectNoTarget("helper update while PV absent")
	drainRequests()
	recreatedPV := f.pv.DeepCopy()
	recreatedPV.ResourceVersion = ""
	assert.NilError(t, virtualClient.Create(runCtx, recreatedPV))
	fireAdd(recreatedPV.DeepCopy())
	expectTarget("PV recreate")

	cancel()
	select {
	case err := <-started:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("controller stop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("controller did not stop within 2s")
	}
}

func TestExternalPopulatorDependencyModifyControllerRetriesTransientMapperError(t *testing.T) {
	f := newExternalPopulatorDependencyFixture()
	hostTarget, hostHelper, hostPV := newExternalPopulatorHostHandoffObjects(f)

	hostClient := testingutil.NewFakeClient(
		scheme.Scheme,
		hostTarget,
		hostHelper,
		hostPV,
	)
	virtualClient := testingutil.NewFakeClient(
		scheme.Scheme,
		f.target.DeepCopy(),
		f.pv.DeepCopy(),
	)
	registerCtx := syncertesting.NewFakeRegisterContext(testingutil.NewFakeConfig(), hostClient, virtualClient)
	baseManager := testingutil.NewFakeManager(virtualClient)
	recordingCache := newExternalPopulatorRecordingCache(baseManager.GetCache())
	recordingManager := &externalPopulatorRecordingManager{
		Manager:        baseManager,
		recordingCache: recordingCache,
	}
	registerCtx.VirtualManager = recordingManager

	syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, registerCtx, New)
	pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
	pvcSyncer.useFakePersistentVolumes = true

	targetKey := types.NamespacedName{
		Namespace: f.target.Namespace,
		Name:      f.target.Name,
	}
	hostTargetKey := types.NamespacedName{
		Namespace: hostTarget.Namespace,
		Name:      hostTarget.Name,
	}
	reconcileTarget := func() (ctrl.Result, error) {
		currentVirtual := &corev1.PersistentVolumeClaim{}
		if err := syncCtx.VirtualClient.Get(syncCtx.Context, targetKey, currentVirtual); err != nil {
			return ctrl.Result{}, err
		}
		currentHost := &corev1.PersistentVolumeClaim{}
		if err := syncCtx.HostClient.Get(syncCtx.Context, hostTargetKey, currentHost); err != nil {
			return ctrl.Result{}, err
		}
		return pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
			currentHost.DeepCopy(),
			currentHost.DeepCopy(),
			currentVirtual.DeepCopy(),
			currentVirtual.DeepCopy(),
		))
	}

	firstResult, err := reconcileTarget()
	assert.NilError(t, err)
	assert.Check(t, firstResult.IsZero(), "first transient result was %#v", firstResult)
	preRecoveryHostTarget := &corev1.PersistentVolumeClaim{}
	assert.NilError(t, hostClient.Get(syncCtx.Context, hostTargetKey, preRecoveryHostTarget))
	assert.Equal(t, preRecoveryHostTarget.Spec.VolumeName, "")
	preRecoveryHostPV := &corev1.PersistentVolume{}
	assert.NilError(t, hostClient.Get(
		syncCtx.Context,
		types.NamespacedName{Name: f.pv.Name},
		preRecoveryHostPV,
	))
	assert.Check(
		t,
		claimRefMatchesPersistentVolumeClaim(preRecoveryHostPV.Spec.ClaimRef, hostHelper),
		"pre-recovery host PV claimRef = %#v, want helper %s/%s",
		preRecoveryHostPV.Spec.ClaimRef,
		hostHelper.Namespace,
		hostHelper.Name,
	)

	convergedPV := &corev1.PersistentVolume{}
	assert.NilError(t, virtualClient.Get(syncCtx.Context, types.NamespacedName{Name: f.pv.Name}, convergedPV))
	convergedPV.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: f.target.Namespace,
		Name:      f.target.Name,
		UID:       f.target.UID,
	}
	assert.NilError(t, virtualClient.Update(syncCtx.Context, convergedPV))

	injectedErr := errors.New("injected first dependency mapper lookup failure")
	flakyMapperClient := &externalPopulatorFlakyMapperClient{
		Client:             virtualClient,
		remainingGetErrors: 1,
		getErr:             injectedErr,
	}
	pvcSyncer.virtualClient = flakyMapperClient

	type reconcileOutcome struct {
		request ctrl.Request
		result  ctrl.Result
		err     error
	}
	outcomes := make(chan reconcileOutcome, 4)
	controllerBuilder := ctrl.NewControllerManagedBy(recordingManager).
		Named(fmt.Sprintf(
			"external-populator-dependency-retry-test-%d",
			externalPopulatorControllerTestID.Add(1),
		)).
		For(&corev1.PersistentVolumeClaim{})
	controllerBuilder, err = pvcSyncer.ModifyController(registerCtx, controllerBuilder)
	assert.NilError(t, err)

	retryRunnables := recordingManager.registeredRunnables()
	assert.Equal(t, len(retryRunnables), 1)
	retryRunnable := retryRunnables[0]
	controller, err := controllerBuilder.Build(reconcile.Func(func(_ context.Context, request ctrl.Request) (ctrl.Result, error) {
		if request.NamespacedName != targetKey {
			return ctrl.Result{}, nil
		}
		result, reconcileErr := reconcileTarget()
		outcomes <- reconcileOutcome{
			request: request,
			result:  result,
			err:     reconcileErr,
		}
		return result, reconcileErr
	}))
	assert.NilError(t, err)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	retryStopped := make(chan error, 1)
	go func() {
		retryStopped <- retryRunnable.Start(runCtx)
	}()
	controllerStopped := make(chan error, 1)
	go func() {
		controllerStopped <- controller.Start(runCtx)
	}()

	waitDeadline := time.NewTimer(2 * time.Second)
	waitTicker := time.NewTicker(5 * time.Millisecond)
	for {
		if len(recordingCache.handlersFor(&corev1.PersistentVolumeClaim{})) == 2 {
			break
		}
		select {
		case err := <-controllerStopped:
			t.Fatalf("controller stopped before watch registration: %v", err)
		case err := <-retryStopped:
			t.Fatalf("retry worker stopped before dependency event: %v", err)
		case <-waitDeadline.C:
			t.Fatalf(
				"watch registration timed out: pvc handlers=%d",
				len(recordingCache.handlersFor(&corev1.PersistentVolumeClaim{})),
			)
		case <-waitTicker.C:
		}
	}
	waitDeadline.Stop()
	waitTicker.Stop()

	// Fire the guest PV claimRef handoff exactly once. The helper is already
	// absent from the API, so the recovered target reconcile may complete.
	// The fast mapper consumes the injected Get error; no second dependency
	// event is sent after recovery.
	for _, eventHandler := range recordingCache.handlersFor(&corev1.PersistentVolume{}) {
		eventHandler.OnUpdate(f.pv.DeepCopy(), convergedPV.DeepCopy())
	}

	select {
	case outcome := <-outcomes:
		if outcome.request.NamespacedName != targetKey {
			t.Fatalf("recovered mapper enqueued %s, want %s", outcome.request.NamespacedName, targetKey)
		}
		assert.NilError(t, outcome.err)
		assert.Check(t, outcome.result.IsZero(), "recovered target result was %#v", outcome.result)
	case <-time.After(3 * time.Second):
		t.Fatalf("recovered mapper did not enqueue exact target %s within 3s", targetKey)
	}

	assert.Equal(t, flakyMapperClient.injectedErrors(), 1)
	actualHostTarget := &corev1.PersistentVolumeClaim{}
	assert.NilError(t, hostClient.Get(syncCtx.Context, hostTargetKey, actualHostTarget))
	actualHostPV := &corev1.PersistentVolume{}
	assert.NilError(t, hostClient.Get(
		syncCtx.Context,
		types.NamespacedName{Name: f.pv.Name},
		actualHostPV,
	))
	actualVirtualTarget := &corev1.PersistentVolumeClaim{}
	assert.NilError(t, virtualClient.Get(syncCtx.Context, targetKey, actualVirtualTarget))
	if actualHostTarget.Spec.VolumeName != f.pv.Name ||
		!claimRefMatchesPersistentVolumeClaim(actualHostPV.Spec.ClaimRef, actualHostTarget) ||
		actualVirtualTarget.Status.Phase != corev1.ClaimBound {
		t.Fatalf(
			"recovered handoff: host_volume=%q host_claim_ref=%#v virtual_phase=%q; want %q/target/%q",
			actualHostTarget.Spec.VolumeName,
			actualHostPV.Spec.ClaimRef,
			actualVirtualTarget.Status.Phase,
			f.pv.Name,
			corev1.ClaimBound,
		)
	}

	cancel()
	for label, stopped := range map[string]<-chan error{
		"controller":   controllerStopped,
		"retry worker": retryStopped,
	} {
		select {
		case err := <-stopped:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("%s stop: %v", label, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not stop within 2s", label)
		}
	}
}

func TestExternalPopulatorDependencyRetryContinuesAfterFiniteBudget(t *testing.T) {
	f := newExternalPopulatorDependencyFixture()
	syncCtx, pvcSyncer := newExternalPopulatorMapperTestSyncer(
		t,
		f.target.DeepCopy(),
		f.pv.DeepCopy(),
	)
	flakyMapperClient := &externalPopulatorFlakyMapperClient{
		Client:             syncCtx.VirtualClient,
		remainingGetErrors: externalPopulatorDependencyMaxRetries,
		getErr:             errors.New("dependency mapper fails through retry budget"),
	}
	pvcSyncer.virtualClient = flakyMapperClient

	retryQueue := workqueue.NewTypedRateLimitingQueue(
		workqueue.NewTypedItemExponentialFailureRateLimiter[*externalPopulatorDependencyRetry](
			time.Millisecond,
			time.Millisecond,
		),
	)
	targetQueue := workqueue.NewTypedRateLimitingQueue(
		workqueue.NewTypedItemExponentialFailureRateLimiter[ctrl.Request](
			time.Millisecond,
			time.Millisecond,
		),
	)
	defer targetQueue.ShutDown()

	retry := &externalPopulatorDependencyRetry{
		object:      f.helper.DeepCopy(),
		targetQueue: targetQueue,
	}
	runCtx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() {
		stopped <- pvcSyncer.runExternalPopulatorDependencyRetryWorker(runCtx, retryQueue)
	}()
	retryQueue.AddRateLimited(retry)

	deadline := time.NewTimer(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		if targetQueue.Len() == 1 {
			break
		}

		select {
		case err := <-stopped:
			t.Fatalf("retry worker stopped before dependency recovery: %v", err)
		case <-deadline.C:
			t.Fatalf(
				"recovered mapper did not enqueue target after retry budget: injected_errors=%d requeues=%d retry_queue_len=%d target_queue_len=%d",
				flakyMapperClient.injectedErrors(),
				retryQueue.NumRequeues(retry),
				retryQueue.Len(),
				targetQueue.Len(),
			)
		case <-ticker.C:
		}
	}

	assert.Equal(t, flakyMapperClient.injectedErrors(), externalPopulatorDependencyMaxRetries)
	request, shutdown := targetQueue.Get()
	assert.Assert(t, !shutdown)
	assert.Equal(t, request.NamespacedName, types.NamespacedName{
		Namespace: f.target.Namespace,
		Name:      f.target.Name,
	})
	targetQueue.Done(request)
	cancel()
	select {
	case err := <-stopped:
		assert.NilError(t, err)
	case <-time.After(time.Second):
		t.Fatal("retry worker did not stop within 1s")
	}
}

func TestSyncExternalPopulatorTopologyErrorDoesNotReplayFreshHostSnapshot(t *testing.T) {
	f := newExternalPopulatorDependencyFixture()
	f.target.Spec.Resources.Requests = corev1.ResourceList{
		corev1.ResourceStorage: resource.MustParse("1Gi"),
	}
	hostTarget, hostHelper, hostPV := newExternalPopulatorHostHandoffObjects(f)
	hostTarget.Annotations["example.test/external-writer"] = "initial"
	hostTarget.Annotations[bindCompletedAnnotation] = "yes"
	hostTarget.Spec.Resources.Requests = corev1.ResourceList{
		corev1.ResourceStorage: resource.MustParse("1Gi"),
	}
	virtualPV := f.pv.DeepCopy()
	virtualPV.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: f.target.Namespace,
		Name:      f.target.Name,
		UID:       f.target.UID,
	}

	hostClient := testingutil.NewFakeClient(
		scheme.Scheme,
		hostTarget,
		hostHelper,
		hostPV,
	)
	virtualClient := testingutil.NewFakeClient(
		scheme.Scheme,
		f.target.DeepCopy(),
		virtualPV,
	)
	registerCtx := syncertesting.NewFakeRegisterContext(testingutil.NewFakeConfig(), hostClient, virtualClient)
	syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, registerCtx, New)
	pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
	pvcSyncer.useFakePersistentVolumes = true

	targetKey := types.NamespacedName{Namespace: f.target.Namespace, Name: f.target.Name}
	hostTargetKey := types.NamespacedName{Namespace: hostTarget.Namespace, Name: hostTarget.Name}
	eventHost := &corev1.PersistentVolumeClaim{}
	assert.NilError(t, hostClient.Get(syncCtx.Context, hostTargetKey, eventHost))
	eventVirtual := &corev1.PersistentVolumeClaim{}
	assert.NilError(t, virtualClient.Get(syncCtx.Context, targetKey, eventVirtual))
	initialHostResourceVersion := eventHost.ResourceVersion

	firstWriter := &corev1.PersistentVolumeClaim{}
	assert.NilError(t, hostClient.Get(syncCtx.Context, hostTargetKey, firstWriter))
	firstWriter.Annotations["example.test/external-writer"] = "fresh-read"
	firstWriter.Spec.Resources.Requests = corev1.ResourceList{
		corev1.ResourceStorage: resource.MustParse("2Gi"),
	}
	assert.NilError(t, hostClient.Update(syncCtx.Context, firstWriter))

	topologyErr := errors.New("injected helper topology list failure after fresh host read")
	deferredConflict := kerrors.NewConflict(
		schema.GroupResource{Resource: "persistentvolumeclaims"},
		f.target.Name,
		errors.New("injected deferred virtual patch conflict"),
	)
	errorClient := &externalPopulatorTopologyListErrorAfterWriterClient{
		Client:      virtualClient,
		hostClient:  hostClient,
		hostTarget:  hostTargetKey,
		topologyErr: topologyErr,
		updateErr:   deferredConflict,
	}
	syncCtx.VirtualClient = errorClient

	result, err := pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
		eventHost.DeepCopy(),
		eventHost,
		eventVirtual.DeepCopy(),
		eventVirtual,
	))
	if !errors.Is(err, topologyErr) {
		t.Fatalf("Sync() error = %v, want wrapped topology error %v", err, topologyErr)
	}
	assert.Check(t, result.IsZero(), "topology error result was %#v", result)
	if eventHost.ResourceVersion == initialHostResourceVersion ||
		eventHost.Annotations["example.test/external-writer"] != "fresh-read" {
		t.Fatalf(
			"ensure did not install the fresh host snapshot: initial_rv=%q event_rv=%q writer=%q",
			initialHostResourceVersion,
			eventHost.ResourceVersion,
			eventHost.Annotations["example.test/external-writer"],
		)
	}

	listCalls, injectedUpdateErrors, writerErr := errorClient.state()
	assert.Equal(t, listCalls, 2)
	assert.Equal(t, injectedUpdateErrors, 1)
	assert.NilError(t, writerErr)

	actualHostTarget := &corev1.PersistentVolumeClaim{}
	assert.NilError(t, hostClient.Get(syncCtx.Context, hostTargetKey, actualHostTarget))
	if actualHostTarget.Annotations["example.test/external-writer"] != "after-fresh-read" ||
		actualHostTarget.Spec.Resources.Requests.Storage().Cmp(resource.MustParse("3Gi")) != 0 ||
		actualHostTarget.Spec.VolumeName != "" {
		t.Fatalf(
			"deferred host patch replayed a stale snapshot: writer=%q storage=%s volume=%q; want after-fresh-read/3Gi/empty",
			actualHostTarget.Annotations["example.test/external-writer"],
			actualHostTarget.Spec.Resources.Requests.Storage().String(),
			actualHostTarget.Spec.VolumeName,
		)
	}
	actualHostPV := &corev1.PersistentVolume{}
	assert.NilError(t, hostClient.Get(
		syncCtx.Context,
		types.NamespacedName{Name: f.pv.Name},
		actualHostPV,
	))
	assert.Check(
		t,
		claimRefMatchesPersistentVolumeClaim(actualHostPV.Spec.ClaimRef, hostHelper),
		"topology error changed host PV claimRef to %#v",
		actualHostPV.Spec.ClaimRef,
	)
}

func TestContainsOnlyConflictErrors(t *testing.T) {
	sentinel := errors.New("sentinel non-conflict")
	conflict := kerrors.NewConflict(
		schema.GroupResource{Resource: "persistentvolumeclaims"},
		"target",
		errors.New("conflict"),
	)
	pureAggregate := utilerrors.NewAggregate([]error{
		conflict,
		fmt.Errorf("wrapped: %w", conflict),
	})
	mixedAggregate := utilerrors.NewAggregate([]error{
		sentinel,
		conflict,
	})

	for _, tt := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "single conflict", err: conflict, want: true},
		{name: "wrapped conflict", err: fmt.Errorf("wrapped: %w", conflict), want: true},
		{name: "pure aggregate", err: pureAggregate, want: true},
		{name: "wrapped pure aggregate", err: fmt.Errorf("wrapped: %w", pureAggregate), want: true},
		{name: "single non-conflict", err: sentinel, want: false},
		{name: "mixed aggregate", err: mixedAggregate, want: false},
		{name: "wrapped mixed aggregate", err: fmt.Errorf("wrapped: %w", mixedAggregate), want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, containsOnlyConflictErrors(tt.err), tt.want)
		})
	}
}

func TestSyncExternalPopulatorDirectMaterializationConcurrentWriterRetriesFromFreshState(t *testing.T) {
	const (
		virtualNamespace = "testns"
		virtualPVName    = "restore-populated-pv"
	)

	dataProtectionGroup := dataProtectionAPIGroup
	virtualTarget := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "target",
			Namespace: virtualNamespace,
			UID:       types.UID("target-pvc-uid"),
			Annotations: map[string]string{
				"example.test/virtual-writer": "preserve",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: virtualPVName,
			DataSourceRef: &corev1.TypedObjectReference{
				APIGroup: &dataProtectionGroup,
				Kind:     dataProtectionBackupKind,
				Name:     "backup",
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimPending,
			Conditions: []corev1.PersistentVolumeClaimCondition{
				{
					Type:   externalPopulatorPopulateConditionType,
					Status: corev1.ConditionTrue,
					Reason: externalPopulatorRestoreConditionReasonSucceeded,
				},
			},
		},
	}
	virtualPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: virtualPVName},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			ClaimRef: &corev1.ObjectReference{
				Namespace: virtualTarget.Namespace,
				Name:      virtualTarget.Name,
				UID:       virtualTarget.UID,
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	hostTranslator := translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)
	hostTargetName := hostTranslator.HostName(nil, virtualTarget.Name, virtualTarget.Namespace)
	hostTarget := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hostTargetName.Name,
			Namespace: testingutil.DefaultTestTargetNamespace,
			UID:       types.UID("host-target-pvc-uid"),
			Annotations: map[string]string{
				translate.NameAnnotation:          virtualTarget.Name,
				translate.NamespaceAnnotation:     virtualTarget.Namespace,
				translate.UIDAnnotation:           string(virtualTarget.UID),
				translate.KindAnnotation:          corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim").String(),
				translate.HostNamespaceAnnotation: testingutil.DefaultTestTargetNamespace,
				translate.HostNameAnnotation:      hostTargetName.Name,
			},
			Labels: map[string]string{
				translate.MarkerLabel:    translate.VClusterName,
				translate.NamespaceLabel: virtualTarget.Namespace,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("512Mi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase:    corev1.ClaimPending,
			Capacity: corev1.ResourceList{},
		},
	}
	hostPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: virtualPVName},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				APIVersion: corev1.SchemeGroupVersion.Version,
				Kind:       "PersistentVolumeClaim",
				Namespace:  hostTarget.Namespace,
				Name:       hostTarget.Name,
				UID:        hostTarget.UID,
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	test := &syncertesting.SyncTest{
		Name:                 "direct materialization rebases before deferred patch",
		InitialVirtualState:  []runtime.Object{virtualTarget.DeepCopy(), virtualPV.DeepCopy()},
		InitialPhysicalState: []runtime.Object{hostTarget.DeepCopy(), hostPV.DeepCopy()},
		Sync: func(registerCtx *synccontext.RegisterContext) {
			syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, registerCtx, New)
			pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
			pvcSyncer.useFakePersistentVolumes = true

			targetKey := types.NamespacedName{
				Namespace: virtualTarget.Namespace,
				Name:      virtualTarget.Name,
			}
			hostTargetKey := types.NamespacedName{
				Namespace: hostTarget.Namespace,
				Name:      hostTarget.Name,
			}
			concurrentClient := &externalPopulatorConcurrentWriterClient{
				Client: syncCtx.HostClient,
				target: hostTargetKey,
			}
			syncCtx.HostClient = concurrentClient

			reconcileTarget := func() ctrl.Result {
				t.Helper()
				currentVirtual := &corev1.PersistentVolumeClaim{}
				assert.NilError(t, syncCtx.VirtualClient.Get(syncCtx.Context, targetKey, currentVirtual))
				currentHost := &corev1.PersistentVolumeClaim{}
				assert.NilError(t, syncCtx.HostClient.Get(syncCtx.Context, hostTargetKey, currentHost))
				result, err := pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
					currentHost.DeepCopy(),
					currentHost.DeepCopy(),
					currentVirtual.DeepCopy(),
					currentVirtual.DeepCopy(),
				))
				assert.NilError(t, err)
				return result
			}

			firstResult := reconcileTarget()
			assert.Equal(t, firstResult.RequeueAfter, time.Second)
			assert.NilError(t, concurrentClient.concurrentErr)
			assert.Assert(t, concurrentClient.directRV != "")
			assert.Assert(t, concurrentClient.concurrentRV != "")
			assert.Assert(t, concurrentClient.directRV != concurrentClient.concurrentRV)

			hostAfterConflict := &corev1.PersistentVolumeClaim{}
			assert.NilError(t, syncCtx.HostClient.Get(syncCtx.Context, hostTargetKey, hostAfterConflict))
			assert.Equal(t, hostAfterConflict.Spec.VolumeName, virtualPVName)
			assert.Equal(t, hostAfterConflict.Annotations["example.test/concurrent-writer"], "preserve")
			hostStorageAfterConflict := hostAfterConflict.Spec.Resources.Requests[corev1.ResourceStorage]
			assert.Equal(t, hostStorageAfterConflict.String(), "2Gi")

			virtualAfterConflict := &corev1.PersistentVolumeClaim{}
			assert.NilError(t, syncCtx.VirtualClient.Get(syncCtx.Context, targetKey, virtualAfterConflict))
			assert.Equal(t, virtualAfterConflict.Status.Phase, corev1.ClaimBound)
			virtualCapacityAfterConflict := virtualAfterConflict.Status.Capacity[corev1.ResourceStorage]
			assert.Equal(t, virtualCapacityAfterConflict.String(), "1Gi")
			assert.Equal(t, virtualAfterConflict.Annotations["example.test/virtual-writer"], "preserve")

			secondResult := reconcileTarget()
			assert.Check(t, secondResult.IsZero())

			hostAfterRetry := &corev1.PersistentVolumeClaim{}
			assert.NilError(t, syncCtx.HostClient.Get(syncCtx.Context, hostTargetKey, hostAfterRetry))
			assert.Equal(t, hostAfterRetry.Annotations["example.test/concurrent-writer"], "preserve")
			hostStorageAfterRetry := hostAfterRetry.Spec.Resources.Requests[corev1.ResourceStorage]
			assert.Equal(t, hostStorageAfterRetry.String(), "1Gi")

			virtualAfterRetry := &corev1.PersistentVolumeClaim{}
			assert.NilError(t, syncCtx.VirtualClient.Get(syncCtx.Context, targetKey, virtualAfterRetry))
			assert.Equal(t, virtualAfterRetry.Status.Phase, corev1.ClaimBound)
			assert.Equal(t, virtualAfterRetry.Annotations["example.test/virtual-writer"], "preserve")
		},
	}
	test.Run(t, syncertesting.NewFakeRegisterContext)
}

func TestTranslateSelectorPreservesNilAndExplicitEmptyStorageClass(t *testing.T) {
	empty := ""
	tests := []struct {
		name             string
		storageClassName *string
	}{
		{
			name:             "nil remains nil",
			storageClassName: nil,
		},
		{
			name:             "explicit empty remains empty",
			storageClassName: &empty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pvc := &corev1.PersistentVolumeClaim{
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: tt.storageClassName,
				},
			}

			(&persistentVolumeClaimSyncer{}).translateSelector(nil, pvc)

			if tt.storageClassName == nil {
				assert.Assert(t, pvc.Spec.StorageClassName == nil)
				return
			}
			assert.Assert(t, pvc.Spec.StorageClassName != nil)
			assert.Equal(t, *pvc.Spec.StorageClassName, "")
		})
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
			Conditions: []corev1.PersistentVolumeClaimCondition{
				{
					Type:   externalPopulatorPopulateConditionType,
					Status: corev1.ConditionTrue,
					Reason: externalPopulatorRestoreConditionReasonSucceeded,
				},
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
	dataProtectionPopulateSucceededPvcWithVolumeName := dataProtectionBackupPendingPvcWithVolumeName.DeepCopy()
	dataProtectionPopulateSucceededPvcWithVolumeName.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
		{
			Type:   externalPopulatorPopulateConditionType,
			Status: corev1.ConditionTrue,
			Reason: externalPopulatorRestoreConditionReasonSucceeded,
		},
	}
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
			Type:   externalPopulatorPopulateConditionType,
			Status: corev1.ConditionTrue,
			Reason: externalPopulatorRestoreConditionReasonProvisioned,
		},
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
	dataProtectionNoDataRestorePendingPvcWithVolumeName := dataProtectionNoDataRestorePvcWithVolumeName.DeepCopy()
	dataProtectionNoDataRestorePendingPvcWithVolumeName.Status.Phase = corev1.ClaimPending
	dataProtectionNoDataRestorePendingPvcWithVolumeName.Status.Capacity = nil
	dataProtectionNoDataRestorePvcWithVolumeNameWithoutCreatorClosed := dataProtectionNoDataRestorePendingPvcWithVolumeName.DeepCopy()
	dataProtectionNoDataRestorePvcWithVolumeNameWithoutCreatorClosed.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
		{
			Type:   externalPopulatorRestoreConditionType,
			Status: corev1.ConditionTrue,
			Reason: externalPopulatorRestoreConditionReasonProvisioned,
		},
	}
	dataProtectionDeletingNoDataRestorePvcWithVolumeName := dataProtectionNoDataRestorePendingPvcWithVolumeName.DeepCopy()
	dataProtectionDeletingNoDataRestorePvcWithVolumeName.Finalizers = []string{"kubernetes.io/pvc-protection"}
	dataProtectionDeletingNoDataRestorePvcWithVolumeName.DeletionTimestamp = &metav1.Time{Time: time.Now()}
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
	dataProtectionNoDataHostPendingWithBackupSourceAndObjectUID := dataProtectionNoDataHostPendingWithBackupSource.DeepCopy()
	dataProtectionNoDataHostPendingWithBackupSourceAndObjectUID.UID = types.UID("host-target-pvc-uid")
	dataProtectionNoDataHostPendingAfterGuestMaterializedWithObjectUID := dataProtectionNoDataHostPendingAfterGuestMaterialized.DeepCopy()
	dataProtectionNoDataHostPendingAfterGuestMaterializedWithObjectUID.UID = dataProtectionNoDataHostPendingWithBackupSourceAndObjectUID.UID
	dataProtectionDataRestoreHostPvc := dataProtectionHostPendingPvcWithUID.DeepCopy()
	dataProtectionDataRestoreHostPvc.Spec = corev1.PersistentVolumeClaimSpec{
		VolumeName: dataProtectionPopulatedPV.Name,
	}
	dataProtectionDataRestoreHostPvc.Status = *dataProtectionBackupPvc.Status.DeepCopy()
	dataProtectionDataRestoreHostPendingPvc := dataProtectionDataRestoreHostPvc.DeepCopy()
	dataProtectionDataRestoreHostPendingPvc.Status = *dataProtectionBackupPendingPvcWithVolumeName.Status.DeepCopy()
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
	unrelatedDataProtectionPopulateHelperPvc := dataProtectionPopulateHelperPvc.DeepCopy()
	unrelatedDataProtectionPopulateHelperPvc.Name = "kb-populate-other-target-pvc-uid"
	unrelatedDataProtectionPopulateHelperPvc.UID = types.UID("other-populate-helper-pvc-uid")
	unrelatedDataProtectionPopulateHelperPvc.Spec.VolumeName = "other-restore-populated-pv"
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
	dataProtectionHostPVBoundToNoDataTarget := dataProtectionHostPVBoundToTarget.DeepCopy()
	dataProtectionHostPVBoundToNoDataTarget.Spec.ClaimRef.UID = dataProtectionNoDataHostPendingWithBackupSourceAndObjectUID.UID

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

	dataProtectionWFFCTargetNode4Pvc := withStorageClassAndSelectedNode(dataProtectionPopulateSucceededPvcWithVolumeName, waitForFirstConsumerStorageClassName, "node4", false)
	dataProtectionWFFCTargetNode4BoundPvc := dataProtectionWFFCTargetNode4Pvc.DeepCopy()
	dataProtectionWFFCTargetNode4BoundPvc.Status = *dataProtectionBackupPendingPvcWithVolumeNameBoundStatus.Status.DeepCopy()
	dataProtectionWFFCTargetNode4BoundPvc.Status.Conditions = append(
		[]corev1.PersistentVolumeClaimCondition(nil),
		dataProtectionWFFCTargetNode4Pvc.Status.Conditions...,
	)
	dataProtectionHostWFFCTargetNode4Pvc := withStorageClassAndSelectedNode(dataProtectionHostPendingPvcWithUID, waitForFirstConsumerStorageClassName, "node4", true)
	dataProtectionHostWFFCMaterializedTargetNode4Pvc := dataProtectionHostWFFCTargetNode4Pvc.DeepCopy()
	dataProtectionHostWFFCMaterializedTargetNode4Pvc.Spec.VolumeName = dataProtectionPopulatedPV.Name
	dataProtectionHostWFFCTargetNode2Pvc := withStorageClassAndSelectedNode(dataProtectionHostPendingPvcWithUID, waitForFirstConsumerStorageClassName, "node2", true)

	dataProtectionHostWFFCHelperNode2Pvc := withStorageClassAndSelectedNode(dataProtectionHostPopulateHelperPvc, waitForFirstConsumerStorageClassName, "node2", true)
	dataProtectionHostWFFCHelperNode4Pvc := withStorageClassAndSelectedNode(dataProtectionHostPopulateHelperPvc, waitForFirstConsumerStorageClassName, "node4", true)

	dataProtectionWFFCVirtualPVNode2BoundToTarget := withRequiredNodeAffinity(dataProtectionPopulatedPV, "node2")
	dataProtectionWFFCHostPVNode2BoundToHelper := withRequiredNodeAffinity(dataProtectionHostPVBoundToHelper, "node2")
	dataProtectionWFFCVirtualPVNode4BoundToTarget := withRequiredNodeAffinity(dataProtectionPopulatedPV, "node4")
	dataProtectionWFFCHostPVNode4BoundToHelper := withRequiredNodeAffinity(dataProtectionHostPVBoundToHelper, "node4")
	dataProtectionWFFCHostPVNode4BoundToTarget := dataProtectionWFFCHostPVNode4BoundToHelper.DeepCopy()
	dataProtectionWFFCHostPVNode4BoundToTarget.Spec.ClaimRef = dataProtectionHostPVBoundToTarget.Spec.ClaimRef.DeepCopy()
	terminatingDependencyTimestamp := metav1.NewTime(time.Unix(1_700_000_000, 0).UTC())
	dataProtectionWFFCTerminatingHostPVNode4BoundToHelper := dataProtectionWFFCHostPVNode4BoundToHelper.DeepCopy()
	dataProtectionWFFCTerminatingHostPVNode4BoundToHelper.DeletionTimestamp = &terminatingDependencyTimestamp
	dataProtectionWFFCTerminatingHostPVNode4BoundToHelper.Finalizers = []string{"kubernetes.io/pv-protection"}
	dataProtectionHostWFFCTerminatingHelperNode4Pvc := dataProtectionHostWFFCHelperNode4Pvc.DeepCopy()
	dataProtectionHostWFFCTerminatingHelperNode4Pvc.DeletionTimestamp = &terminatingDependencyTimestamp
	dataProtectionHostWFFCTerminatingHelperNode4Pvc.Finalizers = []string{"kubernetes.io/pvc-protection"}
	terminatingNode4 := node4.DeepCopy()
	terminatingNode4.DeletionTimestamp = &terminatingDependencyTimestamp
	terminatingNode4.Finalizers = []string{"test.vcluster.loft.sh/review-hold"}

	dataProtectionWFFCTargetWithoutSelectedNodePvc := withStorageClassAndSelectedNode(dataProtectionPopulateSucceededPvcWithVolumeName, waitForFirstConsumerStorageClassName, "", false)
	dataProtectionHostWFFCTargetWithoutSelectedNodePvc := withStorageClassAndSelectedNode(dataProtectionHostPendingPvcWithUID, waitForFirstConsumerStorageClassName, "", true)
	dataProtectionHostWFFCHelperWithoutSelectedNodePvc := withStorageClassAndSelectedNode(dataProtectionHostPopulateHelperPvc, waitForFirstConsumerStorageClassName, "", true)

	immediateMode := storagev1.VolumeBindingImmediate
	immediateStorageClassName := "immediate-sc"
	immediateStorageClass := &storagev1.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: immediateStorageClassName},
		VolumeBindingMode: &immediateMode,
	}
	dataProtectionImmediateTargetPvc := withStorageClassAndSelectedNode(dataProtectionPopulateSucceededPvcWithVolumeName, immediateStorageClassName, "", false)
	dataProtectionImmediateTargetBoundPvc := dataProtectionImmediateTargetPvc.DeepCopy()
	dataProtectionImmediateTargetBoundPvc.Status = *dataProtectionBackupPendingPvcWithVolumeNameBoundStatus.Status.DeepCopy()
	dataProtectionImmediateTargetBoundPvc.Status.Conditions = append(
		[]corev1.PersistentVolumeClaimCondition(nil),
		dataProtectionImmediateTargetPvc.Status.Conditions...,
	)
	dataProtectionHostImmediateTargetPvc := withStorageClassAndSelectedNode(dataProtectionHostPendingPvcWithUID, immediateStorageClassName, "", true)
	dataProtectionHostImmediateMaterializedTargetPvc := dataProtectionHostImmediateTargetPvc.DeepCopy()
	dataProtectionHostImmediateMaterializedTargetPvc.Spec.VolumeName = dataProtectionPopulatedPV.Name
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
			Name: "Delay materialized data protection restore host target while populate helper exists",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				dataProtectionPopulateHelperPvc.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
					dataProtectionPopulateHelperPvc.DeepCopy(),
				},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(dataProtectionBackupPendingPvcWithVolumeName.DeepCopy()))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, externalPopulatorNoDataRestoreBackoff)
			},
		},
		{
			Name: "Do not cross block materialized data protection restore target on unrelated populate helper",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				unrelatedDataProtectionPopulateHelperPvc.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
					unrelatedDataProtectionPopulateHelperPvc.DeepCopy(),
				},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionDataRestoreHostPendingPvc.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(dataProtectionBackupPendingPvcWithVolumeName.DeepCopy()))
				assert.NilError(t, err)
				assert.Check(t, result.IsZero())
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
				assert.Check(t, result.IsZero())
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
				dataProtectionWFFCVirtualPVNode2BoundToTarget.DeepCopy(),
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
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCVirtualPVNode2BoundToTarget.DeepCopy()},
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
				dataProtectionWFFCVirtualPVNode2BoundToTarget.DeepCopy(),
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
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCVirtualPVNode2BoundToTarget.DeepCopy()},
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
				dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				dataProtectionWFFCHostPVNode4BoundToHelper.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy()},
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
				dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy(),
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
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy()},
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
			Name: "Keep WFFC external-populator handoff pending when host PV is terminating",
			InitialVirtualState: []runtime.Object{
				waitForFirstConsumerStorageClass.DeepCopy(),
				dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				dataProtectionWFFCTerminatingHostPVNode4BoundToHelper.DeepCopy(),
				node4.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy()},
				storagev1.SchemeGroupVersion.WithKind("StorageClass"):  {waitForFirstConsumerStorageClass.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCTerminatingHostPVNode4BoundToHelper.DeepCopy()},
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
				assert.Check(t, result.RequeueAfter == 2*time.Second, "expected a 2s topology requeue, got %s", result.RequeueAfter)
				assertSinglePVCEvent(t, recorder, "Warning ExternalPopulatorTopologyNotReady", dataProtectionWFFCTerminatingHostPVNode4BoundToHelper.Name, "terminating")
				checkExternalPopulatorHandoffPending(
					t,
					syncCtx,
					types.NamespacedName{Namespace: dataProtectionHostWFFCTargetNode4Pvc.Namespace, Name: dataProtectionHostWFFCTargetNode4Pvc.Name},
					types.NamespacedName{Namespace: dataProtectionWFFCTargetNode4Pvc.Namespace, Name: dataProtectionWFFCTargetNode4Pvc.Name},
					types.NamespacedName{Namespace: dataProtectionHostWFFCHelperNode4Pvc.Namespace, Name: dataProtectionHostWFFCHelperNode4Pvc.Name},
					dataProtectionWFFCTerminatingHostPVNode4BoundToHelper.Name,
				)
			},
		},
		{
			Name: "Keep WFFC external-populator handoff pending when host helper PVC is terminating",
			InitialVirtualState: []runtime.Object{
				waitForFirstConsumerStorageClass.DeepCopy(),
				dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionHostWFFCTerminatingHelperNode4Pvc.DeepCopy(),
				dataProtectionWFFCHostPVNode4BoundToHelper.DeepCopy(),
				node4.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy()},
				storagev1.SchemeGroupVersion.WithKind("StorageClass"):  {waitForFirstConsumerStorageClass.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCTerminatingHelperNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCHostPVNode4BoundToHelper.DeepCopy()},
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
				assert.Check(t, result.RequeueAfter == 2*time.Second, "expected a 2s topology requeue, got %s", result.RequeueAfter)
				assertSinglePVCEvent(t, recorder, "Warning ExternalPopulatorTopologyNotReady", dataProtectionHostWFFCTerminatingHelperNode4Pvc.Name, "terminating")
				checkExternalPopulatorHandoffPending(
					t,
					syncCtx,
					types.NamespacedName{Namespace: dataProtectionHostWFFCTargetNode4Pvc.Namespace, Name: dataProtectionHostWFFCTargetNode4Pvc.Name},
					types.NamespacedName{Namespace: dataProtectionWFFCTargetNode4Pvc.Namespace, Name: dataProtectionWFFCTargetNode4Pvc.Name},
					types.NamespacedName{Namespace: dataProtectionHostWFFCTerminatingHelperNode4Pvc.Namespace, Name: dataProtectionHostWFFCTerminatingHelperNode4Pvc.Name},
					dataProtectionWFFCHostPVNode4BoundToHelper.Name,
				)
			},
		},
		{
			Name: "Keep WFFC external-populator handoff pending when selected host Node is terminating",
			InitialVirtualState: []runtime.Object{
				waitForFirstConsumerStorageClass.DeepCopy(),
				dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				dataProtectionWFFCHostPVNode4BoundToHelper.DeepCopy(),
				terminatingNode4.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy()},
				storagev1.SchemeGroupVersion.WithKind("StorageClass"):  {waitForFirstConsumerStorageClass.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostWFFCTargetNode4Pvc.DeepCopy(),
					dataProtectionHostWFFCHelperNode4Pvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCHostPVNode4BoundToHelper.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("Node"):             {terminatingNode4.DeepCopy()},
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
				assertSinglePVCEvent(t, recorder, "Warning ExternalPopulatorTopologyNotReady", terminatingNode4.Name, "terminating")
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
			Name: "Bridge WFFC external-populator handoff when target, host helper, and PV all select node4",
			InitialVirtualState: []runtime.Object{
				waitForFirstConsumerStorageClass.DeepCopy(),
				dataProtectionWFFCTargetNode4Pvc.DeepCopy(),
				dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy(),
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
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionWFFCVirtualPVNode4BoundToTarget.DeepCopy()},
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
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostWFFCTargetWithoutSelectedNodePvc.DeepCopy(),
				dataProtectionHostWFFCHelperWithoutSelectedNodePvc.DeepCopy(),
				dataProtectionHostPVBoundToHelper.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionWFFCTargetWithoutSelectedNodePvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionPopulatedPV.DeepCopy()},
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
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostImmediateTargetPvc.DeepCopy(),
				dataProtectionHostImmediateHelperPvc.DeepCopy(),
				dataProtectionHostPVBoundToHelper.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionImmediateTargetBoundPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionPopulatedPV.DeepCopy()},
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
			Name: "Keep data protection populated host pv on helper while helper still exists",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				dataProtectionPopulatedPVBoundToHelper.DeepCopy(),
				dataProtectionPopulateHelperPvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostPendingWithBackupSource.DeepCopy(),
				dataProtectionHostPopulateHelperPvc.DeepCopy(),
				dataProtectionHostPVBoundToHelper.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
					dataProtectionPopulateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionPopulatedPVBoundToHelper.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostPendingWithBackupSource.DeepCopy(),
					dataProtectionHostPopulateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionHostPVBoundToHelper.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, objectSyncer := syncertesting.FakeStartSyncer(t, ctx, New)
				pvcSyncer := objectSyncer.(*persistentVolumeClaimSyncer)
				pvcSyncer.useFakePersistentVolumes = true

				result, err := pvcSyncer.Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingWithBackupSource.DeepCopy(),
					dataProtectionHostPendingWithBackupSource.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, 2*time.Second)
			},
		},
		{
			Name: "Keep data protection target pending until deleting populate helper is absent",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				dataProtectionPopulatedPVBoundToHelper.DeepCopy(),
				dataProtectionDeletingPopulateHelperPvc.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostPendingWithBackupSource.DeepCopy(),
				dataProtectionHostPopulateHelperPvc.DeepCopy(),
				dataProtectionHostPVBoundToHelper.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
					dataProtectionDeletingPopulateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionPopulatedPVBoundToHelper.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionHostPendingWithBackupSource.DeepCopy(),
					dataProtectionHostPopulateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionHostPVBoundToHelper.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingWithBackupSource.DeepCopy(),
					dataProtectionHostPendingWithBackupSource.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
					dataProtectionBackupPendingPvcWithVolumeName.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, 2*time.Second)
			},
		},
		{
			Name: "Do not patch host pv before populator helper creation is terminally closed",
			InitialVirtualState: []runtime.Object{
				dataProtectionNoDataRestorePvcWithVolumeNameWithoutCreatorClosed.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
				dataProtectionHostPopulateHelperPvc.DeepCopy(),
				dataProtectionHostPVBoundToHelper.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestorePvcWithVolumeNameWithoutCreatorClosed.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionNoDataHostPendingAfterGuestMaterialized.DeepCopy(),
					dataProtectionHostPopulateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionHostPVBoundToHelper.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
					dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
					dataProtectionNoDataRestorePvcWithVolumeNameWithoutCreatorClosed.DeepCopy(),
					dataProtectionNoDataRestorePvcWithVolumeNameWithoutCreatorClosed.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, 2*time.Second)
			},
		},
		{
			Name: "Do not accept premature exact target handoff before populator creator is terminally closed",
			InitialVirtualState: []runtime.Object{
				dataProtectionNoDataRestorePvcWithVolumeNameWithoutCreatorClosed.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionNoDataHostPendingWithBackupSourceAndObjectUID.DeepCopy(),
				dataProtectionHostPVBoundToNoDataTarget.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataRestorePvcWithVolumeNameWithoutCreatorClosed.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataHostPendingAfterGuestMaterializedWithObjectUID.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionHostPVBoundToNoDataTarget.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionNoDataHostPendingWithBackupSourceAndObjectUID.DeepCopy(),
					dataProtectionNoDataHostPendingWithBackupSourceAndObjectUID.DeepCopy(),
					dataProtectionNoDataRestorePvcWithVolumeNameWithoutCreatorClosed.DeepCopy(),
					dataProtectionNoDataRestorePvcWithVolumeNameWithoutCreatorClosed.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, 2*time.Second)
			},
		},
		{
			Name: "Do not accept exact target handoff for deleting fresh target",
			InitialVirtualState: []runtime.Object{
				dataProtectionDeletingNoDataRestorePvcWithVolumeName.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionNoDataHostPendingWithBackupSourceAndObjectUID.DeepCopy(),
				dataProtectionHostPVBoundToNoDataTarget.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionDeletingNoDataRestorePvcWithVolumeName.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {dataProtectionNoDataHostPendingAfterGuestMaterializedWithObjectUID.DeepCopy()},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"):      {dataProtectionHostPVBoundToNoDataTarget.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionNoDataHostPendingWithBackupSourceAndObjectUID.DeepCopy(),
					dataProtectionNoDataHostPendingWithBackupSourceAndObjectUID.DeepCopy(),
					dataProtectionNoDataRestorePendingPvcWithVolumeName.DeepCopy(),
					dataProtectionNoDataRestorePendingPvcWithVolumeName.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, result.RequeueAfter, 2*time.Second)
			},
		},
		{
			Name: "Revalidate populate helper absence before final host pv claim ref patch",
			InitialVirtualState: []runtime.Object{
				dataProtectionNoDataRestorePvcWithVolumeName.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
				dataProtectionHostPopulateHelperPvc.DeepCopy(),
				dataProtectionHostPVBoundToHelper.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionNoDataRestorePvcWithVolumeName.DeepCopy(),
					dataProtectionPopulateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionPopulatedPV.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionNoDataHostPendingAfterGuestMaterialized.DeepCopy(),
					dataProtectionHostPopulateHelperPvc.DeepCopy(),
				},
				corev1.SchemeGroupVersion.WithKind("PersistentVolume"): {dataProtectionHostPVBoundToHelper.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				syncer.(*persistentVolumeClaimSyncer).useFakePersistentVolumes = true

				raceClient := &injectHelperAfterHostPVGetClient{
					Client:        syncCtx.HostClient,
					virtualClient: syncCtx.VirtualClient,
					helper:        dataProtectionPopulateHelperPvc,
					hostPVName:    dataProtectionHostPVBoundToHelper.Name,
				}
				syncCtx.HostClient = raceClient

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
					dataProtectionNoDataHostPendingWithBackupSource.DeepCopy(),
					dataProtectionNoDataRestorePvcWithVolumeName.DeepCopy(),
					dataProtectionNoDataRestorePvcWithVolumeName.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Equal(t, raceClient.hostPVGets, 2)
				assert.Check(t, raceClient.injected)
				assert.Equal(t, raceClient.patchCalls, 0)
				assert.Equal(t, result.RequeueAfter, 2*time.Second)
			},
		},
		{
			Name: "Refresh stale data protection host pv target claim ref uid",
			InitialVirtualState: []runtime.Object{
				dataProtectionBackupPvc.DeepCopy(),
				dataProtectionPopulatedPV.DeepCopy(),
			},
			InitialPhysicalState: []runtime.Object{
				dataProtectionHostPendingPvcWithObjectUID.DeepCopy(),
				dataProtectionHostPVBoundToTargetStaleUID.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"): {
					dataProtectionBackupPvc.DeepCopy(),
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

				result, err := syncer.(*persistentVolumeClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					dataProtectionHostPendingPvcWithObjectUID.DeepCopy(),
					dataProtectionHostPendingPvcWithObjectUID.DeepCopy(),
					dataProtectionBackupPvc.DeepCopy(),
					dataProtectionBackupPvc.DeepCopy(),
				))
				assert.NilError(t, err)
				assert.Check(t, result.IsZero())
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
